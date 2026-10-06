"""Execute MP's real V2 methods with isolated storage/event dependencies.

Set MP115_REFERENCE to a checkout of the pinned MP source. We do not vendor MP
or need a running server, downloader, TMDB account, or 115 account for these tests.
"""
import ast
import copy
import enum
import os
import re
import shutil
import sys
import threading
import time
import types
import typing
from pathlib import Path
from types import SimpleNamespace

from pydantic import BaseModel, Field
from jinja2 import Template
from sqlalchemy import Boolean, Column, Index, Integer, JSON, String, create_engine
from sqlalchemy.orm import DeclarativeBase, declared_attr, sessionmaker

from tests.support import load


REFERENCE = Path(os.environ.get("MP115_REFERENCE", "__missing_reference__"))


def selected(path, names, namespace, methods=None, bases=None):
    tree = ast.parse((REFERENCE / path).read_text(encoding="utf-8-sig"))
    nodes = [node for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in names]
    if methods is not None:
        for node in nodes:
            if isinstance(node, ast.ClassDef):
                node.body = [m for m in node.body if not isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef))
                             or m.name in methods]
    if bases:
        for node in nodes:
            if isinstance(node, ast.ClassDef):
                node.bases = [ast.Name(id=bases, ctx=ast.Load())]
                node.keywords = []
    module = ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[]))
    exec(compile(module, str(REFERENCE / path), "exec"), namespace)


def module(name):
    if name in sys.modules:
        return sys.modules[name]
    result = types.ModuleType(name)
    result.__path__ = []
    sys.modules[name] = result
    if "." in name:
        parent, attr = name.rsplit(".", 1)
        setattr(module(parent), attr, result)
    return result


class MediaType(str, enum.Enum):
    MOVIE = "电影"
    TV = "电视剧"


class MetaBase:
    def __init__(self, title="", isfile=True):
        self.name, self.org_string, self.year = title, title, "2026"
        self.type, self.begin_season, self.season = MediaType.MOVIE, 1, "S01"
        self.episode, self.episode_list, self.season_episode = "E01", [1], "S01E01"

    def to_dict(self):
        return {**vars(self), "type": self.type.value}


class MetaVideo(MetaBase):
    pass


class Media:
    def __init__(self):
        self.type, self.source, self.title, self.year = MediaType.MOVIE, "themoviedb", "Film", "2026"
        self.category, self.tmdb_id, self.title_year = "电影", 123, "Film (2026)"
        self.imdb_id = self.tvdb_id = self.douban_id = self.bangumi_id = self.anilist_id = None
        self.episode_group, self.season = None, 1

    def to_dict(self):
        return {**vars(self), "type": self.type.value, "media_id": str(self.tmdb_id)}

    def from_dict(self, data):
        self.__dict__.update(data)
        self.type = MediaType(self.type)

    def clear(self):
        pass

    def get_poster_image(self):
        return None

    get_message_image = get_poster_image


class Singleton(type):
    instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls.instances:
            cls.instances[cls] = super().__call__(*args, **kwargs)
        return cls.instances[cls]


class SQLBase(DeclarativeBase):
    @declared_attr.directive
    def __tablename__(cls):
        return cls.__name__.lower()

    def create(self, db):
        db.add(self)
        db.commit()

    def update(self, db, payload):
        for key, value in payload.items():
            setattr(self, key, value)
        db.add(self)
        db.commit()


class Harness:
    def __init__(self, directory):
        self.events, self.notifications = [], []
        self.batch_lock = threading.Lock()
        self.local_calls = 0
        self.normal_uploads = 0
        namespace = dict(vars(typing), BaseModel=BaseModel, Field=Field, Path=Path, re=re,
                         __name__="app.schemas", WINDOWS_DRIVE_PATTERN=re.compile(r"^[A-Za-z]:"))
        selected("app/schemas/file.py", {"FileURI", "FileItem"}, namespace)
        selected("app/schemas/context.py", {"MetaInfo", "MediaInfo"}, namespace)
        selected("app/schemas/system.py", {"TransferDirectoryConf"}, namespace)
        namespace.update(TmdbEpisode=BaseModel, DownloadHistory=BaseModel)
        selected("app/schemas/transfer.py", {"TransferTask", "TransferInfo", "TransferJobTask", "TransferJob"}, namespace)
        self.schemas = module("app.schemas")
        for name in ("FileItem", "TransferTask", "TransferInfo", "TransferJobTask", "TransferJob",
                     "TransferDirectoryConf", "MetaInfo", "MediaInfo"):
            setattr(self.schemas, name, namespace[name])
        self.FileItem, self.TransferInfo, self.Task = namespace["FileItem"], namespace["TransferInfo"], namespace["TransferTask"]
        logger = SimpleNamespace(**{name: lambda *a, **kw: None for name in ("debug", "info", "warn", "warning", "error")})
        self.settings = SimpleNamespace(SCRAP_FOLLOW_TMDB=True, AI_AGENT_ENABLE=False, AI_AGENT_RETRY_TRANSFER=False,
            RMT_MEDIAEXT=[".mkv", ".mp4"], RMT_SUBEXT=[".srt"], RMT_AUDIOEXT=[".flac"],
            RENAME_FORMAT=lambda _: "{{title}}/{{title}}{{fileExt}}",
            MP_DOMAIN=lambda value: value)
        module("app.log").logger = logger
        module("app.core.context").MediaInfo = Media
        coremeta = module("app.core.meta")
        coremeta.MetaBase, coremeta.MetaVideo, coremeta.MetaAnime = MetaBase, MetaVideo, MetaVideo
        module("app.schemas.types").MediaType = MediaType
        history_namespace = dict(vars(typing), __name__="app.db.models.transferhistory", Base=SQLBase,
            Column=Column, Boolean=Boolean, Integer=Integer, JSON=JSON, String=String, Index=Index,
            get_id_column=lambda: Column(Integer, primary_key=True))
        selected("app/db/models/transferhistory.py", {"TransferHistory"}, history_namespace, methods=set())
        self.History = history_namespace["TransferHistory"]
        engine = create_engine("sqlite:///" + str(Path(directory) / "history.db"))
        SQLBase.metadata.create_all(engine)
        self.db = sessionmaker(engine, expire_on_commit=False)()
        factory = sessionmaker(engine)  # Match MP: expire_on_commit=True and detached query results.
        module("app.db").SessionFactory = factory
        def get(cls, db, rid):
            with factory() as session:
                return session.get(cls, rid)
        def by_src(cls, db, src, storage=None):
            with factory() as session:
                return session.query(cls).filter_by(src=src, **({"src_storage": storage} if storage else {})).first()
        def delete(cls, db, rid):
            with factory() as session:
                session.delete(session.get(cls, rid))
                session.commit()
        def create(record, db):
            with factory() as session:
                session.add(record)
                session.commit()
        def update(record, db, payload):
            with factory() as session:
                for key, value in payload.items():
                    setattr(record, key, value)
                session.add(record)
                session.commit()
        self.History.get, self.History.get_by_src, self.History.delete = classmethod(get), classmethod(by_src), classmethod(delete)
        self.History.create, self.History.update = create, update
        module("app.db.models.transferhistory").TransferHistory = self.History
        owner = self
        class DbOper:
            def __init__(self):
                self._db = None
        oper_ns = dict(vars(typing), __name__="app.db.transferhistory_oper", time=time, DbOper=DbOper,
            TransferHistory=self.History, MediaInfo=Media, MetaBase=MetaBase,
            FileItem=self.FileItem, TransferInfo=self.TransferInfo)
        selected("app/db/transferhistory_oper.py", {"TransferHistoryOper"}, oper_ns)
        self.Oper = oper_ns["TransferHistoryOper"]
        module("app.db.transferhistory_oper").TransferHistoryOper = self.Oper
        class Events:
            def send_event(self, kind, payload):
                owner.events.append((kind, payload))
                return None
        events = Events()
        eventtypes = SimpleNamespace(**{v: v for v in ("TransferComplete", "TransferFailed", "MetadataScrape",
            "SubtitleTransferComplete", "SubtitleTransferFailed", "AudioTransferComplete", "AudioTransferFailed")})
        chain_events = SimpleNamespace(**{name: name for name in ("StorageOperSelection", "TransferIntercept",
            "TransferOverwriteCheck", "TransferRenameBuild", "TransferRename")})
        class Data:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)
                self.storage_oper, self.cancel = None, False
        def resolve_media_identity(media):
            return media.source, str(media.tmdb_id)
        native_ns = dict(vars(typing), __name__="app.chain.transfer", Path=Path, threading=threading,
            deepcopy=copy.deepcopy, monotonic=time.monotonic, logger=logger, job_lock=self.batch_lock,
            schemas=self.schemas, MediaInfo=Media, MetaBase=MetaBase, resolve_media_identity=resolve_media_identity,
            TransferTask=self.Task, TransferInfo=self.TransferInfo, TransferJob=namespace["TransferJob"],
            TransferJobTask=namespace["TransferJobTask"], FileItem=self.FileItem,
            TransferHistoryOper=self.Oper, settings=self.settings, MediaType=MediaType, re=re)
        selected("app/chain/transfer.py", {"JobManager"}, native_ns)
        self.JobManager = native_ns["JobManager"]
        class Local:
            def delete(self, item):
                Path(item.path).unlink(missing_ok=True)
                return True
        module("app.modules.filemanager.storages.local").LocalStorage = Local
        module("app.modules.filemanager.storages.u115").U115Pan = lambda: SimpleNamespace(access_token="test")
        handler_ns = {**native_ns, "__name__": "app.modules.filemanager.transhandler", "StorageBase": object,
            "eventmanager": events, "TransferInterceptEventData": Data, "ChainEventType": chain_events,
            "TransferOverwriteCheckEventData": Data, "TransferRenameBuildEventData": Data,
            "TransferRenameEventData": Data, "StorageQueryError": type("StorageQueryError", (Exception,), {}),
            "TmdbEpisode": BaseModel, "TransferDirectoryConf": namespace["TransferDirectoryConf"],
            "Template": Template, "JINJA2_VAR_PATTERN": re.compile(r"\{\{.*?}}", re.DOTALL),
            "MetaInfoPath": lambda _: SimpleNamespace(season=None, episode=None, part=None)}
        selected("app/helper/directory.py", {"DirectoryHelper"}, handler_ns, methods={"get_media_root_path"})
        selected("app/modules/filemanager/transhandler.py", {"TransHandler"}, handler_ns,
            methods={"__update_result", "__transfer_command", "__transfer_file", "transfer_media",
                     "__build_preview_item", "__delete_version_files", "get_dest_dir", "get_dest_path", "get_rename_path"})
        self.Handler = handler_ns["TransHandler"]
        # Only template context building (metadata services) is isolated; native
        # rendering, directory planning, overwrite rules and transfers run below.
        self.Handler.get_naming_dict = staticmethod(lambda meta, mediainfo, file_ext=None, **kw:
            {"title": mediainfo.title, "fileExt": file_ext or ""})
        selected("app/modules/filemanager/__init__.py", {"FileManagerModule"}, handler_ns,
                 methods={"transfer"}, bases="object")
        filemanager = handler_ns["FileManagerModule"]()
        filemanager._FileManagerModule__get_storage_oper = lambda _: object()
        class ChainBase:
            def transfer(self, fileitem, meta, mediainfo, target_directory=None, target_storage=None,
                         target_path=None, transfer_type=None, episodes_info=None, scrape=False,
                         library_type_folder=False, library_category_folder=False, source_oper=None,
                         target_oper=None, preview=False):
                if target_storage == "u115":
                    return filemanager.transfer(fileitem=fileitem, meta=meta, mediainfo=mediainfo,
                        target_directory=target_directory, target_storage=target_storage, target_path=target_path,
                        transfer_type=transfer_type, scrape=scrape, episodes_info=episodes_info,
                        library_type_folder=library_type_folder, library_category_folder=library_category_folder,
                        source_oper=source_oper, target_oper=target_oper, preview=preview)
                final = Path(target_path or target_directory.library_path) / fileitem.name
                destination = owner.FileItem(storage=target_storage, path=final.as_posix(), name=final.name,
                    type="file", fileid="remote-id", size=fileitem.size)
                info = owner.TransferInfo(fileitem=fileitem, target_item=destination,
                    target_diritem=owner.FileItem(storage=target_storage, type="dir", path=final.parent.as_posix()),
                    transfer_type=transfer_type, file_list=[fileitem.path], file_list_new=[final.as_posix()],
                    need_scrape=bool(scrape), need_notify=True, file_count=1, total_size=fileitem.size)
                if preview:
                    return info
                if target_storage == "local":
                    owner.local_calls += 1
                    final.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(fileitem.path, final)
                    return info
                if target_oper is None:
                    owner.normal_uploads += 1
                    return info
                result = owner.TransferInfo()
                # MP's REAL delete-before-upload and move-source-delete code.
                item, reason = owner.Handler()._TransHandler__transfer_file(fileitem=fileitem, meta=meta,
                    mediainfo=mediainfo, target_storage=target_storage, target_file=final,
                    transfer_type=transfer_type, over_flag=True, source_oper=source_oper,
                    target_oper=target_oper, result=result)
                info.success, info.message, info.target_item = bool(item), reason, item
                return info
        ChainBase.transfer.__module__ = "app.chain"
        native_ns.update(ChainBase=ChainBase, ConfigReloadMixin=object, Singleton=Singleton,
            MediaChain=lambda: SimpleNamespace(supplement_tmdb_info=lambda media, meta: media),
            normalize_media_source=lambda v: v, StorageOperSelectionEventData=Data,
            eventmanager=events, ChainEventType=chain_events, EventType=eventtypes,
            Notification=lambda **kw: kw, NotificationType=SimpleNamespace(Manual="Manual"),
            SystemConfigKey=SimpleNamespace(TransferExcludeWords="exclude", MountedLocalDiskDeleteEmptyDirs="dirs"),
            SystemConfigOper=lambda: SimpleNamespace(get=lambda key: None),
            StorageChain=lambda: SimpleNamespace(delete_media_file=lambda *a, **kw: None))
        module("app.db.systemconfig_oper").SystemConfigOper = native_ns["SystemConfigOper"]
        module("app.schemas.types").SystemConfigKey = native_ns["SystemConfigKey"]
        methods = {"__handle_transfer", "__default_callback", "__get_transfer_target_dir_path", "_requires_automatic_category",
            "__is_media_file", "__is_subtitle_file", "__is_audio_file", "__mark_torrent_completed_if_done",
            "__should_delete_empty_source_directories", "_can_delete_torrent",
            "__register_scrape_batch_task", "__close_scrape_batch", "__record_scrape_target", "__finish_scrape_batch_task",
            "__flush_scrape_batch_if_ready", "__send_metadata_scrape_event"}
        selected("app/chain/transfer.py", {"TransferChain"}, native_ns, methods=methods)
        self.Chain = native_ns["TransferChain"]
        def initialize(chain):
            chain.jobview = owner.JobManager()
            chain._success_target_files, chain._scrape_batches = {}, {}
            chain.eventmanager = events
            chain._media_exts = owner.settings.RMT_MEDIAEXT
            chain._allowed_exts = owner.settings.RMT_MEDIAEXT
            chain._subtitle_exts = owner.settings.RMT_SUBEXT
            chain._audio_exts = owner.settings.RMT_AUDIOEXT
        self.Chain.__init__ = initialize
        self.Chain.post_message = lambda chain, msg: owner.notifications.append(msg)
        self.Chain.send_transfer_message = lambda *a, **kw: owner.notifications.append(kw)
        self.Chain.list_torrents = lambda *a, **kw: []
        self.Chain._is_blocked_by_exclude_words = lambda *a, **kw: False
        self.Chain.build_failed_transfer_buttons = lambda *a: []
        self.Chain._TransferChain__is_torrent_download_completed = lambda *a: True
        self.Chain.transfer_completed = lambda *a, **kw: None
        module("app.chain.transfer").TransferChain = self.Chain
        module("app.chain.transfer").job_lock = self.batch_lock
        self.bridge = load("bridge")
        self.bridge.TransferChain, self.bridge.TransferHistoryOper = self.Chain, self.Oper
        self.bridge.TransferHistory = self.History
        self.bridge.SessionFactory = factory
        self.bridge.FileItem, self.bridge.TransferInfo, self.bridge.TransferTask = self.FileItem, self.TransferInfo, self.Task
        self.bridge.job_lock = self.batch_lock
        self.bridge.LocalStorage = Local
        self.chain = self.Chain()

    def close(self):
        self.db.close()
        self.db.bind.dispose()
        SQLBase.registry.dispose()
        SQLBase.metadata.clear()

    def task(self, path, target="u115", batch="batch", mode="copy"):
        return self.Task(fileitem=self.FileItem(storage="local", path=str(path), name=Path(path).name,
            type="file", extension=Path(path).suffix.lstrip("."), size=Path(path).stat().st_size),
            meta=MetaVideo(Path(path).stem), mediainfo=Media(), transfer_type=mode,
            target_directory=self.schemas.TransferDirectoryConf(library_path="/library", library_storage=target,
                transfer_type=mode, overwrite_mode="always"), target_storage=target, target_path=Path("/library"),
            transfer_batch_id=batch, scrape=True)
