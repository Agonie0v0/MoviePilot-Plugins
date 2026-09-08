"""
阡陌居签到插件 (QmjSign)
版本: 1.2.3
原作者: madrays
增强修改:
- v1.2.3: 优化插件配置表单布局与色彩层级，4列均分开关色彩区分，长Cookie整行呼吸空间，4+4+4网络参数网格，警示色历史清理与结构化配置指南。
- v1.2.2: 增加清空历史记录功能（支持设置表单开关、远程命令/qmjsign_clear与API三种方式）；配置高清插件图标（PNG格式及全URL引用）；优化账号登录参数与密码空格处理，增加账号被锁定(login_strike)防重试保护与精准引导提示。
- v1.2.1: 增强自动登录容错机制：支持UTF-8 BOM自动清洗；优先识别auth Cookie；增加会话有效性兜底探测与错误文本清洗。
- v1.2.0: 增加账号密码自动登录与Cookie自动更新持久化；支持Discuz安全提问；
          增加网络代理(Proxy)与自定义超时时间配置；
          优化Cookie有效性校验机制（区分网络超时与会话失效，彻底解决误报Cookie过期与ReadTimeout问题）；
          优化重试与异常容错能力。
- v1.1.3: 签到心情与签到文字改为随机选择（DSU 心情库 + 对应文案）
- v1.1.2: 修复签到失败（“未定义操作”）：改用 https、补齐 qdmode/todaysay/fastreply 参数
- v1.0.0: 初始版本，基于QD签到模板实现
"""
import time
import random
import requests
import re
import json
from datetime import datetime, timedelta
from typing import Any, List, Dict, Tuple, Optional

import pytz
from requests.adapters import HTTPAdapter, Retry
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.plugins import _PluginBase
from app.log import logger
from app.schemas import NotificationType

try:
    from app.schemas.types import EventType
    from app.core.event import eventmanager, Event
except ImportError:
    try:
        from app.sdk.events import eventmanager, Event
        from app.schemas.types import EventType
    except ImportError:
        EventType = None
        eventmanager = None
        Event = None

if eventmanager is not None and EventType is not None:
    _register_action = eventmanager.register(EventType.PluginAction)
else:
    def _register_action(f):
        return f



def _extract_sign_message(html_content: str) -> Optional[str]:
    """
    从 Discuz AJAX 响应（<root><![CDATA[...]]></root>）中提取提示消息文本。
    兼容 <div class="c"> 与 <div class="c altw">，并优先提取 alert_error/alert_right/alert_info。
    """
    def _clean(segment: str) -> str:
        segment = re.sub(r'<script.*?</script>', '', segment, flags=re.S)
        segment = re.sub(r'<[^>]+>', '', segment)
        return segment.strip()

    # 1) 优先提取 alert_xxx 中的消息
    alert_match = re.search(r'<div class="alert_(?:error|right|info)">(.*?)</div>', html_content, re.S)
    if alert_match:
        msg = _clean(alert_match.group(1))
        if msg:
            return msg

    # 2) 回退：提取 <div class="c"> 或 <div class="c altw"> 内容
    div_match = re.search(r'<div class="c(?: altw)?">(.*?)</div>', html_content, re.S)
    if div_match:
        msg = _clean(div_match.group(1))
        if msg:
            return msg

    return None


# DSU 每日签到心情选项（qdxq 参数值 -> 含义）
_SIGN_MOODS = [
    ("kx", "开心"),
    ("ng", "难过"),
    ("ym", "郁闷"),
    ("wl", "无聊"),
    ("nu", "生气"),
    ("ch", "擦汗"),
    ("fd", "奋斗"),
    ("yl", "慵懒"),
    ("shuai", "衰"),
]

# 各心情对应的签到文字，签到时会随机挑选一条
_SIGN_TEXTS = {
    "kx": ["今天开心 ing", "开心的一天，签到打卡~", "心情美美哒，来签个到"],
    "ng": ["今天有点难过，还是来签个到", "心情低落，签到打卡"],
    "ym": ["有点郁闷，签个到解解闷", "今天心情郁闷，求安慰"],
    "wl": ["今天好无聊，来签个到", "无聊的一天，签到打卡"],
    "nu": ["生气中，先签个到", "今天有点生气！"],
    "ch": ["忙得满头大汗，来签个到", "擦汗，终于有空签到了"],
    "fd": ["奋斗的一天，签到打卡", "加油！努力奋斗，先签个到"],
    "yl": ["慵懒地签个到", "今天有点慵懒，签完继续躺"],
    "shuai": ["今天有点衰，签个到转运", "签到，希望转运"],
}

_SIGN_TEXTS_FALLBACK = ["今日签到", "每日签到", "打卡签到", "签到成功，新的一天"]

# Discuz 安全提问选项定义
_SECURITY_QUESTIONS = [
    {"title": "无安全提问", "value": "0"},
    {"title": "母亲的名字", "value": "1"},
    {"title": "爷爷的名字", "value": "2"},
    {"title": "父亲出生的城市", "value": "3"},
    {"title": "你其中一位老师的名字", "value": "4"},
    {"title": "你个人计算机的型号", "value": "5"},
    {"title": "你最喜欢的餐馆名称", "value": "6"},
    {"title": "驾驶执照最后四位数字", "value": "7"},
]


class qmjsign(_PluginBase):
    # 插件名称
    plugin_name = "阡陌居签到"
    # 插件描述
    plugin_desc = "自动完成阡陌居每日签到与威望红包，支持账号密码自动登录更新Cookie、失败重试与历史记录"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/Agonie0v0/MoviePilot-Plugins/main/icons/qmj.png"
    # 插件版本
    plugin_version = "1.2.2"
    # 插件作者
    plugin_author = "Agonie"
    # 作者主页
    author_url = "https://github.com/Agonie0v0"
    # 插件配置项ID前缀
    plugin_config_prefix = "qmjsign_"
    # 加载顺序
    plugin_order = 1
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled: bool = False
    _cookie: Optional[str] = None
    _notify: bool = True
    _onlyonce: bool = False
    _cron: Optional[str] = "0 8 * * *"
    _max_retries: int = 3
    _retry_interval: int = 30
    _history_days: int = 30
    _manual_trigger: bool = False
    _draw_prestige_enabled: bool = False
    _username: Optional[str] = None
    _password: Optional[str] = None
    _questionid: str = "0"
    _answer: Optional[str] = None
    _proxy: Optional[str] = None
    _timeout: int = 20

    # 定时器
    _scheduler: Optional[BackgroundScheduler] = None
    _current_trigger_type: Optional[str] = None

    def init_plugin(self, config: dict = None):
        # 停止现有任务
        self.stop_service()

        logger.info("============= qmjsign 初始化 =============")
        try:
            if config:
                # 清空历史记录指令检查
                if config.get("clear_history"):
                    self.save_data('sign_history', [])
                    self.save_data('last_credits_overview', {})
                    logger.info("已清空阡陌居签到历史记录与统计缓存")

                self._enabled = bool(config.get("enabled", False))
                self._cookie = (config.get("cookie") or "").strip()
                self._notify = bool(config.get("notify", True))
                self._cron = config.get("cron") or "0 8 * * *"
                self._onlyonce = bool(config.get("onlyonce", False))
                self._max_retries = int(config.get("max_retries", 3))
                self._retry_interval = int(config.get("retry_interval", 30))
                self._history_days = int(config.get("history_days", 30))
                self._draw_prestige_enabled = bool(config.get("draw_prestige", False))
                self._username = (config.get("username") or "").strip()
                self._password = str(config.get("password") or "")
                self._questionid = str(config.get("questionid", "0"))
                self._answer = (config.get("answer") or "").strip()
                self._proxy = (config.get("proxy") or "").strip()
                self._timeout = int(config.get("timeout", 20))

                logger.info(
                    f"qmjsign 配置已载入: enabled={self._enabled}, notify={self._notify}, "
                    f"cron={self._cron}, max_retries={self._max_retries}, "
                    f"has_account={bool(self._username and self._password)}, "
                    f"has_cookie={bool(self._cookie)}, draw_prestige={self._draw_prestige_enabled}, "
                    f"proxy={'已配置' if self._proxy else '无'}, timeout={self._timeout}s"
                )

            # 清理所有可能的延长重试任务
            self._clear_extended_retry_tasks()

            if self._onlyonce:
                logger.info("执行一次性签到")
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                self._manual_trigger = True
                self._scheduler.add_job(
                    func=self.sign,
                    trigger='date',
                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                    name="阡陌居签到"
                )
                self._onlyonce = False
                self._sync_config(onlyonce=False)

                # 启动任务
                if self._scheduler.get_jobs():
                    self._scheduler.print_jobs()
                    self._scheduler.start()

        except Exception as e:
            logger.error(f"qmjsign 初始化错误: {str(e)}", exc_info=True)

    def _sync_config(self, onlyonce: bool = False):
        """同步更新插件配置到持久化存储"""
        try:
            self.update_config({
                "enabled": self._enabled,
                "notify": self._notify,
                "onlyonce": onlyonce,
                "cookie": self._cookie or "",
                "cron": self._cron,
                "max_retries": self._max_retries,
                "retry_interval": self._retry_interval,
                "history_days": self._history_days,
                "draw_prestige": self._draw_prestige_enabled,
                "username": self._username or "",
                "password": self._password or "",
                "questionid": self._questionid or "0",
                "answer": self._answer or "",
                "proxy": self._proxy or "",
                "timeout": self._timeout,
                "clear_history": False
            })
        except Exception as e:
            logger.warning(f"qmjsign 更新配置失败: {str(e)}")

    def _get_session(self) -> requests.Session:
        """创建配置好的 requests.Session"""
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive"
        })

        # 代理设置
        if self._proxy:
            session.proxies.update({
                "http": self._proxy,
                "https": self._proxy
            })

        # 请求自动重试适配器（针对服务端 50x 错误）
        retry = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        # 注入现有 Cookie
        if self._cookie:
            try:
                for cookie_item in self._cookie.split(';'):
                    if '=' in cookie_item:
                        name, value = cookie_item.strip().split('=', 1)
                        session.cookies.set(name.strip(), value.strip(), domain="1000qm.vip")
            except Exception as e:
                logger.warning(f"注入Cookie时出错: {str(e)}")

        return session

    def _save_session_cookie(self, session: requests.Session):
        """将 Session 中获得的 Cookie 格式化并持久化保存"""
        try:
            cookie_dict = requests.utils.dict_from_cookiejar(session.cookies)
            if not cookie_dict:
                return

            # 合并已有 Cookie 和 Session 最新 Cookie
            merged = {}
            if self._cookie:
                for item in self._cookie.split(';'):
                    if '=' in item:
                        k, v = item.strip().split('=', 1)
                        merged[k.strip()] = v.strip()
            merged.update(cookie_dict)

            # 序列化为规范字符串
            new_cookie_str = "; ".join(f"{k}={v}" for k, v in merged.items() if k)
            self._cookie = new_cookie_str
            logger.info(f"已更新并保存最新 Cookie (共 {len(merged)} 项)")
            self._sync_config(onlyonce=False)
        except Exception as e:
            logger.error(f"保存 Cookie 出错: {str(e)}")

    def _auto_login(self, session: requests.Session) -> Tuple[bool, str]:
        """
        通过 Discuz! 移动 API 自动登录以获取全新 Session 与 Cookie，避开网页端顶象滑块验证码。
        返回: (是否成功, 提示信息)
        """
        if not (self._username and self._password):
            return False, "未配置用户名或密码"

        logger.info(f"正在尝试使用账号 [{self._username}] 登录阡陌居...")
        try:
            # 步骤 1：先访问 mobile login 接口获取最新的 saltkey、cookiepre 和 formhash
            init_url = "https://www.1000qm.vip/api/mobile/index.php?version=4&module=login"
            resp_init = session.get(init_url, timeout=self._timeout)
            data_init = resp_init.json() if resp_init.status_code == 200 else {}
            variables = data_init.get("Variables", {}) or {}
            formhash = variables.get("formhash")

            if not formhash:
                # 备用：尝试从首页拉取 formhash
                logger.warning("移动接口未返回 formhash，尝试从首页获取...")
                resp_home = session.get("https://www.1000qm.vip/", timeout=self._timeout)
                fh_match = re.search(r'name="formhash" value="([^"]*)"', resp_home.text)
                if fh_match:
                    formhash = fh_match.group(1)

            if not formhash:
                return False, "获取登录 formhash 失败"

            # 步骤 2：提交登录
            post_url = "https://www.1000qm.vip/api/mobile/index.php?version=4&module=login&loginsubmit=yes"
            post_data = {
                "formhash": formhash,
                "loginfield": "username",
                "fastloginfield": "username",
                "username": self._username,
                "password": self._password,
                "questionid": self._questionid if self._questionid else "0",
                "answer": self._answer or "",
                "cookietime": "2592000",  # 30天记住登录
                "loginsubmit": "yes"
            }

            headers = {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": "https://www.1000qm.vip/"
            }

            resp_login = session.post(post_url, data=post_data, headers=headers, timeout=self._timeout)

            # 容错策略 1：检查 Session Cookies 中是否已获得 auth 认证凭据
            # （Discuz! 登录成功时无论是否重定向或输出格式如何，均会下发 *_auth Cookie）
            cookie_dict = requests.utils.dict_from_cookiejar(session.cookies)
            has_auth = any(k.endswith('_auth') and len(v) > 10 for k, v in cookie_dict.items())
            if has_auth:
                logger.info("检测到登录凭据 Cookie (auth) 已下发，正在验证会话有效性...")
                valid, check_msg = self._check_cookie_valid(session)
                if valid:
                    logger.info(f"账号 [{self._username}] 登录验证成功！({check_msg})")
                    self._save_session_cookie(session)
                    return True, f"登录成功 ({check_msg})"

            # 容错策略 2：多重解析 JSON（剥离 UTF-8 BOM，或从文本中提取 JSON 块）
            res_json = None
            raw_bytes = resp_login.content or b""
            raw_text = ""
            try:
                raw_text = raw_bytes.decode("utf-8-sig", errors="replace").strip()
            except Exception:
                raw_text = (resp_login.text or "").strip()

            if raw_text:
                try:
                    res_json = json.loads(raw_text)
                except Exception:
                    # 尝试用正则提取 JSON 结构
                    json_match = re.search(r'\{[\s\S]*\}', raw_text)
                    if json_match:
                        try:
                            res_json = json.loads(json_match.group(0))
                        except Exception:
                            pass

            # 容错策略 3：未解析出 JSON 时进行会话有效性兜底测试
            if not res_json:
                valid, check_msg = self._check_cookie_valid(session)
                if valid:
                    logger.info(f"响应非标准 JSON，但会话有效性校验通过！({check_msg})")
                    self._save_session_cookie(session)
                    return True, f"登录成功 ({check_msg})"

                # 清洗 HTML 标签，避免日志中出现空白或裸标签导致无法查看原因
                clean_text = re.sub(r'<script.*?</script>', '', raw_text, flags=re.S)
                clean_text = re.sub(r'<style.*?</style>', '', clean_text, flags=re.S)
                clean_text = re.sub(r'<[^>]+>', ' ', clean_text)
                clean_text = ' '.join(clean_text.split())

                diag_info = (
                    f"HTTP {resp_login.status_code}, 重定向: {len(resp_login.history)}次, "
                    f"字节数: {len(raw_bytes)}"
                )
                logger.error(f"阡陌居自动登录响应异常 ({diag_info}): {repr(clean_text[:120]) if clean_text else '（内容为空）'}")
                return False, f"登录响应异常: {clean_text[:50] if clean_text else '空响应/网络阻断'}"

            # 步骤 3：解析标准 JSON 响应
            res_vars = res_json.get("Variables", {}) or {}
            res_msg = res_json.get("Message", {}) or {}
            msg_val = res_msg.get("messageval", "")
            msg_str = res_msg.get("messagestr", "")

            uid = str(res_vars.get("member_uid", "0"))
            auth = res_vars.get("auth")
            uname = res_vars.get("member_username", "")

            # 成功判定：Variables 下发了 auth 或 member_uid 大于 0，或 messageval 为 login_succeed
            if (auth and uid != "0") or (uid not in ["0", "", None] and uname) or ("login_succeed" in msg_val):
                logger.info(f"账号 [{uname or self._username}] (UID: {uid}) 自动登录成功！")
                self._save_session_cookie(session)
                return True, f"登录成功 (UID: {uid})"

            # 失败信息解析与友好映射
            error_map = {
                "login_invalid": "用户名或密码错误，请核对论坛账号与密码（注意区分大小写）",
                "login_question_empty": "论坛账号已设置安全提问，请在插件配置中选择提问并填写答案",
                "login_question_invalid": "安全提问答案错误，请核对插件配置中的提问与答案",
                "login_strike": "登录失败次数过多，账号被论坛临时锁定15分钟（请等待15分钟解锁，切勿频繁重试）",
                "login_clearcookies": "登录状态异常，已清理Cookie",
            }
            error_desc = error_map.get(msg_val)
            if not error_desc:
                error_desc = msg_str if (msg_str and not msg_str.startswith("mobile:")) else f"登录失败: {msg_val or '未知错误'}"

            if msg_val == "login_strike":
                logger.warning(
                    "⚠️ 提示：论坛密码或安全提问错误次数已达上限，账号已被论坛安全策略临时锁定 15 分钟。"
                    "请暂停测试并等待至少 15 分钟解锁；切勿频繁点击运行，否则每次尝试都会重置 15 分钟锁定计时！"
                    "解锁后建议先在浏览器隐身窗口登录一次，确认账号、密码和安全提问无误后再在插件中保存配置。"
                )

            logger.error(f"阡陌居自动登录失败: {error_desc}")
            return False, error_desc

        except requests.Timeout:
            logger.error(f"自动登录请求超时 (超时限制: {self._timeout}s)")
            return False, f"登录请求超时 ({self._timeout}s)"
        except Exception as e:
            logger.error(f"自动登录出现异常: {str(e)}", exc_info=True)
            return False, f"登录异常: {str(e)}"

    def _check_cookie_valid(self, session: requests.Session) -> Tuple[Optional[bool], str]:
        """
        检查 Cookie 是否有效。
        返回 (is_valid, message):
            (True, msg): Cookie 有效且已登录
            (False, msg): Cookie 已失效或未登录
            (None, msg): 网络连接错误/超时（非 Cookie 过期，需重试）
        """
        try:
            # 优先使用轻量的 Discuz! 移动接口验证，耗时仅百毫秒级
            check_url = "https://www.1000qm.vip/api/mobile/index.php?version=4&module=login"
            resp = session.get(check_url, timeout=self._timeout)
            if resp.status_code == 200:
                try:
                    res_json = resp.json()
                    variables = res_json.get("Variables", {}) or {}
                    uid = str(variables.get("member_uid", "0"))
                    uname = variables.get("member_username", "")
                    if uid not in ["0", "", None] and uname:
                        logger.info(f"Cookie 验证成功，当前登录用户: {uname} (UID: {uid})")
                        return True, f"已登录: {uname}"
                except Exception:
                    pass

            # 回退：检查网页端特征
            resp_home = session.get("https://www.1000qm.vip/", timeout=self._timeout)
            html = resp_home.text
            if any(k in html for k in ["退出", "个人资料", "我的空间", "用户中心"]):
                logger.info("Cookie 验证成功 (通过页面特征确认)")
                return True, "Cookie有效"

            logger.warning("Cookie 验证未通过：未检测到登录状态")
            return False, "Cookie已失效或未登录"

        except (requests.Timeout, requests.ConnectionError) as net_err:
            logger.warning(f"检查 Cookie 时网络连接异常或超时: {str(net_err)}")
            return None, f"网络请求超时或异常: {str(net_err)}"
        except Exception as e:
            logger.warning(f"检查 Cookie 时出错: {str(e)}")
            return False, f"Cookie校验异常: {str(e)}"

    def sign(self, retry_count=0, extended_retry=0):
        """
        执行签到，支持双轨鉴权（Cookie + 账号密码自动登录）、网络超时自动重试。
        """
        start_time = datetime.now()
        sign_timeout = 300  # 签到总执行最长5分钟

        # 保存当前执行的触发类型
        self._current_trigger_type = "手动触发" if self._is_manual_trigger() else "定时触发"

        # 如果是定时任务且不是重试，检查是否有正在运行的延长重试任务
        if retry_count == 0 and extended_retry == 0 and not self._is_manual_trigger():
            if self._has_running_extended_retry():
                logger.warning("检测到有正在运行的延长重试任务，跳过本次执行")
                return {
                    "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                    "status": "跳过: 有正在进行的重试任务"
                }

        logger.info("============= 开始签到 =============")
        notification_sent = False
        sign_dict = None

        if retry_count > 0:
            logger.info(f"当前为第 {retry_count} 次常规重试")
        if extended_retry > 0:
            logger.info(f"当前为第 {extended_retry} 次延长重试")

        try:
            # 检查今日是否已成功签到
            if not self._is_manual_trigger() and self._is_already_signed_today():
                logger.info("根据历史记录，今日已成功签到，跳过本次执行")
                sign_dict = {
                    "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                    "status": "跳过: 今日已签到",
                }

                # 即使已签到，也尝试领取每日威望红包（如开启）
                try:
                    if self._draw_prestige_enabled:
                        logger.info("（已签到分支）开始执行每日威望红包任务...")
                        session_tmp = self._get_session()
                        prestige_info = self._claim_daily_prestige_reward(session_tmp)
                        if prestige_info:
                            sign_dict.update(prestige_info)
                        session_tmp.close()
                except Exception as e:
                    logger.warning(f"（已签到分支）领取每日威望红包出错（忽略）: {str(e)}")

                # 读取历史记录中的积分奖励并补全
                history = self.get_data('sign_history') or []
                today = datetime.now().strftime('%Y-%m-%d')
                today_success = [
                    record for record in history
                    if record.get("date", "").startswith(today)
                    and record.get("status") in ["签到成功", "已签到"]
                ]
                if today_success:
                    last_success = max(today_success, key=lambda x: x.get("date", ""))
                    sign_dict.update({
                        "message": last_success.get("message"),
                        "points": last_success.get("points"),
                        "days": last_success.get("days"),
                        "coins_gain": last_success.get("coins_gain")
                    })

                # 发送重复签到通知
                if self._notify:
                    last_sign_time = self._get_last_sign_time()
                    title = "【ℹ️ 阡陌居重复签到】"
                    text = (
                        f"📢 执行结果\n"
                        f"━━━━━━━━━━\n"
                        f"🕐 时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"📍 方式：{self._current_trigger_type}\n"
                        f"ℹ️ 状态：今日已完成签到 ({last_sign_time})\n"
                        f"━━━━━━━━━━\n"
                        f"📊 签到信息\n"
                        f"💬 消息：{sign_dict.get('message', '—')}\n"
                        f"🪙 当日奖励：铜币 +{sign_dict.get('coins_gain', '—')} | 威望 +{sign_dict.get('prestige_gain', '—')}\n"
                        f"━━━━━━━━━━\n"
                        f"🧧 威望红包（汇总）\n"
                        f"🪙 铜币：{sign_dict.get('coins_total', '—')}\n"
                        f"🥇 威望：{sign_dict.get('prestige_total', '—')}\n"
                        f"🤝 贡献：{sign_dict.get('contribution_total', '—')}\n"
                        f"📚 发书数：{sign_dict.get('books_total', '—')}\n"
                        f"📈 积分：{sign_dict.get('credits_total', '—')}\n"
                        f"🏆 总积分：{sign_dict.get('credits_sum', '—')}\n"
                        f"━━━━━━━━━━"
                    )
                    self.post_message(
                        mtype=NotificationType.SiteMessage,
                        title=title,
                        text=text
                    )
                return sign_dict

            # 创建请求会话
            session = self._get_session()

            # 鉴权与 Cookie 有效性检查流程
            need_login = False
            if self._cookie:
                logger.info(f"已配置 Cookie (长度 {len(self._cookie)} 字符)，正在校验有效性...")
                is_valid, check_reason = self._check_cookie_valid(session)
                if is_valid is True:
                    logger.info("Cookie 校验有效，继续签到")
                elif is_valid is None:
                    # 网络故障 / 超时
                    logger.warning(f"Cookie 校验遇到网络错误: {check_reason}")
                    if retry_count < self._max_retries:
                        logger.info(f"将在 {self._retry_interval} 秒后进行第 {retry_count + 1} 次重试...")
                        time.sleep(self._retry_interval)
                        return self.sign(retry_count + 1, extended_retry)

                    sign_dict = {
                        "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                        "status": f"签到失败: 网络超时/异常 ({check_reason})",
                    }
                    self._save_sign_history(sign_dict)
                    if self._notify:
                        self.post_message(
                            mtype=NotificationType.SiteMessage,
                            title="【❌ 阡陌居签到失败】",
                            text=f"❌ 网络多次连接超时或错误: {check_reason}。如网络不稳定，建议在插件设置中配置代理。"
                        )
                    return sign_dict
                else:
                    # Cookie 确实已失效
                    logger.warning("现有 Cookie 已失效或过期")
                    need_login = True
            else:
                logger.info("未配置 Cookie，将使用账号密码登录")
                need_login = True

            # 触发自动登录（若 Cookie 过期或未配置）
            if need_login:
                if self._username and self._password:
                    logger.info("开始通过账号密码自动登录并刷新 Cookie...")
                    login_ok, login_msg = self._auto_login(session)
                    if not login_ok:
                        logger.error(f"自动登录失败: {login_msg}")
                        # 账号被论坛临时锁定时，立即中止，严禁重试避免重置锁定时间
                        if "锁定" in login_msg:
                            sign_dict = {
                                "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                                "status": "签到失败: 账号被锁定 (需等待15分钟)",
                                "message": login_msg
                            }
                            self._save_sign_history(sign_dict)
                            if self._notify:
                                self.post_message(
                                    mtype=NotificationType.SiteMessage,
                                    title="【⚠️ 阡陌居账号被临时锁定】",
                                    text=(
                                        "⚠️ 论坛提示密码或提问错误次数过多，账号被临时锁定15分钟。\n"
                                        "请暂停测试，等待至少15分钟后再试；切勿频繁运行以免延长锁定！\n"
                                        "建议待解锁后在浏览器中验证登录，核对账号密码及是否开启了安全提问。"
                                    )
                                )
                            return sign_dict

                        # 登录失败，若是网络原因可以重试
                        if ("超时" in login_msg or "HTTP" in login_msg) and retry_count < self._max_retries:
                            logger.info(f"登录遇到网络波动，{self._retry_interval} 秒后重试...")
                            time.sleep(self._retry_interval)
                            return self.sign(retry_count + 1, extended_retry)

                        sign_dict = {
                            "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                            "status": f"签到失败: 自动登录失败 - {login_msg}",
                            "message": login_msg
                        }
                        self._save_sign_history(sign_dict)
                        if self._notify:
                            self.post_message(
                                mtype=NotificationType.SiteMessage,
                                title="【❌ 阡陌居登录失败】",
                                text=f"❌ 自动登录失败: {login_msg}。请检查账号、密码或安全提问配置。"
                            )
                        return sign_dict
                else:
                    logger.error("未配置有效 Cookie 且未配置账号密码，无法继续签到")
                    sign_dict = {
                        "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                        "status": "签到失败: 未配置Cookie或账号密码",
                    }
                    self._save_sign_history(sign_dict)
                    if self._notify:
                        self.post_message(
                            mtype=NotificationType.SiteMessage,
                            title="【❌ 阡陌居签到失败】",
                            text="❌ 未配置有效 Cookie，且未填写账号密码。请在插件设置中配置 Cookie 或账号密码。"
                        )
                    return sign_dict

            # 领取每日威望红包（可选开关）
            prestige_info = None
            try:
                if self._draw_prestige_enabled:
                    logger.info("开始领取每日威望红包任务...")
                    prestige_info = self._claim_daily_prestige_reward(session)
                else:
                    logger.info("领取每日威望红包已关闭，跳过此步骤")
            except Exception as e:
                logger.warning(f"领取每日威望红包过程中出错（忽略继续签到）: {str(e)}")

            # 步骤 1: 访问签到页或首页获取 formhash
            logger.info("正在获取签到 formhash...")
            formhash = None
            try:
                sign_page_resp = session.get("https://www.1000qm.vip/plugin.php?id=dsu_paulsign:sign", timeout=self._timeout)
                sign_page_html = sign_page_resp.text

                # 检查页面是否已提示今天签到过
                if "您今天已经签到过了" in sign_page_html or "已经签到" in sign_page_html:
                    logger.info("签到页面显示今日已完成签到")
                    sign_dict = {
                        "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                        "status": "已签到",
                        "message": "今日已完成签到"
                    }
                    if prestige_info:
                        sign_dict.update(prestige_info)
                    self._save_sign_history(sign_dict)
                    self._save_last_sign_date()
                    if self._notify:
                        self._send_sign_notification(sign_dict)
                    return sign_dict

                # 提取 formhash
                fh_match = re.search(r'name="formhash" value="([^"]*)"', sign_page_html)
                if fh_match:
                    formhash = fh_match.group(1)

                if not formhash:
                    # 备用：从首页查找
                    home_resp = session.get("https://www.1000qm.vip/", timeout=self._timeout)
                    fh_match2 = re.search(r'name="formhash" value="([^"]*)"', home_resp.text)
                    if fh_match2:
                        formhash = fh_match2.group(1)

                if not formhash:
                    logger.error("未找到 formhash 参数")
                    if retry_count < self._max_retries:
                        logger.info(f"将在 {self._retry_interval} 秒后进行第 {retry_count + 1} 次重试...")
                        time.sleep(self._retry_interval)
                        return self.sign(retry_count + 1, extended_retry)

                    sign_dict = {
                        "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                        "status": "签到失败: 未找到formhash参数",
                    }
                    self._save_sign_history(sign_dict)
                    if self._notify:
                        self.post_message(
                            mtype=NotificationType.SiteMessage,
                            title="【❌ 阡陌居签到失败】",
                            text="❌ 未找到 formhash 参数，请检查站点是否变更或账号权限。"
                        )
                    return sign_dict

                logger.info(f"成功获取 formhash: {formhash[:10]}...")

            except requests.Timeout:
                logger.error(f"获取 formhash 超时 ({self._timeout}s)")
                if retry_count < self._max_retries:
                    logger.info(f"将在 {self._retry_interval} 秒后进行第 {retry_count + 1} 次重试...")
                    time.sleep(self._retry_interval)
                    return self.sign(retry_count + 1, extended_retry)

                sign_dict = {
                    "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                    "status": "签到失败: 页面访问多次超时",
                }
                self._save_sign_history(sign_dict)
                if self._notify:
                    self.post_message(
                        mtype=NotificationType.SiteMessage,
                        title="【❌ 阡陌居签到失败】",
                        text="❌ 访问站点页面多次超时，所有重试均失败。建议在配置中设置代理或增大超时时间。"
                    )
                return sign_dict

            # 步骤 2: 执行 DSU 签到
            logger.info("正在提交签到请求...")
            sign_url = "https://www.1000qm.vip/plugin.php?id=dsu_paulsign:sign&operation=qiandao&infloat=1&inajax=1"

            # 随机选择心情与文案
            qdxq, mood_label = random.choice(_SIGN_MOODS)
            todaysay = random.choice(_SIGN_TEXTS.get(qdxq) or _SIGN_TEXTS_FALLBACK)
            logger.info(f"本次签到心情: {mood_label}({qdxq})，签到文案: {todaysay}")

            post_data = {
                "formhash": formhash,
                "qdxq": qdxq,
                "qdmode": "1",
                "todaysay": todaysay,
                "fastreply": "0"
            }

            session.headers.update({
                "Origin": "https://www.1000qm.vip",
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": "https://www.1000qm.vip/plugin.php?id=dsu_paulsign:sign"
            })

            try:
                response = session.post(sign_url, data=post_data, timeout=self._timeout)
                html_content = response.text
                debug_resp = html_content[:500]
                logger.info(f"签到响应内容预览: {debug_resp}")

                log_message = _extract_sign_message(html_content)
                if log_message:
                    logger.info(f"签到响应解析结果: {log_message}")

                    if any(k in log_message for k in ["未定义操作", "失败", "错误", "无效", "无法", "不能"]):
                        logger.error(f"签到失败: {log_message}")
                        sign_dict = {
                            "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                            "status": f"签到失败: {log_message}",
                            "message": log_message
                        }
                        self._save_sign_history(sign_dict)
                        if self._notify:
                            self.post_message(
                                mtype=NotificationType.SiteMessage,
                                title="【❌ 阡陌居签到失败】",
                                text=f"❌ 签到失败: {log_message}"
                            )
                        return sign_dict

                    elif "已经签到" in log_message or "已签到" in log_message:
                        logger.info("今日已完成签到")
                        sign_dict = {
                            "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                            "status": "已签到",
                            "message": log_message
                        }
                        if prestige_info:
                            sign_dict.update(prestige_info)
                        self._save_sign_history(sign_dict)
                        self._save_last_sign_date()
                        if self._notify:
                            self._send_sign_notification(sign_dict)
                        return sign_dict

                    elif "成功" in log_message or "签到" in log_message:
                        logger.info("签到成功！")
                        sign_dict = {
                            "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                            "status": "签到成功",
                            "message": log_message
                        }
                        if prestige_info:
                            sign_dict.update(prestige_info)

                        try:
                            coins_match = re.search(r'铜币[^0-9+]*\+?(\d+)', log_message)
                            if coins_match:
                                sign_dict["coins_gain"] = coins_match.group(1)
                            sign_dict["days"] = "—"
                        except Exception as e:
                            logger.warning(f"提取积分信息失败: {str(e)}")

                        self._save_sign_history(sign_dict)
                        self._save_last_sign_date()
                        if self._notify:
                            self._send_sign_notification(sign_dict)
                        return sign_dict

                    else:
                        logger.error(f"签到返回未知消息: {log_message}")
                        sign_dict = {
                            "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                            "status": f"签到失败: {log_message}",
                            "message": log_message
                        }
                        self._save_sign_history(sign_dict)
                        if self._notify:
                            self.post_message(
                                mtype=NotificationType.SiteMessage,
                                title="【❌ 阡陌居签到失败】",
                                text=f"❌ 签到失败: {log_message}"
                            )
                        return sign_dict

                else:
                    logger.error(f"签到请求已提交，但未能解析到响应消息: {debug_resp}")
                    if retry_count < self._max_retries:
                        logger.info(f"将在 {self._retry_interval} 秒后进行第 {retry_count + 1} 次重试...")
                        time.sleep(self._retry_interval)
                        return self.sign(retry_count + 1, extended_retry)

                    sign_dict = {
                        "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                        "status": "签到失败: 未找到响应消息",
                    }
                    self._save_sign_history(sign_dict)
                    if self._notify:
                        self.post_message(
                            mtype=NotificationType.SiteMessage,
                            title="【❌ 阡陌居签到失败】",
                            text="❌ 签到失败: 未能解析到响应消息，请检查站点状态。"
                        )
                    return sign_dict

            except requests.Timeout:
                logger.error("签到提交请求超时")
                if retry_count < self._max_retries:
                    logger.info(f"将在 {self._retry_interval} 秒后进行第 {retry_count + 1} 次重试...")
                    time.sleep(self._retry_interval)
                    return self.sign(retry_count + 1, extended_retry)

                sign_dict = {
                    "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                    "status": "签到失败: 签到提交请求超时",
                }
                self._save_sign_history(sign_dict)
                if self._notify:
                    self.post_message(
                        mtype=NotificationType.SiteMessage,
                        title="【❌ 阡陌居签到失败】",
                        text="❌ 签到提交请求多次超时，所有重试均已失败。"
                    )
                return sign_dict

        except Exception as e:
            logger.error(f"签到主流程异常: {str(e)}", exc_info=True)
            sign_dict = {
                "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                "status": f"签到失败: {str(e)}",
            }
            self._save_sign_history(sign_dict)
            if self._notify and not notification_sent:
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title="【❌ 阡陌居签到异常】",
                    text=f"❌ 签到异常终止: {str(e)}"
                )
            return sign_dict
        finally:
            try:
                if 'session' in locals() and session:
                    session.close()
            except Exception:
                pass

    def _claim_daily_prestige_reward(self, session: Optional[requests.Session]) -> Dict[str, Any]:
        """
        领取每日威望红包任务：
        1) GET https://www.1000qm.vip/home.php?mod=task&do=apply&id=1
        2) GET https://www.1000qm.vip/home.php?mod=task&do=draw&id=1
        3) 查看完成页 GET https://www.1000qm.vip/home.php?mod=task&item=done
        4) 拉取财富总览 https://www.1000qm.vip/home.php?mod=spacecp&ac=credit&showcredit=1
        """
        apply_url = "https://www.1000qm.vip/home.php?mod=task&do=apply&id=1"
        draw_url = "https://www.1000qm.vip/home.php?mod=task&do=draw&id=1"
        done_url = "https://www.1000qm.vip/home.php?mod=task&item=done"

        close_session = False
        if session is None:
            session = self._get_session()
            close_session = True

        info = {}
        try:
            headers = {
                "Referer": "https://www.1000qm.vip/home.php?mod=task",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            }

            # 1. 申请任务
            logger.info("正在申请威望红包任务...")
            session.get(apply_url, headers=headers, timeout=self._timeout)

            # 2. 领取任务
            logger.info("正在领取威望红包...")
            resp_draw = session.get(draw_url, headers=headers, timeout=self._timeout)
            text_draw = resp_draw.text or ""

            # 3. 完成页
            resp_done = session.get(done_url, headers=headers, timeout=self._timeout)
            text_done = resp_done.text or ""

            # 解析威望奖励与总积分
            try:
                m_gain = re.search(r"积分\s*威望\s*(\d+)", text_done)
                if m_gain:
                    info["prestige_gain"] = m_gain.group(1)

                m_total = re.search(r"积分:\s*(\d+)", text_done)
                if m_total:
                    info["credits_total"] = m_total.group(1)

                m_done = re.search(r"完成于\s*([0-9\-: ]+)", text_done)
                if m_done:
                    info["days"] = m_done.group(1).strip()
            except Exception as e:
                logger.warning(f"解析威望红包信息失败: {str(e)}")

            # 4. 拉取财富总览
            try:
                credit_url = "https://www.1000qm.vip/home.php?mod=spacecp&ac=credit&showcredit=1"
                resp_credit = session.get(credit_url, headers=headers, timeout=self._timeout)
                text_credit = resp_credit.text or ""

                def _search_num(label: str):
                    m = re.search(rf"<em>\s*{label}\s*:\s*</em>\s*(\d+)", text_credit)
                    return m.group(1) if m else None

                coins_total = _search_num("铜币")
                prestige_total = _search_num("威望")
                contrib_total = _search_num("贡献")
                books_total = _search_num("发书数")
                credits_user = _search_num("积分")

                # 总积分
                credits_sum = None
                m_sum = re.search(r"总积分\s*:\s*(\d+)", text_credit)
                if m_sum:
                    credits_sum = m_sum.group(1)

                overview = {}
                if coins_total: overview["coins_total"] = coins_total
                if prestige_total: overview["prestige_total"] = prestige_total
                if contrib_total: overview["contribution_total"] = contrib_total
                if books_total: overview["books_total"] = books_total
                if credits_user: overview["credits_total"] = credits_user
                if credits_sum: overview["credits_sum"] = credits_sum

                if overview:
                    self.save_data('last_credits_overview', overview)
                    info.update(overview)
                    logger.info(f"成功获取财富总览: 铜币={coins_total}, 威望={prestige_total}, 总积分={credits_sum}")
            except Exception as e:
                logger.warning(f"获取财富总览失败: {str(e)}")

        except Exception as e:
            logger.warning(f"领取每日威望红包任务异常: {str(e)}")
        finally:
            if close_session:
                session.close()

        return info

    def _save_sign_history(self, sign_data):
        """保存签到历史记录并按保留天数清理"""
        try:
            history = self.get_data('sign_history') or []
            history.append(sign_data)

            # 按保留天数清理
            cutoff_date = datetime.now() - timedelta(days=self._history_days)
            filtered_history = []
            for record in history:
                date_str = record.get("date", "")
                try:
                    record_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                    if record_date >= cutoff_date:
                        filtered_history.append(record)
                except ValueError:
                    filtered_history.append(record)

            self.save_data('sign_history', filtered_history)
            logger.info(f"保存签到历史记录，当前共有 {len(filtered_history)} 条记录")
        except Exception as e:
            logger.error(f"保存签到历史记录出错: {str(e)}")

    def _send_sign_notification(self, sign_dict):
        """发送签到结果通知"""
        status = sign_dict.get("status", "")
        success = status in ["签到成功", "已签到"]
        status_icon = "✅" if success else "❌"
        title = f"【{status_icon} 阡陌居签到结果】"

        text = (
            f"📢 执行结果\n"
            f"━━━━━━━━━━\n"
            f"🕐 时间：{sign_dict.get('date', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}\n"
            f"📍 方式：{self._current_trigger_type or '自动触发'}\n"
            f"📌 状态：{status}\n"
            f"━━━━━━━━━━\n"
            f"📊 签到信息\n"
            f"💬 消息：{sign_dict.get('message', '—')}\n"
            f"🪙 当日奖励：铜币 +{sign_dict.get('coins_gain', '—')} | 威望 +{sign_dict.get('prestige_gain', '—')}\n"
            f"━━━━━━━━━━\n"
            f"🧧 财富汇总\n"
            f"🪙 铜币：{sign_dict.get('coins_total', '—')}\n"
            f"🥇 威望：{sign_dict.get('prestige_total', '—')}\n"
            f"🤝 贡献：{sign_dict.get('contribution_total', '—')}\n"
            f"📚 发书数：{sign_dict.get('books_total', '—')}\n"
            f"📈 积分：{sign_dict.get('credits_total', '—')}\n"
            f"🏆 总积分：{sign_dict.get('credits_sum', '—')}\n"
            f"━━━━━━━━━━"
        )
        self.post_message(
            mtype=NotificationType.SiteMessage,
            title=title,
            text=text
        )

    def get_state(self) -> bool:
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            logger.info(f"注册定时服务: {self._cron}")
            return [{
                "id": "qmjsign",
                "name": "阡陌居签到",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.sign,
                "kwargs": {}
            }]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """构建配置表单"""
        return [
            {
                'component': 'VForm',
                'content': [
                    # 1. 顶部核心功能开关（4列均分，色彩分明，文案对称）
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'sm': 6, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {
                                        'model': 'enabled',
                                        'label': '启用插件',
                                        'color': 'primary'
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'sm': 6, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {
                                        'model': 'notify',
                                        'label': '开启签到通知',
                                        'color': 'info'
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'sm': 6, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {
                                        'model': 'draw_prestige',
                                        'label': '领取威望红包',
                                        'color': 'success'
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'sm': 6, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {
                                        'model': 'onlyonce',
                                        'label': '立即运行一次',
                                        'color': 'warning'
                                    }
                                }]
                            }
                        ]
                    },
                    # 2. 账号与密码认证（6 + 6 对称网格）
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {
                                        'model': 'username',
                                        'label': '论坛用户名 / 账号',
                                        'placeholder': '配置后Cookie过期可全自动重新登录续期'
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {
                                        'model': 'password',
                                        'label': '论坛密码',
                                        'type': 'password',
                                        'placeholder': '请输入阡陌居登录密码'
                                    }
                                }]
                            }
                        ]
                    },
                    # 3. 安全提问（6 + 6 对称网格）
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VSelect',
                                    'props': {
                                        'model': 'questionid',
                                        'label': '安全提问（若未设置请选无）',
                                        'items': _SECURITY_QUESTIONS
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {
                                        'model': 'answer',
                                        'label': '安全提问答案',
                                        'placeholder': '若账号设置了安全提问请在此填写'
                                    }
                                }]
                            }
                        ]
                    },
                    # 4. 站点 Cookie 独立整行（cols: 12，给长串Cookie充分呼吸空间）
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {
                                        'model': 'cookie',
                                        'label': '站点 Cookie（可选，填入账号密码将自动登录生成并持久化保存）',
                                        'placeholder': '如已配置账号密码可完全留空，系统自动登录并回填保存'
                                    }
                                }]
                            }
                        ]
                    },
                    # 5. 定时周期与网络代理（6 + 6，两者均为外部环境配置）
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VCronField',
                                    'props': {
                                        'model': 'cron',
                                        'label': '签到周期 (Cron 表达式)'
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {
                                        'model': 'proxy',
                                        'label': '网络代理（可选）',
                                        'placeholder': '例如 http://127.0.0.1:7890，无代理请留空'
                                    }
                                }]
                            }
                        ]
                    },
                    # 6. 请求参数三列等宽（4 + 4 + 4 = 12，告别原先4个小框挤压变形）
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {
                                        'model': 'timeout',
                                        'label': '网络超时时间(秒)',
                                        'type': 'number',
                                        'placeholder': '20'
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {
                                        'model': 'max_retries',
                                        'label': '最大重试次数',
                                        'type': 'number',
                                        'placeholder': '3'
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {
                                        'model': 'retry_interval',
                                        'label': '重试间隔(秒)',
                                        'type': 'number',
                                        'placeholder': '30'
                                    }
                                }]
                            }
                        ]
                    },
                    # 7. 数据维护设置（6 + 6，左右基线对齐，警示色开关）
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {
                                        'model': 'history_days',
                                        'label': '历史保留天数',
                                        'type': 'number',
                                        'placeholder': '30'
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {
                                        'model': 'clear_history',
                                        'label': '清空历史记录 (保存时立即执行)',
                                        'color': 'error'
                                    }
                                }]
                            }
                        ]
                    },
                    # 8. 结构化说明卡片（图文层次清晰，呼吸均匀）
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [{
                                    'component': 'VAlert',
                                    'props': {
                                        'type': 'info',
                                        'variant': 'tonal',
                                        'text': (
                                            '💡【配置指南与说明】\n'
                                            '• 🔑 自动更新Cookie：配置用户名和密码后，Cookie 失效将自动登录并持久化保存，彻底告别频繁手动抓包。\n'
                                            '• 🛡️ 安全提问：若论坛账号启用了安全提问，请选择相应问题并填写答案；否则保持“无安全提问”。\n'
                                            '• 🌐 代理与超时：网络直连 1000qm.vip 若经常波动或超时，可在网络代理填入本地代理，并适当调大超时时间。\n'
                                            '• 🚀 签到测试：保存配置后可勾选顶部的“立即运行一次”，系统将在后台立刻执行签到与威望红包任务。\n'
                                            '• 🧹 历史清理：如需清空历史记录，勾选“清空历史记录”后点击保存即可，执行完毕后开关会自动复位。'
                                        )
                                    }
                                }]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "notify": True,
            "onlyonce": False,
            "draw_prestige": False,
            "username": "",
            "password": "",
            "questionid": "0",
            "answer": "",
            "cookie": "",
            "proxy": "",
            "timeout": 20,
            "cron": "0 8 * * *",
            "max_retries": 3,
            "retry_interval": 30,
            "history_days": 30,
            "clear_history": False
        }

    def get_page(self) -> List[dict]:
        """构建详情页面展示签到历史与财富概览"""
        historys = self.get_data('sign_history') or []
        credits_overview = self.get_data('last_credits_overview') or {}

        history_rows = []
        if historys:
            sorted_history = sorted(historys, key=lambda x: x.get("date", ""), reverse=True)
            for history in sorted_history:
                status_text = history.get("status", "未知")
                status_color = "success" if any(k in status_text for k in ["签到成功", "已签到"]) else "error"
                history_rows.append({
                    'component': 'tr',
                    'content': [
                        {'component': 'td', 'props': {'class': 'text-caption'}, 'text': history.get("date", "")},
                        {
                            'component': 'td',
                            'content': [{
                                'component': 'VChip',
                                'props': {'color': status_color, 'size': 'small', 'variant': 'outlined'},
                                'text': status_text
                            }]
                        },
                        {'component': 'td', 'text': history.get('message', '—')},
                        {'component': 'td', 'text': f"铜币 +{history.get('coins_gain', '—')} | 威望 +{history.get('prestige_gain', '—')}"}
                    ]
                })

        overview_card = []
        if credits_overview:
            def chip(label, key, color='primary'):
                return {
                    'component': 'VChip',
                    'props': {'size': 'small', 'variant': 'outlined', 'color': color, 'class': 'mr-2 mb-2'},
                    'text': f"{label} {credits_overview.get(key, '—')}"
                }

            overview_card = [{
                'component': 'VCard',
                'props': {'variant': 'outlined', 'class': 'mb-4'},
                'content': [
                    {'component': 'VCardTitle', 'props': {'class': 'text-h6'}, 'text': '💰 账户财富汇总'},
                    {'component': 'VCardText', 'content': [{'component': 'div', 'content': [
                        chip('铜币', 'coins_total', 'amber-darken-2'),
                        chip('威望', 'prestige_total', 'success'),
                        chip('贡献', 'contribution_total'),
                        chip('发书数', 'books_total'),
                        chip('积分', 'credits_total', 'info'),
                        chip('总积分', 'credits_sum', 'deep-purple')
                    ]}]}
                ]
            }]

        table_card = [
            {
                'component': 'VCard',
                'props': {'variant': 'outlined', 'class': 'mb-4'},
                'content': [
                    {'component': 'VCardTitle', 'props': {'class': 'text-h6'}, 'text': '📊 阡陌居签到历史'},
                    {
                        'component': 'VCardText',
                        'content': [
                            {
                                'component': 'VAlert',
                                'props': {
                                    'type': 'info',
                                    'variant': 'tonal',
                                    'density': 'compact',
                                    'class': 'mb-3',
                                    'text': '提示：如需清理或重置签到历史，可在插件设置中开启【清空历史记录】并点击保存，或发送命令 /qmjsign_clear。'
                                }
                            },
                            {
                                'component': 'VTable',
                                'props': {'hover': True, 'density': 'compact'},
                                'content': [
                                    {
                                        'component': 'thead',
                                        'content': [{
                                            'component': 'tr',
                                            'content': [
                                                {'component': 'th', 'text': '时间'},
                                                {'component': 'th', 'text': '状态'},
                                                {'component': 'th', 'text': '消息'},
                                                {'component': 'th', 'text': '奖励'}
                                            ]
                                        }]
                                    },
                                    {
                                        'component': 'tbody',
                                        'content': history_rows if history_rows else [{
                                            'component': 'tr',
                                            'content': [{
                                                'component': 'td',
                                                'props': {'colspan': 4, 'class': 'text-center text-muted py-4'},
                                                'text': '暂无历史记录'
                                            }]
                                        }]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]

        return overview_card + table_card

    def stop_service(self):
        """停止服务"""
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error(f"停止 qmjsign 调度服务失败: {str(e)}")

    def _clear_extended_retry_tasks(self):
        try:
            self.save_data('current_retry_task', None)
        except Exception:
            pass

    def _has_running_extended_retry(self) -> bool:
        return False

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """注册远程控制命令"""
        if not EventType:
            return []
        return [
            {
                "cmd": "/qmjsign_clear",
                "event": EventType.PluginAction,
                "desc": "清空阡陌居签到历史记录",
                "category": "签到",
                "data": {"action": "qmjsign_clear_history"},
            },
            {
                "cmd": "/qmjsign_sign",
                "event": EventType.PluginAction,
                "desc": "立即执行一次阡陌居签到",
                "category": "签到",
                "data": {"action": "qmjsign_sign_now"},
            }
        ]

    @_register_action
    def handle_plugin_commands(self, event: Event):
        """处理插件远程命令事件"""
        if not event or not event.event_data:
            return
        action = event.event_data.get("action")
        if action == "qmjsign_clear_history":
            self.save_data('sign_history', [])
            self.save_data('last_credits_overview', {})
            logger.info("已通过远程命令清空阡陌居签到历史记录")
            self.post_message(
                channel=event.event_data.get("channel"),
                title="【🧹 阡陌居签到】",
                text="已成功清空所有历史签到记录与财富统计缓存！",
                userid=event.event_data.get("user")
            )
        elif action == "qmjsign_sign_now":
            self.post_message(
                channel=event.event_data.get("channel"),
                title="【🚀 阡陌居签到】",
                text="收到立即签到指令，开始执行签到任务...",
                userid=event.event_data.get("user")
            )
            self.sign()

    def get_api(self) -> List[Dict[str, Any]]:
        """注册插件对外 API 接口"""
        return [
            {
                "path": "/clear_history",
                "endpoint": self.clear_history_api,
                "methods": ["GET", "POST"],
                "auth": "bear",
                "summary": "清空阡陌居签到历史记录",
            }
        ]

    def clear_history_api(self):
        """清空历史记录 API 端点"""
        self.save_data('sign_history', [])
        self.save_data('last_credits_overview', {})
        logger.info("已通过 API 清空阡陌居签到历史记录")
        return {"code": 0, "message": "历史记录已成功清空"}

    def _is_manual_trigger(self) -> bool:
        """检查是否手动触发"""
        import inspect
        for frame in inspect.stack():
            if frame.function == 'sign_in_api':
                return True

        if hasattr(self, '_manual_trigger') and self._manual_trigger:
            self._manual_trigger = False
            return True

        return False

    def _is_already_signed_today(self) -> bool:
        """检查今天是否已成功签到"""
        today = datetime.now().strftime('%Y-%m-%d')
        history = self.get_data('sign_history') or []
        today_records = [
            record for record in history
            if record.get("date", "").startswith(today)
            and record.get("status") in ["签到成功", "已签到"]
        ]
        if today_records:
            return True

        last_sign_date = self.get_data('last_sign_date')
        if last_sign_date:
            try:
                last_sign_datetime = datetime.strptime(last_sign_date, '%Y-%m-%d %H:%M:%S')
                if last_sign_datetime.strftime('%Y-%m-%d') == today:
                    return bool(history and history[-1].get("status") in ["签到成功", "已签到"])
            except Exception:
                pass
        return False

    def _save_last_sign_date(self):
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.save_data('last_sign_date', now)

    def _get_last_sign_time(self) -> str:
        last_sign_date = self.get_data('last_sign_date')
        if last_sign_date:
            try:
                return datetime.strptime(last_sign_date, '%Y-%m-%d %H:%M:%S').strftime('%H:%M:%S')
            except Exception:
                pass
        return "今天早些时候"


# 兼容旧版类名引用
QmjSign = qmjsign
