import hashlib
import copy
import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx

from tests.support import remote, store


class NoSleep:
    def is_set(self):
        return False

    def wait(self, delay):
        return False


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = store.QueueStore(Path(self.temp.name) / "queue.db")

    def tearDown(self):
        self.temp.cleanup()

    def test_parallel_enrollment_is_deduplicated(self):
        with ThreadPoolExecutor(max_workers=12) as executor:
            jobs = list(executor.map(lambda _: self.db.enqueue("local:A.mkv", {}), range(24)))
        self.assertEqual(len({job[0]["id"] for job in jobs}), 1)
        self.assertEqual(sum(job[1] for job in jobs), 1)

    def test_one_worker_claim_per_job(self):
        row, _ = self.db.enqueue("A", {})
        self.db.update(row["id"], ready=1)
        with ThreadPoolExecutor(max_workers=12) as executor:
            claimed = list(executor.map(lambda _: self.db.claim(), range(12)))
        self.assertEqual(sum(r is not None for r in claimed), 1)

    def test_unactivated_job_is_not_executed(self):
        self.db.enqueue("A", {})
        self.assertIsNone(self.db.claim())

    def test_future_job_does_not_block_due_job(self):
        a, _ = self.db.enqueue("A", {})
        b, _ = self.db.enqueue("B", {})
        self.db.update(a["id"], ready=1, next_at=99999999999)
        self.db.update(b["id"], ready=1)
        self.assertEqual(self.db.claim()["id"], b["id"])

    def test_restart_recovers_inflight_but_keeps_paused(self):
        a, _ = self.db.enqueue("A", {"remote_id": "123", "result": {"success": True}})
        b, _ = self.db.enqueue("B", {})
        self.db.update(a["id"], ready=1)
        self.db.claim()
        self.db.command(b["id"], "pause")
        recovered = store.QueueStore(self.db.path)
        recovered.recover()
        self.assertEqual(recovered.get(a["id"])["state"], "waiting")
        self.assertEqual(recovered.get(a["id"])["payload"]["remote_id"], "123")
        self.assertEqual(recovered.get(b["id"])["state"], "paused")

    def test_controls_cannot_race_upload(self):
        row, _ = self.db.enqueue("A", {})
        self.db.update(row["id"], ready=1)
        self.db.claim()
        for action in ("pause", "resume", "cancel"):
            with self.assertRaises(ValueError):
                self.db.command(row["id"], action)

    def test_cancel_releases_dedup_slot_and_resume_resets_timeout(self):
        row, _ = self.db.enqueue("A", {"wait_since": 0})
        self.db.command(row["id"], "pause")
        self.assertGreater(self.db.command(row["id"], "resume")["payload"]["wait_since"], 0)
        self.db.command(row["id"], "cancel")
        _, fresh = self.db.enqueue("A", {})
        self.assertTrue(fresh)

    def test_manual_queue_has_its_own_claim_and_is_deduplicated(self):
        a, _ = self.db.enqueue("A", {})
        b, _ = self.db.enqueue("B", {})
        for row in (a, b):
            self.db.update(row["id"], ready=1)
        self.db.command(a["id"], "upload")
        with ThreadPoolExecutor(max_workers=8) as executor:
            claims = list(executor.map(lambda _: self.db.claim(manual=True), range(8)))
        self.assertEqual(sum(r is not None for r in claims), 1)
        self.assertEqual(self.db.get(a["id"])["state"], "uploading")
        self.assertEqual(self.db.claim()["id"], b["id"])
        for action in ("upload", "pause", "resume", "cancel"):
            with self.assertRaises(ValueError):
                self.db.command(a["id"], action)

    def test_restart_pauses_manual_upload_and_preserves_parts(self):
        row, _ = self.db.enqueue("A", {"upload_session": {"parts": [1]}})
        self.db.update(row["id"], ready=1)
        self.db.command(row["id"], "upload")
        self.db.claim(manual=True)
        self.db.recover()
        recovered = self.db.get(row["id"])
        self.assertEqual(recovered["state"], "paused")
        self.assertEqual(recovered["payload"]["upload_session"]["parts"], [1])
        self.assertIsNone(self.db.claim())
        self.assertIsNone(self.db.claim(manual=True))

    def test_resume_resets_only_auto_counter_and_manual_claim_does_not_increment_it(self):
        row, _ = self.db.enqueue("A", {"auto_attempts": 4})
        self.db.update(row["id"], ready=1, attempts=7)
        self.db.command(row["id"], "pause")
        self.db.command(row["id"], "resume")
        self.db.command(row["id"], "upload")
        claimed = self.db.claim(manual=True)
        self.assertEqual(claimed["payload"]["auto_attempts"], 0)
        self.assertEqual(claimed["attempts"], 8)


class APITests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "A.mkv"
        self.path.write_bytes(b"test movie content")
        self.hashes = remote.hash_file(self.path, NoSleep())
        self.backend = SimpleNamespace(access_token="secret-token", base_url="https://proapi.115.com")
        self.calls = []
        self.clients = []

    def tearDown(self):
        for api in self.clients:
            api.close()
        self.temp.cleanup()

    def api(self, replies):
        replies = iter(replies)
        def handler(request):
            self.calls.append(request)
            reply = next(replies)
            if isinstance(reply, Exception):
                raise reply
            status, body = reply
            return httpx.Response(status, json=body)
        api = remote.OpenAPI(self.backend, lambda **kw: SimpleNamespace(**kw), NoSleep(),
                            httpx.Client(transport=httpx.MockTransport(handler)))
        self.clients.append(api)
        return api

    def test_native_delete_uses_exact_old_file_id_without_backup_or_move(self):
        api = self.api([(200, {"state": True, "data": {"file_id": "42"}}),
                        (200, {"state": True, "data": {}}),
                        (200, {"state": False, "code": 430004})])
        self.assertTrue(api.delete(SimpleNamespace(type="file", fileid="42", path="/library/A.mkv")))
        self.assertEqual([call.url.path for call in self.calls],
                         ["/open/folder/get_info", "/open/ufile/delete", "/open/folder/get_info"])
        self.assertEqual(self.calls[1].content, b"file_ids=42")

    def test_native_delete_never_removes_replaced_file_or_directory(self):
        api = self.api([(200, {"state": True, "data": {"file_id": "99"}})])
        with self.assertRaises(remote.PauseTask):
            api.delete(SimpleNamespace(type="file", fileid="42", path="/library/A.mkv"))
        with self.assertRaises(remote.PauseTask):
            api.delete(SimpleNamespace(type="dir", fileid="10", path="/library"))
        self.assertEqual(len(self.calls), 1)

    def test_native_delete_reconciles_missing_old_file_and_visibility_delay(self):
        api = self.api([(200, {"state": False, "code": 430004})])
        item = SimpleNamespace(type="file", fileid="42", path="/library/A.mkv")
        self.assertTrue(api.delete(item))
        api = self.api([(200, {"state": True, "data": {"file_id": "42"}}),
                        (200, {"state": True, "data": {}}),
                        (200, {"state": True, "data": {"file_id": "42"}})])
        with self.assertRaises(remote.RetryLater):
            api.delete(item)

    def test_miss_never_requests_oss_or_upload_token(self):
        api = self.api([(200, {"state": True, "code": 0, "data": {"status": 1, "bucket": "oss"}})])
        with self.assertRaises(remote.NotInstant):
            api.instant(self.path, SimpleNamespace(fileid="10"), "A.mkv", self.hashes)
        self.assertEqual([r.url.path for r in self.calls], ["/open/upload/init"])
        self.assertNotIn(b"test movie content", self.calls[0].content)

    def test_second_verification_uses_inclusive_range(self):
        api = self.api([(200, {"state": True, "code": 0, "data": {
            "code": 701, "sign_check": "1-4", "sign_key": "key", "pick_code": "pick"}}),
            (200, {"state": True, "code": 0, "data": {"status": 2, "file_id": "12"}})])
        self.assertEqual(api.instant(self.path, SimpleNamespace(fileid="10"), "A.mkv", self.hashes), "12")
        expected = hashlib.sha1(self.path.read_bytes()[1:5]).hexdigest().upper().encode()
        self.assertIn(b"sign_val=" + expected, self.calls[1].content)

    def test_second_verification_rejects_invalid_range(self):
        for span in ("-1-10", "0-9999999", "9-3", "invalid"):
            api = self.api([(200, {"state": True, "code": 0,
                "data": {"code": 700, "sign_check": span}})])
            with self.assertRaises(remote.PauseTask):
                api.instant(self.path, SimpleNamespace(fileid="10"), "A.mkv", self.hashes)

    def test_expired_auth_pauses_without_exposing_token(self):
        api = self.api([(401, {"token": "secret-token"})])
        with self.assertRaises(remote.PauseTask) as exc:
            api.raw_id("12")
        self.assertNotIn("secret-token", str(exc.exception))

    def test_rate_limit_does_not_modify_native_cooldown(self):
        self.backend._limit_until = 0
        api = self.api([(429, {})])
        with self.assertRaises(remote.RetryLater):
            api.raw_id("12")
        self.assertEqual(self.backend._limit_until, 0)
        self.assertEqual(len(self.calls), 1)

    def test_network_exception_is_sanitized(self):
        api = self.api([httpx.ConnectError("request secret-token")])
        with self.assertRaises(remote.RetryLater) as exc:
            api.raw_id("12")
        self.assertNotIn("secret-token", str(exc.exception))

    def test_size_and_hash_mismatch_cannot_be_success(self):
        for fields in ({"size_byte": 1}, {"size_byte": self.path.stat().st_size, "sha1": "wrong"}):
            api = self.api([(200, {"state": True, "code": 0, "data":
                {"file_id": "12", "file_category": "1", **fields}})])
            with self.assertRaises(remote.PauseTask):
                api.verify("12", "/A.mkv", self.hashes)

    def test_metadata_delay_is_retryable(self):
        api = self.api([(200, {"code": 430004, "state": False})])
        with self.assertRaises(remote.RetryLater):
            api.verify("12", "/A.mkv", self.hashes)

    def test_get_info_accepts_single_object_and_array(self):
        info = {"file_id": "12", "file_category": "1", "size_byte": self.path.stat().st_size,
                "sha1": self.hashes["sha1"]}
        for data in (info, [info]):
            api = self.api([(200, {"state": True, "code": 0, "data": data}),
                            (200, {"state": True, "code": 0, "data": data})])
            self.assertEqual(api.raw_path("/A.mkv")["file_id"], "12")
            self.assertEqual(api.verify("12", "/A.mkv", self.hashes).fileid, "12")

    def test_get_info_rejects_ambiguous_or_wrong_id(self):
        api = self.api([(200, {"state": True, "code": 0, "data": [{"file_id": "12"}, {"file_id": "13"}]})])
        with self.assertRaises(remote.RetryLater):
            api.raw_path("/A.mkv")
        api = self.api([(200, {"state": True, "code": 0, "data": {"file_id": "13"}})])
        with self.assertRaises(remote.PauseTask):
            api.raw_id("12")

    def test_file_changed_while_hashing_is_paused(self):
        with patch.object(remote, "fingerprint", side_effect=[{"size": 1}, {"size": 2}]):
            with self.assertRaises(remote.PauseTask):
                remote.hash_file(self.path, NoSleep())

    def test_stop_interrupts_hashing(self):
        stop = threading.Event()
        stop.set()
        with self.assertRaises(remote.RetryLater):
            remote.hash_file(self.path, stop)

    def upload_api(self):
        return self.api([(200, {"state": True, "data": {
            "endpoint": "http://oss.example", "AccessKeyId": "id", "AccessKeySecret": "secret",
            "SecurityToken": "token"}}), (200, {"state": True, "data": {
                "callback": {"callback": "callback-url", "callback_var": "vars"}}})])

    def test_manual_upload_checkpoints_parts_and_uses_tls_without_persisting_credentials(self):
        import oss2
        from unittest.mock import Mock
        api = self.upload_api()
        bucket = Mock()
        bucket.init_multipart_upload.return_value.upload_id = "upload-1"
        chunks = []
        def upload_part(*args, **kwargs):
            chunks.append(kwargs["data"].read())
            return SimpleNamespace(etag="receipt")
        bucket.upload_part.side_effect = upload_part
        bucket.complete_multipart_upload.return_value = SimpleNamespace(status=200,
            resp=SimpleNamespace(response=SimpleNamespace(json=lambda: {"state": True})))
        payload, snapshots = {}, []
        with patch.object(oss2, "Bucket", return_value=bucket) as factory:
            api.upload(self.path, SimpleNamespace(fileid="10"), self.hashes,
                {"bucket": "bucket", "object": "object", "pick_code": "pick"}, payload,
                lambda: snapshots.append(copy.deepcopy(payload)))
        self.assertEqual(b"".join(chunks), self.path.read_bytes())
        self.assertEqual(factory.call_args.args[1], "https://oss.example")
        self.assertTrue(payload["upload_confirmed"])
        self.assertTrue(snapshots[-2]["upload_session"]["completing"])
        self.assertNotIn("secret", json.dumps(payload))
        self.assertNotIn("callback-url", json.dumps(payload))

    def test_manual_retry_resumes_same_session_after_lost_complete_response(self):
        import oss2
        from unittest.mock import Mock
        bucket = Mock()
        bucket.init_multipart_upload.return_value.upload_id = "upload-1"
        bucket.upload_part.return_value.etag = "receipt"
        bucket.complete_multipart_upload.side_effect = RuntimeError("signed-url-secret")
        payload = {}
        with patch.object(oss2, "Bucket", return_value=bucket):
            with self.assertRaises(remote.RetryLater) as exc:
                self.upload_api().upload(self.path, SimpleNamespace(fileid="10"), self.hashes,
                    {"bucket": "bucket", "object": "object", "pick_code": "pick"}, payload, lambda: None)
            self.assertNotIn("signed-url-secret", str(exc.exception))
            bucket.complete_multipart_upload.side_effect = None
            bucket.complete_multipart_upload.return_value = SimpleNamespace(status=200,
                resp=SimpleNamespace(response=SimpleNamespace(json=lambda: {"state": True})))
            self.upload_api().upload(self.path, SimpleNamespace(fileid="10"), self.hashes,
                                     None, payload, lambda: None)
        self.assertEqual(bucket.init_multipart_upload.call_count, 1)
        self.assertEqual(bucket.upload_part.call_count, 1)
        self.assertEqual(bucket.complete_multipart_upload.call_count, 2)
        self.assertEqual(bucket.complete_multipart_upload.call_args.args[1], "upload-1")

    def test_source_change_during_upload_never_commits(self):
        import oss2
        from unittest.mock import Mock
        bucket = Mock()
        bucket.init_multipart_upload.return_value.upload_id = "upload-1"
        def mutate(*args, **kwargs):
            self.path.write_bytes(b"changed file contents")
            return SimpleNamespace(etag="receipt")
        bucket.upload_part.side_effect = mutate
        with patch.object(oss2, "Bucket", return_value=bucket):
            with self.assertRaises(remote.PauseTask):
                self.upload_api().upload(self.path, SimpleNamespace(fileid="10"), self.hashes,
                    {"bucket": "bucket", "object": "object", "pick_code": "pick"}, {}, lambda: None)
        bucket.complete_multipart_upload.assert_not_called()

    def test_retry_after_part_failure_continues_from_saved_offset(self):
        import oss2
        from unittest.mock import Mock
        bucket = Mock()
        bucket.init_multipart_upload.return_value.upload_id = "upload-1"
        attempts, received = [], {}
        failed = False
        def upload_part(key, upload_id, number, data):
            nonlocal failed
            attempts.append(number)
            block = data.read()
            if number == 2 and not failed:
                failed = True
                raise RuntimeError("connection lost")
            received[number] = block
            return SimpleNamespace(etag=f"receipt-{number}")
        bucket.upload_part.side_effect = upload_part
        bucket.complete_multipart_upload.return_value = SimpleNamespace(status=200,
            resp=SimpleNamespace(response=SimpleNamespace(json=lambda: {"state": True})))
        payload = {}
        with patch.object(oss2, "Bucket", return_value=bucket), patch.object(oss2, "determine_part_size", return_value=8):
            with self.assertRaises(remote.RetryLater):
                self.upload_api().upload(self.path, SimpleNamespace(fileid="10"), self.hashes,
                    {"bucket": "bucket", "object": "object", "pick_code": "pick"}, payload, lambda: None)
            self.assertEqual(len(payload["upload_session"]["parts"]), 1)
            self.upload_api().upload(self.path, SimpleNamespace(fileid="10"), self.hashes,
                                     None, payload, lambda: None)
        self.assertEqual(attempts, [1, 2, 2, 3])
        self.assertEqual(b"".join(received.values()), self.path.read_bytes())
        bucket.init_multipart_upload.assert_called_once()

    def test_stop_interrupts_upload_body(self):
        import io
        stop = threading.Event()
        reader = remote.StoppableReader(io.BytesIO(b"data"), stop)
        stop.set()
        with self.assertRaises(remote.RetryLater):
            reader.read()


if __name__ == "__main__":
    unittest.main()
