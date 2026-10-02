import hashlib
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


if __name__ == "__main__":
    unittest.main()
