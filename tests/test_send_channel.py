"""Channel routing regressions; no radio traffic or production database access."""

import importlib
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class SendChannelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp_dir.cleanup)
        with patch.dict(os.environ, {
            "FIREFLY_DATABASE_FILE": str(Path(cls.temp_dir.name) / "test.db"),
            "FIREFLY_RUNTIME_DIR": str(Path(cls.temp_dir.name) / "runtime"),
        }), patch("mesh_runtime.SharedPacketReceiver.start"):
            cls.module = importlib.import_module("app")

    def setUp(self):
        self.profile = {
            "id": "test-profile",
            "node_id": "!12345678",
            "long_name": "Test",
            "short_name": "TEST",
            "channels": [
                {"name": "MeshOregon", "key": "AQ=="},
                {"name": "local", "key": "Ag=="},
            ],
        }
        self.server = self.module.UDPChatServer()
        self.transport_channel = None

        def prepare_transport(profile):
            self.transport_channel = self.module._profile_channel_num(profile)
            return object()

        self.interface = self.enter_patch("create_interface_for_profile", side_effect=prepare_transport)
        self.manager = self.enter_patch("virtual_node_manager")
        self.database = self.enter_patch("db")
        self.enter_patch("socketio")
        self.enter_patch("_emit_message_to_profile_rooms")
        self.server.start(self.module._effective_profile(self.profile, channel_index=0))
        self.interface.reset_mock()

    def enter_patch(self, name, **kwargs):
        patcher = patch.object(self.module, name, **kwargs)
        result = patcher.start()
        self.addCleanup(patcher.stop)
        return result

    def test_send_uses_explicit_channel_despite_stale_session(self):
        self.enter_patch("udp_server", new=self.server)
        self.enter_patch("_get_session_profile", return_value=self.profile)
        transmitted_channels = []
        self.manager.send_text.side_effect = lambda *a, **kw: transmitted_channels.append(self.transport_channel) or 123
        for index in (1, 0):
            with self.module.app.test_request_context("/api/send-message", method="POST", json={
                "message": "channel routing test", "channel_index": index,
            }):
                # The saved selection still points at the first channel.
                self.module.session["current_channel_index"] = 0
                response = self.module.send_message.__wrapped__()
                self.assertEqual(response.status_code, 200)
            expected = self.module._profile_channel_num(
                self.module._effective_profile(self.profile, channel_index=index)
            )
            self.assertEqual(transmitted_channels[-1], expected)
            self.assertEqual(self.database.store_message.call_args.kwargs["channel"], expected)
        self.assertEqual(self.interface.call_count, 2)

    def test_same_channel_reuses_transport(self):
        self.server._ensure_sender_profile_transport(self.profile)
        self.interface.assert_not_called()

    def test_key_change_rebuilds_transport(self):
        self.profile["channels"][0]["key"] = "Aw=="
        self.server._ensure_sender_profile_transport(self.profile)
        self.interface.assert_called_once()

    def test_failed_channel_switch_does_not_transmit(self):
        selected = self.module._effective_profile(self.profile, channel_index=1)
        self.server.restart_with_profile = Mock(return_value=False)
        self.assertFalse(self.server.send_message("test", selected))
        self.manager.send_text.assert_not_called()
        self.database.store_message.assert_not_called()

    def test_invalid_explicit_channel_is_rejected(self):
        sender = self.enter_patch("udp_server")
        self.enter_patch("_get_session_profile", return_value=self.profile)
        for index in (-1, 2, "1", 1.5, True, None):
            with self.subTest(index=index), self.module.app.test_request_context(
                "/api/send-message", method="POST", json={"message": "test", "channel_index": index}
            ):
                _, status = self.module.send_message.__wrapped__()
                self.assertEqual(status, 400)
        sender.send_message.assert_not_called()


if __name__ == "__main__":
    unittest.main()
