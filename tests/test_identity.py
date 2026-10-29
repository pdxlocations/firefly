import importlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from database import Database
import mesh_runtime as mesh
from runtime_files import read_identity_payload, session_secret, database_connection


class IdentityMigrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.env = patch.dict(os.environ, {
            'FIREFLY_RUNTIME_DIR': str(self.root / 'runtime'),
            'FIREFLY_DATABASE_FILE': str(self.root / 'old.db'),
        })
        self.env.start()
        self.db = Database(str(self.root / 'old.db'))
        self.db.create_user('u', 'user', 'unused')

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def profile(self, name='p', node_id='!deadbeef'):
        self.assertTrue(self.db.create_profile(name, 'u', node_id, 'Node', 'NODE', [{'name': 'LongFast', 'key': 'AQ=='}], 0))
        return self.db.get_profile(name)

    def migrate(self):
        mesh.migrate_profile_identities(self.db, '239.0.0.69', 4403)

    def test_old_database_without_keys_and_repeat_startup(self):
        self.profile()
        with database_connection(self.db.db_path) as con:
            con.execute("INSERT INTO messages (message_id, sender_num, content, channel) VALUES ('old', 3735928559, 'history', 1)")
        self.migrate()
        profile = self.db.get_profile('p')
        path = mesh._config_path(profile)
        first = json.loads(path.read_text())
        self.assertNotIn('node_id', first)
        self.assertEqual(profile['node_id'], read_identity_payload(path)[1])
        self.assertEqual(profile['hop_limit'], 0)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.migrate()
        self.assertEqual(self.db.get_profile('p')['node_id'], profile['node_id'])
        self.assertEqual(json.loads(path.read_text())['security'], first['security'])
        self.assertEqual(len(list((self.root/'runtime'/'backups').iterdir())), 1)
        with database_connection(self.db.db_path) as con:
            self.assertEqual(con.execute("SELECT sender_num FROM messages").fetchone()[0], 3735928559)
        node = mesh.FireflyVirtualNode(path)
        self.assertEqual(node.node_num, mesh.owner_node_num(profile))
        mesh.MeshNodeStore(profile).ensure_owner_node()

    def test_existing_key_preserved(self):
        p = self.profile()
        mesh.prepare_profile_identity(p)
        path = mesh._config_path(p)
        key = json.loads(path.read_text())['security']['private_key']
        self.migrate()
        self.assertEqual(json.loads(path.read_text())['security']['private_key'], key)

    def test_missing_and_corrupt_keys_fail_closed(self):
        self.profile()
        self.migrate()
        path = mesh._config_path(self.db.get_profile('p'))
        saved = path.read_text()
        for bad in ('{', json.dumps({'security': {'private_key': 'bad'}}), json.dumps({'security': {}})):
            path.write_text(bad)
            with self.assertRaises((ValueError, TypeError)):
                self.migrate()
            self.assertEqual(path.read_text(), bad)
        path.unlink()
        with self.assertRaises(ValueError):
            self.migrate()
        path.write_text(saved)
        self.migrate()

    def test_collision_changes_no_database_rows(self):
        a = self.profile()
        b = self.profile('q', '!12345678')
        mesh.prepare_profile_identity(a)
        path_b = mesh._config_path(b)
        path_b.write_text(mesh._config_path(a).read_text())
        with self.assertRaisesRegex(ValueError, 'collision'):
            self.migrate()
        self.assertEqual(self.db.get_profile('p')['node_id'], '!deadbeef')
        self.assertEqual(self.db.get_profile('q')['node_id'], '!12345678')

    def test_two_profiles_exchange_signed_channel_dm_and_ack(self):
        from meshtastic.protobuf import mesh_pb2
        from mudp.singleton import conn
        import vnode.runtime as vnode_runtime
        import meshdb
        self.profile()
        self.profile('q', '!12345678')
        self.migrate()
        a, b = self.db.get_profile('p'), self.db.get_profile('q')
        mesh.MeshNodeStore(a).ensure_owner_node()
        mesh.MeshNodeStore(b).ensure_owner_node()
        node_a = mesh.FireflyVirtualNode(mesh._config_path(a))
        manager = mesh.VirtualNodeManager('239.0.0.69', 4403)
        public_a = manager.get_profile_public_key(a)
        with patch.object(node_a, 'connect_send_socket'), patch.object(conn, 'sendto') as send, patch.object(conn, 'socket', object()), patch.object(vnode_runtime, 'register_pending_ack'), patch.object(mesh, 'register_pending_ack'):
            node_a.send_text(mesh.BROADCAST_NODE_NUM, 'signed hello')
            packet = mesh_pb2.MeshPacket.FromString(send.call_args.args[0])
            self.assertEqual(getattr(packet, 'from'), mesh.owner_node_num(a))
            decoded = manager.decode_packet_for_profile(b, packet)
            self.assertEqual(decoded.decoded.payload, b'signed hello')
            normalized = meshdb.normalize_packet(decoded, 'udp')
            self.assertTrue(normalized['decoded']['xeddsaSignature'])
            self.assertFalse(normalized['xeddsaSigned'])
            mesh.MeshNodeStore(b).record_packet(decoded)
            node_a.send_text(mesh.BROADCAST_NODE_NUM, 'x' * 200)
            large = mesh_pb2.MeshPacket.FromString(send.call_args.args[0])
            decoded_large = manager.decode_packet_for_profile(b, large)
            self.assertNotIn('xeddsaSignature', meshdb.normalize_packet(decoded_large, 'udp')['decoded'])
            manager.virtual_node = node_a
            send.reset_mock()
            manager.send_text('private hello', destination=mesh.owner_node_num(b))
            self.assertEqual(send.call_count, 1)
            dm = mesh_pb2.MeshPacket.FromString(send.call_args.args[0])
            self.assertEqual(dm.channel, 0)
            self.assertTrue(dm.want_ack)
            self.assertTrue(dm.pki_encrypted)
            plain = manager.decode_packet_for_profile(b, dm, sender_public_key=public_a)
            self.assertIsNotNone(plain)
            self.assertEqual(plain.decoded.payload, b'private hello')
            manager.send_ack_for_profile(b, plain)
            ack = mesh_pb2.MeshPacket.FromString(send.call_args.args[0])
            self.assertEqual(getattr(ack, 'from'), mesh.owner_node_num(b))
            self.assertEqual(ack.to, mesh.owner_node_num(a))
            # A missing recipient key must fail instead of sending text with
            # channel encryption, which vnode's default auto mode permits.
            send.reset_mock()
            with self.assertRaisesRegex(ValueError, 'stored public key'):
                manager.send_text('must stay private', destination=0x12345678)
            send.assert_not_called()

    def test_session_secret_persists(self):
        with patch.dict(os.environ, {'FIREFLY_SECRET_KEY': 'change-me'}):
            self.assertEqual(session_secret(self.root), session_secret(self.root))

    def test_dm_uses_nodeinfo_key_from_another_configured_channel(self):
        from meshtastic.protobuf import mesh_pb2, portnums_pb2
        from mudp.singleton import conn
        from vnode.crypto import generate_keypair, decrypt_dm
        self.profile()
        self.migrate()
        profile = self.db.get_profile('p')
        profile['channels'].append({'name': 'Other', 'key': 'Ag=='})
        remote_num = 0x4e016635
        public_key, private_key = generate_keypair()
        # Receive the exchange reply on LongFast, then send from Other.
        reply = mesh_pb2.MeshPacket()
        setattr(reply, 'from', remote_num)
        reply.to = mesh.owner_node_num(profile)
        reply.channel = mesh.generate_hash('LongFast', 'AQ==')
        reply.decoded.portnum = portnums_pb2.PortNum.NODEINFO_APP
        reply.decoded.payload = mesh_pb2.User(
            id=mesh.node_id_from_num(remote_num), public_key=public_key,
        ).SerializeToString()
        mesh.MeshNodeStore(profile).record_packet(reply)
        profile.update(channel='Other', key='Ag==')
        config_path = mesh.ensure_profile_config(profile, '239.0.0.69', 4403)
        node = mesh.FireflyVirtualNode(config_path, profile=profile)
        self.assertEqual(node._lookup_public_key(remote_num), public_key)
        manager = mesh.VirtualNodeManager('239.0.0.69', 4403)
        manager.virtual_node = node
        with patch.object(node, 'connect_send_socket'), patch.object(conn, 'sendto') as send:
            manager.send_text('cross-channel key', destination=remote_num)
            self.assertEqual(send.call_count, 1)
            packet = mesh_pb2.MeshPacket.FromString(send.call_args.args[0])
            self.assertTrue(packet.pki_encrypted)
            plain = decrypt_dm(
                receiver_private_key=private_key,
                sender_public_key=manager.get_profile_public_key(profile),
                packet_id=packet.id, from_node=node.node_num, payload=packet.encrypted,
            )
            self.assertEqual(mesh_pb2.Data.FromString(plain).payload, b'cross-channel key')
        # Removing LongFast from the profile must remove access to that key.
        profile['channels'] = [profile['channels'][1]]
        restricted_node = mesh.FireflyVirtualNode(config_path, profile=profile)
        self.assertIsNone(restricted_node._lookup_public_key(remote_num))


class ProfileApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name)
        cls.env = patch.dict(os.environ, {'FIREFLY_RUNTIME_DIR': str(root/'runtime'), 'FIREFLY_DATABASE_FILE': str(root/'app.db')})
        cls.env.start()
        with patch.object(mesh.SharedPacketReceiver, 'start'):
            cls.module = importlib.import_module('app')
        cls.module.app.config.update(TESTING=True)
        cls.module.db.create_user('api-user', 'api-user', 'unused')

    @classmethod
    def tearDownClass(cls):
        cls.env.stop()
        cls.tmp.cleanup()

    def setUp(self):
        self.client = self.module.app.test_client()
        with self.client.session_transaction() as session:
            session['user_id'] = 'api-user'
            session['csrf_token'] = 'test-token'
        self.headers = {'X-CSRF-Token': 'test-token'}
        self.payload = {'long_name': 'Test Node', 'short_name': 'TEST', 'channels': [{'name': 'LongFast', 'key': 'AQ=='}], 'hop_limit': 0}

    def test_creation_edit_identity_and_session_refresh(self):
        result = self.client.post('/api/profiles', json=self.payload, headers=self.headers)
        self.assertEqual(result.status_code, 200, result.json)
        pid = result.json['profile_id']
        profile = self.module.db.get_profile(pid)
        key = json.loads(mesh._config_path(profile).read_text())['security']
        self.assertEqual(profile['node_id'], result.json['node_id'])
        self.assertEqual(profile['hop_limit'], 0)
        with self.client.session_transaction() as session:
            session['current_profile'] = dict(profile, node_id='!deadbeef')
        changed = dict(self.payload, long_name='Renamed')
        response = self.client.put('/api/profiles/'+pid, json=changed, headers=self.headers)
        self.assertEqual(response.status_code, 200, response.json)
        profile = self.module.db.get_profile(pid)
        self.assertEqual(json.loads(mesh._config_path(profile).read_text())['security'], key)
        self.assertEqual(profile['node_id'], result.json['node_id'])
        with self.client.session_transaction() as session:
            self.assertEqual(session['current_profile']['node_id'], profile['node_id'])
        self.assertEqual(self.client.put('/api/profiles/'+pid, json=dict(changed, node_id='!12345678'), headers=self.headers).status_code, 400)
        self.assertEqual(self.client.put('/api/profiles/'+pid, json=dict(changed, channels=[{'name': 'Bad', 'key': 'invalid'}]), headers=self.headers).status_code, 400)
        self.assertFalse(self.module.db.update_profile(pid, 'api-user', '!12345678', 'Bad', 'BAD', self.payload['channels']))
        mesh.migrate_profile_identities(self.module.db, '239.0.0.69', 4403)

    def test_registration_form_includes_working_csrf_token(self):
        import re
        client = self.module.app.test_client()
        page = client.get('/')
        token = re.search(r'name="csrf-token" content="([^"]+)"', page.text).group(1)
        result = client.post('/register', data={'username': 'form-user', 'password': 'test-password', 'csrf_token': token})
        self.assertEqual(result.status_code, 302)
        created = client.post('/api/profiles', json=self.payload, headers={'X-CSRF-Token': token})
        self.assertEqual(created.status_code, 200, created.json)

    def test_invalid_overrides_and_csrf(self):
        self.assertEqual(self.client.post('/api/profiles', json=self.payload).status_code, 403)
        self.assertEqual(self.client.post('/api/profiles', json=dict(self.payload, node_id='!deadbeef'), headers=self.headers).status_code, 400)
        self.assertEqual(self.client.post('/api/profiles', json=['bad'], headers=self.headers).status_code, 400)
        page = self.client.get('/')
        self.assertEqual(page.status_code, 200)
        self.assertIn(b'csrf-token', page.data)
        self.assertNotIn(b'function createRandomNodeId', page.data)


if __name__ == '__main__':
    unittest.main()
