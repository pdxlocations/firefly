#!/usr/bin/env python3

import json
import math
import os
import random
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from local_deps import bootstrap_local_dependency, ensure_dependency_version

bootstrap_local_dependency("meshdb")
bootstrap_local_dependency("mudp")
bootstrap_local_dependency("vnode")
ensure_dependency_version("meshdb", "0.2.0")
ensure_dependency_version("mudp", "1.5.7")
ensure_dependency_version("vnode", "0.1.10")

import meshdb
from meshtastic import mesh_pb2, portnums_pb2
from mudp import UDPPacketStream
from mudp.encryption import decrypt_packet, encrypt_packet
from mudp.reliability import build_routing_ack_data, compute_reply_hop_limit, register_pending_ack
from mudp.singleton import conn
from pubsub import pub
from vnode import VirtualNode, parse_node_id, resolve_hw_model
from vnode.crypto import b64_decode, decrypt_dm, derive_public_key
from encryption import generate_hash
from firefly_logging import configure_logging, get_logger, make_log_print

configure_logging()
logger = get_logger("firefly.mesh")
print = make_log_print(logger)


BROADCAST_NODE_NUM = 0xFFFFFFFF


def _normalized_profile_node_id(profile: Dict) -> str:
    node_id = (profile or {}).get("node_id")
    if not isinstance(node_id, str):
        raise ValueError(f"invalid node_id {node_id!r}")

    normalized = node_id.strip().lower()
    if len(normalized) != 9 or not normalized.startswith("!"):
        raise ValueError(f"invalid node_id {node_id!r}")

    int(normalized[1:], 16)
    return normalized


def _runtime_root() -> Path:
    runtime_dir = os.getenv("FIREFLY_RUNTIME_DIR")
    if runtime_dir:
        root = Path(runtime_dir).expanduser()
    else:
        db_path = Path(os.getenv("FIREFLY_DATABASE_FILE", "firefly.db")).expanduser()
        db_parent = db_path.parent if db_path.parent != Path("") else Path.cwd()
        root = db_parent / "firefly_runtime"
    root.mkdir(parents=True, exist_ok=True)
    return root.resolve()


def _profile_root(profile: Dict) -> Path:
    root = _runtime_root() / "profiles" / str(profile["id"])
    root.mkdir(parents=True, exist_ok=True)
    return root


def _config_path(profile: Dict) -> Path:
    return _profile_root(profile) / "node.json"


def _meshdb_root(profile: Optional[Dict] = None) -> Path:
    root = _runtime_root() / "meshdb"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _legacy_meshdb_root(profile: Dict) -> Path:
    root = _profile_root(profile) / "meshdb"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _firefly_db_path() -> Path:
    return Path(os.getenv("FIREFLY_DATABASE_FILE", "firefly.db")).expanduser()


def owner_node_num(profile: Dict) -> int:
    normalized_node_id = _normalized_profile_node_id(profile)
    return int(parse_node_id(normalized_node_id))


def node_id_from_num(node_num: int) -> str:
    return f"!{int(node_num):08x}"


def _profile_channel_hashes(profile: Dict) -> List[int]:
    channels = profile.get("channels")
    if isinstance(channels, list) and channels:
        hashes = []
        for channel in channels:
            if not isinstance(channel, dict):
                continue
            channel_name = (channel.get("name") or "").strip()
            channel_key = (channel.get("key") or "").strip()
            if not channel_name or not channel_key:
                continue
            hashes.append(generate_hash(channel_name, channel_key))
        if hashes:
            return hashes

    channel_name = (profile.get("channel") or "").strip()
    channel_key = (profile.get("key") or "").strip()
    if channel_name and channel_key:
        return [generate_hash(channel_name, channel_key)]
    return []


def _profile_channel_map(profile: Dict) -> Dict[int, str]:
    mapping: Dict[int, str] = {}
    channels = profile.get("channels")
    if isinstance(channels, list) and channels:
        for channel in channels:
            if not isinstance(channel, dict):
                continue
            channel_name = (channel.get("name") or "").strip()
            channel_key = (channel.get("key") or "").strip()
            if not channel_name or not channel_key:
                continue
            try:
                mapping[generate_hash(channel_name, channel_key)] = channel_name
            except Exception:
                continue
    else:
        channel_name = (profile.get("channel") or "").strip()
        channel_key = (profile.get("key") or "").strip()
        if channel_name and channel_key:
            mapping[generate_hash(channel_name, channel_key)] = channel_name
    return mapping


def _meshdb_storage_name(channel_name: Optional[str]) -> Optional[str]:
    if not isinstance(channel_name, str):
        return None
    slug = "".join(ch for ch in channel_name.strip().lower() if ch.isalnum())
    return slug or None


def _resolve_profile_channel_for_packet(
    profile: Dict, request_packet: Optional[mesh_pb2.MeshPacket] = None
) -> tuple[Optional[str], Optional[str]]:
    requested_channel = None
    if request_packet is not None:
        try:
            requested_channel = int(getattr(request_packet, "channel", 0) or 0)
        except Exception:
            requested_channel = None

    channels = profile.get("channels")
    if isinstance(channels, list):
        fallback = None
        for channel in channels:
            if not isinstance(channel, dict):
                continue
            channel_name = (channel.get("name") or "").strip()
            channel_key = (channel.get("key") or "").strip()
            if not channel_name or not channel_key:
                continue
            if fallback is None:
                fallback = (channel_name, channel_key)
            if requested_channel is None:
                continue
            try:
                if generate_hash(channel_name, channel_key) == requested_channel:
                    return channel_name, channel_key
            except Exception:
                continue
        if fallback is not None:
            return fallback

    channel_name = (profile.get("channel") or "").strip()
    channel_key = (profile.get("key") or "").strip()
    if channel_name and channel_key:
        return channel_name, channel_key
    return None, None


def ensure_profile_config(profile: Dict, mcast_group: str, mcast_port: int) -> Path:
    config_path = _config_path(profile)
    meshdb_root = _meshdb_root(profile)

    existing_security = {}
    if config_path.exists():
        try:
            existing_payload = json.loads(config_path.read_text(encoding="utf-8"))
            existing_security = dict(existing_payload.get("security", {}))
        except Exception:
            existing_security = {}

    payload = {
        "node_id": profile["node_id"],
        "long_name": profile["long_name"],
        "short_name": profile["short_name"],
        "hw_model": "PRIVATE_HW",
        "role": "CLIENT",
        "is_licensed": False,
        "hop_limit": int(profile.get("hop_limit", 3) or 3),
        "broadcasts": {
            "send_startup_nodeinfo": False,
            "nodeinfo_interval_seconds": 900,
        },
        "position": {
            "enabled": False,
            "latitude": None,
            "longitude": None,
            "altitude": None,
            "position_interval_seconds": 900,
        },
        "channel": {
            "name": profile["channel"],
            "psk": profile["key"],
        },
        "udp": {
            "mcast_group": mcast_group,
            "mcast_port": int(mcast_port),
        },
        "meshdb": {
            "path": str(meshdb_root),
        },
        "security": {
            "private_key": existing_security.get("private_key", ""),
        },
    }

    config_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return config_path


class VirtualNodeManager:
    def __init__(self, mcast_group: str, mcast_port: int, shared_receiver=None):
        self.mcast_group = mcast_group
        self.mcast_port = int(mcast_port)
        self.virtual_node: Optional[VirtualNode] = None
        self.current_profile_id: Optional[str] = None
        self.shared_receiver = shared_receiver

    def start(self, profile: Dict) -> VirtualNode:
        self.stop()
        config_path = ensure_profile_config(profile, self.mcast_group, self.mcast_port)
        vnode = FireflyVirtualNode(config_path, shared_receiver=self.shared_receiver)
        if self.shared_receiver is None or not self.shared_receiver.running:
            print("[VNODE] Starting full virtual node runtime")
            vnode.start()
        else:
            print("[VNODE] Shared receiver active; preparing send-only virtual node identity")
        self.virtual_node = vnode
        self.current_profile_id = str(profile["id"])
        return vnode

    def stop(self) -> None:
        if self.virtual_node is not None:
            self.virtual_node.stop()
        self.virtual_node = None
        self.current_profile_id = None

    @property
    def running(self) -> bool:
        return self.virtual_node is not None

    def send_text(
        self,
        message: str,
        destination: int = BROADCAST_NODE_NUM,
        hop_limit: Optional[int] = None,
        reply_id: Optional[int] = None,
    ) -> int:
        if self.virtual_node is None:
            raise RuntimeError("Virtual node is not running")
        return self.virtual_node.send_text(int(destination), message, hop_limit=hop_limit, reply_id=reply_id)

    def send_nodeinfo(self, destination: int = BROADCAST_NODE_NUM, *, want_response: bool = False) -> int:
        if self.virtual_node is None:
            raise RuntimeError("Virtual node is not running")
        return self.virtual_node.send_nodeinfo(int(destination), want_response=want_response)

    def send_traceroute(self, destination: int, hop_limit: Optional[int] = None) -> int:
        if self.virtual_node is None:
            raise RuntimeError("Virtual node is not running")
        route_discovery = mesh_pb2.RouteDiscovery()
        packet = self.virtual_node.sendData(
            route_discovery,
            destinationId=int(destination),
            portNum=portnums_pb2.PortNum.TRACEROUTE_APP,
            wantResponse=True,
            hopLimit=hop_limit,
        )
        return int(packet.id)

    def get_profile_public_key(self, profile: Dict) -> Optional[bytes]:
        private_key_b64 = self._get_profile_private_key(profile)
        if not private_key_b64:
            return None
        try:
            return derive_public_key(b64_decode(private_key_b64))
        except Exception:
            return None

    def decode_packet_for_profile(
        self,
        profile: Dict,
        packet: mesh_pb2.MeshPacket,
        *,
        sender_public_key: Optional[bytes] = None,
    ) -> Optional[mesh_pb2.MeshPacket]:
        decoded_packet = mesh_pb2.MeshPacket()
        decoded_packet.CopyFrom(packet)
        if decoded_packet.HasField("decoded"):
            return decoded_packet

        if int(getattr(decoded_packet, "channel", 0) or 0) == 0:
            private_key_b64 = self._get_profile_private_key(profile)
            if not private_key_b64 or sender_public_key is None:
                return None
            try:
                plaintext = decrypt_dm(
                    receiver_private_key=b64_decode(private_key_b64),
                    sender_public_key=sender_public_key,
                    packet_id=int(getattr(decoded_packet, "id", 0) or 0),
                    from_node=int(getattr(decoded_packet, "from", 0) or 0),
                    payload=bytes(decoded_packet.encrypted),
                )
                data = mesh_pb2.Data()
                data.ParseFromString(plaintext)
                decoded_packet.decoded.CopyFrom(data)
                decoded_packet.pki_encrypted = True
                decoded_packet.public_key = sender_public_key
                return decoded_packet
            except Exception:
                return None

        channel_name, channel_key = _resolve_profile_channel_for_packet(profile, decoded_packet)
        if not channel_name or not channel_key:
            return None
        try:
            data = decrypt_packet(decoded_packet, channel_key, silent=True)
        except Exception:
            data = None
        if data is None:
            return None
        decoded_packet.decoded.CopyFrom(data)
        return decoded_packet

    def send_ack_for_profile(self, profile: Dict, request_packet: mesh_pb2.MeshPacket) -> int:
        channel_name, channel_key = _resolve_profile_channel_for_packet(profile, request_packet)
        if not channel_name or not channel_key:
            raise ValueError("Profile is missing a usable channel configuration")

        profile_node_num = owner_node_num(profile)
        destination = int(getattr(request_packet, "from", 0) or 0)
        if destination <= 0:
            raise ValueError("Request packet is missing a valid sender")

        ack_data = build_routing_ack_data(
            request_id=int(getattr(request_packet, "id", 0) or 0),
            error_reason=mesh_pb2.Routing.Error.NONE,
        )

        packet = mesh_pb2.MeshPacket()
        packet.id = random.getrandbits(32)
        setattr(packet, "from", profile_node_num)
        packet.to = destination
        packet.want_ack = bool(
            getattr(request_packet, "want_ack", False)
            and request_packet.HasField("decoded")
            and int(request_packet.decoded.portnum or 0) == int(portnums_pb2.PortNum.TEXT_MESSAGE_APP)
            and int(getattr(request_packet, "to", BROADCAST_NODE_NUM) or BROADCAST_NODE_NUM) == profile_node_num
        )
        packet.channel = generate_hash(channel_name, channel_key)

        hop_limit = int(compute_reply_hop_limit(request_packet))
        packet.hop_limit = hop_limit
        packet.hop_start = hop_limit
        packet.priority = mesh_pb2.MeshPacket.Priority.ACK
        packet.encrypted = encrypt_packet(channel_name, channel_key, packet, ack_data)

        if getattr(conn, "socket", None) is None:
            conn.setup_multicast(self.mcast_group, int(self.mcast_port))

        raw_packet = packet.SerializeToString()
        register_pending_ack(packet, raw_packet)
        conn.sendto(raw_packet, (conn.host, conn.port))
        return int(packet.id)

    def _get_profile_private_key(self, profile: Dict) -> Optional[str]:
        config_path = ensure_profile_config(profile, self.mcast_group, self.mcast_port)
        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
        except Exception:
            return None
        private_key = str(payload.get("security", {}).get("private_key", "")).strip()
        return private_key or None


class SharedPacketReceiver:
    def __init__(self, mcast_group: str, mcast_port: int):
        self.mcast_group = mcast_group
        self.mcast_port = int(mcast_port)
        self.stream: Optional[UDPPacketStream] = None

    @property
    def running(self) -> bool:
        return self.stream is not None

    def start(self) -> None:
        if self.stream is not None:
            return
        stream = UDPPacketStream(
            self.mcast_group,
            self.mcast_port,
            key=None,
            parse_payload=False,
        )
        stream.start()
        self.stream = stream

    def stop(self) -> None:
        if self.stream is None:
            return
        self.stream.stop()
        self.stream = None


class FireflyVirtualNode(VirtualNode):
    def __init__(self, config_path, *, shared_receiver: Optional[SharedPacketReceiver] = None) -> None:
        super().__init__(config_path)
        self._shared_receiver = shared_receiver
        self._firefly_started = False

    def _channel_hash(self) -> int:
        return generate_hash(self.config.channel.name, self.config.channel.psk)

    def _channel_name(self) -> str:
        return str(self.config.channel.name or "").strip()

    def _storage_name(self) -> Optional[str]:
        return _meshdb_storage_name(self._channel_name())

    def _storage_channel_for_packet(self, packet: mesh_pb2.MeshPacket) -> int:
        try:
            packet_channel = int(getattr(packet, "channel", 0) or 0)
        except Exception:
            packet_channel = 0
        return packet_channel if packet_channel > 0 else self._channel_hash()

    def _profile_record_for_node_num(self, node_num: int) -> Optional[Dict]:
        if node_num in (None, 0):
            return None

        db_path = _firefly_db_path()
        if not db_path.exists():
            return None

        normalized_node_id = node_id_from_num(int(node_num))
        try:
            with sqlite3.connect(db_path) as con:
                con.row_factory = sqlite3.Row
                row = con.execute(
                    """
                    SELECT id, user_id, node_id, long_name, short_name, channel, key, hop_limit
                    FROM profiles
                    WHERE LOWER(node_id) = ?
                    LIMIT 1
                    """,
                    (normalized_node_id.lower(),),
                ).fetchone()
        except Exception:
            return None

        return dict(row) if row else None

    def _local_profile_public_key(self, node_num: int) -> Optional[bytes]:
        profile = self._profile_record_for_node_num(node_num)
        if not profile:
            return None

        config_path = _config_path(profile)
        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
        except Exception:
            return None

        private_key_b64 = str(payload.get("security", {}).get("private_key", "")).strip()
        if not private_key_b64:
            return None
        try:
            return derive_public_key(b64_decode(private_key_b64))
        except Exception:
            return None

    def _seed_owner_record(self) -> None:
        Path(self.meshdb_path).mkdir(parents=True, exist_ok=True)
        meshdb.set_default_db_path(self.meshdb_path)
        meshdb.NodeDB(
            self.node_num,
            self.meshdb_path,
            channel=self._channel_hash(),
            channel_name=self._channel_name(),
            storage_name=self._storage_name(),
        ).upsert(
            node_num=self.node_num,
            long_name=self.config.long_name,
            short_name=self.config.short_name,
            hw_model=str(resolve_hw_model(self.config.hw_model)),
            role=str(self.config.role),
            is_licensed=int(self.config.is_licensed),
            public_key=self._public_key_b64,
        )

    def _resolve_destination(self, destination):
        if isinstance(destination, int):
            return destination
        text = str(destination).strip()
        if text.startswith("!"):
            return parse_node_id(text)
        resolved = meshdb.get_node_num(
            text,
            owner_node_num=self.node_num,
            db_path=self.meshdb_path,
            channel=self._channel_hash(),
            channel_name=self._channel_name(),
            storage_name=self._storage_name(),
        )
        if isinstance(resolved, list):
            raise ValueError(f"Destination '{destination}' is ambiguous: {resolved}")
        if resolved is None:
            raise ValueError(f"Unknown destination '{destination}'")
        return int(resolved)

    def _lookup_public_key(self, node_num: int) -> Optional[bytes]:
        if int(node_num) == self.node_num:
            key = self._public_key_b64
            return b64_decode(key) if key else None

        row = meshdb.get_nodeinfo(
            int(node_num),
            owner_node_num=self.node_num,
            db_path=self.meshdb_path,
            channel=self._channel_hash(),
            channel_name=self._channel_name(),
            storage_name=self._storage_name(),
        )
        if isinstance(row, dict):
            public_key = str(row.get("public_key", "")).strip()
            if public_key:
                try:
                    return b64_decode(public_key)
                except Exception:
                    pass

        return self._local_profile_public_key(int(node_num))

    def _known_node_count(self) -> int:
        try:
            node_db = meshdb.NodeDB(
                self.node_num,
                self.meshdb_path,
                channel=self._channel_hash(),
                channel_name=self._channel_name(),
                storage_name=self._storage_name(),
            )
            node_db.ensure_table()
            with node_db.connect() as con:
                row = con.execute(f"SELECT COUNT(*) FROM {node_db.table}").fetchone()
            return max(int(row[0]) if row else 0, 1)
        except (sqlite3.Error, ValueError, TypeError):
            return max(len(self._last_nodeinfo_seen), 1)

    def _persist_packet(self, packet: mesh_pb2.MeshPacket) -> None:
        normalized = meshdb.normalize_packet(packet, "udp")
        meshdb.handle_packet(
            normalized,
            node_database_number=self.node_num,
            db_path=self.meshdb_path,
            channel=self._storage_channel_for_packet(packet),
            channel_name=self._channel_name(),
            storage_name=self._storage_name(),
        )

    def start(self) -> None:
        if self._firefly_started:
            return
        self._stop.clear()
        pub.subscribe(self._handle_raw_packet, self.RAW_PACKET_TOPIC)
        pub.subscribe(self._handle_unique_packet, self.PACKET_TOPIC)
        pub.subscribe(self._handle_compat_response_packet, self.RECEIVE_TOPIC)
        pub.subscribe(self._handle_compat_ack, self.ACK_TOPIC)
        pub.subscribe(self._handle_compat_nak, self.NAK_TOPIC)

        if self._shared_receiver is None or not self._shared_receiver.running:
            self.stream = UDPPacketStream(
                self.config.udp.mcast_group,
                int(self.config.udp.mcast_port),
                key=self.config.channel.psk,
                parse_payload=False,
            )
            self.stream.start()

        if self.config.broadcasts.send_startup_nodeinfo:
            self.send_nodeinfo()
        self._broadcast_thread = threading.Thread(
            target=self._broadcast_loop,
            name="vnode-nodeinfo-broadcast",
            daemon=True,
        )
        self._broadcast_thread.start()
        self._firefly_started = True

    def stop(self) -> None:
        if not self._firefly_started and self.stream is None:
            return
        super().stop()
        self._firefly_started = False


class MeshNodeStore:
    def __init__(self, profile: Dict):
        self.profile = profile
        self.profile_id = str(profile["id"])
        self.owner_node_num = owner_node_num(profile)
        self.db_path = str(_meshdb_root(profile))
        self.firefly_db_path = str(_firefly_db_path())
        self.channel_names = _profile_channel_map(profile)
        self.channel_nums = list(dict.fromkeys(_profile_channel_hashes(profile)))
        if not self.channel_nums:
            self.channel_nums = [generate_hash(profile["channel"], profile["key"])]
        self.channel_num = self.channel_nums[0]
        self._migrate_legacy_profile_nodes()

    def _iter_channel_nums(self) -> List[int]:
        return list(dict.fromkeys(self.channel_nums or [self.channel_num]))

    def _channel_scope(self, channel_num=None) -> int:
        normalized = self._coerce_int(channel_num)
        if normalized is not None and normalized > 0:
            return normalized
        return self.channel_num

    def _channel_name_for(self, channel_num=None) -> Optional[str]:
        return self.channel_names.get(self._channel_scope(channel_num))

    def _storage_name_for(self, channel_num=None) -> Optional[str]:
        return _meshdb_storage_name(self._channel_name_for(channel_num))

    def _node_db(self, channel_num=None):
        node_db = meshdb.NodeDB(
            self.owner_node_num,
            self.db_path,
            channel=self._channel_scope(channel_num),
            channel_name=self._channel_name_for(channel_num),
            storage_name=self._storage_name_for(channel_num),
        )
        node_db.ensure_table()
        return node_db

    def _location_db(self, channel_num=None):
        location_db = meshdb.LocationDB(
            self.owner_node_num,
            self.db_path,
            channel=self._channel_scope(channel_num),
            channel_name=self._channel_name_for(channel_num),
            storage_name=self._storage_name_for(channel_num),
        )
        location_db.ensure_table()
        return location_db

    def _legacy_migration_marker(self) -> Path:
        return Path(self.db_path) / f".legacy_nodes_profile_{self.profile_id}"

    def _migrate_legacy_profile_nodes(self) -> None:
        marker_path = self._legacy_migration_marker()
        if marker_path.exists():
            return

        try:
            if len(self._iter_channel_nums()) != 1:
                return

            legacy_root = _legacy_meshdb_root(self.profile)
            legacy_db_file = legacy_root / f"{self.owner_node_num}.db"
            if not legacy_db_file.exists():
                return

            legacy_node_db = meshdb.NodeDB(self.owner_node_num, str(legacy_root))
            legacy_node_db.ensure_table()
            shared_node_db = self._node_db(self.channel_num)

            with legacy_node_db.connect() as con:
                con.row_factory = sqlite3.Row
                rows = con.execute(f"SELECT * FROM {legacy_node_db.table}").fetchall()

            for row in rows:
                shared_node_db.upsert(
                    node_num=row["node_num"],
                    long_name=row["long_name"],
                    short_name=row["short_name"],
                    macaddr=row["macaddr"],
                    hw_model=row["hw_model"],
                    role=row["role"],
                    is_licensed=row["is_licensed"],
                    public_key=row["public_key"],
                    is_unmessagable=row["is_unmessagable"],
                    last_heard=row["last_heard"],
                    hops_away=row["hops_away"],
                    snr=row["snr"],
                )
        except Exception:
            pass
        finally:
            marker_path.touch(exist_ok=True)

    def ensure_owner_node(self) -> None:
        for channel_num in self._iter_channel_nums():
            self._node_db(channel_num).upsert(
                node_num=self.owner_node_num,
                long_name=self.profile.get("long_name") or None,
                short_name=self.profile.get("short_name") or None,
                hw_model="PRIVATE_HW",
                role="CLIENT",
                last_heard=int(time.time()),
                hops_away=0,
            )

    def record_packet(self, packet) -> Dict[str, bool]:
        packet_dict = meshdb.normalize_packet(packet, "mudp")
        storage_channel = self._channel_scope(packet_dict.get("channel"))
        stored = meshdb.handle_packet(
            packet_dict,
            node_database_number=self.owner_node_num,
            db_path=self.db_path,
            channel=storage_channel,
            channel_name=self._channel_name_for(storage_channel),
            storage_name=self._storage_name_for(storage_channel),
        )
        self._persist_hops_from_packet(packet_dict, storage_channel=storage_channel)
        return stored

    def get_node(self, node_num: int) -> Optional[Dict]:
        normalized_node_num = int(node_num)
        fallback_hops = self._get_fallback_hops(normalized_node_num)
        merged_node = None

        for channel_num in self._iter_channel_nums():
            node_db = self._node_db(channel_num)
            try:
                with node_db.connect() as con:
                    con.row_factory = sqlite3.Row
                    row = con.execute(
                        f"SELECT * FROM {node_db.table} WHERE node_num = ?",
                        (str(normalized_node_num),),
                    ).fetchone()
            except Exception:
                row = None

            if not row:
                continue

            merged_node = self._merge_node_records(
                merged_node,
                self._row_to_dict(
                    row,
                    fallback_hops=fallback_hops,
                    location=self._get_latest_location(normalized_node_num, channel_num=channel_num),
                    channel_num=channel_num,
                ),
            )

        return merged_node

    def list_nodes(self) -> List[Dict]:
        fallback_hops = self._get_fallback_hops_map()
        location_map = self._get_latest_locations_map()
        nodes_by_num: Dict[int, Dict] = {}

        for channel_num in self._iter_channel_nums():
            node_db = self._node_db(channel_num)
            try:
                with node_db.connect() as con:
                    con.row_factory = sqlite3.Row
                    rows = con.execute(
                        f"SELECT * FROM {node_db.table} ORDER BY COALESCE(last_heard, 0) DESC, long_name ASC, node_num ASC"
                    ).fetchall()
            except Exception:
                rows = []

            for row in rows:
                node_num = int(row["node_num"])
                if node_num == self.owner_node_num:
                    continue
                nodes_by_num[node_num] = self._merge_node_records(
                    nodes_by_num.get(node_num),
                    self._row_to_dict(
                        row,
                        fallback_hops=fallback_hops.get(node_num),
                        location=location_map.get(node_num),
                        channel_num=channel_num,
                    ),
                )

        return sorted(
            nodes_by_num.values(),
            key=lambda node: (
                str(node.get("last_seen") or ""),
                str(node.get("long_name") or ""),
                int(node.get("node_num") or 0),
            ),
            reverse=True,
        )

    def count_nodes(self) -> int:
        return len(self.list_nodes())

    def _get_fallback_hops(self, node_num: int) -> Optional[int]:
        hops_map = self._get_fallback_hops_map(node_num=node_num)
        return hops_map.get(int(node_num))

    def _get_fallback_hops_map(self, node_num: Optional[int] = None) -> Dict[int, int]:
        hops_map = self._get_meshdb_message_hops_map(node_num=node_num)
        if node_num is not None and int(node_num) in hops_map:
            return hops_map

        firefly_hops = self._get_firefly_message_hops_map(node_num=node_num)
        for sender_num, hops_away in firefly_hops.items():
            hops_map.setdefault(sender_num, hops_away)
        return hops_map

    def _persist_hops_from_packet(self, packet_dict: Dict, storage_channel: Optional[int] = None) -> None:
        sender_num = self._coerce_int(packet_dict.get("from"))
        if sender_num is None or sender_num == self.owner_node_num:
            return

        hops_away = self._hops_from_metadata(packet_dict.get("hopStart"), packet_dict.get("hopLimit"))
        if hops_away is None:
            return

        self._persist_node_hops(sender_num, hops_away, channel_num=storage_channel)

    def _persist_node_hops(self, node_num: int, hops_away: int, channel_num: Optional[int] = None) -> None:
        if node_num == self.owner_node_num:
            return

        try:
            self._node_db(channel_num).upsert(node_num=node_num, hops_away=hops_away)
        except Exception:
            pass

    def _get_meshdb_message_hops_map(self, node_num: Optional[int] = None) -> Dict[int, int]:
        latest_hops: Dict[int, tuple[int, int]] = {}
        node_value = str(int(node_num)) if node_num is not None else None

        for channel_num in self._iter_channel_nums():
            node_db = self._node_db(channel_num)
            try:
                with node_db.connect() as con:
                    con.row_factory = sqlite3.Row
                    for table_name in self._message_table_names(con):
                        query = f"""
                            SELECT node_num, hop_start, hop_limit, timestamp
                            FROM "{table_name}"
                            WHERE hop_start IS NOT NULL
                              AND hop_limit IS NOT NULL
                        """
                        params: List[object] = []
                        if node_value is not None:
                            query += " AND node_num = ?"
                            params.append(node_value)

                        for row in con.execute(query, params):
                            sender_num = self._coerce_int(row["node_num"])
                            if sender_num is None or sender_num == self.owner_node_num:
                                continue

                            hops_away = self._hops_from_metadata(row["hop_start"], row["hop_limit"])
                            if hops_away is None:
                                continue

                            timestamp = self._coerce_int(row["timestamp"]) or 0
                            self._update_latest_hops(latest_hops, sender_num, timestamp, hops_away)
            except Exception:
                continue

        return {sender_num: hops_away for sender_num, (_, hops_away) in latest_hops.items()}

    def _get_firefly_message_hops_map(self, node_num: Optional[int] = None) -> Dict[int, int]:
        if not os.path.exists(self.firefly_db_path):
            return {}

        scope_clauses: List[str] = []
        params: List[object] = []
        if self.profile_id:
            scope_clauses.append("owner_profile_id = ?")
            params.append(self.profile_id)
        if self.channel_nums:
            placeholders = ", ".join("?" for _ in self.channel_nums)
            scope_clauses.append(f"channel IN ({placeholders})")
            params.extend(self.channel_nums)
        if not scope_clauses:
            return {}

        query = f"""
            SELECT sender_num, hop_start, hop_limit, timestamp
            FROM messages
            WHERE sender_num IS NOT NULL
              AND hop_start IS NOT NULL
              AND hop_limit IS NOT NULL
              AND ({' OR '.join(scope_clauses)})
        """
        if node_num is not None:
            query += " AND sender_num = ?"
            params.append(int(node_num))

        latest_hops: Dict[int, tuple[str, int]] = {}
        try:
            with sqlite3.connect(self.firefly_db_path) as con:
                con.row_factory = sqlite3.Row
                cursor = con.execute(query, params)
                for row in cursor.fetchall():
                    sender_num = self._coerce_int(row["sender_num"])
                    if sender_num is None or sender_num == self.owner_node_num:
                        continue

                    hops_away = self._hops_from_metadata(row["hop_start"], row["hop_limit"])
                    if hops_away is None:
                        continue
                    timestamp = str(row["timestamp"] or "")
                    self._update_latest_hops(latest_hops, sender_num, timestamp, hops_away)
        except Exception:
            return {}

        return {sender_num: hops_away for sender_num, (_, hops_away) in latest_hops.items()}

    def _message_table_names(self, con: sqlite3.Connection) -> List[str]:
        rows = con.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name LIKE ?
            ORDER BY name
            """,
            ("%_messages",),
        ).fetchall()
        return [str(row[0]) for row in rows]

    def _hops_from_metadata(self, hop_start, hop_limit) -> Optional[int]:
        hop_start_value = self._coerce_int(hop_start)
        hop_limit_value = self._coerce_int(hop_limit)
        if hop_start_value is None or hop_limit_value is None:
            return None
        return max(hop_start_value - hop_limit_value, 0)

    def _coerce_int(self, value) -> Optional[int]:
        try:
            if value in (None, ""):
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    def _update_latest_hops(self, latest_hops: Dict, sender_num: int, timestamp, hops_away: int) -> None:
        previous = latest_hops.get(sender_num)
        if previous is None or timestamp > previous[0] or (timestamp == previous[0] and hops_away < previous[1]):
            latest_hops[sender_num] = (timestamp, hops_away)

    def _get_latest_location(self, node_num: int, channel_num: Optional[int] = None) -> Optional[Dict]:
        latest_location = None
        channel_nums = [self._channel_scope(channel_num)] if channel_num is not None else self._iter_channel_nums()

        for scoped_channel_num in channel_nums:
            location_db = self._location_db(scoped_channel_num)
            try:
                with location_db.connect() as con:
                    con.row_factory = sqlite3.Row
                    row = con.execute(
                        f"""
                        SELECT timestamp, latitude, longitude, latitude_i, longitude_i, altitude
                        FROM {location_db.table}
                        WHERE node_num = ?
                        ORDER BY timestamp DESC
                        LIMIT 1
                        """,
                        (str(int(node_num)),),
                    ).fetchone()
            except Exception:
                continue

            if not row:
                continue

            latitude, longitude = self._normalize_location(
                row["latitude"],
                row["longitude"],
                row["latitude_i"],
                row["longitude_i"],
            )
            if latitude is None or longitude is None:
                continue

            timestamp = self._coerce_int(row["timestamp"]) or 0
            if latest_location is not None and timestamp < (self._coerce_int(latest_location.get("timestamp")) or 0):
                continue
            latest_location = {
                "timestamp": row["timestamp"],
                "latitude": latitude,
                "longitude": longitude,
                "altitude": row["altitude"],
            }

        return latest_location

    def _get_latest_locations_map(self) -> Dict[int, Dict]:
        locations: Dict[int, Dict] = {}
        for channel_num in self._iter_channel_nums():
            location_db = self._location_db(channel_num)
            try:
                with location_db.connect() as con:
                    con.row_factory = sqlite3.Row
                    rows = con.execute(
                        f"""
                        SELECT node_num, timestamp, latitude, longitude, latitude_i, longitude_i, altitude
                        FROM {location_db.table}
                        """
                    ).fetchall()
            except Exception:
                continue

            for row in rows:
                node_num = self._coerce_int(row["node_num"])
                if node_num is None:
                    continue
                latitude, longitude = self._normalize_location(
                    row["latitude"],
                    row["longitude"],
                    row["latitude_i"],
                    row["longitude_i"],
                )
                if latitude is None or longitude is None:
                    continue
                existing = locations.get(node_num)
                timestamp = self._coerce_int(row["timestamp"]) or 0
                if existing is not None and timestamp < (self._coerce_int(existing.get("timestamp")) or 0):
                    continue
                locations[node_num] = {
                    "timestamp": row["timestamp"],
                    "latitude": latitude,
                    "longitude": longitude,
                    "altitude": row["altitude"],
                }
        return locations

    def _normalize_location(self, latitude, longitude, latitude_i=None, longitude_i=None) -> tuple[Optional[float], Optional[float]]:
        normalized_lat = self._coerce_float(latitude)
        normalized_lon = self._coerce_float(longitude)
        int_lat = self._coerce_int(latitude_i)
        int_lon = self._coerce_int(longitude_i)

        if self._is_valid_coordinate_pair(normalized_lat, normalized_lon):
            if not self._looks_like_null_island(normalized_lat, normalized_lon) or int_lat in (None, 0) or int_lon in (None, 0):
                return normalized_lat, normalized_lon

        if int_lat is not None and int_lon is not None:
            derived_lat = int_lat / 1e7
            derived_lon = int_lon / 1e7
            if self._is_valid_coordinate_pair(derived_lat, derived_lon):
                return derived_lat, derived_lon

        return None, None

    def _coerce_float(self, value) -> Optional[float]:
        try:
            if value in (None, ""):
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def _is_valid_coordinate_pair(self, latitude: Optional[float], longitude: Optional[float]) -> bool:
        if latitude is None or longitude is None:
            return False
        if not math.isfinite(latitude) or not math.isfinite(longitude):
            return False
        return -90 <= latitude <= 90 and -180 <= longitude <= 180

    def _looks_like_null_island(self, latitude: Optional[float], longitude: Optional[float]) -> bool:
        if latitude is None or longitude is None:
            return False
        return abs(latitude) < 1e-9 and abs(longitude) < 1e-9

    def _generated_node_long_name(self, node_num: int) -> str:
        suffix = f"{int(node_num):08x}"[-4:]
        return f"Meshtastic {suffix}"

    def _generated_node_short_name(self, node_num: int) -> str:
        return f"{int(node_num):08x}"[-4:]

    def _is_fallback_node_label(self, value, node_num: int) -> bool:
        normalized_value = str(value or "").strip().lower()
        if not normalized_value:
            return True
        fallback_candidates = {
            str(node_id_from_num(node_num) or "").lower(),
            str(self._generated_node_long_name(node_num) or "").lower(),
            str(self._generated_node_short_name(node_num) or "").lower(),
            "unknown",
        }
        return normalized_value in fallback_candidates

    def _merge_node_records(self, existing: Optional[Dict], incoming: Optional[Dict]) -> Optional[Dict]:
        if not incoming:
            return existing
        if not existing:
            return dict(incoming)

        merged = dict(existing)
        node_num = int(incoming.get("node_num") or existing.get("node_num") or 0)

        def prefer(key: str, *, treat_fallback: bool = False, fallback_values=None) -> None:
            current_value = merged.get(key)
            incoming_value = incoming.get(key)
            if incoming_value in (None, ""):
                return
            if current_value in (None, ""):
                merged[key] = incoming_value
                return
            if treat_fallback and self._is_fallback_node_label(current_value, node_num) and not self._is_fallback_node_label(incoming_value, node_num):
                merged[key] = incoming_value
                return
            if fallback_values and current_value in fallback_values and incoming_value not in fallback_values:
                merged[key] = incoming_value
                return
            if key in {"last_heard", "position_timestamp"}:
                if str(incoming_value) > str(current_value):
                    merged[key] = incoming_value
            if key == "last_seen" and str(incoming_value) > str(current_value):
                merged[key] = incoming_value

        prefer("node_id")
        prefer("long_name", treat_fallback=True)
        prefer("short_name", treat_fallback=True)
        prefer("hw_model", fallback_values={"UNSET"})
        prefer("role", fallback_values={"CLIENT"})
        prefer("public_key")
        prefer("macaddr")
        prefer("is_licensed")
        prefer("is_unmessagable")
        prefer("hops_away")
        prefer("snr")
        prefer("latitude")
        prefer("longitude")
        prefer("altitude")
        prefer("position_timestamp")
        prefer("last_heard")
        prefer("last_seen")
        prefer("first_seen")

        return merged

    def _row_to_dict(
        self,
        row: sqlite3.Row,
        fallback_hops: Optional[int] = None,
        location: Optional[Dict] = None,
        channel_num: Optional[int] = None,
    ) -> Dict:
        node_num = int(row["node_num"])
        hops_away = row["hops_away"] if row["hops_away"] is not None else fallback_hops
        if row["hops_away"] is None and fallback_hops is not None:
            self._persist_node_hops(node_num, fallback_hops, channel_num=channel_num)
        node = {
            "node_num": node_num,
            "node_id": node_id_from_num(node_num),
            "long_name": row["long_name"] or node_id_from_num(node_num),
            "short_name": row["short_name"] or node_id_from_num(node_num)[-4:],
            "macaddr": row["macaddr"] or None,
            "hw_model": row["hw_model"] or "UNSET",
            "role": row["role"] or "CLIENT",
            "public_key": row["public_key"] or None,
            "is_licensed": bool(row["is_licensed"]) if row["is_licensed"] is not None else False,
            "is_unmessagable": bool(row["is_unmessagable"]) if row["is_unmessagable"] is not None else False,
            "hops_away": hops_away,
            "snr": row["snr"],
            "last_heard": row["last_heard"],
            "last_seen": _epoch_to_iso(row["last_heard"]),
        }
        node["latitude"] = location.get("latitude") if location else None
        node["longitude"] = location.get("longitude") if location else None
        node["altitude"] = location.get("altitude") if location else None
        node["position_timestamp"] = _epoch_to_iso(location.get("timestamp")) if location and location.get("timestamp") else None
        node["first_seen"] = node["last_seen"]
        return node


def count_nodes_for_profiles(profiles: Dict[str, Dict]) -> int:
    node_nums = set()
    for profile in profiles.values():
        try:
            for node in MeshNodeStore(profile).list_nodes():
                node_num = node.get("node_num")
                if node_num is not None:
                    node_nums.add(int(node_num))
        except Exception:
            continue
    return len(node_nums)


def find_meshdb_node_id_conflict(profiles, node_id: str, exclude_profile_id: Optional[str] = None) -> Optional[Dict]:
    normalized_node_id = (node_id or "").strip().lower()
    if not normalized_node_id:
        return None

    try:
        target_node_num = int(parse_node_id(normalized_node_id))
    except Exception:
        return None

    profile_iterable = profiles.values() if isinstance(profiles, dict) else (profiles or [])
    excluded_profile_id = str(exclude_profile_id) if exclude_profile_id else None
    excluded_owner_num = None

    if excluded_profile_id:
        for profile in profile_iterable:
            if not isinstance(profile, dict):
                continue
            if str(profile.get("id") or "") != excluded_profile_id:
                continue
            try:
                excluded_owner_num = owner_node_num(profile)
            except Exception:
                excluded_owner_num = None
            break

    seen_channels = set()
    shared_meshdb_root = _meshdb_root()

    for profile in profile_iterable:
        if not isinstance(profile, dict):
            continue

        profile_id = str(profile.get("id") or "")
        try:
            profile_owner_num = owner_node_num(profile)
        except Exception:
            continue

        channel_nums = list(dict.fromkeys(_profile_channel_hashes(profile)))
        if not channel_nums:
            continue

        for channel_num in channel_nums:
            if channel_num in seen_channels:
                continue
            seen_channels.add(channel_num)

            node_db = meshdb.NodeDB(
                profile_owner_num,
                str(shared_meshdb_root),
                channel=channel_num,
                channel_name=_profile_channel_map(profile).get(channel_num),
                storage_name=_meshdb_storage_name(_profile_channel_map(profile).get(channel_num)),
            )
            meshdb_file = Path(node_db.db_path)
            if not meshdb_file.exists():
                continue

            try:
                node_db.ensure_table()
                with node_db.connect() as con:
                    con.row_factory = sqlite3.Row
                    row = con.execute(
                        f"SELECT node_num, long_name, short_name FROM {node_db.table} WHERE node_num = ? LIMIT 1",
                        (str(target_node_num),),
                    ).fetchone()
            except sqlite3.OperationalError:
                continue
            except Exception:
                continue

            if not row:
                continue

            matched_node_num = int(row["node_num"])
            if excluded_owner_num is not None and matched_node_num == excluded_owner_num:
                continue

            return {
                "owner_profile_id": profile_id or None,
                "owner_profile_node_id": profile.get("node_id"),
                "owner_profile_long_name": profile.get("long_name"),
                "node_num": matched_node_num,
                "node_id": node_id_from_num(matched_node_num),
                "long_name": row["long_name"] or None,
                "short_name": row["short_name"] or None,
                "meshdb_file": str(meshdb_file),
                "channel_num": channel_num,
            }

    return None


def _epoch_to_iso(value) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).astimezone().isoformat()
    except Exception:
        return None
