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

import meshdb
from meshtastic import mesh_pb2, portnums_pb2
from mudp import UDPPacketStream
from mudp.encryption import decrypt_packet, encrypt_packet
from mudp.reliability import build_routing_ack_data, compute_reply_hop_limit, register_pending_ack
from mudp.singleton import conn
from pubsub import pub
from vnode import VirtualNode, parse_node_id
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


def _meshdb_root(profile: Dict) -> Path:
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

    def send_nodeinfo(self, destination: int = BROADCAST_NODE_NUM) -> int:
        if self.virtual_node is None:
            raise RuntimeError("Virtual node is not running")
        return self.virtual_node.send_nodeinfo(int(destination))

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
        self.channel_nums = _profile_channel_hashes(profile)
        self.channel_num = self.channel_nums[0] if self.channel_nums else generate_hash(profile["channel"], profile["key"])
        self.node_db = meshdb.NodeDB(self.owner_node_num, self.db_path)
        self.node_db.ensure_table()
        self.location_db = meshdb.LocationDB(self.owner_node_num, self.db_path)
        self.location_db.ensure_table()

    def ensure_owner_node(self) -> None:
        self.node_db.upsert(
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
        stored = meshdb.handle_packet(packet_dict, node_database_number=self.owner_node_num, db_path=self.db_path)
        self._persist_hops_from_packet(packet_dict)
        return stored

    def get_node(self, node_num: int) -> Optional[Dict]:
        with self.node_db.connect() as con:
            con.row_factory = sqlite3.Row
            cursor = con.execute(
                f"SELECT * FROM {self.node_db.table} WHERE node_num = ?",
                (str(int(node_num)),),
            )
            row = cursor.fetchone()
        if not row:
            return None
        return self._row_to_dict(
            row,
            fallback_hops=self._get_fallback_hops(int(node_num)),
            location=self._get_latest_location(int(node_num)),
        )

    def list_nodes(self) -> List[Dict]:
        with self.node_db.connect() as con:
            con.row_factory = sqlite3.Row
            cursor = con.execute(
                f"SELECT * FROM {self.node_db.table} ORDER BY COALESCE(last_heard, 0) DESC, long_name ASC, node_num ASC"
            )
            rows = cursor.fetchall()
        fallback_hops = self._get_fallback_hops_map()
        location_map = self._get_latest_locations_map()
        return [
            self._row_to_dict(
                row,
                fallback_hops=fallback_hops.get(int(row["node_num"])),
                location=location_map.get(int(row["node_num"])),
            )
            for row in rows
            if int(row["node_num"]) != self.owner_node_num
        ]

    def count_nodes(self) -> int:
        with self.node_db.connect() as con:
            cursor = con.execute(
                f"SELECT COUNT(*) FROM {self.node_db.table} WHERE node_num != ?",
                (str(self.owner_node_num),),
            )
            row = cursor.fetchone()
        return int(row[0]) if row else 0

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

    def _persist_hops_from_packet(self, packet_dict: Dict) -> None:
        sender_num = self._coerce_int(packet_dict.get("from"))
        if sender_num is None or sender_num == self.owner_node_num:
            return

        hops_away = self._hops_from_metadata(packet_dict.get("hopStart"), packet_dict.get("hopLimit"))
        if hops_away is None:
            return

        self._persist_node_hops(sender_num, hops_away)

    def _persist_node_hops(self, node_num: int, hops_away: int) -> None:
        if node_num == self.owner_node_num:
            return

        try:
            self.node_db.upsert(node_num=node_num, hops_away=hops_away)
        except Exception:
            pass

    def _get_meshdb_message_hops_map(self, node_num: Optional[int] = None) -> Dict[int, int]:
        latest_hops: Dict[int, tuple[int, int]] = {}
        node_value = str(int(node_num)) if node_num is not None else None

        try:
            with self.node_db.connect() as con:
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
            return {}

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
        prefix = f"{self.owner_node_num}_"
        rows = con.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name LIKE ?
            ORDER BY name
            """,
            (f"{prefix}%_messages",),
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

    def _get_latest_location(self, node_num: int) -> Optional[Dict]:
        try:
            with self.location_db.connect() as con:
                con.row_factory = sqlite3.Row
                row = con.execute(
                    f"""
                    SELECT timestamp, latitude, longitude, latitude_i, longitude_i, altitude
                    FROM {self.location_db.table}
                    WHERE node_num = ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (str(int(node_num)),),
                ).fetchone()
        except Exception:
            return None

        if not row:
            return None

        latitude, longitude = self._normalize_location(row["latitude"], row["longitude"], row["latitude_i"], row["longitude_i"])
        if latitude is None or longitude is None:
            return None
        return {
            "timestamp": row["timestamp"],
            "latitude": latitude,
            "longitude": longitude,
            "altitude": row["altitude"],
        }

    def _get_latest_locations_map(self) -> Dict[int, Dict]:
        try:
            with self.location_db.connect() as con:
                con.row_factory = sqlite3.Row
                rows = con.execute(
                    f"""
                    SELECT node_num, timestamp, latitude, longitude, latitude_i, longitude_i, altitude
                    FROM {self.location_db.table}
                    """
                ).fetchall()
        except Exception:
            return {}

        locations: Dict[int, Dict] = {}
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

    def _row_to_dict(self, row: sqlite3.Row, fallback_hops: Optional[int] = None, location: Optional[Dict] = None) -> Dict:
        node_num = int(row["node_num"])
        hops_away = row["hops_away"] if row["hops_away"] is not None else fallback_hops
        if row["hops_away"] is None and fallback_hops is not None:
            self._persist_node_hops(node_num, fallback_hops)
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
    total = 0
    for profile in profiles.values():
        try:
            total += MeshNodeStore(profile).count_nodes()
        except Exception:
            continue
    return total


def _epoch_to_iso(value) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).astimezone().isoformat()
    except Exception:
        return None
