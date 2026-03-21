#!/usr/bin/env python3

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import meshdb
from vnode import VirtualNode, parse_node_id
from encryption import generate_hash


BROADCAST_NODE_NUM = 0xFFFFFFFF


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
    return int(parse_node_id(profile["node_id"]))


def node_id_from_num(node_num: int) -> str:
    return f"!{int(node_num):08x}"


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
    def __init__(self, mcast_group: str, mcast_port: int):
        self.mcast_group = mcast_group
        self.mcast_port = int(mcast_port)
        self.virtual_node: Optional[VirtualNode] = None
        self.current_profile_id: Optional[str] = None

    def start(self, profile: Dict) -> VirtualNode:
        self.stop()
        config_path = ensure_profile_config(profile, self.mcast_group, self.mcast_port)
        vnode = VirtualNode(config_path)
        vnode.start()
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


class MeshNodeStore:
    def __init__(self, profile: Dict):
        self.profile = profile
        self.owner_node_num = owner_node_num(profile)
        self.db_path = str(_meshdb_root(profile))
        self.firefly_db_path = str(_firefly_db_path())
        self.channel_num = generate_hash(profile["channel"], profile["key"])
        self.node_db = meshdb.NodeDB(self.owner_node_num, self.db_path)
        self.node_db.ensure_table()

    def record_packet(self, packet) -> Dict[str, bool]:
        packet_dict = meshdb.normalize_packet(packet, "mudp")
        return meshdb.handle_packet(packet_dict, node_database_number=self.owner_node_num, db_path=self.db_path)

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
        return self._row_to_dict(row, fallback_hops=self._get_fallback_hops(int(node_num)))

    def list_nodes(self) -> List[Dict]:
        with self.node_db.connect() as con:
            con.row_factory = sqlite3.Row
            cursor = con.execute(
                f"SELECT * FROM {self.node_db.table} ORDER BY COALESCE(last_heard, 0) DESC, long_name ASC, node_num ASC"
            )
            rows = cursor.fetchall()
        fallback_hops = self._get_fallback_hops_map()
        return [
            self._row_to_dict(row, fallback_hops=fallback_hops.get(int(row["node_num"])))
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
        if not os.path.exists(self.firefly_db_path):
            return {}

        query = """
            SELECT sender_num, hop_start, hop_limit
            FROM messages
            WHERE channel = ?
              AND sender_num IS NOT NULL
              AND hop_start IS NOT NULL
              AND hop_limit IS NOT NULL
              AND COALESCE(message_type, 'channel') = 'channel'
        """
        params: List[object] = [self.channel_num]
        if node_num is not None:
            query += " AND sender_num = ?"
            params.append(int(node_num))
        query += " ORDER BY timestamp DESC"

        hops_map: Dict[int, int] = {}
        try:
            with sqlite3.connect(self.firefly_db_path) as con:
                con.row_factory = sqlite3.Row
                cursor = con.execute(query, params)
                for row in cursor.fetchall():
                    sender_num = int(row["sender_num"])
                    if sender_num in hops_map:
                        continue
                    hop_start = row["hop_start"]
                    hop_limit = row["hop_limit"]
                    if hop_start is None or hop_limit is None:
                        continue
                    hops_map[sender_num] = max(int(hop_start) - int(hop_limit), 0)
        except Exception:
            return {}

        return hops_map

    def _row_to_dict(self, row: sqlite3.Row, fallback_hops: Optional[int] = None) -> Dict:
        node_num = int(row["node_num"])
        hops_away = row["hops_away"] if row["hops_away"] is not None else fallback_hops
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
