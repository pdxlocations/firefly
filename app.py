#!/usr/bin/env python3

import base64
from collections import deque

from datetime import datetime
from functools import wraps
import re
import unicodedata
from google.protobuf import text_format
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, has_request_context, flash, send_from_directory
from flask_socketio import SocketIO, emit, join_room, leave_room, rooms
import uuid
import os
import socketio as py_socketio
import engineio
import meshtastic
from pubsub import pub
from werkzeug.security import generate_password_hash, check_password_hash

from meshtastic.protobuf import mesh_pb2, portnums_pb2
from mudp import node
from mudp.reliability import is_ack, is_nak, parse_routing
from database import Database
from encryption import generate_hash
from firefly_logging import configure_logging, get_logger, make_log_print
from mesh_runtime import (
    MeshNodeStore,
    SharedPacketReceiver,
    VirtualNodeManager,
    count_nodes_for_profiles,
    find_meshdb_node_id_conflict,
)
from vnode.crypto import b64_decode

configure_logging()
logger = get_logger("firefly.app")
print = make_log_print(logger)


MCAST_GRP = os.getenv("FIREFLY_MCAST_GRP", "224.0.0.69")
MCAST_PORT = int(os.getenv("FIREFLY_UDP_PORT", "4403"))
SOCKETIO_CLIENT_VERSION = "4.7.5"
BROADCAST_NODE_NUM = 0xFFFFFFFF
DEFAULT_SECRET_KEY = "dev-secret-key-change-in-production"
NODE_ID_PATTERN = re.compile(r"^![0-9a-f]{8}$")
SHORT_NAME_ALNUM_PATTERN = re.compile(r"^[A-Za-z0-9]{4}$")

# Cross-source de-duplication cache for user-visible packet handling.
_DEDUP_CACHE = set()
_DEDUP_QUEUE = deque(maxlen=500)

# Initialize database with environment variable path for Docker persistence
db_path = os.getenv('FIREFLY_DATABASE_FILE', 'firefly.db')
print(f"[DATABASE] Initializing database at: {db_path}")

# Ensure database directory exists
db_dir = os.path.dirname(db_path)
if db_dir and not os.path.exists(db_dir):
    os.makedirs(db_dir, exist_ok=True)
    print(f"[DATABASE] Created database directory: {db_dir}")

db = Database(db_path)

# Map WebSocket session IDs to Flask session IDs for cleanup
websocket_to_flask_sessions = {}


def _get_session_profile():
    """Get the current profile from session storage"""
    if not has_request_context():
        return None
    profile = session.get("current_profile")
    current_user = _get_session_user()
    if profile and current_user and profile.get("user_id") == current_user.get("id"):
        return profile
    return None


def _set_session_profile(profile):
    """Set the current profile in session storage"""
    if not has_request_context():
        return
    session["current_profile"] = profile


def _clear_session_profile():
    """Clear the current profile from session storage"""
    if not has_request_context():
        return
    session.pop("current_profile", None)
    session.pop("current_channel_index", None)


def _get_session_channel_index():
    if not has_request_context():
        return 0
    try:
        return int(session.get("current_channel_index", 0))
    except (TypeError, ValueError):
        return 0


def _set_session_channel_index(channel_index):
    if not has_request_context():
        return
    try:
        session["current_channel_index"] = max(0, int(channel_index))
    except (TypeError, ValueError):
        session["current_channel_index"] = 0


def _get_session_user():
    if not has_request_context():
        return None
    user_id = session.get("user_id")
    if not user_id:
        return None
    return db.get_user_by_id(user_id)


def _set_session_user(user):
    if not has_request_context():
        return
    session["user_id"] = user["id"]


def _clear_session_user():
    if not has_request_context():
        return
    session.pop("user_id", None)
    _clear_session_profile()


def _unregister_current_session_transport():
    if not has_request_context():
        return
    if not session.get("current_profile"):
        return
    session_id = f"flask_session_{id(session)}"
    try:
        udp_server.unregister_session(session_id)
    except Exception as e:
        print(f"[SESSION] Failed to unregister UDP session {session_id}: {e}")


def _wants_json_response():
    if not has_request_context():
        return False
    if request.path.startswith("/api/"):
        return True
    best = request.accept_mimetypes.best
    return best == "application/json" and request.accept_mimetypes[best] >= request.accept_mimetypes["text/html"]


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if _get_session_user():
            return view(*args, **kwargs)
        if _wants_json_response():
            return jsonify({"error": "Authentication required"}), 401
        return redirect(url_for("index"))
    return wrapped


def _normalize_node_id(node_id):
    value = (node_id or "").strip()
    return value.lower() if value.startswith("!") else value


def _normalize_text_field(value):
    return value.strip() if isinstance(value, str) else ""


def _is_valid_short_name(short_name):
    value = _normalize_text_field(short_name)
    if not value:
        return False
    if SHORT_NAME_ALNUM_PATTERN.fullmatch(value):
        return len(value.encode("utf-8")) <= 4
    return (
        len(value) == 1
        and len(value.encode("utf-8")) <= 4
        and unicodedata.category(value) == "So"
    )


def _is_valid_channel_key(channel_key):
    value = _normalize_text_field(channel_key).replace("-", "+").replace("_", "/")
    if not value:
        return False
    try:
        decoded = base64.b64decode(value.encode("ascii"), validate=True)
    except Exception:
        return False
    return len(decoded) in {1, 16, 32}


def _validate_profile_request_payload(data):
    normalized_node_id = _normalize_node_id(_normalize_text_field((data or {}).get("node_id")))
    long_name = _normalize_text_field((data or {}).get("long_name"))
    short_name = _normalize_text_field((data or {}).get("short_name"))
    channels = _profile_channels(data)

    if not normalized_node_id or not long_name or not short_name or not channels:
        return None, "node_id, long_name, short_name, and at least one channel are required"
    if not NODE_ID_PATTERN.fullmatch(normalized_node_id):
        return None, "node_id must be ! followed by 8 hexadecimal characters"
    if len(long_name) >= 32:
        return None, "long_name must be fewer than 32 characters"
    if not _is_valid_short_name(short_name):
        return None, "short_name must be 1 emoji without modifiers or exactly 4 alphanumeric characters"

    hop_limit = (data or {}).get("hop_limit", 3)
    if not isinstance(hop_limit, int) or hop_limit < 0 or hop_limit > 7:
        return None, "hop_limit must be an integer between 0 and 7"

    return {
        "node_id": normalized_node_id,
        "long_name": long_name,
        "short_name": short_name,
        "channels": channels,
        "hop_limit": hop_limit,
    }, None


def _profile_channels(profile):
    if not profile:
        return []
    channels = profile.get("channels")
    if isinstance(channels, list) and channels:
        return [
            {
                "name": (channel.get("name") or "").strip(),
                "key": (channel.get("key") or "").strip(),
            }
            for channel in channels
            if isinstance(channel, dict) and (channel.get("name") or "").strip() and (channel.get("key") or "").strip()
        ]
    channel_name = (profile.get("channel") or "").strip()
    channel_key = (profile.get("key") or "").strip()
    if channel_name and channel_key:
        return [{"name": channel_name, "key": channel_key}]
    return []


def _profile_node_id_conflict_error(node_id, exclude_profile_id=None):
    if db.node_id_in_use(node_id, exclude_profile_id=exclude_profile_id):
        return "Node ID is already in use by another profile."

    conflict = find_meshdb_node_id_conflict(
        profile_manager.get_all_profiles(),
        node_id,
        exclude_profile_id=exclude_profile_id,
    )
    if conflict:
        print(
            f"[PROFILES] Rejected node_id {node_id} because it already exists in meshdb "
            f"for profile {conflict.get('owner_profile_id')} at {conflict.get('meshdb_file')}"
        )
        return "Node ID is already in use by another node in the mesh database."

    return None


def _selected_channel_index(profile=None):
    channels = _profile_channels(profile or _get_session_profile())
    if not channels:
        return 0
    if profile is not None:
        try:
            selected_index = int(profile.get("selected_channel_index", 0) or 0)
        except (AttributeError, TypeError, ValueError):
            selected_index = 0
        if "selected_channel_index" in profile or not has_request_context():
            return min(max(selected_index, 0), len(channels) - 1)
    return min(_get_session_channel_index(), len(channels) - 1)


def _selected_channel(profile=None):
    channels = _profile_channels(profile or _get_session_profile())
    if not channels:
        return None
    return channels[_selected_channel_index(profile)]


def _effective_profile(profile=None, channel_index=None):
    profile = profile or _get_session_profile()
    if not profile:
        return None
    effective = dict(profile)
    channels = _profile_channels(profile)
    effective["channels"] = channels
    if channels:
        if channel_index is None:
            selected_index = _selected_channel_index(profile)
        else:
            selected_index = max(0, min(int(channel_index), len(channels) - 1))
        selected = channels[selected_index]
        effective["channel"] = selected["name"]
        effective["key"] = selected["key"]
        effective["selected_channel_index"] = selected_index
    else:
        effective["selected_channel_index"] = 0
    return effective


def _current_profile_channel_num():
    """Compute the expected channel number for the current profile using name+key hash.
    Returns an int channel number or None if unavailable.
    """
    try:
        current_profile = _effective_profile()
        if not current_profile:
            return None
        ch_name = current_profile.get("channel")
        key = current_profile.get("key")
        if not ch_name or not key:
            return None
        return generate_hash(ch_name, key)
    except Exception:
        return None


def _profile_channel_num(profile):
    try:
        profile = _effective_profile(profile)
        if not profile:
            return None
        ch_name = profile.get("channel")
        key = profile.get("key")
        if not ch_name or not key:
            return None
        return generate_hash(ch_name, key)
    except Exception:
        return None


def _channel_number_for_config(channel):
    if not isinstance(channel, dict):
        return None
    name = (channel.get("name") or "").strip()
    key = (channel.get("key") or "").strip()
    if not name or not key:
        return None
    try:
        return generate_hash(name, key)
    except Exception:
        return None


def _profile_channels_with_numbers(profile):
    channels = []
    for channel in _profile_channels(profile):
        channel_data = dict(channel)
        channel_data["channel_number"] = _channel_number_for_config(channel)
        channels.append(channel_data)
    return channels


def _serialize_profile_for_client(profile, interface_status=None):
    if not profile:
        return None

    channels = _profile_channels_with_numbers(profile)
    response_data = dict(profile)
    response_data["channels"] = channels

    if channels:
        selected_channel_index = min(max(_selected_channel_index(profile), 0), len(channels) - 1)
        response_data["selected_channel_index"] = selected_channel_index
        response_data["selected_channel"] = channels[selected_channel_index]
        response_data["channel_number"] = channels[selected_channel_index].get("channel_number")
    else:
        response_data["selected_channel_index"] = 0
        response_data["selected_channel"] = None
        response_data["channel_number"] = None

    if interface_status is not None:
        response_data["interface_status"] = interface_status

    return response_data


def _profile_node_num(profile):
    try:
        node_id = profile.get("node_id") if profile else None
        if isinstance(node_id, str) and node_id.startswith("!"):
            return int(node_id[1:], 16)
    except Exception:
        pass
    return None


def _node_id_from_num(node_num):
    if node_num is None:
        return None
    return f"!{int(node_num):08x}"


def _is_direct_message(packet, profile=None):
    packet_to = getattr(packet, "to", None)
    if packet_to in (None, 0, BROADCAST_NODE_NUM):
        return False
    profile = profile or udp_server.get_active_profile()
    my_num = _profile_node_num(profile) if profile else None
    if my_num is None:
        return True
    return int(packet_to) == int(my_num)


def _already_seen(key):
    """Return True if we've already processed a packet with this key."""
    if key in _DEDUP_CACHE:
        return True
    _DEDUP_CACHE.add(key)
    _DEDUP_QUEUE.append(key)
    if len(_DEDUP_CACHE) > _DEDUP_QUEUE.maxlen:
        old = _DEDUP_QUEUE.popleft()
        _DEDUP_CACHE.discard(old)
    return False


def _routing_error_name(error_reason):
    if error_reason is None:
        return None
    try:
        return mesh_pb2.Routing.Error.Name(int(error_reason))
    except Exception:
        return str(error_reason)


def _emit_message_update(message):
    if not message:
        return

    room_name = None
    if (message.get("message_type") or "channel") == "dm" and message.get("owner_profile_id"):
        room_name = f"profile_{message['owner_profile_id']}"
    elif message.get("channel") is not None:
        room_name = f"channel_{message['channel']}"

    if not room_name:
        return

    try:
        socketio.emit("message_update", message, room=room_name)
    except Exception as e:
        print(f"[ACK] Failed to broadcast message update for packet {message.get('packet_id')}: {e}")


def _apply_ack_update(packet_id, ack_status, ack_error=None):
    updated_messages = db.update_message_ack_status(packet_id, ack_status, ack_error)
    if not updated_messages:
        return

    for message in updated_messages:
        _emit_message_update(message)


def _emit_node_update(profile_id, node):
    if not profile_id or not node:
        return

    try:
        socketio.emit("node_update", {"profile_id": profile_id, "node": node}, room=f"profile_{profile_id}")
    except Exception as e:
        print(f"[NODEINFO] Failed to broadcast node update for profile {profile_id}: {e}")


shared_packet_receiver = SharedPacketReceiver(MCAST_GRP, MCAST_PORT)
virtual_node_manager = VirtualNodeManager(MCAST_GRP, MCAST_PORT, shared_receiver=shared_packet_receiver)
shared_packet_receiver.start()


def _get_mesh_store(profile=None):
    profile = _effective_profile(profile)
    if not profile or not profile.get("node_id"):
        return None
    normalized_node_num = _profile_node_num(profile)
    if normalized_node_num is None:
        print(
            f"[MESHDB] Skipping node store for profile {profile.get('id')!r}: "
            f"invalid node_id {profile.get('node_id')!r}"
        )
        return None
    try:
        return MeshNodeStore(profile)
    except Exception as e:
        print(
            f"[MESHDB] Failed to create node store for profile {profile.get('id')!r} "
            f"with node_id {profile.get('node_id')!r}: {e}"
        )
        return None


def _seed_profile_mesh_identity(profile):
    mesh_store = _get_mesh_store(profile)
    if not mesh_store:
        return False
    try:
        mesh_store.ensure_owner_node()
        return True
    except Exception as e:
        print(f"[MESHDB] Failed to seed owner node for profile {profile.get('id')}: {e}")
        return False


def _mesh_stores_for_profiles(profiles):
    stores = []
    seen_profile_ids = set()
    for profile in profiles or []:
        if not isinstance(profile, dict):
            continue
        profile_id = profile.get("id")
        if not profile_id or profile_id in seen_profile_ids:
            continue
        mesh_store = _get_mesh_store(profile)
        if mesh_store:
            stores.append((profile, mesh_store))
            seen_profile_ids.add(profile_id)
    return stores


def _profiles_matching_packet(packet):
    matched_profiles = []
    seen_profile_ids = set()

    packet_to = getattr(packet, "to", None)
    if packet_to not in (None, 0, BROADCAST_NODE_NUM):
        owner_profile = db.get_profile_by_node_num(packet_to)
        if owner_profile and owner_profile.get("id") not in seen_profile_ids:
            matched_profiles.append(owner_profile)
            seen_profile_ids.add(owner_profile.get("id"))

    try:
        packet_channel = int(getattr(packet, "channel", 0) or 0)
    except Exception:
        packet_channel = 0

    if packet_channel <= 0:
        return matched_profiles

    try:
        all_profiles = db.get_all_profiles()
    except Exception:
        return matched_profiles

    for profile in all_profiles.values():
        profile_id = profile.get("id")
        if profile_id in seen_profile_ids:
            continue
        for channel in _profile_channels(profile):
            if _channel_number_for_config(channel) == packet_channel:
                matched_profiles.append(profile)
                seen_profile_ids.add(profile_id)
                break

    return matched_profiles


def _matching_mesh_stores_for_packet(packet):
    return _mesh_stores_for_profiles(_profiles_matching_packet(packet))


def _find_sender_node_for_packet(packet, sender_num):
    if sender_num in (None, 0):
        return None

    local_sender_profile = db.get_profile_by_node_num(sender_num)
    if local_sender_profile:
        return {
            "long_name": local_sender_profile.get("long_name"),
            "short_name": local_sender_profile.get("short_name"),
        }

    for _profile, mesh_store in _matching_mesh_stores_for_packet(packet):
        try:
            found_node = mesh_store.get_node(sender_num)
        except Exception:
            found_node = None
        if found_node:
            return found_node

    active_profile = udp_server.get_active_profile()
    if active_profile:
        mesh_store = _get_mesh_store(active_profile)
        if mesh_store:
            try:
                return mesh_store.get_node(sender_num)
            except Exception:
                return None
    return None


def _emit_message_to_profile_rooms(message, profiles):
    if not message:
        return
    emitted_profile_ids = set()
    for profile in profiles or []:
        profile_id = profile.get("id") if isinstance(profile, dict) else None
        if not profile_id or profile_id in emitted_profile_ids:
            continue
        socketio.emit("new_message", message, room=f"profile_{profile_id}")
        emitted_profile_ids.add(profile_id)


def _display_name_for_node_num(node_num):
    if node_num in (None, 0):
        return None

    local_profile = db.get_profile_by_node_num(node_num)
    if local_profile:
        return local_profile.get("short_name") or local_profile.get("long_name") or local_profile.get("node_id")

    try:
        all_profiles = db.get_all_profiles()
    except Exception:
        all_profiles = {}

    for profile in all_profiles.values():
        mesh_store = _get_mesh_store(profile)
        if not mesh_store:
            continue
        try:
            found_node = mesh_store.get_node(node_num)
        except Exception:
            found_node = None
        if found_node:
            return found_node.get("short_name") or found_node.get("long_name") or found_node.get("node_id")

    return _node_id_from_num(node_num)


def _message_has_fallback_sender_display(message):
    if not message:
        return True
    sender_display = (message.get("sender_display") or "").strip()
    sender_id = (message.get("sender") or "").strip()
    sender_display_lower = sender_display.lower()
    sender_id_lower = sender_id.lower()
    if not sender_display or sender_display_lower == "unknown" or (sender_id and sender_display_lower == sender_id_lower):
        return True

    sender_num = message.get("sender_num")
    try:
        suffix = f"{int(sender_num):08x}"[-4:]
        generated_display = f"Meshtastic {suffix}"
    except (TypeError, ValueError):
        generated_display = None
    return bool(generated_display and sender_display_lower == generated_display.lower())


def _hydrate_message_sender_labels(messages, profile):
    mesh_store = _get_mesh_store(profile)
    if not mesh_store:
        return messages

    for message in messages or []:
        sender_num = message.get("sender_num")
        if sender_num in (None, 0):
            continue

        try:
            found_node = mesh_store.get_node(int(sender_num))
        except Exception:
            found_node = None
        if not found_node:
            continue

        preferred_display = found_node.get("long_name") or found_node.get("short_name") or found_node.get("node_id")
        if preferred_display and _message_has_fallback_sender_display(message):
            message["sender_display"] = preferred_display
            if message.get("direction") == "received" and message.get("channel") is not None:
                db.update_received_message_sender_display(message.get("channel"), int(sender_num), preferred_display)

        if found_node.get("short_name"):
            message["sender_short_name"] = found_node.get("short_name")

    return messages


def _get_chat_payload(profile):
    effective_profile = _effective_profile(profile)
    channel_number = _profile_channel_num(effective_profile)
    channel_messages = []
    seen_channel_numbers = set()
    for channel in _profile_channels(profile):
        channel_hash = _channel_number_for_config(channel)
        if channel_hash is None or channel_hash in seen_channel_numbers:
            continue
        seen_channel_numbers.add(channel_hash)
        channel_messages.extend(db.get_messages_for_channel(channel_hash))
    dm_messages = db.get_dm_messages_for_profile(effective_profile["id"]) if effective_profile else []
    _hydrate_message_sender_labels(channel_messages, effective_profile)
    _hydrate_message_sender_labels(dm_messages, effective_profile)
    return {
        "channel_messages": channel_messages,
        "dm_messages": dm_messages,
        "channel_number": channel_number,
        "selected_channel_index": effective_profile.get("selected_channel_index", 0) if effective_profile else 0,
    }


def _interface_status_for_profile(profile) -> str:
    if shared_packet_receiver.running:
        return "started"
    effective_profile = _effective_profile(profile)
    if not effective_profile:
        return "stopped"
    profile_channel_hash = generate_hash(effective_profile.get("channel", ""), effective_profile.get("key", ""))
    if (
        udp_server.running
        and udp_server.current_profile_id == effective_profile.get("id")
        and udp_server.current_channel_hash == profile_channel_hash
    ):
        return "started"
    return "stopped"


def _dm_owner_profile_for_packet(packet: mesh_pb2.MeshPacket):
    """Resolve which local profile should own a received DM."""
    target_node_num = getattr(packet, "to", None)
    if target_node_num in (None, 0, BROADCAST_NODE_NUM):
        return None
    return db.get_profile_by_node_num(target_node_num)


def _ensure_local_dm_ack(packet: mesh_pb2.MeshPacket, owner_profile) -> None:
    if not owner_profile:
        return
    if not getattr(packet, "want_ack", False):
        return
    if is_ack(packet) or is_nak(packet):
        return

    try:
        ack_packet_id = udp_server.send_ack_for_profile(owner_profile, packet)
        print(
            f"[ACK] Sent routing ACK for local DM packet {getattr(packet, 'id', None)} "
            f"as profile {owner_profile.get('id')} (ack packet {ack_packet_id})"
        )
    except Exception as e:
        print(
            f"[ACK] Failed to send routing ACK for local DM packet {getattr(packet, 'id', None)} "
            f"as profile {owner_profile.get('id')}: {e}"
        )


def _coerce_public_key_bytes(value):
    if not value:
        return None
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        try:
            return b64_decode(value.strip())
        except Exception:
            return None
    return None


def _sender_public_key_for_local_packet(owner_profile, sender_num):
    if sender_num in (None, 0):
        return None

    local_sender_profile = db.get_profile_by_node_num(sender_num)
    if local_sender_profile:
        local_public_key = virtual_node_manager.get_profile_public_key(local_sender_profile)
        if local_public_key is not None:
            return local_public_key

    mesh_store = _get_mesh_store(owner_profile) if owner_profile else None
    if not mesh_store:
        return None

    sender_node = mesh_store.get_node(sender_num)
    if not sender_node:
        return None
    return _coerce_public_key_bytes(sender_node.get("public_key"))


def _maybe_ack_local_direct_packet(packet: mesh_pb2.MeshPacket) -> None:
    owner_profile = _dm_owner_profile_for_packet(packet)
    if not owner_profile:
        return

    pkt_id = int(getattr(packet, "id", 0) or 0)
    if pkt_id > 0 and _already_seen(("local-direct-ack", owner_profile.get("id"), pkt_id)):
        return

    if packet.HasField("decoded"):
        if is_ack(packet) or is_nak(packet):
            return
        if int(getattr(packet.decoded, "portnum", 0) or 0) not in (
            int(portnums_pb2.PortNum.TEXT_MESSAGE_APP),
            int(portnums_pb2.PortNum.TEXT_MESSAGE_COMPRESSED_APP),
        ):
            return

    _ensure_local_dm_ack(packet, owner_profile)


def _decode_local_direct_packet(packet: mesh_pb2.MeshPacket):
    owner_profile = _dm_owner_profile_for_packet(packet)
    if not owner_profile:
        return None, None
    if packet.HasField("decoded"):
        return packet, owner_profile

    sender_public_key = _sender_public_key_for_local_packet(
        owner_profile,
        getattr(packet, "from", None),
    )
    decoded_packet = udp_server.decode_packet_for_profile(
        owner_profile,
        packet,
        sender_public_key=sender_public_key,
    )
    return decoded_packet, owner_profile


def _decode_packet_for_service(packet: mesh_pb2.MeshPacket):
    if packet.HasField("decoded"):
        return packet

    decoded_packet, owner_profile = _decode_local_direct_packet(packet)
    if decoded_packet is not None:
        return decoded_packet

    packet_channel = int(getattr(packet, "channel", 0) or 0)
    if packet_channel == 0:
        return None

    try:
        all_profiles = db.get_all_profiles()
    except Exception:
        return None

    for profile in all_profiles.values():
        for channel in _profile_channels(profile):
            try:
                if generate_hash(channel["name"], channel["key"]) != packet_channel:
                    continue
            except Exception:
                continue
            decoded_packet = udp_server.decode_packet_for_profile(profile, packet)
            if decoded_packet is not None:
                return decoded_packet
    return None


def _unread_state_for_profile(profile):
    effective_profile = _effective_profile(profile)
    if not effective_profile:
        return {"unread_channel_counts": {}, "unread_dm_counts": {}}

    channel_numbers = [
        channel.get("channel_number")
        for channel in _profile_channels_with_numbers(effective_profile)
        if channel.get("channel_number") is not None
    ]
    unread_channel_counts = db.get_unread_channel_counts(effective_profile["id"], channel_numbers)
    unread_dm_counts = db.get_unread_dm_counts(effective_profile["id"])
    return {
        "unread_channel_counts": unread_channel_counts,
        "unread_dm_counts": unread_dm_counts,
    }


_EXTRA_PORTNUM_FACTORIES = {
    int(portnums_pb2.PortNum.KEY_VERIFICATION_APP): mesh_pb2.KeyVerification,
    int(portnums_pb2.PortNum.STORE_FORWARD_PLUSPLUS_APP): mesh_pb2.StoreForwardPlusPlus,
    int(portnums_pb2.PortNum.NODE_STATUS_APP): mesh_pb2.StatusMessage,
}

_TEXT_PORTNUMS = {
    int(portnums_pb2.PortNum.TEXT_MESSAGE_APP),
    int(portnums_pb2.PortNum.RANGE_TEST_APP),
    int(portnums_pb2.PortNum.DETECTION_SENSOR_APP),
}


def _payload_factory_for_portnum(portnum):
    protocol = meshtastic.protocols.get(int(portnum or 0))
    if protocol and protocol.protobufFactory:
        return protocol.protobufFactory
    return _EXTRA_PORTNUM_FACTORIES.get(int(portnum or 0))


def _decode_payload_for_log(packet: mesh_pb2.MeshPacket):
    if not packet.HasField("decoded"):
        return None, None

    payload = bytes(packet.decoded.payload or b"")
    if not payload:
        return None, None

    portnum = int(packet.decoded.portnum or 0)
    if portnum in _TEXT_PORTNUMS:
        return "text", payload.decode("utf-8", "replace")

    factory = _payload_factory_for_portnum(portnum)
    if not factory:
        return None, None

    decoded_payload = factory()
    try:
        decoded_payload.ParseFromString(payload)
    except Exception:
        return None, None
    return "protobuf", decoded_payload


def _parse_route_discovery(packet: mesh_pb2.MeshPacket):
    if not packet.HasField("decoded"):
        return None

    payload = bytes(packet.decoded.payload or b"")
    if not payload:
        return mesh_pb2.RouteDiscovery()

    route_discovery = mesh_pb2.RouteDiscovery()
    try:
        route_discovery.ParseFromString(payload)
        return route_discovery
    except Exception:
        pass

    try:
        text_payload = payload.decode("utf-8", "ignore")
        text_format.Parse(text_payload, route_discovery)
        return route_discovery
    except Exception:
        return None


def _format_traceroute_path(origin_node_num, intermediate_nodes, snr_values, destination_node_num):
    unknown_snr = -128
    hops = []

    route_nodes = []
    if origin_node_num is not None:
        route_nodes.append(int(origin_node_num))
    route_nodes.extend(int(node_num) for node_num in intermediate_nodes or [])
    if destination_node_num is not None:
        route_nodes.append(int(destination_node_num))

    for index, node_num in enumerate(route_nodes):
        label = _display_name_for_node_num(node_num) or str(node_num)
        snr_value = None
        if snr_values and index < len(snr_values):
            raw_snr = int(snr_values[index])
            if raw_snr != unknown_snr:
                snr_value = raw_snr / 4
        hops.append(
            {
                "node_num": node_num,
                "node_id": _node_id_from_num(node_num),
                "label": label,
                "snr": snr_value,
            }
        )
    return hops


def _emit_traceroute_result(packet: mesh_pb2.MeshPacket, route_discovery: mesh_pb2.RouteDiscovery):
    owner_profile = _dm_owner_profile_for_packet(packet)
    if not owner_profile:
        matching_profiles = _profiles_matching_packet(packet)
        owner_profile = matching_profiles[0] if matching_profiles else None
    if not owner_profile:
        return

    result = {
        "packet_id": int(getattr(packet, "id", 0) or 0),
        "request_id": int(getattr(packet.decoded, "request_id", 0) or 0) if packet.HasField("decoded") else 0,
        "owner_profile_id": owner_profile.get("id"),
        "from_node_num": int(getattr(packet, "from", 0) or 0),
        "to_node_num": int(getattr(packet, "to", 0) or 0),
        "route_towards": _format_traceroute_path(
            getattr(packet, "to", None),
            list(route_discovery.route),
            list(route_discovery.snr_towards),
            getattr(packet, "from", None),
        ),
        "route_back": _format_traceroute_path(
            getattr(packet, "from", None),
            list(route_discovery.route_back),
            list(route_discovery.snr_back),
            getattr(packet, "to", None),
        ),
    }
    socketio.emit("traceroute_result", result, room=f"profile_{owner_profile.get('id')}")


def _packet_type_for_log(packet: mesh_pb2.MeshPacket) -> str:
    if packet.HasField("decoded"):
        try:
            return portnums_pb2.PortNum.Name(packet.decoded.portnum)
        except Exception:
            return str(int(getattr(packet.decoded, "portnum", 0) or 0))
    if getattr(packet, "encrypted", b""):
        return "ENCRYPTED"
    return "RAW"


def _payload_preview_for_log(packet: mesh_pb2.MeshPacket, max_len: int = 140) -> str:
    payload_kind, decoded_payload = _decode_payload_for_log(packet)
    if payload_kind == "text":
        preview = decoded_payload.encode("unicode_escape").decode("ascii")
    elif payload_kind == "protobuf":
        preview = text_format.MessageToString(decoded_payload, as_utf8=True).strip().replace("\n", " ")
    elif packet.HasField("decoded"):
        payload = bytes(packet.decoded.payload or b"")
        if payload:
            preview = payload[:24].hex(" ")
            if len(payload) > 24:
                preview += f" ... ({len(payload)} bytes)"
        else:
            preview = ""
    else:
        preview = f"{len(bytes(getattr(packet, 'encrypted', b'')))} encrypted bytes"

    preview = " ".join(str(preview).split())
    if len(preview) > max_len:
        return preview[: max_len - 3] + "..."
    return preview


def _log_packet_summary(packet: mesh_pb2.MeshPacket, addr=None):
    sender = _node_id_from_num(getattr(packet, "from", None)) or str(getattr(packet, "from", None))
    destination = getattr(packet, "to", None)
    if destination == BROADCAST_NODE_NUM:
        destination_label = "broadcast"
    else:
        destination_label = _node_id_from_num(destination) or str(destination)
    packet_type = _packet_type_for_log(packet)
    payload_preview = _payload_preview_for_log(packet)
    source_addr = addr[0] if isinstance(addr, tuple) and len(addr) >= 1 else addr
    print(
        f"[RECV] src={source_addr} from={sender} to={destination_label} "
        f"type={packet_type} payload={payload_preview}"
    )


def on_recieve(packet: mesh_pb2.MeshPacket, addr=None):
    supplemental_packet = _decode_packet_for_service(packet)
    packet_for_storage = supplemental_packet or packet

    _maybe_ack_local_direct_packet(packet)
    _log_packet_summary(packet_for_storage, addr=addr)

    for profile, mesh_store in _matching_mesh_stores_for_packet(packet_for_storage):
        try:
            mesh_store.record_packet(packet_for_storage)
        except Exception as e:
            print(f"[MESHDB] Failed to record packet for profile {profile.get('id')}: {e}")

    if supplemental_packet is not None and supplemental_packet.HasField("decoded"):
        supplemental_portnum = int(getattr(supplemental_packet.decoded, "portnum", 0) or 0)
        if supplemental_portnum == int(portnums_pb2.PortNum.TEXT_MESSAGE_APP):
            on_text_message(supplemental_packet, addr=addr)
        elif supplemental_portnum == int(portnums_pb2.PortNum.NODEINFO_APP):
            on_nodeinfo(supplemental_packet, addr=addr)
        elif supplemental_portnum == int(portnums_pb2.PortNum.TRACEROUTE_APP):
            on_traceroute(supplemental_packet, addr=addr)
        elif supplemental_portnum == int(portnums_pb2.PortNum.ROUTING_APP):
            routing = parse_routing(supplemental_packet)
            if routing is not None:
                if is_ack(supplemental_packet):
                    on_ack_message(supplemental_packet, routing=routing, addr=addr, pending=None)
                elif is_nak(supplemental_packet):
                    on_nak_message(supplemental_packet, routing=routing, addr=addr, pending=None)


def _my_node_num():
    """Return numeric node id for the active profile."""
    active_profile = udp_server.get_active_profile()
    if active_profile:
        active_num = _profile_node_num(active_profile)
        if active_num is not None:
            return active_num
    try:
        nid = getattr(node, "node_id", None)
        if isinstance(nid, str) and nid.startswith("!"):
            return int(nid[1:], 16)
    except Exception:
        pass
    return None


def _is_from_me(packet):
    try:
        my_num = _my_node_num()
        sender_num = getattr(packet, "from", None)
        return (my_num is not None) and (sender_num == my_num)
    except Exception:
        return False


def on_text_message(packet: mesh_pb2.MeshPacket, addr=None):
    msg = packet.decoded.payload.decode("utf-8", "ignore")
    if _is_from_me(packet):
        # Ignore our own messages received back from the network (we already mirror locally)
        return

    pkt_id = getattr(packet, "id", 0) or 0
    if not pkt_id:
        return
    if _already_seen(("id", pkt_id)):
        return

    print(f"[RECV] From: {getattr(packet, 'from', None)} Channel: {getattr(packet, 'channel', None)} Message: {msg}")

    # Store and broadcast the message for all relevant local profiles.
    try:
        sender_num = getattr(packet, "from", None)
        dm_owner_profile = _dm_owner_profile_for_packet(packet)
        message_type = "dm" if dm_owner_profile else "channel"
        target_node_num = getattr(packet, "to", None) if message_type == "dm" else None
        if target_node_num in (0, BROADCAST_NODE_NUM):
            target_node_num = None
        reply_packet_id = getattr(packet.decoded, "reply_id", None) or None
        owner_profile_id = dm_owner_profile.get("id") if dm_owner_profile else None
        message_channel = getattr(packet, "channel", None)
        if message_channel is None:
            matching_profiles = _profiles_matching_packet(packet)
            if matching_profiles:
                message_channel = _profile_channel_num(matching_profiles[0])

        # Look up node name from database if available
        sender_display = f"!{hex(sender_num)[2:].zfill(8)}" if sender_num else "Unknown"
        sender_short_name = None

        if sender_num:
            try:
                found_node = _find_sender_node_for_packet(packet, sender_num)
                if found_node:
                    sender_short_name = found_node.get("short_name") or None
                if found_node and found_node.get("long_name"):
                    sender_display = found_node["long_name"]
                    print(f"[MESSAGE] Using node name: {sender_display} for {sender_num}")
                else:
                    print(f"[MESSAGE] No node name found for {sender_num}, using hex: {sender_display}")

            except Exception as e:
                print(f"[MESSAGE] Error looking up node name: {e}")

        message = {
            "id": str(uuid.uuid4()),
            "sender_num": sender_num,
            "sender": f"!{hex(sender_num)[2:].zfill(8)}" if sender_num else "Unknown",
            "sender_display": sender_display,
            "sender_short_name": sender_short_name,
            "content": msg,
            "timestamp": datetime.now().isoformat(),
            "sender_ip": (addr[0] if isinstance(addr, tuple) and len(addr) >= 1 else "mesh"),
            "direction": "received",
            "message_type": message_type,
            "packet_id": pkt_id,
            "reply_packet_id": reply_packet_id,
            "target_node_num": target_node_num,
            "target": _node_id_from_num(target_node_num),
            "owner_profile_id": owner_profile_id,
            "channel": message_channel,
        }

        if message_channel is not None and (message_type == "channel" or owner_profile_id):
            print(f"[MESSAGE] Storing {message_type} message on channel {message_channel}: {msg[:50]}...")
            db.store_message(
                message_id=message["id"],
                packet_id=pkt_id,
                sender_num=getattr(packet, "from", None),
                sender_display=message["sender_display"],
                content=msg,
                sender_ip=message["sender_ip"],
                direction="received",
                channel=message_channel,
                message_type=message_type,
                reply_packet_id=reply_packet_id,
                target_node_num=target_node_num,
                owner_profile_id=owner_profile_id,
                hop_limit=packet.hop_limit,
                hop_start=packet.hop_start,
                rx_snr=packet.rx_snr,
                rx_rssi=packet.rx_rssi,
            )
        elif message_type == "dm":
            print(f"[MESSAGE] DM not addressed to a local profile - skipping storage for: {msg[:50]}...")
        else:
            # If no channel info, skip storage (can't determine channel)
            print(f"[MESSAGE] No channel info - skipping database storage for: {msg[:50]}...")

        if message_type == "channel" and message_channel is not None:
            room_name = f"channel_{message_channel}"
            print(f"[MESSAGE] Attempting to broadcast to WebSocket room: {room_name}")
            print(f"[MESSAGE] Message content: {msg[:50]}{'...' if len(msg) > 50 else ''}")
            print(f"[MESSAGE] From node: {sender_num} ({sender_display})")
            try:
                socketio.emit("new_message", message, room=room_name)
                _emit_message_to_profile_rooms(message, _profiles_matching_packet(packet))
                print(f"[MESSAGE] ✅ Successfully broadcasted message to room {room_name}")
            except Exception as e:
                print(f"[MESSAGE] ❌ Error broadcasting to room {room_name}: {e}")
                import traceback

                traceback.print_exc()
        elif message_type == "dm" and owner_profile_id:
            room_name = f"profile_{owner_profile_id}"
            try:
                socketio.emit("new_message", message, room=room_name)
                print(f"[MESSAGE] ✅ Successfully broadcasted DM to room {room_name}")
            except Exception as e:
                print(f"[MESSAGE] ❌ Error broadcasting DM to room {room_name}: {e}")
        else:
            print(f"[MESSAGE] No channel info - message not broadcasted to WebSocket")
    except Exception as e:
        print(f"Failed to emit incoming message: {e}")


def on_nodeinfo(packet: mesh_pb2.MeshPacket, addr=None):
    """Handle NODEINFO_APP packets and store/update node information"""
    print(f"\n[NODEINFO_DEBUG] on_nodeinfo called with packet from {getattr(packet, 'from', None)}")

    # We'll process nodeinfo for all matching profiles, not just current one
    print(f"[NODEINFO_DEBUG] Processing nodeinfo for channel {getattr(packet, 'channel', None)}")

    if not packet.HasField("decoded") or packet.decoded.portnum != portnums_pb2.PortNum.NODEINFO_APP:
        print(
            f"[NODEINFO_DEBUG] Not a NODEINFO packet: decoded={packet.HasField('decoded')}, portnum={packet.decoded.portnum if packet.HasField('decoded') else 'N/A'}"
        )
        return

    sender_num = getattr(packet, "from", None)
    if not sender_num:
        print(f"[NODEINFO_DEBUG] No sender number")
        return

    pkt_id = getattr(packet, "id", 0) or 0
    if pkt_id and _already_seen(("nodeinfo", pkt_id)):
        print(f"[NODEINFO_DEBUG] Already processed packet {pkt_id}")
        return

    print(f"[NODEINFO_DEBUG] Processing NODEINFO from {sender_num}, packet ID {pkt_id}")

    # Check if this packet is addressed to us and has want_response: True
    packet_to = getattr(packet, "to", None)
    my_node_num = _my_node_num()

    print(f"[NODEINFO_DEBUG] Packet 'to': {packet_to}, My node num: {my_node_num}")

    if packet_to == my_node_num and my_node_num is not None:
        print(f"[NODEINFO_RESPONSE] Packet addressed to us ({my_node_num})")

        # Check for want_response in the packet
        # This might be in the decoded bitfield or we need to parse the payload more carefully
        want_response = getattr(packet.decoded, "want_response", False)

        # Log detailed packet info for debugging
        print(f"[NODEINFO_RESPONSE] Packet analysis:")
        print(f"  - want_response attr: {getattr(packet.decoded, 'want_response', 'N/A')}")
        print(
            f"  - bitfield: {getattr(packet.decoded, 'bitfield', 'N/A')} (binary: {bin(getattr(packet.decoded, 'bitfield', 0)) if hasattr(packet.decoded, 'bitfield') and packet.decoded.bitfield else 'N/A'})"
        )
        print(f"  - Final want_response: {want_response}")

        if want_response:
            print(f"[NODEINFO_RESPONSE] Sending nodeinfo response to {sender_num}")
            try:
                udp_server.send_nodeinfo(destination=sender_num)
                print(f"[NODEINFO_RESPONSE] ✅ Sent nodeinfo response to node {sender_num}")
            except Exception as e:
                print(f"[NODEINFO_RESPONSE] ❌ Failed to send nodeinfo response: {e}")
        else:
            print(f"[NODEINFO_RESPONSE] No response requested")
    else:
        print(f"[NODEINFO_DEBUG] Packet not addressed to us (to: {packet_to}, us: {my_node_num})")

    try:
        target_profiles = _profiles_matching_packet(packet)
        if not target_profiles:
            print("[NODEINFO_DEBUG] No matching local profiles; skipping node persistence")
            return

        for profile in target_profiles:
            mesh_store = _get_mesh_store(profile)
            if not mesh_store:
                print(
                    f"[NODEINFO_DEBUG] No mesh store for profile {profile.get('id')}; "
                    f"skipping persistence for node {sender_num}"
                )
                continue

            before = mesh_store.get_node(sender_num)
            stored = mesh_store.record_packet(packet)
            after = mesh_store.get_node(sender_num)
            print(
                f"[NODEINFO_DEBUG] meshdb stored packet for profile {profile.get('id')}: {stored}"
            )

            if after is None:
                print(
                    f"[NODEINFO_DEBUG] meshdb did not return node {sender_num} after NODEINFO packet "
                    f"for profile {profile.get('id')}"
                )
                continue

            if before is None:
                print(
                    f"\n[NODEINFO] NEW NODE {sender_num} for profile {profile.get('id')}: "
                    f"{after.get('long_name')} ({after.get('short_name')})"
                )
            elif before != after:
                print(
                    f"\n[NODEINFO] UPDATED NODE {sender_num} for profile {profile.get('id')}: "
                    f"{after.get('long_name')} ({after.get('short_name')})"
                )
            else:
                print(
                    f"\n[NODEINFO] SEEN NODE {sender_num} for profile {profile.get('id')}: "
                    f"{after.get('long_name')} ({after.get('short_name')})"
                )

            display_name = after.get("long_name") or after.get("short_name") or after.get("node_id")
            if display_name:
                updated_rows = db.update_received_message_sender_display(
                    _profile_channel_num(profile),
                    sender_num,
                    display_name,
                )
                if updated_rows:
                    print(
                        f"[NODEINFO] Updated {updated_rows} stored message labels for node {sender_num} "
                        f"on profile {profile.get('id')}"
                    )

            _emit_node_update(profile.get("id"), after)

    except Exception as e:
        print(f"Error processing nodeinfo: {e}")


def on_traceroute(packet: mesh_pb2.MeshPacket, addr=None):
    del addr
    if _is_from_me(packet):
        return
    pkt_id = getattr(packet, "id", 0) or 0
    if pkt_id and _already_seen(("traceroute", pkt_id)):
        return

    route_discovery = _parse_route_discovery(packet)
    if route_discovery is None:
        print(f"[TRACEROUTE] Failed to parse traceroute payload for packet {pkt_id}")
        return

    print(
        f"[TRACEROUTE] from={_node_id_from_num(getattr(packet, 'from', None))} "
        f"to={_node_id_from_num(getattr(packet, 'to', None))} "
        f"towards={len(route_discovery.route)} back={len(route_discovery.route_back)}"
    )
    _emit_traceroute_result(packet, route_discovery)


def on_ack_message(packet, routing=None, addr=None, pending=None):
    del addr, pending
    if not packet.HasField("decoded"):
        return

    request_id = int(getattr(packet.decoded, "request_id", 0) or 0)
    if request_id <= 0:
        return

    sender_num = getattr(packet, "from", None)
    ack_status = "implicit_ack" if sender_num is not None and _my_node_num() == int(sender_num) else "ack"
    print(f"[ACK] Received {ack_status} for packet {request_id}")
    _apply_ack_update(request_id, ack_status)


def on_nak_message(packet, routing=None, addr=None, pending=None):
    del addr, pending
    if not packet.HasField("decoded"):
        return

    request_id = int(getattr(packet.decoded, "request_id", 0) or 0)
    if request_id <= 0:
        return

    error_name = _routing_error_name(getattr(routing, "error_reason", None))
    print(f"[ACK] Received NAK for packet {request_id}: {error_name}")
    _apply_ack_update(request_id, "nak", error_name)


def on_max_retransmit(pending=None, error_reason=None):
    request_id = int(getattr(pending, "packet_id", 0) or 0)
    if request_id <= 0:
        return

    error_name = _routing_error_name(error_reason) or "MAX_RETRANSMIT"
    print(f"[ACK] Max retransmit reached for packet {request_id}: {error_name}")
    _apply_ack_update(request_id, "max_retransmit", error_name)


def on_meshtastic_text(packet, interface=None):
    del interface
    raw_packet = packet.get("raw") if isinstance(packet, dict) else None
    if isinstance(raw_packet, mesh_pb2.MeshPacket) and raw_packet.HasField("decoded"):
        on_text_message(raw_packet, addr=("mesh", 0))


def on_meshtastic_routing(packet, interface=None):
    del interface
    raw_packet = packet.get("raw") if isinstance(packet, dict) else None
    if not isinstance(raw_packet, mesh_pb2.MeshPacket) or not raw_packet.HasField("decoded"):
        return

    routing = parse_routing(raw_packet)
    if routing is None:
        return

    if is_ack(raw_packet):
        on_ack_message(raw_packet, routing=routing, addr=("mesh", 0), pending=None)
    elif is_nak(raw_packet):
        on_nak_message(raw_packet, routing=routing, addr=("mesh", 0), pending=None)


pub.subscribe(on_recieve, "mesh.rx.packet")
pub.subscribe(on_text_message, "mesh.rx.text")
pub.subscribe(on_meshtastic_text, "meshtastic.receive.text")
pub.subscribe(on_meshtastic_routing, "meshtastic.receive.routing")
pub.subscribe(on_nodeinfo, "mesh.rx.port.4")  # NODEINFO_APP
pub.subscribe(on_traceroute, f"mesh.rx.port.{int(portnums_pb2.PortNum.TRACEROUTE_APP)}")
pub.subscribe(on_ack_message, "mesh.rx.ack")
pub.subscribe(on_nak_message, "mesh.rx.nak")
pub.subscribe(on_max_retransmit, "mesh.tx.max_retransmit")


app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("FIREFLY_SECRET_KEY", DEFAULT_SECRET_KEY)
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading",
    logger=get_logger("socketio.server"),
    engineio_logger=get_logger("engineio.server"),
)


@app.route("/service-worker.js")
def service_worker():
    response = send_from_directory(app.static_folder, "service-worker.js", mimetype="application/javascript")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["Service-Worker-Allowed"] = "/"
    return response


@app.context_processor
def inject_versions():
    return {
        "socketio_client_version": SOCKETIO_CLIENT_VERSION,
        "python_socketio_version": getattr(py_socketio, "__version__", "unknown"),
        "python_engineio_version": getattr(engineio, "__version__", "unknown"),
        "current_user": _get_session_user(),
    }


class ProfileManager:
    def __init__(self, database):
        self.db = database

    def get_all_profiles(self, user_id=None):
        """Get all profiles"""
        return self.db.get_all_profiles(user_id=user_id)

    def get_profile(self, profile_id, user_id=None):
        """Get a specific profile"""
        return self.db.get_profile(profile_id, user_id=user_id)

    def create_profile(self, profile_id, user_id, node_id, long_name, short_name, channels, hop_limit=3):
        """Create a new profile"""
        return self.db.create_profile(profile_id, user_id, node_id, long_name, short_name, channels, hop_limit)

    def update_profile(self, profile_id, user_id, node_id, long_name, short_name, channels, hop_limit=3):
        """Update an existing profile"""
        return self.db.update_profile(profile_id, user_id, node_id, long_name, short_name, channels, hop_limit)

    def delete_profile(self, profile_id, user_id):
        """Delete a profile"""
        return self.db.delete_profile(profile_id, user_id)


def create_interface_for_profile(profile):
    """Create a virtual node for the profile."""
    if not profile:
        print("[INTERFACE] No profile provided, cannot create interface")
        return None

    profile_key = profile.get("key", "")
    if not profile_key:
        print(f"[INTERFACE] Profile {profile.get('id', 'unknown')} has no key")
        return None

    try:
        interface = virtual_node_manager.start(profile)
        print(
            f"[INTERFACE] Created virtual node for profile {profile.get('long_name', 'unknown')} with key {profile_key[:8]}..."
        )
        return interface

    except Exception as e:
        print(f"[INTERFACE] Error creating interface: {e}")
        return None


class UDPChatServer:
    def __init__(self):
        self.running = False
        self.current_profile_id = None
        self.current_channel_hash = None
        self.current_profile = None
        self.last_send_error = None
        self.active_sessions = {}  # Maps session_id -> {profile_id, channel_hash, timestamp}

    def start(self, profile=None):
        """Start the virtual node receiver with profile-specific identity."""
        if not profile:
            print("[UDP] Cannot start without a profile (need encryption key)")
            return False

        vnode = create_interface_for_profile(profile)
        if not vnode:
            return False

        try:
            self.running = True
            effective_profile = _effective_profile(profile)
            self.current_profile_id = effective_profile.get("id")
            self.current_profile = effective_profile
            self.current_channel_hash = generate_hash(effective_profile.get("channel", ""), effective_profile.get("key", ""))
            print(
                f"[UDP] Started virtual node for profile {effective_profile.get('long_name', 'unknown')} on channel {self.current_channel_hash}"
            )
            print(f"[UDP] Listening on {MCAST_GRP}:{MCAST_PORT} with vnode identity")
            return True
        except Exception as e:
            print(f"[UDP] Failed to start virtual node: {e}")
            return False

    def stop(self):
        """Stop the active virtual node."""
        self.running = False
        self.current_profile_id = None
        self.current_channel_hash = None
        self.current_profile = None
        self.active_sessions.clear()
        try:
            if virtual_node_manager.running:
                virtual_node_manager.stop()
                print("[UDP] Virtual node stopped")
        except Exception as e:
            print(f"[UDP] Error stopping interface: {e}")
    
    def register_session(self, session_id, profile_id, channel_hash):
        """Register a session as using this UDP interface"""
        import time
        self.active_sessions[session_id] = {
            "profile_id": profile_id,
            "channel_hash": channel_hash,
            "timestamp": time.time()
        }
        print(f"[UDP] Registered session {session_id} for profile {profile_id} on channel {channel_hash}")
        print(f"[UDP] Active sessions: {len(self.active_sessions)}")
    
    def unregister_session(self, session_id):
        """Unregister a session from using this UDP interface"""
        if session_id in self.active_sessions:
            session_info = self.active_sessions.pop(session_id)
            print(f"[UDP] Unregistered session {session_id} (was using channel {session_info['channel_hash']})")
            print(f"[UDP] Active sessions remaining: {len(self.active_sessions)}")
            
            # If no more sessions are active, stop the interface
            if not self.active_sessions and self.running:
                print("[UDP] No active sessions remaining, stopping interface")
                self.stop()
        else:
            print(f"[UDP] Warning: Attempted to unregister unknown session {session_id}")

    def get_sessions_for_profile(self, profile_id):
        """Get all active sessions using a specific profile."""
        sessions = []
        for session_id, session_info in self.active_sessions.items():
            if session_info["profile_id"] == profile_id:
                sessions.append(session_id)
        return sessions

    def can_switch_transport(self, profile_id, channel_hash, requester_session_id=None):
        """Check if we can safely switch to a new vnode identity and channel."""
        if not self.running:
            return True

        if self.current_profile_id == profile_id and self.current_channel_hash == channel_hash:
            return True

        other_sessions = [sid for sid in self.active_sessions if sid != requester_session_id]
        if not other_sessions:
            return True

        print(
            f"[UDP] Cannot switch transport: {len(other_sessions)} other sessions are using profile {self.current_profile_id} channel {self.current_channel_hash}"
        )
        return False

    def restart_with_profile(self, profile, session_id=None):
        """Start or reuse the active vnode for a profile."""
        if not profile:
            return False
        effective_profile = _effective_profile(profile)
        channel_hash = generate_hash(effective_profile.get("channel", ""), effective_profile.get("key", ""))

        if self.running and self.current_profile_id == effective_profile.get("id") and self.current_channel_hash == channel_hash:
            print(
                f"[UDP] Reusing existing virtual node for profile {effective_profile.get('long_name', 'unknown')} on channel {channel_hash}"
            )
            return True

        if not self.can_switch_transport(effective_profile.get("id"), channel_hash, requester_session_id=session_id):
            print(
                f"[UDP] Cannot switch to profile {effective_profile.get('id')} channel {channel_hash} - other sessions are active on profile {self.current_profile_id}"
            )
            return False

        print(f"[UDP] Starting virtual node for profile {effective_profile.get('long_name', 'unknown')}")
        return self.start(effective_profile)

    def reconnect_with_profile(self, profile, session_id=None):
        """Force a fresh transport restart for a profile."""
        if not profile:
            return False
        effective_profile = _effective_profile(profile)
        channel_hash = generate_hash(effective_profile.get("channel", ""), effective_profile.get("key", ""))

        if not self.can_switch_transport(effective_profile.get("id"), channel_hash, requester_session_id=session_id):
            print(
                f"[UDP] Cannot reconnect to profile {effective_profile.get('id')} channel {channel_hash} - other sessions are active on profile {self.current_profile_id}"
            )
            return False

        if self.running:
            print(
                f"[UDP] Forcing transport restart from profile {self.current_profile_id} to {effective_profile.get('id')}"
            )
            self.stop()

        return self.start(effective_profile)

    def get_active_profile(self):
        return self.current_profile

    def _ensure_sender_profile_transport(self, sender_profile, session_id=None):
        if not sender_profile:
            self.last_send_error = "No sender profile selected"
            return None

        sender_profile = _effective_profile(sender_profile)
        if not self.running or self.current_profile_id != sender_profile.get("id"):
            print("[UDP] Active virtual node does not match sender profile, restarting...")
            if not self.restart_with_profile(sender_profile, session_id=session_id):
                self.last_send_error = "Transmit identity could not be started for this profile"
                return None
        return sender_profile

    def send_nodeinfo(self, destination=None):
        if not self.running:
            raise RuntimeError("Virtual node is not running")
        return virtual_node_manager.send_nodeinfo(destination if destination is not None else 0xFFFFFFFF)

    def exchange_nodeinfo(self, sender_profile, destination, session_id=None):
        self.last_send_error = None
        sender_profile = self._ensure_sender_profile_transport(sender_profile, session_id=session_id)
        if not sender_profile:
            return False
        try:
            return virtual_node_manager.send_nodeinfo(int(destination), want_response=True)
        except Exception as e:
            self.last_send_error = str(e) or "Failed to exchange node info"
            print(f"[NODEINFO] Failed to exchange node info with {destination}: {e}")
            return False

    def send_traceroute(self, sender_profile, destination, session_id=None, hop_limit=None):
        self.last_send_error = None
        sender_profile = self._ensure_sender_profile_transport(sender_profile, session_id=session_id)
        if not sender_profile:
            return False
        try:
            effective_hop_limit = hop_limit if hop_limit is not None else sender_profile.get("hop_limit", 3)
            return virtual_node_manager.send_traceroute(int(destination), hop_limit=int(effective_hop_limit))
        except Exception as e:
            self.last_send_error = str(e) or "Failed to send traceroute"
            print(f"[TRACEROUTE] Failed to send traceroute to {destination}: {e}")
            return False

    def send_ack_for_profile(self, profile, request_packet):
        return virtual_node_manager.send_ack_for_profile(profile, request_packet)

    def decode_packet_for_profile(self, profile, packet, sender_public_key=None):
        return virtual_node_manager.decode_packet_for_profile(
            profile,
            packet,
            sender_public_key=sender_public_key,
        )

    def send_message(
        self,
        message_content,
        sender_profile,
        session_id=None,
        message_type="channel",
        target_node_num=None,
        reply_packet_id=None,
    ):
        """Send a text message via vnode."""
        self.last_send_error = None
        sender_profile = self._ensure_sender_profile_transport(sender_profile, session_id=session_id)
        if not sender_profile:
            return False
        sender_channel_hash = generate_hash(sender_profile.get("channel", ""), sender_profile.get("key", ""))
        print(f"[SEND] Debug: Profile '{sender_profile.get('long_name', 'Unknown')}' wants channel {sender_channel_hash}")
        print(f"[SEND] Debug: UDP server running={self.running}, current_profile={self.current_profile_id}")

        try:
            hop_limit = sender_profile.get("hop_limit", 3)
            ack_requested = message_type == "dm"
            sender_channel = generate_hash(sender_profile.get("channel", ""), sender_profile.get("key", ""))
            if message_type == "dm":
                if target_node_num in (None, 0, BROADCAST_NODE_NUM):
                    print("[SEND] Refusing to send DM without a valid target node")
                    self.last_send_error = "Direct messages require a valid target node"
                    return False
                print(f"[SEND] Sending DM to {target_node_num} with hop_limit={hop_limit}")
                sent_packet_id = virtual_node_manager.send_text(
                    message_content,
                    destination=int(target_node_num),
                    hop_limit=hop_limit,
                    reply_id=reply_packet_id,
                )
            else:
                print(f"[SEND] Sending channel message with hop_limit={hop_limit}")
                sent_packet_id = virtual_node_manager.send_text(
                    message_content,
                    hop_limit=hop_limit,
                    reply_id=reply_packet_id,
                )

            sender_node_id = sender_profile.get("node_id", "Unknown")
            my_node_num = None
            try:
                if sender_node_id and sender_node_id.startswith("!"):
                    my_node_num = int(sender_node_id[1:], 16)
            except Exception:
                pass
            message = {
                "id": str(uuid.uuid4()),
                "packet_id": sent_packet_id,
                "sender": sender_node_id,
                "sender_num": my_node_num,
                "sender_display": sender_profile.get("long_name", "Unknown"),
                "sender_short_name": sender_profile.get("short_name") or None,
                "content": message_content,
                "timestamp": datetime.now().isoformat(),
                "sender_ip": "self",  # Keep for backward compatibility but use sender for identity
                "direction": "sent",
                "message_type": message_type,
                "channel": sender_channel if message_type == "channel" else None,
                "reply_packet_id": reply_packet_id,
                "target_node_num": int(target_node_num) if target_node_num not in (None, "", 0, BROADCAST_NODE_NUM) else None,
                "target": _node_id_from_num(target_node_num) if target_node_num not in (None, "", 0, BROADCAST_NODE_NUM) else None,
                "owner_profile_id": sender_profile.get("id") if message_type == "dm" else None,
                "ack_requested": ack_requested,
                "ack_status": "pending" if ack_requested else None,
                "ack_error": None,
                "ack_updated_at": datetime.now().isoformat() if ack_requested else None,
            }
            local_recipient_message = None

            if message_type == "dm" and message["target_node_num"] is not None:
                recipient_profile = db.get_profile_by_node_num(message["target_node_num"])
                if recipient_profile and recipient_profile.get("id") != sender_profile.get("id"):
                    local_recipient_message = {
                        "id": str(uuid.uuid4()),
                        "packet_id": sent_packet_id,
                        "sender": sender_node_id,
                        "sender_num": my_node_num,
                        "sender_display": message["sender_display"],
                        "sender_short_name": message["sender_short_name"],
                        "content": message_content,
                        "timestamp": message["timestamp"],
                        "sender_ip": "local",
                        "direction": "received",
                        "message_type": "dm",
                        "channel": sender_channel,
                        "reply_packet_id": reply_packet_id,
                        "target_node_num": message["target_node_num"],
                        "target": message["target"],
                        "owner_profile_id": recipient_profile.get("id"),
                        "ack_requested": False,
                        "ack_status": None,
                        "ack_error": None,
                        "ack_updated_at": None,
                    }

            # Store sent message in database and broadcast to WebSocket
            try:
                if sender_channel is not None:
                    print(f"[SEND] Storing sent message on channel {sender_channel}: {message_content[:50]}...")
                    db.store_message(
                        message_id=message["id"],
                        packet_id=sent_packet_id,
                        sender_num=my_node_num,
                        sender_display=message["sender_display"],
                        content=message_content,
                        sender_ip="self",
                        direction="sent",
                        channel=sender_channel,
                        message_type=message_type,
                        reply_packet_id=reply_packet_id,
                        target_node_num=message["target_node_num"],
                        owner_profile_id=message["owner_profile_id"],
                        hop_limit=None,
                        hop_start=None,
                        rx_snr=None,
                        rx_rssi=None,
                        ack_requested=message["ack_requested"],
                        ack_status=message["ack_status"],
                        ack_error=message["ack_error"],
                        ack_updated_at=message["ack_updated_at"],
                    )
                    print(f"[SEND] Sent message stored successfully on channel {sender_channel}")

                    if local_recipient_message is not None:
                        db.store_message(
                            message_id=local_recipient_message["id"],
                            packet_id=sent_packet_id,
                            sender_num=my_node_num,
                            sender_display=local_recipient_message["sender_display"],
                            content=message_content,
                            sender_ip=local_recipient_message["sender_ip"],
                            direction="received",
                            channel=sender_channel,
                            message_type="dm",
                            reply_packet_id=reply_packet_id,
                            target_node_num=local_recipient_message["target_node_num"],
                            owner_profile_id=local_recipient_message["owner_profile_id"],
                            hop_limit=None,
                            hop_start=None,
                            rx_snr=None,
                            rx_rssi=None,
                            ack_requested=False,
                            ack_status=None,
                            ack_error=None,
                            ack_updated_at=None,
                        )
                        recipient_room_name = f"profile_{local_recipient_message['owner_profile_id']}"
                        socketio.emit("new_message", local_recipient_message, room=recipient_room_name)
                        print(
                            f"[SEND] Stored and broadcasted local recipient DM copy to room {recipient_room_name}"
                        )
                    
                    room_name = f"channel_{sender_channel}" if message_type == "channel" else f"profile_{sender_profile.get('id')}"
                    try:
                        socketio.emit("new_message", message, room=room_name)
                        if message_type == "channel":
                            _emit_message_to_profile_rooms(message, [sender_profile])
                        print(f"[SEND] ✅ Broadcasted sent {message_type} message to room {room_name}")
                    except Exception as e:
                        print(f"[SEND] ❌ Error broadcasting sent message to room {room_name}: {e}")
                        import traceback
                        traceback.print_exc()
                else:
                    print(f"[SEND] Cannot determine channel from sender profile - message not stored in database")
            except Exception as e:
                print(f"Failed to store/broadcast sent message: {e}")
                import traceback
                traceback.print_exc()
            return True
        except Exception as e:
            print(f"Error sending message via mudp: {e}")
            self.last_send_error = str(e) or "Failed to send message"
            return False


# Initialize managers
profile_manager = ProfileManager(db)
udp_server = UDPChatServer()


@app.route("/register", methods=["POST"])
def register():
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""

    if not username or not password:
        flash("Username and password are required.", "error")
        return redirect(url_for("index"))

    if db.get_user_by_username(username):
        flash("That username is already in use.", "error")
        return redirect(url_for("index"))

    user_id = str(uuid.uuid4())
    password_hash = generate_password_hash(password)
    if not db.create_user(user_id, username, password_hash):
        flash("Unable to create account.", "error")
        return redirect(url_for("index"))

    if db.count_users() == 1:
        claimed_profiles = db.claim_orphan_profiles(user_id)
        if claimed_profiles:
            print(f"[AUTH] Claimed {claimed_profiles} orphan profiles for first user {username}")

    user = db.get_user_by_id(user_id)
    _unregister_current_session_transport()
    _clear_session_user()
    _set_session_user(user)
    flash("Account created.", "success")
    return redirect(url_for("index"))


@app.route("/login", methods=["POST"])
def login():
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    is_async_request = request.headers.get("X-Requested-With") == "fetch"

    def fail(message, status=400):
        if is_async_request:
            return jsonify({"error": message}), status
        flash(message, "error")
        return redirect(url_for("index"))

    if not username:
        return fail("Username is required.")
    if not password:
        return fail("Password is required.")

    user = db.get_user_by_username(username)
    if not user:
        return fail("No account exists for that username.", 404)
    if not check_password_hash(user["password_hash"], password):
        return fail("Password is incorrect.", 401)

    _unregister_current_session_transport()
    _clear_session_user()
    _set_session_user(user)
    if is_async_request:
        return jsonify({"redirect_url": url_for("index")})
    return redirect(url_for("index"))


@app.route("/logout", methods=["POST"])
@login_required
def logout():
    _unregister_current_session_transport()
    _clear_session_user()
    flash("Signed out.", "success")
    return redirect(url_for("index"))


@app.route("/account/password", methods=["POST"])
@login_required
def update_account_password():
    current_user = _get_session_user()
    current_password = request.form.get("current_password") or ""
    new_password = request.form.get("new_password") or ""
    confirm_password = request.form.get("confirm_password") or ""

    if not current_password or not new_password or not confirm_password:
        flash("Current password, new password, and confirmation are required.", "error")
        return redirect(url_for("index"))

    if not check_password_hash(current_user["password_hash"], current_password):
        flash("Current password is incorrect.", "error")
        return redirect(url_for("index"))

    if new_password != confirm_password:
        flash("New password and confirmation do not match.", "error")
        return redirect(url_for("index"))

    password_hash = generate_password_hash(new_password)
    if not db.update_user_password(current_user["id"], password_hash):
        flash("Unable to update password.", "error")
        return redirect(url_for("index"))

    flash("Password updated.", "success")
    return redirect(url_for("index"))


@app.route("/account/delete", methods=["POST"])
@login_required
def delete_account():
    current_user = _get_session_user()
    current_password = request.form.get("current_password") or ""

    if not current_password:
        flash("Current password is required to delete your account.", "error")
        return redirect(url_for("index"))

    if not check_password_hash(current_user["password_hash"], current_password):
        flash("Current password is incorrect.", "error")
        return redirect(url_for("index"))

    _unregister_current_session_transport()
    _clear_session_user()

    if not db.delete_user(current_user["id"]):
        flash("Unable to delete account.", "error")
        return redirect(url_for("index"))

    flash("Account deleted.", "success")
    return redirect(url_for("index"))


@app.route("/")
def index():
    """Main chat interface"""
    current_user = _get_session_user()
    if not current_user:
        return render_template("auth.html")

    profiles = profile_manager.get_all_profiles(user_id=current_user["id"])
    current_profile = _get_session_profile()
    return render_template("index.html", profiles=profiles, current_profile=current_profile)


@app.route("/profiles")
@login_required
def profiles():
    """Redirect legacy profile route into the single-page app."""
    return redirect(url_for("index", tab="profiles"))


@app.route("/nodes")
@login_required
def nodes():
    """Redirect legacy nodes route into the single-page app."""
    return redirect(url_for("index", tab="nodes"))


@app.route("/map")
@login_required
def map_view():
    """Redirect legacy map route into the single-page app."""
    return redirect(url_for("index", tab="map"))


@app.route("/api/profiles", methods=["GET"])
@login_required
def get_profiles():
    """Get all profiles"""
    current_user = _get_session_user()
    return jsonify(profile_manager.get_all_profiles(user_id=current_user["id"]))


@app.route("/api/profiles", methods=["POST"])
@login_required
def create_profile():
    """Create a new profile"""
    data = request.get_json(silent=True) or {}
    current_user = _get_session_user()
    validated, error = _validate_profile_request_payload(data)
    if error:
        return jsonify({"error": error}), 400

    normalized_node_id = validated["node_id"]
    channels = validated["channels"]
    for index, channel in enumerate(channels, start=1):
        if not _is_valid_channel_key(channel.get("key")):
            return jsonify({"error": f"channel {index} key must be base64 for 1 byte, 128 bits, or 256 bits"}), 400
    conflict_error = _profile_node_id_conflict_error(normalized_node_id)
    if conflict_error:
        return jsonify({"error": conflict_error}), 409

    hop_limit = validated["hop_limit"]
    profile_id = str(uuid.uuid4())
    success = profile_manager.create_profile(
        profile_id,
        current_user["id"],
        normalized_node_id,
        validated["long_name"],
        validated["short_name"],
        channels,
        hop_limit,
    )

    if success:
        created_profile = profile_manager.get_profile(profile_id, user_id=current_user["id"])
        if created_profile:
            _seed_profile_mesh_identity(created_profile)
        return jsonify({"profile_id": profile_id, "message": "Profile created successfully"})
    else:
        return jsonify({"error": "Failed to create profile. Node ID may already be in use."}), 409


@app.route("/api/profiles/<profile_id>", methods=["PUT"])
@login_required
def update_profile(profile_id):
    """Update an existing profile"""
    data = request.get_json(silent=True) or {}
    current_user = _get_session_user()
    existing_profile = profile_manager.get_profile(profile_id, user_id=current_user["id"])
    if not existing_profile:
        return jsonify({"error": "Profile not found"}), 404

    validated, error = _validate_profile_request_payload(data)
    if error:
        return jsonify({"error": error}), 400

    normalized_node_id = validated["node_id"]
    channels = validated["channels"]
    conflict_error = _profile_node_id_conflict_error(normalized_node_id, exclude_profile_id=profile_id)
    if conflict_error:
        return jsonify({"error": conflict_error}), 409

    hop_limit = validated["hop_limit"]

    success = profile_manager.update_profile(
        profile_id,
        current_user["id"],
        normalized_node_id,
        validated["long_name"],
        validated["short_name"],
        channels,
        hop_limit,
    )

    if success:
        updated_profile = profile_manager.get_profile(profile_id, user_id=current_user["id"])
        if updated_profile:
            _seed_profile_mesh_identity(updated_profile)
        return jsonify({"message": "Profile updated successfully"})
    else:
        return jsonify({"error": "Failed to update profile"}), 500


@app.route("/api/profiles/<profile_id>", methods=["DELETE"])
@login_required
def delete_profile(profile_id):
    """Delete a profile"""
    current_profile = _get_session_profile()
    current_user = _get_session_user()
    success = profile_manager.delete_profile(profile_id, current_user["id"])

    if success:
        # If this was the current profile, unset it
        if current_profile and current_profile.get("id") == profile_id:
            _clear_session_profile()
        return jsonify({"message": "Profile deleted successfully"})
    else:
        return jsonify({"error": "Profile not found"}), 404


@app.route("/api/current-profile", methods=["GET"])
@login_required
def get_current_profile():
    """Get the current active profile with interface status and channel number"""
    current_profile = _get_session_profile()
    if current_profile:
        interface_status = _interface_status_for_profile(current_profile)

        # Get channel number for WebSocket room joining
        expected_channel = _current_profile_channel_num()

        # Return profile with interface status and channel number
        response_data = _serialize_profile_for_client(current_profile, interface_status=interface_status)
        response_data["channel_number"] = expected_channel
        response_data.update(_unread_state_for_profile(current_profile))
        return jsonify(response_data)
    else:
        return jsonify(None)


@app.route("/api/current-profile", methods=["POST"])
@login_required
def set_current_profile():
    """Set the current active profile and restart interface with new key"""

    data = request.get_json()
    profile_id = data.get("profile_id")
    requested_channel_index = data.get("channel_index", 0)

    if not profile_id:
        # Unset profile and unregister session
        _unregister_current_session_transport()
        _clear_session_profile()
        print("[PROFILE] Profile unset, session unregistered")
        return jsonify(
            {"message": "Profile unset", "profile": None, "channel_messages": [], "dm_messages": []}
        )

    current_user = _get_session_user()
    profile = profile_manager.get_profile(profile_id, user_id=current_user["id"])
    print(f"[API] set_current_profile -> requested id={profile_id} exists={bool(profile)}")

    if profile:
        # Unregister any previous profile for this session
        old_profile = _get_session_profile()
        session_id = f"flask_session_{id(session)}"
        if old_profile:
            udp_server.unregister_session(session_id)
        
        # Set current profile in session
        _set_session_profile(profile)
        _set_session_channel_index(requested_channel_index)
        current_profile = profile

        print(f"[PROFILE] Loaded profile {profile.get('long_name', 'unknown')} for this session")

        # Calculate channel hash for session registration
        channel_hash = _current_profile_channel_num()
        
        # Get messages for the newly selected profile's channel
        chat_payload = _get_chat_payload(current_profile)
        expected_channel = chat_payload["channel_number"]
        if expected_channel is not None:
            unread_messages = db.get_unread_messages_for_channel(current_profile["id"], expected_channel)
            unread_count = len(unread_messages)
            print(
                f"[PROFILE] Loaded {len(chat_payload['channel_messages'])} channel messages across configured channels, {len(chat_payload['dm_messages'])} dms, {unread_count} unread for selected channel {expected_channel}"
            )
        else:
            unread_count = 0  # Can't determine messages without channel
            print(f"[PROFILE] No channel determined - no messages loaded")

        # Send WebSocket notification about profile switch with unread count
        try:
            # We can't emit to a specific session from a regular route, but we can include
            # the notification data in the response for the frontend to handle
            print(
                f"[PROFILE] Profile switch notification prepared: {profile.get('long_name', 'Unknown')} ({unread_count} unread)"
            )
        except Exception as e:
            print(f"[PROFILE] Error preparing profile switch notification: {e}")

        # Note: We DON'T update last_seen here because user hasn't actually viewed the messages yet
        # Last seen will be updated when user actually loads/views the messages

        if unread_count > 0:
            print(f"[PROFILE] Profile switched - {unread_count} unread messages available")
        else:
            print(f"[PROFILE] Profile switched - user is caught up on messages")

        response_profile = _serialize_profile_for_client(current_profile)
        unread_state = _unread_state_for_profile(current_profile)
        return jsonify(
            {
                "message": "Profile selected",
                "profile": response_profile,
                "interface_status": _interface_status_for_profile(current_profile),
                "messages": chat_payload["channel_messages"],
                "channel_messages": chat_payload["channel_messages"],
                "dm_messages": chat_payload["dm_messages"],
                "channel_number": expected_channel,
                "unread_count": unread_count,
                "selected_channel_index": response_profile.get("selected_channel_index", 0),
                **unread_state,
            }
        )
    else:
        return jsonify({"error": "Profile not found"}), 404


@app.route("/api/current-channel", methods=["POST"])
@login_required
def set_current_channel():
    """Switch the active channel within the current profile."""
    current_profile = _get_session_profile()
    if not current_profile:
        return jsonify({"error": "No profile selected"}), 400

    data = request.get_json() or {}
    channel_index = data.get("channel_index", 0)
    channels = _profile_channels(current_profile)
    try:
        channel_index = int(channel_index)
    except (TypeError, ValueError):
        return jsonify({"error": "channel_index must be an integer"}), 400

    if channel_index < 0 or channel_index >= len(channels):
        return jsonify({"error": "channel_index out of range"}), 400

    _set_session_channel_index(channel_index)

    chat_payload = _get_chat_payload(current_profile)
    if chat_payload["channel_number"] is not None:
        db.update_profile_last_seen(current_profile["id"], chat_payload["channel_number"])
    response_profile = _serialize_profile_for_client(current_profile)
    unread_state = _unread_state_for_profile(current_profile)

    return jsonify(
        {
            "message": "Channel selected",
            "profile": response_profile,
            "interface_status": _interface_status_for_profile(current_profile),
            "channel_messages": chat_payload["channel_messages"],
            "dm_messages": chat_payload["dm_messages"],
            "channel_number": chat_payload["channel_number"],
            "selected_channel_index": channel_index,
            **unread_state,
        }
    )


@app.route("/api/reconnect-profile", methods=["POST"])
@login_required
def reconnect_profile():
    current_profile = _get_session_profile()
    if not current_profile:
        return jsonify({"error": "No profile selected"}), 400

    session_id = f"flask_session_{id(session)}"
    effective_profile = _effective_profile(current_profile)
    channel_hash = _profile_channel_num(effective_profile)
    interface_started = udp_server.reconnect_with_profile(current_profile, session_id=session_id)
    if interface_started:
        udp_server.register_session(session_id, current_profile.get("id"), channel_hash)
        print(f"[PROFILE] Reconnected virtual node transport for profile {current_profile.get('long_name', 'unknown')}")

        def send_nodeinfo_delayed():
            try:
                import time

                time.sleep(0.5)
                print(f"[NODEINFO] Sending vnode nodeinfo for profile {current_profile.get('long_name')}")
                udp_server.send_nodeinfo()
                print(
                    f"[NODEINFO] Sent nodeinfo packet for {current_profile.get('long_name', 'Unknown')} ({current_profile.get('node_id', 'Unknown')})"
                )
            except Exception as e:
                print(f"[NODEINFO] Error sending nodeinfo packet: {e}")

        import threading

        nodeinfo_thread = threading.Thread(target=send_nodeinfo_delayed, daemon=True)
        nodeinfo_thread.start()

    chat_payload = _get_chat_payload(current_profile)
    response_profile = _serialize_profile_for_client(current_profile)
    unread_state = _unread_state_for_profile(current_profile)

    if interface_started:
        return jsonify(
            {
                "message": "Profile reconnected",
                "profile": response_profile,
                "interface_status": "started",
                "channel_messages": chat_payload["channel_messages"],
                "dm_messages": chat_payload["dm_messages"],
                "channel_number": chat_payload["channel_number"],
                "selected_channel_index": response_profile.get("selected_channel_index", 0),
                **unread_state,
            }
        )

    if udp_server.running and udp_server.current_profile_id and udp_server.current_profile_id != current_profile.get("id"):
        current_profile_id = udp_server.current_profile_id
        return jsonify(
            {
                "message": "Cannot reconnect this profile",
                "profile": response_profile,
                "interface_status": "conflict",
                "warning": f"The active virtual node is currently profile {current_profile_id}. Wait for other users to finish before switching identities.",
                "channel_messages": chat_payload["channel_messages"],
                "dm_messages": chat_payload["dm_messages"],
                "channel_number": chat_payload["channel_number"],
                "selected_channel_index": response_profile.get("selected_channel_index", 0),
                **unread_state,
            }
        )

    return jsonify(
        {
            "message": "Receiver active; send transport will reconnect on demand",
            "profile": response_profile,
            "interface_status": _interface_status_for_profile(current_profile),
            "channel_messages": chat_payload["channel_messages"],
            "dm_messages": chat_payload["dm_messages"],
            "channel_number": chat_payload["channel_number"],
            "selected_channel_index": response_profile.get("selected_channel_index", 0),
            **unread_state,
        }
    )


@app.route("/api/messages", methods=["GET"])
@login_required
def get_messages():
    """Get channel and direct messages for the current profile."""
    current_profile = _get_session_profile()
    if not current_profile:
        return jsonify({"channel_messages": [], "dm_messages": []})

    requested_channel_index = request.args.get("channel_index")
    if requested_channel_index is not None:
        try:
            requested_channel_index = int(requested_channel_index)
        except (TypeError, ValueError):
            return jsonify({"error": "channel_index must be an integer"}), 400
        channels = _profile_channels(current_profile)
        if requested_channel_index < 0 or requested_channel_index >= len(channels):
            return jsonify({"error": "channel_index out of range"}), 400
        _set_session_channel_index(requested_channel_index)

    chat_payload = _get_chat_payload(current_profile)
    chat_payload.update(_unread_state_for_profile(current_profile))
    return jsonify(chat_payload)


@app.route("/api/channel-read", methods=["POST"])
@login_required
def mark_channel_read():
    """Mark a channel as read for the current profile."""
    current_profile = _get_session_profile()
    if not current_profile:
        return jsonify({"error": "No profile selected"}), 400

    data = request.get_json(silent=True) or {}
    channel_index = data.get("channel_index", _selected_channel_index(current_profile))
    try:
        channel_index = int(channel_index)
    except (TypeError, ValueError):
        return jsonify({"error": "channel_index must be an integer"}), 400

    effective_profile = _effective_profile(current_profile, channel_index=channel_index)
    channel_number = _profile_channel_num(effective_profile)
    if channel_number is None:
        return jsonify({"error": "Unable to determine channel"}), 400

    db.update_profile_last_seen(current_profile["id"], channel_number)
    return jsonify({"message": "Channel marked read", **_unread_state_for_profile(current_profile)})


@app.route("/api/dm-read", methods=["POST"])
@login_required
def mark_dm_read():
    """Mark a DM thread as read for the current profile."""
    current_profile = _get_session_profile()
    if not current_profile:
        return jsonify({"error": "No profile selected"}), 400

    data = request.get_json(silent=True) or {}
    peer_node_num = data.get("peer_node_num")
    try:
        peer_node_num = int(peer_node_num)
    except (TypeError, ValueError):
        return jsonify({"error": "peer_node_num must be an integer"}), 400

    db.mark_dm_thread_read(current_profile["id"], peer_node_num)
    return jsonify({"message": "DM marked read", **_unread_state_for_profile(current_profile)})


@app.route("/api/threads/channel", methods=["DELETE"])
@login_required
def delete_channel_thread():
    """Delete channel message history for the selected profile channel."""
    current_profile = _get_session_profile()
    if not current_profile:
        return jsonify({"error": "No profile selected"}), 400

    data = request.get_json(silent=True) or {}
    channel_index = data.get("channel_index", _selected_channel_index(current_profile))
    try:
        channel_index = int(channel_index)
    except (TypeError, ValueError):
        return jsonify({"error": "channel_index must be an integer"}), 400

    channels = _profile_channels(current_profile)
    if channel_index < 0 or channel_index >= len(channels):
        return jsonify({"error": "channel_index out of range"}), 400

    channel_hash = _profile_channel_num(_effective_profile(current_profile, channel_index=channel_index))
    if channel_hash is None:
        return jsonify({"error": "Unable to determine channel"}), 400

    deleted_count = db.delete_channel_messages(channel_hash)
    return jsonify({"message": "Channel thread deleted", "deleted_count": deleted_count, "channel_index": channel_index})


@app.route("/api/threads/dm", methods=["DELETE"])
@login_required
def delete_dm_thread():
    """Delete a DM thread for the current profile."""
    current_profile = _get_session_profile()
    if not current_profile:
        return jsonify({"error": "No profile selected"}), 400

    data = request.get_json(silent=True) or {}
    peer_node_num = data.get("peer_node_num")
    try:
        peer_node_num = int(peer_node_num)
    except (TypeError, ValueError):
        return jsonify({"error": "peer_node_num must be an integer"}), 400

    deleted_count = db.delete_dm_thread_for_profile(current_profile["id"], peer_node_num)
    return jsonify({"message": "DM thread deleted", "deleted_count": deleted_count, "peer_node_num": peer_node_num})


@app.route("/api/nodes", methods=["GET"])
@login_required
def get_nodes():
    """Get nodes seen by current profile in meshdb."""
    current_profile = _get_session_profile()
    if not current_profile:
        return jsonify({"error": "No profile selected"}), 400

    mesh_store = _get_mesh_store(current_profile)
    nodes = mesh_store.list_nodes() if mesh_store else []
    return jsonify({"nodes": nodes, "count": len(nodes)})


@app.route("/api/nodes/<int:node_num>", methods=["GET"])
@login_required
def get_node_details(node_num):
    """Get detailed information about a specific node"""
    current_profile = _get_session_profile()
    if not current_profile:
        return jsonify({"error": "No profile selected"}), 400

    mesh_store = _get_mesh_store(current_profile)
    node = mesh_store.get_node(node_num) if mesh_store else None

    if not node:
        return jsonify({"error": "Node not found"}), 404

    return jsonify(node)


@app.route("/api/nodes/<int:node_num>/exchange-nodeinfo", methods=["POST"])
@login_required
def exchange_nodeinfo(node_num):
    current_profile = _get_session_profile()
    if not current_profile:
        return jsonify({"error": "No profile selected"}), 400

    session_id = f"flask_session_{id(session)}"
    packet_id = udp_server.exchange_nodeinfo(current_profile, node_num, session_id=session_id)
    if not packet_id:
        return jsonify({"error": udp_server.last_send_error or "Failed to exchange node info"}), 500

    return jsonify({"message": "Node info request sent", "packet_id": int(packet_id), "node_num": int(node_num)})


@app.route("/api/nodes/<int:node_num>/traceroute", methods=["POST"])
@login_required
def send_node_traceroute(node_num):
    current_profile = _get_session_profile()
    if not current_profile:
        return jsonify({"error": "No profile selected"}), 400

    data = request.get_json(silent=True) or {}
    hop_limit = data.get("hop_limit")
    if hop_limit is not None:
        try:
            hop_limit = int(hop_limit)
        except (TypeError, ValueError):
            return jsonify({"error": "hop_limit must be an integer"}), 400

    session_id = f"flask_session_{id(session)}"
    packet_id = udp_server.send_traceroute(current_profile, node_num, session_id=session_id, hop_limit=hop_limit)
    if not packet_id:
        return jsonify({"error": udp_server.last_send_error or "Failed to send traceroute"}), 500

    return jsonify({"message": "Traceroute sent", "packet_id": int(packet_id), "node_num": int(node_num)})


@app.route("/api/stats", methods=["GET"])
@login_required
def get_stats():
    """Get database statistics"""
    current_user = _get_session_user()
    profiles = profile_manager.get_all_profiles(user_id=current_user["id"])
    base_stats = db.get_stats()
    global_stats = {
        "profiles": len(profiles),
        "total_messages": base_stats["total_messages"],
        "total_nodes": count_nodes_for_profiles(profiles),
    }

    current_profile = _get_session_profile()
    if current_profile:
        mesh_store = _get_mesh_store(current_profile)
        expected_channel = _current_profile_channel_num()
        profile_stats = {
            "nodes": mesh_store.count_nodes() if mesh_store else 0,
            "messages": len(db.get_messages_for_channel(expected_channel)) if expected_channel is not None else 0,
        }
        return jsonify({"profile": profile_stats, "global": global_stats})
    return jsonify({"global": global_stats})


@app.route("/api/send-message", methods=["POST"])
@login_required
def send_message():
    """Send a message"""
    current_profile = _get_session_profile()
    print(f"[API] /api/send-message called. current_profile set? {bool(current_profile)}")
    if not current_profile:
        return jsonify({"error": "No profile selected"}), 400

    data = request.get_json()
    print(f"[API] payload: {data}")
    message_content = data.get("message", "").strip()

    if not message_content:
        return jsonify({"error": "Message cannot be empty"}), 400

    message_type = (data.get("message_type") or "channel").strip().lower()
    if message_type not in {"channel", "dm"}:
        return jsonify({"error": "message_type must be 'channel' or 'dm'"}), 400

    target_node_num = data.get("target_node_num")
    if message_type == "dm":
        try:
            target_node_num = int(target_node_num)
        except (TypeError, ValueError):
            return jsonify({"error": "target_node_num is required for DMs"}), 400

    reply_packet_id = data.get("reply_packet_id")
    if reply_packet_id not in (None, ""):
        try:
            reply_packet_id = int(reply_packet_id)
        except (TypeError, ValueError):
            return jsonify({"error": "reply_packet_id must be an integer"}), 400
    else:
        reply_packet_id = None

    session_id = f"flask_session_{id(session)}"
    success = udp_server.send_message(
        message_content,
        current_profile,
        session_id=session_id,
        message_type=message_type,
        target_node_num=target_node_num,
        reply_packet_id=reply_packet_id,
    )

    if success:
        print("[API] send_message -> success")
        return jsonify({"message": "Message sent successfully"})
    else:
        print("[API] send_message -> FAILED (udp_server.send_message returned False)")
        return jsonify({"error": udp_server.last_send_error or "Failed to send message"}), 500


@app.route("/api/health")
def health():
    profiles = profile_manager.get_all_profiles()
    global_stats = db.get_stats()
    return jsonify(
        {
            "status": "ok",
            "profiles": global_stats["profiles"],
            "messages": global_stats["total_messages"],
            "database": {
                **global_stats,
                "total_nodes": count_nodes_for_profiles(profiles),
            },
        }
    )


@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@socketio.on("connect")
def handle_connect():
    """Handle WebSocket connection"""
    if not _get_session_user():
        print(f"[WEBSOCKET] Rejecting unauthenticated connection: {request.sid}")
        return False

    print(f"Client connected with WebSocket session ID: {request.sid}")
    
    # Get current profile to establish session mapping if one exists
    current_profile = _get_session_profile()
    if current_profile:
        flask_session_id = f"flask_session_{id(session)}"
        websocket_to_flask_sessions[request.sid] = flask_session_id
        print(f"[WEBSOCKET] Mapped WebSocket session {request.sid} to Flask session {flask_session_id}")
    
    emit("status", {"msg": "Connected to chat server"})


@socketio.on("join_channel")
def handle_join_channel(data):
    """Join a WebSocket room for a specific channel"""
    channel = data.get("channel")
    current_profile = _get_session_profile()
    print(f"[WEBSOCKET] join_channel request received: {data}")
    if channel is not None and current_profile:
        allowed_channel = _current_profile_channel_num()
        try:
            requested_channel = int(channel)
        except (TypeError, ValueError):
            print(f"[WEBSOCKET] ❌ join_channel denied for invalid channel: {channel}")
            emit("status", {"msg": "Join denied"})
            return
        if allowed_channel is not None and requested_channel != int(allowed_channel):
            print(f"[WEBSOCKET] ❌ join_channel denied for {channel}; active profile channel is {allowed_channel}")
            emit("status", {"msg": "Join denied"})
            return
        room_name = f"channel_{requested_channel}"
        for existing_room in rooms():
            if existing_room.startswith("channel_") and existing_room != room_name:
                leave_room(existing_room)
                print(f"[WEBSOCKET] Left stale channel room: {existing_room}")
        join_room(room_name)
        print(f"[WEBSOCKET] ✅ Client {request.sid} joined room: {room_name}")
        emit("status", {"msg": f"Joined channel {requested_channel}"})
    else:
        print(f"[WEBSOCKET] ❌ join_channel called with no channel: {data}")


@socketio.on("join_profile")
def handle_join_profile(data):
    """Join a WebSocket room for a specific profile."""
    profile_id = data.get("profile_id")
    current_user = _get_session_user()
    if profile_id and current_user and profile_manager.get_profile(profile_id, user_id=current_user["id"]):
        room_name = f"profile_{profile_id}"
        join_room(room_name)
        print(f"[WEBSOCKET] ✅ Client {request.sid} joined room: {room_name}")
        emit("status", {"msg": f"Joined profile {profile_id}"})
    else:
        print(f"[WEBSOCKET] ❌ join_profile denied: {data}")


@socketio.on("leave_channel")
def handle_leave_channel(data):
    """Leave a WebSocket room for a specific channel"""
    channel = data.get("channel")
    if channel is not None:
        room_name = f"channel_{channel}"
        leave_room(room_name)
        print(f"Client left channel room: {room_name}")
        emit("status", {"msg": f"Left channel {channel}"})


@socketio.on("leave_profile")
def handle_leave_profile(data):
    """Leave a WebSocket room for a specific profile."""
    profile_id = data.get("profile_id")
    if profile_id:
        room_name = f"profile_{profile_id}"
        leave_room(room_name)
        print(f"Client left profile room: {room_name}")
        emit("status", {"msg": f"Left profile {profile_id}"})


@socketio.on("disconnect")
def handle_disconnect():
    """Handle WebSocket disconnection"""
    print(f"Client disconnected: {request.sid}")
    
    # Find the Flask session ID for this WebSocket session and unregister
    flask_session_id = websocket_to_flask_sessions.pop(request.sid, None)
    if flask_session_id:
        try:
            udp_server.unregister_session(flask_session_id)
            print(f"[WEBSOCKET] Flask session {flask_session_id} unregistered from UDP server on WebSocket disconnect")
        except Exception as e:
            print(f"[WEBSOCKET] Error unregistering Flask session {flask_session_id}: {e}")
    else:
        print(f"[WEBSOCKET] No Flask session mapping found for WebSocket session {request.sid}")


if __name__ == "__main__":
    print("")
    print("🔥 Firefly - Meshtastic Node Discovery")
    print("=" * 50)
    print("Enhanced with node tracking and database storage!")
    print("=" * 50)
    print("")
    print("[STARTUP] Using vnode for UDP multicast I/O and node identity")
    print("[STARTUP] Virtual node will start when a profile is selected")

    app.config["TEMPLATES_AUTO_RELOAD"] = True
    print(f"Starting Firefly server on http://localhost:5011 (interface starts with profile selection)...")
    socketio.run(app, host="0.0.0.0", port=5011, debug=True, use_reloader=True, allow_unsafe_werkzeug=True)
