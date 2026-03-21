#!/usr/bin/env python3

from collections import deque

from datetime import datetime
from functools import wraps
from google.protobuf import text_format
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, has_request_context, flash
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
from mesh_runtime import MeshNodeStore, VirtualNodeManager, count_nodes_for_profiles


MCAST_GRP = os.getenv("FIREFLY_MCAST_GRP", "224.0.0.69")
MCAST_PORT = int(os.getenv("FIREFLY_UDP_PORT", "4403"))
PROFILES_FILE = "profiles.json"
SOCKETIO_CLIENT_VERSION = "4.7.5"
BROADCAST_NODE_NUM = 0xFFFFFFFF
DEFAULT_SECRET_KEY = "dev-secret-key-change-in-production"

# Note: current_profile is now stored per-session in session['current_profile']
messages = []

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


def _sync_cached_message(packet_id, **updates):
    if packet_id is None:
        return
    for message in messages:
        if int(message.get("packet_id") or 0) != int(packet_id):
            continue
        message.update(updates)


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
        _sync_cached_message(
            packet_id,
            ack_status=message.get("ack_status"),
            ack_error=message.get("ack_error"),
            ack_updated_at=message.get("ack_updated_at"),
        )
        _emit_message_update(message)


virtual_node_manager = VirtualNodeManager(MCAST_GRP, MCAST_PORT)


def _get_mesh_store(profile=None):
    profile = _effective_profile(profile)
    if not profile or not profile.get("node_id"):
        return None
    try:
        return MeshNodeStore(profile)
    except Exception as e:
        print(f"[MESHDB] Failed to create node store: {e}")
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
    return {
        "channel_messages": channel_messages,
        "dm_messages": dm_messages,
        "channel_number": channel_number,
        "selected_channel_index": effective_profile.get("selected_channel_index", 0) if effective_profile else 0,
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


def _print_packet_with_decoded_payload(packet: mesh_pb2.MeshPacket):
    payload_kind, decoded_payload = _decode_payload_for_log(packet)
    if payload_kind is None:
        print(packet)
        return

    print("from:", getattr(packet, "from", None))
    print("to:", packet.to)
    print("channel:", packet.channel or None)
    print("decoded {")
    port_name = portnums_pb2.PortNum.Name(packet.decoded.portnum) if packet.decoded.portnum else "N/A"
    print("  portnum:", port_name)

    if payload_kind == "text":
        escaped_text = decoded_payload.encode("unicode_escape").decode("ascii")
        print(f'  payload: "{escaped_text}"')
    else:
        print("  payload: {")
        decoded_text = text_format.MessageToString(decoded_payload, as_utf8=True).rstrip()
        for line in decoded_text.splitlines():
            print(f"    {line}")
        print("  }")

    if packet.decoded.dest:
        print("  dest:", packet.decoded.dest)
    if packet.decoded.source:
        print("  source:", packet.decoded.source)
    if packet.decoded.request_id:
        print("  request_id:", packet.decoded.request_id)
    if packet.decoded.reply_id:
        print("  reply_id:", packet.decoded.reply_id)
    if packet.decoded.emoji:
        print("  emoji:", packet.decoded.emoji)
    if packet.decoded.HasField("bitfield"):
        print("  bitfield:", packet.decoded.bitfield)
    if packet.decoded.want_response:
        print("  want_response:", packet.decoded.want_response)
    print("}")

    print("id:", packet.id or None)
    print("rx_time:", packet.rx_time or None)
    print("rx_snr:", packet.rx_snr or None)
    print("hop_limit:", packet.hop_limit or None)
    priority_name = mesh_pb2.MeshPacket.Priority.Name(packet.priority) if packet.priority else "N/A"
    print("priority:", priority_name or None)
    print("rx_rssi:", packet.rx_rssi or None)
    print("hop_start:", packet.hop_start or None)
    print("next_hop:", packet.next_hop or None)
    print("relay_node:", packet.relay_node or None)
    transport_name = (
        mesh_pb2.MeshPacket.TransportMechanism.Name(packet.transport_mechanism)
        if packet.transport_mechanism
        else None
    )
    if transport_name:
        print("transport_mechanism:", transport_name)


def on_recieve(packet: mesh_pb2.MeshPacket, addr=None):
    print(f"\n[RECV] Packet received from {addr}")
    _print_packet_with_decoded_payload(packet)

    active_profile = udp_server.get_active_profile()
    mesh_store = _get_mesh_store(active_profile) if active_profile else None
    if mesh_store:
        try:
            mesh_store.record_packet(packet)
        except Exception as e:
            print(f"[MESHDB] Failed to record packet: {e}")


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

    # Push into in-memory log and notify connected clients
    try:
        sender_num = getattr(packet, "from", None)
        active_profile = udp_server.get_active_profile()
        message_type = "dm" if _is_direct_message(packet, active_profile) else "channel"
        target_node_num = getattr(packet, "to", None) if message_type == "dm" else None
        if target_node_num in (0, BROADCAST_NODE_NUM):
            target_node_num = None
        reply_packet_id = getattr(packet.decoded, "reply_id", None) or None
        owner_profile_id = active_profile.get("id") if (active_profile and message_type == "dm") else None
        message_channel = getattr(packet, "channel", None)
        if message_channel is None and active_profile:
            message_channel = _profile_channel_num(active_profile)

        # Look up node name from database if available
        # Note: We can't access session in UDP packet handler, so we'll look across all profiles
        sender_display = f"!{hex(sender_num)[2:].zfill(8)}" if sender_num else "Unknown"
        sender_short_name = None

        if sender_num:
            try:
                found_node = None
                mesh_store = _get_mesh_store(active_profile) if active_profile else None
                if mesh_store:
                    found_node = mesh_store.get_node(sender_num)

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
        messages.append(message)

        if message_channel is not None:
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
        active_profile = udp_server.get_active_profile()
        mesh_store = _get_mesh_store(active_profile) if active_profile else None
        if not mesh_store:
            print("[NODEINFO_DEBUG] No active mesh store; skipping node persistence")
            return

        before = mesh_store.get_node(sender_num)
        stored = mesh_store.record_packet(packet)
        after = mesh_store.get_node(sender_num)
        print(f"[NODEINFO_DEBUG] meshdb stored packet: {stored}")

        if after is None:
            print(f"[NODEINFO_DEBUG] meshdb did not return node {sender_num} after NODEINFO packet")
            return

        if before is None:
            print(f"\n[NODEINFO] NEW NODE {sender_num}: {after.get('long_name')} ({after.get('short_name')})")
        elif before != after:
            print(f"\n[NODEINFO] UPDATED NODE {sender_num}: {after.get('long_name')} ({after.get('short_name')})")
        else:
            print(f"\n[NODEINFO] SEEN NODE {sender_num}: {after.get('long_name')} ({after.get('short_name')})")

    except Exception as e:
        print(f"Error processing nodeinfo: {e}")


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
pub.subscribe(on_ack_message, "mesh.rx.ack")
pub.subscribe(on_nak_message, "mesh.rx.nak")
pub.subscribe(on_max_retransmit, "mesh.tx.max_retransmit")


app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("FIREFLY_SECRET_KEY", DEFAULT_SECRET_KEY)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading", logger=True, engineio_logger=True)


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
        self.profiles_file = PROFILES_FILE
        # Note: Migration is handled by startup script, not automatically here

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

    def get_active_profile(self):
        return self.current_profile

    def send_nodeinfo(self, destination=None):
        if not self.running:
            raise RuntimeError("Virtual node is not running")
        return virtual_node_manager.send_nodeinfo(destination if destination is not None else 0xFFFFFFFF)

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
        if not sender_profile:
            return False

        sender_profile = _effective_profile(sender_profile)
        sender_channel_hash = generate_hash(sender_profile.get("channel", ""), sender_profile.get("key", ""))
        print(f"[SEND] Debug: Profile '{sender_profile.get('long_name', 'Unknown')}' wants channel {sender_channel_hash}")
        print(f"[SEND] Debug: UDP server running={self.running}, current_profile={self.current_profile_id}")

        if not self.running or self.current_profile_id != sender_profile.get("id"):
            print(f"[UDP] Active virtual node does not match sender profile, restarting...")
            if not self.restart_with_profile(sender_profile, session_id=session_id):
                print(f"[UDP] Failed to restart virtual node for profile")
                return False
        else:
            print(f"[UDP] Reusing existing virtual node for profile {sender_profile.get('id')}")

        try:
            hop_limit = sender_profile.get("hop_limit", 3)
            ack_requested = message_type == "dm"
            sender_channel = generate_hash(sender_profile.get("channel", ""), sender_profile.get("key", ""))
            if message_type == "dm":
                if target_node_num in (None, 0, BROADCAST_NODE_NUM):
                    print("[SEND] Refusing to send DM without a valid target node")
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
            messages.append(message)

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
                    
                    room_name = f"channel_{sender_channel}" if message_type == "channel" else f"profile_{sender_profile.get('id')}"
                    try:
                        socketio.emit("new_message", message, room=room_name)
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

    user = db.get_user_by_username(username)
    if not user or not check_password_hash(user["password_hash"], password):
        flash("Invalid username or password.", "error")
        return redirect(url_for("index"))

    _unregister_current_session_transport()
    _clear_session_user()
    _set_session_user(user)
    return redirect(url_for("index"))


@app.route("/logout", methods=["POST"])
@login_required
def logout():
    _unregister_current_session_transport()
    _clear_session_user()
    flash("Signed out.", "success")
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
    """Profile management page"""
    current_user = _get_session_user()
    profiles = profile_manager.get_all_profiles(user_id=current_user["id"])
    return render_template("profiles.html", profiles=profiles)


@app.route("/nodes")
@login_required
def nodes():
    """Legacy nodes route redirects into the unified index tab view."""
    return redirect(url_for("index", tab="nodes"))


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
    data = request.get_json()
    current_user = _get_session_user()

    normalized_node_id = _normalize_node_id((data or {}).get("node_id"))
    channels = _profile_channels(data)
    required = ["node_id", "long_name", "short_name"]
    if not data or any(not data.get(k) for k in required) or not channels:
        return jsonify({"error": "node_id, long_name, short_name, and at least one channel are required"}), 400
    if db.node_id_in_use(normalized_node_id):
        return jsonify({"error": "Node ID is already in use."}), 409

    # Validate hop_limit if provided
    hop_limit = data.get("hop_limit", 3)
    if not isinstance(hop_limit, int) or hop_limit < 0 or hop_limit > 7:
        return jsonify({"error": "hop_limit must be an integer between 0 and 7"}), 400

    # Create and store the profile
    profile_id = str(uuid.uuid4())
    success = profile_manager.create_profile(
        profile_id, current_user["id"], normalized_node_id, data["long_name"], data["short_name"], channels, hop_limit
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
    data = request.get_json()
    current_user = _get_session_user()
    existing_profile = profile_manager.get_profile(profile_id, user_id=current_user["id"])
    if not existing_profile:
        return jsonify({"error": "Profile not found"}), 404

    normalized_node_id = _normalize_node_id((data or {}).get("node_id"))
    channels = _profile_channels(data)
    required = ["node_id", "long_name", "short_name"]
    if not data or any(not data.get(k) for k in required) or not channels:
        return jsonify({"error": "node_id, long_name, short_name, and at least one channel are required"}), 400
    if db.node_id_in_use(normalized_node_id, exclude_profile_id=profile_id):
        return jsonify({"error": "Node ID is already in use."}), 409

    # Validate hop_limit if provided
    hop_limit = data.get("hop_limit", 3)
    if not isinstance(hop_limit, int) or hop_limit < 0 or hop_limit > 7:
        return jsonify({"error": "hop_limit must be an integer between 0 and 7"}), 400

    success = profile_manager.update_profile(
        profile_id, current_user["id"], normalized_node_id, data["long_name"], data["short_name"], channels, hop_limit
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
        effective_profile = _effective_profile(current_profile)
        profile_channel_hash = generate_hash(effective_profile.get("channel", ""), effective_profile.get("key", ""))
        if (
            udp_server.running
            and udp_server.current_profile_id == current_profile.get("id")
            and udp_server.current_channel_hash == profile_channel_hash
        ):
            interface_status = "started"
        else:
            interface_status = "stopped"

        # Get channel number for WebSocket room joining
        expected_channel = _current_profile_channel_num()

        # Return profile with interface status and channel number
        response_data = _serialize_profile_for_client(current_profile, interface_status=interface_status)
        response_data["channel_number"] = expected_channel
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
        
        # Restart the active virtual node with the selected profile identity
        print(f"[PROFILE] Restarting virtual node for profile {profile.get('long_name', 'unknown')}")
        interface_started = udp_server.restart_with_profile(profile, session_id)

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

        if interface_started:
            udp_server.register_session(session_id, profile.get("id"), channel_hash)
            print(f"[PROFILE] Interface successfully started with vnode profile and session registered")

            def send_nodeinfo_delayed():
                try:
                    import time

                    time.sleep(0.5)
                    print(f"[NODEINFO] Sending vnode nodeinfo for profile {profile.get('long_name')}")
                    udp_server.send_nodeinfo()
                    print(
                        f"[NODEINFO] Sent nodeinfo packet for {profile.get('long_name', 'Unknown')} ({profile.get('node_id', 'Unknown')})"
                    )
                except Exception as e:
                    print(f"[NODEINFO] Error sending nodeinfo packet: {e}")

            import threading

            nodeinfo_thread = threading.Thread(target=send_nodeinfo_delayed, daemon=True)
            nodeinfo_thread.start()

            return jsonify(
                {
                    "message": "Profile set successfully and interface started",
                    "profile": response_profile,
                    "interface_status": "started",
                    "messages": chat_payload["channel_messages"],
                    "channel_messages": chat_payload["channel_messages"],
                    "dm_messages": chat_payload["dm_messages"],
                    "channel_number": expected_channel,
                    "unread_count": unread_count,
                    "selected_channel_index": response_profile.get("selected_channel_index", 0),
                }
            )
        else:
            if udp_server.running and udp_server.current_profile_id:
                current_profile_id = udp_server.current_profile_id
                if current_profile_id != profile.get("id"):
                    print(f"[PROFILE] Profile conflict - cannot switch from {current_profile_id} to {profile.get('id')}")
                    return jsonify(
                        {
                            "message": "Cannot switch to this profile - another profile is currently active",
                            "profile": response_profile,
                            "interface_status": "conflict",
                            "warning": f"The active virtual node is currently profile {current_profile_id}. Wait for other users to finish before switching identities.",
                            "messages": chat_payload["channel_messages"],
                            "channel_messages": chat_payload["channel_messages"],
                            "dm_messages": chat_payload["dm_messages"],
                            "channel_number": expected_channel,
                            "unread_count": unread_count,
                            "selected_channel_index": response_profile.get("selected_channel_index", 0),
                        }
                    )
            
            print(f"[PROFILE] Warning: Profile set but virtual node failed to start")
            return jsonify(
                {
                    "message": "Profile set but virtual node failed to start",
                    "profile": response_profile,
                    "interface_status": "failed",
                    "warning": "Virtual node could not be started. Check logs for details.",
                    "messages": chat_payload["channel_messages"],
                    "channel_messages": chat_payload["channel_messages"],
                    "dm_messages": chat_payload["dm_messages"],
                    "channel_number": expected_channel,
                    "unread_count": unread_count,
                    "selected_channel_index": response_profile.get("selected_channel_index", 0),
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

    session_id = f"flask_session_{id(session)}"
    _set_session_channel_index(channel_index)
    effective_profile = _effective_profile(current_profile)
    channel_hash = _profile_channel_num(effective_profile)
    interface_started = udp_server.restart_with_profile(current_profile, session_id=session_id)
    if interface_started:
        udp_server.register_session(session_id, current_profile.get("id"), channel_hash)

    chat_payload = _get_chat_payload(current_profile)
    response_profile = _serialize_profile_for_client(current_profile)

    return jsonify(
        {
            "message": "Channel selected",
            "profile": response_profile,
            "interface_status": "started" if interface_started else "failed",
            "channel_messages": chat_payload["channel_messages"],
            "dm_messages": chat_payload["dm_messages"],
            "channel_number": chat_payload["channel_number"],
            "selected_channel_index": channel_index,
        }
    )


@app.route("/api/messages", methods=["GET"])
@login_required
def get_messages():
    """Get channel and direct messages for the current profile."""
    current_profile = _get_session_profile()
    if not current_profile:
        return jsonify({"channel_messages": messages, "dm_messages": []})

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
    expected_channel = chat_payload["channel_number"]
    if expected_channel is not None and chat_payload["channel_messages"]:
        db.update_profile_last_seen(current_profile["id"], expected_channel)

    return jsonify(chat_payload)


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
        return jsonify({"error": "Failed to send message"}), 500


@app.route("/api/health")
def health():
    profiles = profile_manager.get_all_profiles()
    global_stats = db.get_stats()
    return jsonify(
        {
            "status": "ok",
            "profiles": global_stats["profiles"],
            "messages": len(messages),  # In-memory messages count
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
