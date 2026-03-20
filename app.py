#!/usr/bin/env python3

from collections import deque

from datetime import datetime
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from flask_socketio import SocketIO, emit, join_room, leave_room
import uuid
import os
import socketio as py_socketio
import engineio
from pubsub import pub

from mudp_compat import apply_mudp_multicast_patch

apply_mudp_multicast_patch()

from meshtastic.protobuf import mesh_pb2, portnums_pb2
from mudp import node
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

# De-duplication cache for received packets to avoid double-display
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
    return session.get("current_profile")


def _set_session_profile(profile):
    """Set the current profile in session storage"""
    session["current_profile"] = profile


def _clear_session_profile():
    """Clear the current profile from session storage"""
    session.pop("current_profile", None)
    session.pop("current_channel_index", None)


def _get_session_channel_index():
    try:
        return int(session.get("current_channel_index", 0))
    except (TypeError, ValueError):
        return 0


def _set_session_channel_index(channel_index):
    try:
        session["current_channel_index"] = max(0, int(channel_index))
    except (TypeError, ValueError):
        session["current_channel_index"] = 0


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
        selected_index = _selected_channel_index(profile) if channel_index is None else max(0, min(int(channel_index), len(channels) - 1))
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
    """Return True if we've already processed a message with this key; otherwise record it and return False."""
    if key in _DEDUP_CACHE:
        return True
    _DEDUP_CACHE.add(key)
    _DEDUP_QUEUE.append(key)
    if len(_DEDUP_CACHE) > _DEDUP_QUEUE.maxlen:
        old = _DEDUP_QUEUE.popleft()
        _DEDUP_CACHE.discard(old)
    return False


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


def _get_chat_payload(profile):
    effective_profile = _effective_profile(profile)
    channel_number = _profile_channel_num(effective_profile)
    channel_messages = db.get_messages_for_channel(channel_number) if channel_number is not None else []
    dm_messages = db.get_dm_messages_for_profile(effective_profile["id"]) if effective_profile else []
    return {
        "channel_messages": channel_messages,
        "dm_messages": dm_messages,
        "channel_number": channel_number,
        "selected_channel_index": effective_profile.get("selected_channel_index", 0) if effective_profile else 0,
    }


def on_recieve(packet: mesh_pb2.MeshPacket, addr=None):
    print(f"\n[RECV] Packet received from {addr}")
    print("from:", getattr(packet, "from", None))
    print("to:", packet.to)
    print("channel:", packet.channel or None)

    if packet.HasField("decoded"):
        port_name = portnums_pb2.PortNum.Name(packet.decoded.portnum) if packet.decoded.portnum else "N/A"
        print("decoded {")
        print("  portnum:", port_name)
        try:
            print("  payload:", packet.decoded.payload.decode("utf-8", "ignore"))
        except Exception:
            print("  payload (raw bytes):", packet.decoded.payload)
        print("  bitfield:", packet.decoded.bitfield or None)
        print("}")
    else:
        print(f"encrypted: { {packet.encrypted} }")

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

        if sender_num:
            try:
                found_node = None
                mesh_store = _get_mesh_store(active_profile) if active_profile else None
                if mesh_store:
                    found_node = mesh_store.get_node(sender_num)

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
            "content": msg,
            "timestamp": datetime.now().isoformat(),
            "sender_ip": (addr[0] if isinstance(addr, tuple) and len(addr) >= 1 else "mesh"),
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


pub.subscribe(on_recieve, "mesh.rx.packet")
pub.subscribe(on_text_message, "mesh.rx.port.1")
pub.subscribe(on_nodeinfo, "mesh.rx.port.4")  # NODEINFO_APP


app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("FIREFLY_SECRET_KEY", DEFAULT_SECRET_KEY)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading", logger=True, engineio_logger=True)


@app.context_processor
def inject_versions():
    return {
        "socketio_client_version": SOCKETIO_CLIENT_VERSION,
        "python_socketio_version": getattr(py_socketio, "__version__", "unknown"),
        "python_engineio_version": getattr(engineio, "__version__", "unknown"),
    }


class ProfileManager:
    def __init__(self, database):
        self.db = database
        self.profiles_file = PROFILES_FILE
        # Note: Migration is handled by startup script, not automatically here

    def get_all_profiles(self):
        """Get all profiles"""
        return self.db.get_all_profiles()

    def get_profile(self, profile_id):
        """Get a specific profile"""
        return self.db.get_profile(profile_id)

    def create_profile(self, profile_id, node_id, long_name, short_name, channels, hop_limit=3):
        """Create a new profile"""
        return self.db.create_profile(profile_id, node_id, long_name, short_name, channels, hop_limit)

    def update_profile(self, profile_id, node_id, long_name, short_name, channels, hop_limit=3):
        """Update an existing profile"""
        return self.db.update_profile(profile_id, node_id, long_name, short_name, channels, hop_limit)

    def delete_profile(self, profile_id):
        """Delete a profile"""
        return self.db.delete_profile(profile_id)
    
    def copy_profile(self, profile_id):
        """Copy an existing profile to a new one with long name appended with (Copy)"""
        profile = self.get_profile(profile_id)
        if not profile:
            return None
        new_profile_id = str(uuid.uuid4())
        new_long_name = f"{profile.get('long_name', 'Profile')} (Copy)"
        return self.create_profile(
            profile_id=new_profile_id,
            node_id=profile.get("node_id", ""),
            long_name=new_long_name,
            short_name=profile.get("short_name", ""),
            channel=profile.get("channel", ""),
            key=profile.get("key", ""),
            hop_limit=profile.get("hop_limit", 3)
        )

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
                "content": message_content,
                "timestamp": datetime.now().isoformat(),
                "sender_ip": "self",  # Keep for backward compatibility but use sender for identity
                "message_type": message_type,
                "reply_packet_id": reply_packet_id,
                "target_node_num": int(target_node_num) if target_node_num not in (None, "", 0, BROADCAST_NODE_NUM) else None,
                "target": _node_id_from_num(target_node_num) if target_node_num not in (None, "", 0, BROADCAST_NODE_NUM) else None,
                "owner_profile_id": sender_profile.get("id") if message_type == "dm" else None,
            }
            messages.append(message)

            # Store sent message in database and broadcast to WebSocket
            try:
                # Calculate channel hash from sender profile directly
                sender_channel = generate_hash(sender_profile.get("channel", ""), sender_profile.get("key", ""))
                
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


@app.route("/")
def index():
    """Main chat interface"""
    profiles = profile_manager.get_all_profiles()
    current_profile = _get_session_profile()
    return render_template("index.html", profiles=profiles, current_profile=current_profile)


@app.route("/profiles")
def profiles():
    """Profile management page"""
    profiles = profile_manager.get_all_profiles()
    return render_template("profiles.html", profiles=profiles)


@app.route("/nodes")
def nodes():
    """Legacy nodes route redirects into the unified index tab view."""
    return redirect(url_for("index", tab="nodes"))


@app.route("/api/profiles", methods=["GET"])
def get_profiles():
    """Get all profiles"""
    return jsonify(profile_manager.get_all_profiles())


@app.route("/api/profiles", methods=["POST"])
def create_profile():
    """Create a new profile"""
    data = request.get_json()

    channels = _profile_channels(data)
    required = ["node_id", "long_name", "short_name"]
    if not data or any(not data.get(k) for k in required) or not channels:
        return jsonify({"error": "node_id, long_name, short_name, and at least one channel are required"}), 400

    # Validate hop_limit if provided
    hop_limit = data.get("hop_limit", 3)
    if not isinstance(hop_limit, int) or hop_limit < 0 or hop_limit > 7:
        return jsonify({"error": "hop_limit must be an integer between 0 and 7"}), 400

    # Create and store the profile
    profile_id = str(uuid.uuid4())
    success = profile_manager.create_profile(
        profile_id, data["node_id"], data["long_name"], data["short_name"], channels, hop_limit
    )

    if success:
        return jsonify({"profile_id": profile_id, "message": "Profile created successfully"})
    else:
        return jsonify({"error": "Failed to create profile"}), 500


@app.route("/api/profiles/<profile_id>", methods=["PUT"])
def update_profile(profile_id):
    """Update an existing profile"""
    data = request.get_json()

    channels = _profile_channels(data)
    required = ["node_id", "long_name", "short_name"]
    if not data or any(not data.get(k) for k in required) or not channels:
        return jsonify({"error": "node_id, long_name, short_name, and at least one channel are required"}), 400

    # Validate hop_limit if provided
    hop_limit = data.get("hop_limit", 3)
    if not isinstance(hop_limit, int) or hop_limit < 0 or hop_limit > 7:
        return jsonify({"error": "hop_limit must be an integer between 0 and 7"}), 400

    success = profile_manager.update_profile(
        profile_id, data["node_id"], data["long_name"], data["short_name"], channels, hop_limit
    )

    if success:
        return jsonify({"message": "Profile updated successfully"})
    else:
        return jsonify({"error": "Profile not found"}), 404


@app.route("/api/profiles/<profile_id>", methods=["DELETE"])
def delete_profile(profile_id):
    """Delete a profile"""
    current_profile = _get_session_profile()
    success = profile_manager.delete_profile(profile_id)

    if success:
        # If this was the current profile, unset it
        if current_profile and current_profile.get("id") == profile_id:
            _clear_session_profile()
        return jsonify({"message": "Profile deleted successfully"})
    else:
        return jsonify({"error": "Profile not found"}), 404
    
@app.route("/api/profiles/<profile_id>/copy", methods=["POST"])
def post_copy_profile(profile_id):
    """Copy an existing profile to a new one with channel name appended with '_copy'"""
    try:
        new_profile = profile_manager.copy_profile(profile_id)
        if new_profile:
            return jsonify({"message": "Profile copied successfully"}), 201
        else:
            return jsonify({"error": "Failed to copy profile"}), 500
    except Exception as e:
        return jsonify({"error": f"Failed to copy profile: {e}"}), 500

@app.route("/api/current-profile", methods=["GET"])
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
        response_data = dict(current_profile)
        response_data["interface_status"] = interface_status
        response_data["channel_number"] = expected_channel
        response_data["selected_channel_index"] = _selected_channel_index(current_profile)
        response_data["selected_channel"] = _selected_channel(current_profile)
        return jsonify(response_data)
    else:
        return jsonify(None)


@app.route("/api/current-profile", methods=["POST"])
def set_current_profile():
    """Set the current active profile and restart interface with new key"""

    data = request.get_json()
    profile_id = data.get("profile_id")
    requested_channel_index = data.get("channel_index", 0)

    if not profile_id:
        # Unset profile and unregister session
        old_profile = _get_session_profile()
        if old_profile:
            # Use a consistent session identifier
            session_id = f"flask_session_{id(session)}"
            udp_server.unregister_session(session_id)
        
        _clear_session_profile()
        print("[PROFILE] Profile unset, session unregistered")
        return jsonify(
            {"message": "Profile unset", "profile": None, "channel_messages": [], "dm_messages": []}
        )

    profile = profile_manager.get_profile(profile_id)
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
                f"[PROFILE] Loaded {len(chat_payload['channel_messages'])} total channel messages, {len(chat_payload['dm_messages'])} dms, {unread_count} unread for channel {expected_channel}"
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

        response_profile = dict(current_profile)
        response_profile["selected_channel_index"] = _selected_channel_index(current_profile)
        response_profile["selected_channel"] = _selected_channel(current_profile)

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
                    "selected_channel_index": _selected_channel_index(current_profile),
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
                            "selected_channel_index": _selected_channel_index(current_profile),
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
                    "selected_channel_index": _selected_channel_index(current_profile),
                }
            )
    else:
        return jsonify({"error": "Profile not found"}), 404


@app.route("/api/current-channel", methods=["POST"])
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
    response_profile = dict(current_profile)
    response_profile["selected_channel_index"] = channel_index
    response_profile["selected_channel"] = channels[channel_index]

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


@app.route("/api/nodes", methods=["GET"])
def get_nodes():
    """Get nodes seen by current profile in meshdb."""
    current_profile = _get_session_profile()
    if not current_profile:
        return jsonify({"error": "No profile selected"}), 400

    mesh_store = _get_mesh_store(current_profile)
    nodes = mesh_store.list_nodes() if mesh_store else []
    return jsonify({"nodes": nodes, "count": len(nodes)})


@app.route("/api/nodes/<int:node_num>", methods=["GET"])
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
def get_stats():
    """Get database statistics"""
    profiles = profile_manager.get_all_profiles()
    base_stats = db.get_stats()
    global_stats = {
        "profiles": base_stats["profiles"],
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
    print(f"[WEBSOCKET] join_channel request received: {data}")
    if channel is not None:
        room_name = f"channel_{channel}"
        join_room(room_name)
        print(f"[WEBSOCKET] ✅ Client {request.sid} joined room: {room_name}")
        emit("status", {"msg": f"Joined channel {channel}"})
    else:
        print(f"[WEBSOCKET] ❌ join_channel called with no channel: {data}")


@socketio.on("join_profile")
def handle_join_profile(data):
    """Join a WebSocket room for a specific profile."""
    profile_id = data.get("profile_id")
    if profile_id:
        room_name = f"profile_{profile_id}"
        join_room(room_name)
        print(f"[WEBSOCKET] ✅ Client {request.sid} joined room: {room_name}")
        emit("status", {"msg": f"Joined profile {profile_id}"})
    else:
        print(f"[WEBSOCKET] ❌ join_profile called with no profile_id: {data}")


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
