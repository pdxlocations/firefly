#!/usr/bin/env python3

import json
from collections import deque

from datetime import datetime
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
import uuid
import os
import socketio as py_socketio
import engineio
from pubsub import pub

from meshtastic.protobuf import mesh_pb2, portnums_pb2
from mudp import UDPPacketStream, node, conn, send_text_message
from database import Database


MCAST_GRP = "224.0.0.69"
MCAST_PORT = 4403
PROFILES_FILE = "profiles.json"
SOCKETIO_CLIENT_VERSION = "4.7.5"

current_profile = None
messages = []

# De-duplication cache for received packets to avoid double-display
_DEDUP_CACHE = set()
_DEDUP_QUEUE = deque(maxlen=500)

# Initialize database
db = Database()


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


# Global interface - will be recreated when profile changes
interface = None


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


def _my_node_num():
    """Return numeric node id derived from node.node_id like '!deadbeef', else None."""
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

    print(f"\n[RECV] From: {getattr(packet, 'from', None)} Message: {msg}")

    # Push into in-memory log and notify connected clients
    try:
        sender_num = getattr(packet, "from", None)
        
        # Look up node name from database if available
        sender_display = str(sender_num) if sender_num else "Unknown"
        if current_profile and sender_num:
            try:
                nodes = db.get_nodes_for_profile(current_profile["id"])
                node = next((n for n in nodes if n["node_num"] == sender_num), None)
                if node and node.get("long_name"):
                    sender_display = node["long_name"]
                    print(f"[MESSAGE] Using node name: {sender_display} for {sender_num}")
                else:
                    # Fallback to hex format if no name found
                    sender_display = f"!{hex(sender_num)[2:].zfill(8)}"
                    print(f"[MESSAGE] No node name found, using hex: {sender_display}")
            except Exception as e:
                print(f"[MESSAGE] Error looking up node name: {e}")
                sender_display = f"!{hex(sender_num)[2:].zfill(8)}" if sender_num else "Unknown"
        
        message = {
            "id": str(uuid.uuid4()),
            "sender": str(sender_num) if sender_num else "Unknown",
            "sender_display": sender_display,
            "content": msg,
            "timestamp": datetime.now().isoformat(),
            "sender_ip": (addr[0] if isinstance(addr, tuple) and len(addr) >= 1 else "mesh"),
        }
        messages.append(message)
        
        # Store message in database if profile is set
        if current_profile:
            db.store_message(
                profile_id=current_profile["id"],
                message_id=message["id"],
                packet_id=pkt_id,
                sender_num=getattr(packet, "from", None),
                sender_display=message["sender_display"],
                content=msg,
                sender_ip=message["sender_ip"],
                direction="received",
                channel=packet.channel,
                hop_limit=packet.hop_limit,
                hop_start=packet.hop_start,
                rx_snr=packet.rx_snr,
                rx_rssi=packet.rx_rssi
            )
        
        socketio.emit("new_message", message)
    except Exception as e:
        print(f"Failed to emit incoming message: {e}")


def on_nodeinfo(packet: mesh_pb2.MeshPacket, addr=None):
    """Handle NODEINFO_APP packets and store/update node information"""
    print(f"\n[NODEINFO_DEBUG] on_nodeinfo called with packet from {getattr(packet, 'from', None)}")
    
    if not current_profile:
        print(f"[NODEINFO_DEBUG] No current profile set, skipping")
        return  # Skip if no profile is selected
    
    print(f"[NODEINFO_DEBUG] Current profile: {current_profile.get('long_name', 'unknown')} ({current_profile.get('id', 'unknown')})")
        
    if not packet.HasField("decoded") or packet.decoded.portnum != portnums_pb2.PortNum.NODEINFO_APP:
        print(f"[NODEINFO_DEBUG] Not a NODEINFO packet: decoded={packet.HasField('decoded')}, portnum={packet.decoded.portnum if packet.HasField('decoded') else 'N/A'}")
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
        
    try:
        # Parse the nodeinfo payload - try different approaches
        print(f"[NODEINFO_DEBUG] Payload length: {len(packet.decoded.payload)} bytes")
        print(f"[NODEINFO_DEBUG] Payload (first 100 chars): {packet.decoded.payload[:100]}")
        
        # First try to parse as User protobuf
        try:
            from meshtastic.protobuf import mesh_pb2
            user = mesh_pb2.User()
            user.ParseFromString(packet.decoded.payload)
            
            node_id = getattr(user, "id", "")
            long_name = getattr(user, "long_name", "")
            short_name = getattr(user, "short_name", "")
            macaddr = getattr(user, "macaddr", b"")
            hw_model_num = getattr(user, "hw_model", 0)
            role_num = getattr(user, "role", 0)
            public_key = getattr(user, "public_key", b"")
            
            print(f"[NODEINFO_DEBUG] Successfully parsed as User protobuf")
            
        except Exception as proto_error:
            print(f"[NODEINFO_DEBUG] Failed to parse as User protobuf: {proto_error}")
            
            # Try to parse as plain text (like your log shows)
            try:
                payload_str = packet.decoded.payload.decode('utf-8')
                print(f"[NODEINFO_DEBUG] Payload as string: {payload_str}")
                
                # Parse the text format from your logs
                # Expected format: id: "!da621930" long_name: "Somebody Once Told Me" short_name: "SMB" macaddr: "..." hw_model: HELTEC_V3
                import re
                
                node_id_match = re.search(r'id: "([^"]+)"', payload_str)
                long_name_match = re.search(r'long_name: "([^"]+)"', payload_str)
                short_name_match = re.search(r'short_name: "([^"]+)"', payload_str)
                hw_model_match = re.search(r'hw_model: ([A-Z_0-9]+)', payload_str)
                
                node_id = node_id_match.group(1) if node_id_match else f"!{hex(sender_num)[2:].zfill(8)}"
                long_name = long_name_match.group(1) if long_name_match else f"Node {sender_num}"
                short_name = short_name_match.group(1) if short_name_match else f"N{str(sender_num)[-4:]}"
                hw_model_num = 0
                role_num = 0
                macaddr = b""
                public_key = b""
                
                # Try to map hardware model string to number if possible
                hw_model = hw_model_match.group(1) if hw_model_match else "UNKNOWN"
                
                print(f"[NODEINFO_DEBUG] Successfully parsed as text: id={node_id}, name={long_name}")
                
            except Exception as text_error:
                print(f"[NODEINFO_DEBUG] Failed to parse as text: {text_error}")
                
                # Last resort - create basic info from sender
                node_id = f"!{hex(sender_num)[2:].zfill(8)}"
                long_name = f"Node {sender_num}"
                short_name = f"N{str(sender_num)[-4:]}"
                hw_model_num = 0
                role_num = 0
                macaddr = b""
                public_key = b""
                hw_model = "UNKNOWN"
                
                print(f"[NODEINFO_DEBUG] Using fallback parsing: id={node_id}, name={long_name}")
        
        # Convert enums to strings (hw_model might already be set for text parsing)
        if 'hw_model' not in locals():
            try:
                from meshtastic.protobuf import config_pb2
                hw_model = config_pb2.Config.DeviceConfig.HwModel.Name(hw_model_num) if hw_model_num else "UNSET"
            except:
                hw_model = str(hw_model_num)
            
        try:
            from meshtastic.protobuf import config_pb2
            role = config_pb2.Config.DeviceConfig.Role.Name(role_num) if role_num else "CLIENT"
        except:
            role = str(role_num)
        
        # Get existing node data to check for changes
        existing_nodes = db.get_nodes_for_profile(current_profile["id"])
        existing_node = next((n for n in existing_nodes if n["node_num"] == sender_num), None)
        
        # Track what has changed
        changes = []
        is_new_node = existing_node is None
        
        if existing_node:
            # Compare fields and track changes
            if existing_node.get("node_id") != node_id:
                changes.append(f"node_id: '{existing_node.get('node_id')}' -> '{node_id}'")
            if existing_node.get("long_name") != long_name:
                changes.append(f"long_name: '{existing_node.get('long_name')}' -> '{long_name}'")
            if existing_node.get("short_name") != short_name:
                changes.append(f"short_name: '{existing_node.get('short_name')}' -> '{short_name}'")
            if existing_node.get("hw_model") != hw_model:
                changes.append(f"hw_model: '{existing_node.get('hw_model')}' -> '{hw_model}'")
            if existing_node.get("role") != role:
                changes.append(f"role: '{existing_node.get('role')}' -> '{role}'")
                
            # Compare MAC address if both exist
            existing_mac = existing_node.get("macaddr")
            new_mac = ':'.join(f'{b:02x}' for b in macaddr) if macaddr else None
            if existing_mac != new_mac and new_mac:  # Only track if new MAC is not empty
                changes.append(f"macaddr: '{existing_mac}' -> '{new_mac}'")
        
        # Log the nodeinfo with change information
        if is_new_node:
            print(f"\n[NODEINFO] NEW NODE {sender_num}: {long_name} ({short_name}) - {hw_model}")
        elif changes:
            print(f"\n[NODEINFO] UPDATED NODE {sender_num}: {long_name} ({short_name}) - {hw_model}")
            print(f"[CHANGES] {', '.join(changes)}")
        else:
            print(f"\n[NODEINFO] SEEN NODE {sender_num}: {long_name} ({short_name}) - {hw_model} (no changes)")
        
        # Store raw nodeinfo for debugging/future use
        raw_nodeinfo = json.dumps({
            "id": node_id,
            "long_name": long_name,
            "short_name": short_name,
            "hw_model": hw_model,
            "hw_model_num": hw_model_num,
            "role": role,
            "role_num": role_num,
            "packet_info": {
                "channel": packet.channel,
                "hop_limit": packet.hop_limit,
                "hop_start": packet.hop_start,
                "rx_snr": packet.rx_snr,
                "rx_rssi": packet.rx_rssi
            },
            "changes": changes,
            "is_new": is_new_node
        })
        
        # Store/update node in database
        success = db.store_node(
            profile_id=current_profile["id"],
            node_num=sender_num,
            node_id=node_id,
            long_name=long_name,
            short_name=short_name,
            macaddr=macaddr if macaddr else None,
            hw_model=hw_model,
            role=role,
            public_key=public_key if public_key else None,
            raw_nodeinfo=raw_nodeinfo
        )
        
        if not success:
            print(f"[ERROR] Failed to store node {sender_num} in database")
        else:
            print(f"[NODEINFO_DEBUG] Successfully stored node {sender_num} ({long_name}) for profile {current_profile.get('id')}")
        
        # Notify connected clients about node update
        try:
            socketio.emit("node_update", {
                "node_num": sender_num,
                "node_id": node_id,
                "long_name": long_name,
                "short_name": short_name,
                "hw_model": hw_model,
                "role": role,
                "is_new": is_new_node,
                "changes": changes,
                "packet_info": {
                    "rx_snr": packet.rx_snr,
                    "rx_rssi": packet.rx_rssi,
                    "hop_limit": packet.hop_limit
                }
            })
        except Exception as e:
            print(f"Failed to emit node update: {e}")
            
    except Exception as e:
        print(f"Error processing nodeinfo: {e}")


pub.subscribe(on_recieve, "mesh.rx.packet")
pub.subscribe(on_text_message, "mesh.rx.port.1")
pub.subscribe(on_nodeinfo, "mesh.rx.port.4")  # NODEINFO_APP


app = Flask(__name__)
app.config["SECRET_KEY"] = "your-secret-key-here"
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

    def create_profile(self, profile_id, node_id, long_name, short_name, channel, key):
        """Create a new profile"""
        return self.db.create_profile(profile_id, node_id, long_name, short_name, channel, key)

    def update_profile(self, profile_id, node_id, long_name, short_name, channel, key):
        """Update an existing profile"""
        return self.db.update_profile(profile_id, node_id, long_name, short_name, channel, key)

    def delete_profile(self, profile_id):
        """Delete a profile"""
        return self.db.delete_profile(profile_id)


def create_interface_for_profile(profile):
    """Create a new MUDP interface with the profile's key"""
    global interface
    
    if not profile:
        print("[INTERFACE] No profile provided, cannot create interface")
        return None
        
    profile_key = profile.get("key", "")
    if not profile_key:
        print(f"[INTERFACE] Profile {profile.get('id', 'unknown')} has no key")
        return None
    
    try:
        # Stop existing interface if running
        if interface:
            try:
                interface.stop()
                print("[INTERFACE] Stopped existing interface")
            except:
                pass
        
        # Create new interface with profile's key
        interface = UDPPacketStream(MCAST_GRP, MCAST_PORT, key=profile_key)
        conn.setup_multicast(MCAST_GRP, MCAST_PORT)
        
        print(f"[INTERFACE] Created interface for profile {profile.get('long_name', 'unknown')} with key {profile_key[:8]}...")
        return interface
        
    except Exception as e:
        print(f"[INTERFACE] Error creating interface: {e}")
        return None


class UDPChatServer:
    def __init__(self):
        self.running = False
        self.current_profile_id = None

    def start(self, profile=None):
        """Start the mudp interface receiver with profile-specific key"""
        global interface
        
        if not profile:
            print("[UDP] Cannot start without a profile (need encryption key)")
            return False
            
        # Create interface for this profile
        interface = create_interface_for_profile(profile)
        if not interface:
            return False
            
        try:
            interface.start()
            self.running = True
            self.current_profile_id = profile.get("id")
            print(f"[UDP] Started interface for profile {profile.get('long_name', 'unknown')}")
            print(f"[UDP] Listening on {MCAST_GRP}:{MCAST_PORT} with profile key")
            return True
        except Exception as e:
            print(f"[UDP] Failed to start mudp interface: {e}")
            return False

    def stop(self):
        """Stop the mudp interface"""
        global interface
        self.running = False
        try:
            if interface:
                interface.stop()
                print("[UDP] Interface stopped")
        except Exception as e:
            print(f"[UDP] Error stopping interface: {e}")
            
    def restart_with_profile(self, profile):
        """Restart the interface with a new profile"""
        print(f"[UDP] Restarting interface for profile {profile.get('long_name', 'unknown')}")
        self.stop()
        return self.start(profile)

    def send_message(self, message_content, sender_profile):
        """Send a text message via mudp"""
        if not sender_profile:
            return False
            
        # Ensure interface is running for current profile
        if not self.running or self.current_profile_id != sender_profile.get("id"):
            print(f"[UDP] Interface not running for current profile, restarting...")
            if not self.restart_with_profile(sender_profile):
                print(f"[UDP] Failed to restart interface for profile")
                return False
                
        try:
            send_text_message(message_content)

            # Mirror to local UI
            message = {
                "id": str(uuid.uuid4()),
                "sender": sender_profile.get("short_name", "Unknown"),
                "sender_display": sender_profile.get("long_name", "Unknown"),
                "content": message_content,
                "timestamp": datetime.now().isoformat(),
                "sender_ip": "self",
            }
            messages.append(message)
            
            # Store sent message in database
            if current_profile:
                try:
                    my_node_num = _my_node_num()
                    db.store_message(
                        profile_id=current_profile["id"],
                        message_id=message["id"],
                        packet_id=None,  # We don't have packet ID for sent messages yet
                        sender_num=my_node_num,
                        sender_display=message["sender_display"],
                        content=message_content,
                        sender_ip="self",
                        direction="sent"
                    )
                except Exception as e:
                    print(f"Failed to store sent message in database: {e}")
            
            try:
                socketio.emit("new_message", message)
            except Exception:
                pass
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
    return render_template("index.html", profiles=profiles, current_profile=current_profile)


@app.route("/profiles")
def profiles():
    """Profile management page"""
    profiles = profile_manager.get_all_profiles()
    return render_template("profiles.html", profiles=profiles)


@app.route("/nodes")
def nodes():
    """Nodes page - display seen nodes"""
    if not current_profile:
        # Show empty page if no profile selected
        return render_template("nodes.html", nodes=[], current_profile=None, stats={})
    
    nodes = db.get_nodes_for_profile(current_profile["id"])
    stats = db.get_stats(current_profile["id"])
    
    return render_template("nodes.html", nodes=nodes, current_profile=current_profile, stats=stats)


@app.route("/api/profiles", methods=["GET"])
def get_profiles():
    """Get all profiles"""
    return jsonify(profile_manager.get_all_profiles())


@app.route("/api/profiles", methods=["POST"])
def create_profile():
    """Create a new profile"""
    data = request.get_json()

    required = ["node_id", "long_name", "short_name", "channel", "key"]
    if not data or any(not data.get(k) for k in required):
        return jsonify({"error": "node_id, long_name, short_name, channel, key are required"}), 400

    # Create and store the profile
    profile_id = str(uuid.uuid4())
    success = profile_manager.create_profile(
        profile_id, data["node_id"], data["long_name"], 
        data["short_name"], data["channel"], data["key"]
    )
    
    if success:
        return jsonify({"profile_id": profile_id, "message": "Profile created successfully"})
    else:
        return jsonify({"error": "Failed to create profile"}), 500


@app.route("/api/profiles/<profile_id>", methods=["PUT"])
def update_profile(profile_id):
    """Update an existing profile"""
    data = request.get_json()

    required = ["node_id", "long_name", "short_name", "channel", "key"]
    if not data or any(not data.get(k) for k in required):
        return jsonify({"error": "node_id, long_name, short_name, channel, key are required"}), 400

    success = profile_manager.update_profile(
        profile_id, data["node_id"], data["long_name"], data["short_name"], data["channel"], data["key"]
    )

    if success:
        return jsonify({"message": "Profile updated successfully"})
    else:
        return jsonify({"error": "Profile not found"}), 404


@app.route("/api/profiles/<profile_id>", methods=["DELETE"])
def delete_profile(profile_id):
    """Delete a profile"""
    global current_profile

    success = profile_manager.delete_profile(profile_id)

    if success:
        # If this was the current profile, unset it
        if current_profile and current_profile.get("id") == profile_id:
            current_profile = None
        return jsonify({"message": "Profile deleted successfully"})
    else:
        return jsonify({"error": "Profile not found"}), 404


@app.route("/api/current-profile", methods=["GET"])
def get_current_profile():
    """Get the current active profile with interface status"""
    if current_profile:
        # Determine interface status based on udp_server state
        if udp_server.running and udp_server.current_profile_id == current_profile.get("id"):
            interface_status = "started"
        else:
            interface_status = "stopped"
            
        # Return profile with interface status
        response_data = dict(current_profile)
        response_data["interface_status"] = interface_status
        return jsonify(response_data)
    else:
        return jsonify(None)


@app.route("/api/current-profile", methods=["POST"])
def set_current_profile():
    """Set the current active profile and restart interface with new key"""
    global current_profile

    data = request.get_json()
    profile_id = data.get("profile_id")

    if not profile_id:
        # Unset profile and stop interface
        current_profile = None
        udp_server.stop()
        print("[PROFILE] Profile unset, interface stopped")
        return jsonify({
            "message": "Profile unset", 
            "profile": None,
            "messages": []  # Clear messages when no profile selected
        })

    profile = profile_manager.get_profile(profile_id)
    print(f"[API] set_current_profile -> requested id={profile_id} exists={bool(profile)}")
    
    if profile:
        # Set current profile
        current_profile = profile
        
        # Update node attributes
        node.channel = profile.get("channel", "")
        node.node_id = profile.get("node_id", "")
        node.long_name = profile.get("long_name", "")
        node.short_name = profile.get("short_name", "")
        node.key = profile.get("key", "")
        
        print(f"[PROFILE] Loaded node attrs from profile {profile.get('long_name', 'unknown')}")
        
        # Restart UDP interface with new profile key
        print(f"[PROFILE] Restarting interface with key from profile {profile.get('long_name', 'unknown')}")
        interface_started = udp_server.restart_with_profile(profile)
        
        # Get messages for the newly selected profile
        profile_messages = db.get_messages_for_profile(current_profile["id"])
        
        if interface_started:
            print(f"[PROFILE] Interface successfully started with profile key")
            return jsonify({
                "message": "Profile set successfully and interface restarted", 
                "profile": current_profile,
                "interface_status": "started",
                "messages": profile_messages
            })
        else:
            print(f"[PROFILE] Warning: Profile set but interface failed to start")
            return jsonify({
                "message": "Profile set but interface failed to start", 
                "profile": current_profile,
                "interface_status": "failed",
                "warning": "You may not receive messages until interface is fixed",
                "messages": profile_messages
            })
    else:
        return jsonify({"error": "Profile not found"}), 404


@app.route("/api/messages", methods=["GET"])
def get_messages():
    """Get messages for current profile"""
    if not current_profile:
        # Return in-memory messages if no profile is set (backward compatibility)
        return jsonify(messages)
    
    # Get messages from database for current profile
    db_messages = db.get_messages_for_profile(current_profile["id"])
    return jsonify(db_messages)


@app.route("/api/nodes", methods=["GET"])
def get_nodes():
    """Get all nodes seen by current profile"""
    if not current_profile:
        return jsonify({"error": "No profile selected"}), 400
    
    nodes = db.get_nodes_for_profile(current_profile["id"])
    return jsonify({"nodes": nodes, "count": len(nodes)})


@app.route("/api/nodes/<int:node_num>", methods=["GET"])
def get_node_details(node_num):
    """Get detailed information about a specific node"""
    if not current_profile:
        return jsonify({"error": "No profile selected"}), 400
    
    nodes = db.get_nodes_for_profile(current_profile["id"])
    node = next((n for n in nodes if n["node_num"] == node_num), None)
    
    if not node:
        return jsonify({"error": "Node not found"}), 404
        
    return jsonify(node)


@app.route("/api/stats", methods=["GET"])
def get_stats():
    """Get database statistics"""
    if current_profile:
        profile_stats = db.get_stats(current_profile["id"])
        global_stats = db.get_stats()
        return jsonify({
            "profile": profile_stats,
            "global": global_stats
        })
    else:
        global_stats = db.get_stats()
        return jsonify({
            "global": global_stats
        })


@app.route("/api/send-message", methods=["POST"])
def send_message():
    """Send a message"""
    print(f"[API] /api/send-message called. current_profile set? {bool(current_profile)}")
    if not current_profile:
        return jsonify({"error": "No profile selected"}), 400

    data = request.get_json()
    print(f"[API] payload: {data}")
    message_content = data.get("message", "").strip()

    if not message_content:
        return jsonify({"error": "Message cannot be empty"}), 400

    success = udp_server.send_message(message_content, current_profile)

    if success:
        print("[API] send_message -> success")
        return jsonify({"message": "Message sent successfully"})
    else:
        print("[API] send_message -> FAILED (udp_server.send_message returned False)")
        return jsonify({"error": "Failed to send message"}), 500


@app.route("/api/health")
def health():
    global_stats = db.get_stats()
    return jsonify({
        "status": "ok", 
        "profiles": global_stats["profiles"],
        "messages": len(messages),  # In-memory messages count
        "database": global_stats
    })


@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@socketio.on("connect")
def handle_connect():
    """Handle WebSocket connection"""
    print("Client connected")
    emit("status", {"msg": "Connected to chat server"})


@socketio.on("disconnect")
def handle_disconnect():
    """Handle WebSocket disconnection"""
    print("Client disconnected")


if __name__ == "__main__":
    print("[STARTUP] Using mudp UDPPacketStream for UDP multicast I/O")
    print("[STARTUP] Interface will start when a profile is selected")
    
    app.config["TEMPLATES_AUTO_RELOAD"] = True
    print(f"Starting Flask-SocketIO server on http://localhost:5011 (interface starts with profile selection)...")
    socketio.run(app, host="0.0.0.0", port=5011, debug=True, use_reloader=True, allow_unsafe_werkzeug=True)
