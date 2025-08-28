#!/usr/bin/env python3

import json

from datetime import datetime
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit, join_room, leave_room
import uuid
import os


import socketio as py_socketio
import engineio

SOCKETIO_CLIENT_VERSION = "4.7.5"  # compatible with python-socketio 5.x


from pubsub import pub

from meshtastic.protobuf import mesh_pb2, portnums_pb2
from mudp import UDPPacketStream, node, conn, send_text_message


MCAST_GRP = "224.0.0.69"
MCAST_PORT = 4403
KEY = "1PG7OiApB1nwvP+rz05pAQ=="

# node.channel = "ShortFast"
# node.node_id = "!deadbeef"
# node.long_name = "UDP Test"
# node.short_name = "UDP"
# node.key = "AQ=="

interface = UDPPacketStream(MCAST_GRP, MCAST_PORT, key=KEY)
conn.setup_multicast(MCAST_GRP, MCAST_PORT)



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

    # print("id:", packet.id or None)
    # print("rx_time:", packet.rx_time or None)
    # print("rx_snr:", packet.rx_snr or None)
    # print("hop_limit:", packet.hop_limit or None)
    # priority_name = mesh_pb2.MeshPacket.Priority.Name(packet.priority) if packet.priority else "N/A"
    # print("priority:", priority_name or None)
    # print("rx_rssi:", packet.rx_rssi or None)
    # print("hop_start:", packet.hop_start or None)
    # print("next_hop:", packet.next_hop or None)
    # print("relay_node:", packet.relay_node or None)



def on_text_message(packet: mesh_pb2.MeshPacket, addr=None):
    msg = packet.decoded.payload.decode("utf-8", "ignore")
    print(f"\n[RECV] From: {getattr(packet, 'from', None)} Message: {msg}")

    # Push into in-memory log and notify connected clients
    try:
        message = {
            'id': str(uuid.uuid4()),
            'sender': str(getattr(packet, 'from', 'Unknown')),
            'sender_display': str(getattr(packet, 'from', 'Unknown')),
            'content': msg,
            'timestamp': datetime.now().isoformat(),
            'sender_ip': (addr[0] if isinstance(addr, tuple) and len(addr) >= 1 else 'mesh')
        }
        messages.append(message)
        socketio.emit('new_message', message)
    except Exception as e:
        print(f"Failed to emit incoming message: {e}")

pub.subscribe(on_recieve, "mesh.rx.packet")
pub.subscribe(on_text_message, "mesh.rx.port.1")




app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading", logger=True, engineio_logger=True)

@app.context_processor
def inject_versions():
    return {
        'socketio_client_version': SOCKETIO_CLIENT_VERSION,
        'python_socketio_version': getattr(py_socketio, '__version__', 'unknown'),
        'python_engineio_version': getattr(engineio, '__version__', 'unknown'),
    }


PROFILES_FILE = 'profiles.json'
MAX_MESSAGE_LENGTH = 1024


# Global variables
current_profile = None
messages = []
udp_socket = None
running = False


class ProfileManager:
    def __init__(self):
        self.profiles_file = PROFILES_FILE
        self.profiles = self.load_profiles()
    
    def load_profiles(self):
        """Load profiles from JSON file"""
        if os.path.exists(self.profiles_file):
            try:
                with open(self.profiles_file, 'r') as f:

                    return json.load(f)
            except:
                return {}
        return {}
    
    def save_profiles(self):
        """Save profiles to JSON file"""
        with open(self.profiles_file, 'w') as f:
            json.dump(self.profiles, f, indent=2)
    
    def create_profile(self, name, display_name, description=""):
        """Create a new profile"""
        profile_id = str(uuid.uuid4())
        self.profiles[profile_id] = {
            'id': profile_id,
            'node_id': name,            # backwards-compat: treat previous 'name' arg as node_id
            'long_name': display_name,  # backwards-compat: treat previous 'display_name' arg as long_name
            'short_name': description if description else '',  # placeholder if old callers pass description
            'channel': '',
            'key': '',
            'created_at': datetime.now().isoformat()
        }
        self.save_profiles()
        return profile_id
    
    def update_profile(self, profile_id, node_id, long_name, short_name, channel, key):
        """Update an existing profile"""
        if profile_id in self.profiles:
            self.profiles[profile_id].update({
                'node_id': node_id,
                'long_name': long_name,
                'short_name': short_name,
                'channel': channel,
                'key': key,
                'updated_at': datetime.now().isoformat()
            })
            self.save_profiles()
            return True
        return False
    
    def delete_profile(self, profile_id):
        """Delete a profile"""
        if profile_id in self.profiles:
            del self.profiles[profile_id]
            self.save_profiles()
            return True
        return False
    
    def get_profile(self, profile_id):
        """Get a specific profile"""
        return self.profiles.get(profile_id)
    
    def get_all_profiles(self):
        """Get all profiles"""
        return self.profiles


class UDPChatServer:
    def __init__(self):
        self.running = False

    def start(self):
        """Start the mudp interface receiver"""
        try:
            interface.start()
            self.running = True
            print(f"Joined multicast group {MCAST_GRP} on port {MCAST_PORT} via mudp")
            return True
        except Exception as e:
            print(f"Failed to start mudp interface: {e}")
            return False

    def stop(self):
        """Stop the mudp interface"""
        self.running = False
        try:
            interface.stop()
        except Exception:
            pass

    def send_message(self, message_content, sender_profile):
        """Send a text message via mudp"""
        if not sender_profile:
            return False
        try:
            send_text_message(message_content)

            # Mirror to local UI
            message = {
                'id': str(uuid.uuid4()),
                'sender': sender_profile.get('short_name', 'Unknown'),
                'sender_display': sender_profile.get('long_name', 'Unknown'),
                'content': message_content,
                'timestamp': datetime.now().isoformat(),
                'sender_ip': 'self'
            }
            messages.append(message)
            try:
                socketio.emit('new_message', message)
            except Exception:
                pass
            return True
        except Exception as e:
            print(f"Error sending message via mudp: {e}")
            return False


# Initialize managers
profile_manager = ProfileManager()
udp_server = UDPChatServer()


@app.route('/')
def index():
    """Main chat interface"""
    profiles = profile_manager.get_all_profiles()
    return render_template('index.html', profiles=profiles, current_profile=current_profile)


@app.route('/profiles')
def profiles():
    """Profile management page"""
    profiles = profile_manager.get_all_profiles()
    return render_template('profiles.html', profiles=profiles)


@app.route('/api/profiles', methods=['GET'])
def get_profiles():
    """Get all profiles"""
    return jsonify(profile_manager.get_all_profiles())


@app.route('/api/profiles', methods=['POST'])
def create_profile():
    """Create a new profile"""
    data = request.get_json()
    
    required = ['node_id', 'long_name', 'short_name', 'channel', 'key']
    if not data or any(not data.get(k) for k in required):
        return jsonify({'error': 'node_id, long_name, short_name, channel, key are required'}), 400

    # Create and store the profile
    profile_id = str(uuid.uuid4())
    profile_manager.profiles[profile_id] = {
        'id': profile_id,
        'node_id': data['node_id'],
        'long_name': data['long_name'],
        'short_name': data['short_name'],
        'channel': data['channel'],
        'key': data['key'],
        'created_at': datetime.now().isoformat()
    }
    profile_manager.save_profiles()

    return jsonify({'profile_id': profile_id, 'message': 'Profile created successfully'})


@app.route('/api/profiles/<profile_id>', methods=['PUT'])
def update_profile(profile_id):
    """Update an existing profile"""
    data = request.get_json()
    
    required = ['node_id', 'long_name', 'short_name', 'channel', 'key']
    if not data or any(not data.get(k) for k in required):
        return jsonify({'error': 'node_id, long_name, short_name, channel, key are required'}), 400

    success = profile_manager.update_profile(
        profile_id,
        data['node_id'],
        data['long_name'],
        data['short_name'],
        data['channel'],
        data['key']
    )
    
    if success:
        return jsonify({'message': 'Profile updated successfully'})
    else:
        return jsonify({'error': 'Profile not found'}), 404


@app.route('/api/profiles/<profile_id>', methods=['DELETE'])
def delete_profile(profile_id):
    """Delete a profile"""
    global current_profile
    
    success = profile_manager.delete_profile(profile_id)
    
    if success:
        # If this was the current profile, unset it
        if current_profile and current_profile.get('id') == profile_id:
            current_profile = None
        return jsonify({'message': 'Profile deleted successfully'})
    else:
        return jsonify({'error': 'Profile not found'}), 404


@app.route('/api/current-profile', methods=['GET'])
def get_current_profile():
    """Get the current active profile"""
    return jsonify(current_profile)


@app.route('/api/current-profile', methods=['POST'])
def set_current_profile():
    """Set the current active profile"""
    global current_profile
    
    data = request.get_json()
    profile_id = data.get('profile_id')
    
    if not profile_id:
        current_profile = None
        return jsonify({'message': 'Profile unset'})
    
    profile = profile_manager.get_profile(profile_id)
    print(f"[API] set_current_profile -> requested id={profile_id} exists={bool(profile)}")
    if profile:
        current_profile = profile
        node.channel = profile.get('channel', '')
        node.node_id = profile.get('node_id', '')
        node.long_name = profile.get('long_name', '')
        node.short_name = profile.get('short_name', '')
        node.key = profile.get('key', '')
        print(f"[PROFILE] Loaded node attrs from profile {profile.get('id', 'unknown')}")
        return jsonify({'message': 'Profile set successfully', 'profile': current_profile})
    else:
        return jsonify({'error': 'Profile not found'}), 404


@app.route('/api/messages', methods=['GET'])
def get_messages():
    """Get all messages"""
    return jsonify(messages)


@app.route('/api/send-message', methods=['POST'])
def send_message():
    """Send a message"""
    print(f"[API] /api/send-message called. current_profile set? {bool(current_profile)}")
    if not current_profile:
        return jsonify({'error': 'No profile selected'}), 400
    
    data = request.get_json()
    print(f"[API] payload: {data}")
    message_content = data.get('message', '').strip()
    
    if not message_content:
        return jsonify({'error': 'Message cannot be empty'}), 400
    
    success = udp_server.send_message(message_content, current_profile)
    
    if success:
        print("[API] send_message -> success")
        return jsonify({'message': 'Message sent successfully'})
    else:
        print("[API] send_message -> FAILED (udp_server.send_message returned False)")
        return jsonify({'error': 'Failed to send message'}), 500


@app.route('/api/health')
def health():
    return jsonify({"status": "ok", "profiles": len(profile_manager.get_all_profiles()), "messages": len(messages)})

@app.after_request
def add_no_cache_headers(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@socketio.on('connect')
def handle_connect():
    """Handle WebSocket connection"""
    print('Client connected')
    emit('status', {'msg': 'Connected to chat server'})


@socketio.on('disconnect')
def handle_disconnect():
    """Handle WebSocket disconnection"""
    print('Client disconnected')


if __name__ == '__main__':
    print("[STARTUP] Using mudp UDPPacketStream for UDP multicast I/O")
    # Start UDP server
    if udp_server.start():
        app.config['TEMPLATES_AUTO_RELOAD'] = True
        print(f"Starting Flask-SocketIO server on http://localhost:5007 (mudp {MCAST_GRP}:{MCAST_PORT}) ...")
        socketio.run(app, host='0.0.0.0', port=5007, debug=True, use_reloader=True, allow_unsafe_werkzeug=True)
    else:
        print("Failed to start UDP server")
