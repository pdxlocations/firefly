#!/usr/bin/env python3

import json
import socket
import threading
import time
from datetime import datetime
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit, join_room, leave_room
import uuid
import os
import struct

import socketio as py_socketio
import engineio

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading", logger=True, engineio_logger=True)

# Configuration
UDP_PORT = int(os.getenv('UDP_PORT', 4403))
MULTICAST_GROUP = os.getenv('MULTICAST_GROUP', '224.0.0.69')
MULTICAST_IFACE = os.getenv('MULTICAST_IFACE', '0.0.0.0')  # interface IP for join (0.0.0.0 = default)
MULTICAST_TTL = int(os.getenv('MULTICAST_TTL', 1))
PROFILES_FILE = 'profiles.json'
MAX_MESSAGE_LENGTH = 1024

# Determine compatible Socket.IO client version for the installed python-socketio
try:
    _sio_ver = py_socketio.__version__
    _engio_ver = engineio.__version__
except Exception:
    _sio_ver = 'unknown'
    _engio_ver = 'unknown'

def _client_version_for_server(server_version: str) -> str:
    try:
        major = int(server_version.split('.')[0])
    except Exception:
        return '4.7.5'  # safe modern default
    # Mapping based on python-socketio compatibility
    if major <= 4:
        return '2.5.0'   # python-socketio 4.x ↔ JS 2.x
    if major == 5:
        return '3.1.3'   # python-socketio 5.x ↔ JS 3.x
    return '4.7.5'       # python-socketio 6.x+ ↔ JS 4.x

SOCKETIO_CLIENT_VERSION = _client_version_for_server(_sio_ver if _sio_ver != 'unknown' else '6.0.0')

@app.context_processor
def inject_versions():
    return {
        'socketio_client_version': SOCKETIO_CLIENT_VERSION,
        'python_socketio_version': _sio_ver,
        'python_engineio_version': _engio_ver,
    }

print(f"[VERSIONS] python-socketio={_sio_ver} engineio={_engio_ver} -> client JS {SOCKETIO_CLIENT_VERSION}")

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
            'name': name,
            'display_name': display_name,
            'description': description,
            'created_at': datetime.now().isoformat()
        }
        self.save_profiles()
        return profile_id
    
    def update_profile(self, profile_id, name, display_name, description=""):
        """Update an existing profile"""
        if profile_id in self.profiles:
            self.profiles[profile_id].update({
                'name': name,
                'display_name': display_name,
                'description': description,
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
    def __init__(self, port=UDP_PORT):
        self.port = port
        self.socket = None
        self.running = False
        
    def start(self):
        """Start the UDP server"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # Allow multiple listeners and quick restarts
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except Exception:
                pass
            self.socket.bind(('', self.port))

            # Join the multicast group on the specified interface
            group_bytes = socket.inet_aton(MULTICAST_GROUP)
            iface_bytes = socket.inet_aton(MULTICAST_IFACE)
            mreq = struct.pack('=4s4s', group_bytes, iface_bytes)
            self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MULTICAST_TTL)
            self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            try:
                self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, iface_bytes)
            except Exception:
                pass
            print(f"Joined multicast group {MULTICAST_GROUP} on iface {MULTICAST_IFACE}, port {self.port}")

            self.running = True
            
            # Start listening thread
            listen_thread = threading.Thread(target=self.listen_for_messages)
            listen_thread.daemon = True
            listen_thread.start()
            
            print(f"UDP Chat Server started on port {self.port}")
            return True
        except Exception as e:
            print(f"Failed to start UDP server: {e}")
            return False
    
    def stop(self):
        """Stop the UDP server"""
        self.running = False
        if self.socket:
            self.socket.close()
    
    def listen_for_messages(self):
        """Listen for incoming UDP messages"""
        while self.running:
            try:
                data, addr = self.socket.recvfrom(MAX_MESSAGE_LENGTH)
                try:
                    print(f"[UDP IN] from {addr[0]}:{addr[1]} {len(data)}B")
                    print(f"[RAW] {data!r}")
                except Exception as e:
                    print(f"[PRINT ERROR] {e}")

                message_data = json.loads(data.decode('utf-8'))
                
                # Add message to global messages list
                message = {
                    'id': str(uuid.uuid4()),
                    'sender': message_data.get('sender', 'Unknown'),
                    'sender_display': message_data.get('sender_display', 'Unknown'),
                    'content': message_data.get('content', ''),
                    'timestamp': message_data.get('timestamp', datetime.now().isoformat()),
                    'sender_ip': addr[0]
                }
                
                messages.append(message)
                
                # Emit to all connected web clients
                socketio.emit('new_message', message)
                
            except Exception as e:
                if self.running:
                    print(f"Error receiving message: {e}")
    
    def send_message(self, message_content, sender_profile):
        """Send a message via UDP broadcast"""
        if not self.socket or not sender_profile:
            return False
        
        try:
            message_data = {
                'sender': sender_profile['name'],
                'sender_display': sender_profile['display_name'],
                'content': message_content,
                'timestamp': datetime.now().isoformat()
            }
            
            message_json = json.dumps(message_data).encode('utf-8')
            self.socket.sendto(message_json, (MULTICAST_GROUP, self.port))
            
            # Add to local messages
            message = {
                'id': str(uuid.uuid4()),
                'sender': sender_profile['name'],
                'sender_display': sender_profile['display_name'],
                'content': message_content,
                'timestamp': message_data['timestamp'],
                'sender_ip': 'self'
            }
            
            messages.append(message)
            socketio.emit('new_message', message)
            
            return True
        except Exception as e:
            print(f"Error sending message: {e}")
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
    
    if not data or not data.get('name') or not data.get('display_name'):
        return jsonify({'error': 'Name and display_name are required'}), 400
    
    profile_id = profile_manager.create_profile(
        data['name'],
        data['display_name'],
        data.get('description', '')
    )
    
    return jsonify({'profile_id': profile_id, 'message': 'Profile created successfully'})


@app.route('/api/profiles/<profile_id>', methods=['PUT'])
def update_profile(profile_id):
    """Update an existing profile"""
    data = request.get_json()
    
    if not data or not data.get('name') or not data.get('display_name'):
        return jsonify({'error': 'Name and display_name are required'}), 400
    
    success = profile_manager.update_profile(
        profile_id,
        data['name'],
        data['display_name'],
        data.get('description', '')
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
    print(f"[STARTUP] Will load client JS @{SOCKETIO_CLIENT_VERSION} for python-socketio {_sio_ver}")
    # Start UDP server
    if udp_server.start():
        app.config['TEMPLATES_AUTO_RELOAD'] = True
        print(f"Starting Flask-SocketIO server on http://localhost:5007 (multicast {MULTICAST_GROUP}:{UDP_PORT}) ...")
        socketio.run(app, host='0.0.0.0', port=5007, debug=True, use_reloader=True, allow_unsafe_werkzeug=True)
    else:
        print("Failed to start UDP server")
