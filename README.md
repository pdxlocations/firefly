# MUDP Chat Application

A real-time web-based chat application that communicates over UDP on the local network. Built with Python Flask and WebSockets for real-time messaging with user profile management.

<img width="1239" height="653" alt="image" src="https://github.com/user-attachments/assets/b8914af6-0343-464b-9bc1-e8403c1ff103" />

## Features

- **Real-time Chat**: Send and receive messages instantly across the Meshtastic network
- **Meshtastic Integration**: Connect to Meshtastic mesh networks via UDP multicast
- **Node Discovery**: Automatically discover and track nodes on the mesh network
- **User Profiles**: Create, edit, and manage multiple Meshtastic profiles
- **Database Storage**: Persistent storage of seen nodes and message history per profile
- **Web Interface**: Clean, responsive web interface built with Bootstrap
- **Real-time Updates**: WebSocket integration for instant message and node discovery
- **Detailed Node Information**: View hardware details, roles, and connection statistics

## Requirements

- Python 3.7+
- Modern web browser with WebSocket support
- Local network access

## Installation

1. **Clone or download this project**
   ```bash
   git clone https://github.com/pdxlocations/mudpchat.git
   cd mudpchat
   ```

2. **Create a virtual environment (recommended)**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Starting the Application

1. **Run the application**
   ```bash
   python run.py
   ```
   You can also run app.py directly for testing, but run.py is the recommended entry point.

2. **Open your web browser**
   - Navigate to `http://localhost:5011`
   - Or access from other devices on the network using your computer's IP address: `http://YOUR_IP:5011`

### Using the Application

1. **Create a Meshtastic Profile**
   - Go to the "Profiles" page
   - Click "Create New Profile"
   - Fill in your Meshtastic node details:
     - **Node ID**: Your Meshtastic node ID (e.g., !deadbeef)
     - **Long Name**: Full display name for your node
     - **Short Name**: Short identifier (4 chars max)
     - **Channel**: Meshtastic channel name
     - **Key**: Encryption key for the channel
   - Click "Create Profile"

2. **Select Your Profile**
   - Return to the "Chat" page
   - Select your profile from the dropdown
   - Click "Set Profile"

3. **Start Using the Network**
   - **Chat**: Type messages to communicate with other nodes
   - **View Nodes**: Check the "Nodes" page to see discovered mesh nodes
   - **Monitor Activity**: Watch real-time updates as nodes join and send messages

### Multiple Instances

To chat with others on your network:

1. Each person should run the application on their device
2. Everyone should be connected to the same local network
3. Each person needs to create their own profile
4. Messages will be automatically shared between all running instances

## Pages and Features

### Chat Page
- Real-time messaging with other Meshtastic nodes
- Profile selection and management
- Connection status monitoring
- Quick overview of recently seen nodes

### Nodes Page
- Comprehensive list of all discovered mesh nodes
- Detailed node information including:
  - Hardware model and role
  - First/last seen timestamps
  - Packet counts and signal information
  - MAC addresses and public keys
- Interactive node details modal
- Statistics overview

### Profiles Page
- Create, edit, and delete Meshtastic profiles
- Configure node ID, names, channel, and encryption key
- Switch between different profiles

## Configuration

You can modify these settings in `app.py`:

- **MCAST_GRP**: Default is 224.0.0.69 (multicast group address)
- **MCAST_PORT**: Default is 4403 (UDP multicast port)
- **Flask port**: Default is 5011 (change in run.py)
- **Database**: SQLite database stored as `mudpchat.db`

## Network Requirements

- Connection to a Meshtastic network via UDP multicast
- UDP multicast port 4403 must be available and not blocked by firewalls
- For best results, ensure your network allows UDP multicast traffic
- Compatible with MUDP (Meshtastic UDP) protocol implementations

## File Structure

```
mudpchat/
├── app.py              # Main Flask application
├── database.py         # Database models and operations
├── encryption.py       # Meshtastic encryption/decryption
├── requirements.txt    # Python dependencies
├── run.py             # Application launcher
├── mudpchat.db        # SQLite database (created automatically)
├── templates/
│   ├── base.html      # Base template with navigation
│   ├── index.html     # Chat interface
│   ├── nodes.html     # Node discovery and details
│   └── profiles.html  # Profile management
└── static/
    ├── css/
    │   └── style.css  # Custom styles
    └── js/
        └── app.js     # JavaScript utilities
```

## Troubleshooting

### Port Already in Use
If you get a "port already in use" error:
1. Change the UDP_PORT in `app.py` to a different number (e.g., 12346)
2. Restart the application
3. Make sure all users use the same port number

### No Messages Received
1. Check that all devices are on the same network
2. Verify that UDP port 12345 is not blocked by firewall
3. Try disabling firewall temporarily for testing
4. Check that the application is running on all devices

### Web Interface Not Loading
1. Make sure Flask is running (you should see startup messages)
2. Try accessing via `http://127.0.0.1:5000` instead of localhost
3. Check that port 5000 is not blocked

### Profile Issues
1. Profiles are stored in `profiles.json` - this file is created automatically
2. If you have profile issues, you can delete `profiles.json` and restart
3. Make sure you select a profile before trying to send messages

## Technical Details

- **Backend**: Python Flask with Flask-SocketIO
- **Database**: SQLite for persistent data storage
- **Frontend**: HTML5, CSS3, JavaScript (ES6+)
- **Real-time**: WebSocket connections for instant updates
- **Networking**: Meshtastic MUDP protocol over UDP multicast
- **Mesh Integration**: Direct integration with Meshtastic protobuf messages
- **Node Discovery**: Automatic NODEINFO_APP packet processing
- **Threading**: Flask-SocketIO uses the threading backend, and UDPPacketStream runs a background listener thread for receiving packets
- **Data Storage**: Profiles, nodes, and messages are stored per-profile in SQLite database
- **Profile Persistence**: Profiles survive application restarts and are safely migrated from JSON

## Data Persistence

- **Profile Storage**: All profiles are stored in `mudpchat.db` SQLite database
- **Automatic Migration**: Existing `profiles.json` files are automatically migrated on first run
- **Persistent Data**: Profiles, nodes, and messages survive application restarts
- **Safe Migration**: Migration only occurs once when database is empty
- **Backup Protection**: Original JSON files are preserved as `.migrated` backups

## Security Considerations

- This application integrates with Meshtastic mesh networks
- Uses Meshtastic's built-in AES encryption for message security
- Node information and messages are stored locally in SQLite database
- Database contains channel encryption keys - protect the database file
- Web interface runs on localhost by default (configure as needed)
- Consider firewall rules if exposing the web interface beyond localhost

## License

This project is provided as-is for educational and personal use.
