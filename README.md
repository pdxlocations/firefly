# MUDP Chat Application

A real-time web-based chat application that communicates over UDP on the local network. Built with Python Flask and WebSockets for real-time messaging with user profile management.

<img width="1239" height="653" alt="image" src="https://github.com/user-attachments/assets/b8914af6-0343-464b-9bc1-e8403c1ff103" />

## Features

- **Real-time Chat**: Send and receive messages instantly across the local network
- **UDP Multicast**: Messages are sent via UDP multicast to all devices on the same network
- **User Profiles**: Create, edit, and manage multiple user profiles
- **Web Interface**: Clean, responsive web interface built with Bootstrap
- **Real-time Updates**: WebSocket integration for instant message delivery
- **Local Network**: No internet required - works entirely on your local network

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

### Using the Chat

1. **Create a Profile**
   - Go to the "Profiles" page
   - Click "Create New Profile"
   - Fill in your username and display name
   - Click "Create Profile"

2. **Select Your Profile**
   - Return to the "Chat" page
   - Select your profile from the dropdown
   - Click "Set Profile"

3. **Start Chatting**
   - Type your message in the input field
   - Press Enter or click "Send"
   - Your message will be broadcast to all other chat applications on the network

### Multiple Instances

To chat with others on your network:

1. Each person should run the application on their device
2. Everyone should be connected to the same local network
3. Each person needs to create their own profile
4. Messages will be automatically shared between all running instances

## Configuration

You can modify these settings in `app.py`:

- **MCAST_GRP**: Default is 224.0.0.69 (multicast group address)
- **MCAST_PORT**: Default is 4403 (UDP multicast port)
- **Flask port**: Default is 5011 (change in run.py)

## Network Requirements

- All devices must be on the same local network
- UDP multicast port 4403 must be available and not blocked by firewalls
- For best results, ensure your network allows UDP multicast traffic

## File Structure

```
mudpchat-2/
├── app.py              # Main Flask application
├── requirements.txt    # Python dependencies
├── profiles.json       # User profiles (created automatically)
├── templates/
│   ├── base.html      # Base template
│   ├── index.html     # Chat interface
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
- **Frontend**: HTML5, CSS3, JavaScript (ES6+)
- **Real-time**: WebSocket connections for instant updates
- **Networking**: UDP sockets for local network communication
- **Storage**: JSON file for profile persistence
- **Threading**: Flask-SocketIO uses the threading backend, and UDPPacketStream runs a background listener thread for receiving packets

## Security Considerations

- This application is designed for local network use only
- No authentication or encryption is implemented
- Messages are sent in plain text over UDP
- Do not use on untrusted networks
- Consider firewall rules if security is a concern

## License

This project is provided as-is for educational and personal use.
