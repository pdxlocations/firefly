#!/usr/bin/env python3
"""
Enhanced MUDP Chat Application with Node Discovery
Startup script with database initialization and feature overview
"""

import sys
import os
import socket
import subprocess
from database import Database


def get_host_ip():
    """Get the host machine's IP address, especially useful in Docker containers"""
    try:
        # Check if we're running in a Docker container
        if os.path.exists('/.dockerenv') or os.environ.get('container'):
            print("🐳 Docker container detected, finding host IP address...")
            
            # Check network mode from docker-compose or environment
            network_mode = os.environ.get('NETWORK_MODE', '')
            is_host_network = (network_mode == 'host' or 
                             os.environ.get('DOCKER_HOST_NETWORK') == 'true')
            
            if is_host_network:
                print("   Using host networking mode")
            else:
                print("   Using bridge networking mode")
            
            # Method 1: Try to get the default route gateway (usually the Docker host)
            try:
                # This works when using host networking
                if is_host_network:
                    # Using host networking, get the actual host IP
                    result = subprocess.run(['ip', 'route', 'show', 'default'], 
                                          capture_output=True, text=True, timeout=5)
                    if result.returncode == 0 and result.stdout:
                        # Parse output like: "default via 192.168.1.1 dev eth0"
                        parts = result.stdout.split()
                        if 'via' in parts:
                            gateway_ip = parts[parts.index('via') + 1]
                            print(f"   Found gateway IP: {gateway_ip}")
                        
                    # Get the machine's IP on the default interface
                    result = subprocess.run(['hostname', '-I'], capture_output=True, text=True, timeout=5)
                    if result.returncode == 0 and result.stdout.strip():
                        host_ips = result.stdout.strip().split()
                        # Filter out localhost and Docker internal IPs
                        for ip in host_ips:
                            if not ip.startswith('127.') and not ip.startswith('172.17.') and not ip.startswith('172.18.'):
                                print(f"   Found host IP: {ip}")
                                return ip
                
                # Method 2: Try to connect to a remote address to determine local IP
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                    s.settimeout(5)
                    # Connect to a remote address (doesn't actually send data)
                    s.connect(("8.8.8.8", 80))
                    local_ip = s.getsockname()[0]
                    if not local_ip.startswith('127.'):
                        print(f"   Found local IP via socket: {local_ip}")
                        return local_ip
            
            except (subprocess.TimeoutExpired, subprocess.CalledProcessError, OSError):
                pass
            
            # Method 3: Check Docker bridge network
            try:
                result = subprocess.run(['ip', 'route', 'show'], 
                                      capture_output=True, text=True, timeout=5)
                if result.returncode == 0:
                    lines = result.stdout.split('\n')
                    for line in lines:
                        # Look for default route
                        if 'default via' in line:
                            parts = line.split()
                            if 'via' in parts:
                                gateway = parts[parts.index('via') + 1]
                                # Gateway is typically the Docker host
                                print(f"   Found Docker host via gateway: {gateway}")
                                return gateway
            except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
                pass
        
        # Fallback: Try to get local machine IP for non-Docker environments
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(5)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            if not local_ip.startswith('127.'):
                return local_ip
                
    except Exception as e:
        print(f"   Warning: Could not detect host IP: {e}")
    
    # Final fallback
    return 'localhost'


def print_banner():
    print("🔥 Firefly - Meshtastic Web Chat")
    print("=" * 50)
    print("Real-time mesh networking with node discovery!")
    print("=" * 50)


def initialize_database():
    """Initialize database"""
    print("\n📊 Initializing Database...")
    db = Database()

    # Show database stats
    stats = db.get_stats()
    print(f"✓ Database initialized with {stats['profiles']} profiles")

    if stats["profiles"] == 0:
        print("ℹ️  No profiles found. Create your first profile via the web interface.")

    return db


def show_features():
    """Display key features of the application"""
    print("\n🚀 NEW FEATURES:")
    print("• Node Discovery - Automatically track Meshtastic nodes on your network")
    print("• Persistent Storage - All nodes and messages saved to SQLite database")
    print("• Per-Profile History - Each profile maintains its own node list and messages")
    print("• Detailed Node Info - Hardware models, roles, signal strength, and more")
    print("• Real-time Updates - WebSocket notifications for new nodes and messages")
    # Get port from environment for display
    display_port = int(os.getenv('FIREFLY_PORT', 5011))
    host_ip = get_host_ip()
    print(f"• Web Interface - Browse to http://{host_ip}:{display_port} after startup")
    print("")
    print("📋 PAGES AVAILABLE:")
    print("• Chat - Send/receive messages with node overview")
    print("• Nodes - Comprehensive list of discovered mesh nodes")
    print("• Profiles - Manage your Meshtastic configurations")


def main():
    """Main startup routine"""
    print_banner()

    try:
        # Initialize database
        db = initialize_database()

        # Show features
        show_features()

        print(f"\n🗄️  Database: firefly.db")
        print(f"🔧 Test script: python3 test_database.py")
        # Get port and host IP for display
        display_port = int(os.getenv('FIREFLY_PORT', 5011))
        host_ip = get_host_ip()
        print(f"🌐 Web interface: http://{host_ip}:{display_port}")
        print("\n" + "=" * 50)
        print("Starting Flask application...")
        print("=" * 50)

        # Import and run the main application
        from app import app, socketio, udp_server

        # Note: UDP server will start when a profile is selected
        print("✓ Meshtastic UDP server ready (will start with profile selection)")
        # Get port from environment variable, default to 5011
        port = int(os.getenv('FIREFLY_PORT', 5011))
        host = os.getenv('FIREFLY_HOST', '0.0.0.0')
        debug = os.getenv('FIREFLY_DEBUG', 'false').lower() == 'true'
        
        # Get the host IP for display (but still bind to the configured host)
        display_ip = get_host_ip() if host == '0.0.0.0' else host
        print(f"🌐 Starting Flask server on http://{display_ip}:{port}")

        socketio.run(
            app,
            host=host,
            port=port,
            debug=debug,
            use_reloader=False,
            allow_unsafe_werkzeug=True,
        )

    except KeyboardInterrupt:
        print("\n\n👋 Shutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error starting application: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
