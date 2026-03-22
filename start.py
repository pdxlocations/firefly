#!/usr/bin/env python3
"""
Enhanced MUDP Chat Application with Node Discovery
Startup script with database initialization and feature overview
"""

import sys
import os
import socket
import subprocess
import importlib.util
import signal
import atexit
from database import Database

REQUIRED_RUNTIME_MODULES = ("meshdb", "vnode")
_SHUTDOWN_IN_PROGRESS = False


def running_in_container():
    return os.path.exists('/.dockerenv') or os.environ.get('container')


def get_network_mode():
    return os.environ.get('NETWORK_MODE', '').strip().lower()


def _missing_runtime_modules():
    missing = []
    for module_name in REQUIRED_RUNTIME_MODULES:
        if importlib.util.find_spec(module_name) is None:
            missing.append(module_name)
    return missing


def ensure_runtime_python():
    missing = _missing_runtime_modules()
    if missing:
        missing_str = ", ".join(missing)
        raise ModuleNotFoundError(
            f"Missing required modules in interpreter {sys.executable}: {missing_str}. "
            "Install dependencies in the currently selected environment."
        )


def get_host_ip():
    """Get the host machine's IP address, especially useful in Docker containers"""
    try:
        # Check if we're running in a Docker container
        if running_in_container():
            print("🐳 Docker container detected, finding host IP address...")
            
            # Check network mode from docker-compose or environment
            network_mode = get_network_mode()
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


def get_display_host_and_port():
    app_host = os.getenv('FIREFLY_HOST', '0.0.0.0')
    app_port = int(os.getenv('FIREFLY_PORT', 5011))

    if running_in_container():
        if get_network_mode() == 'host':
            display_host = get_host_ip() if app_host == '0.0.0.0' else app_host
            return display_host, app_port
        return 'localhost', int(os.getenv('FIREFLY_WEB_PORT', app_port))

    display_host = get_host_ip() if app_host == '0.0.0.0' else app_host
    return display_host, app_port


def get_bind_host_and_port():
    return os.getenv('FIREFLY_HOST', '0.0.0.0'), int(os.getenv('FIREFLY_PORT', 5011))


def print_access_summary():
    display_host, display_port = get_display_host_and_port()
    bind_host, bind_port = get_bind_host_and_port()

    if running_in_container() and get_network_mode() == 'bridge':
        print(f"🌐 Web interface: http://{display_host}:{display_port} (host-published)")
        print(f"🔌 Container bind: http://{bind_host}:{bind_port}")
        return

    print(f"🌐 Web interface: http://{display_host}:{display_port}")


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
    display_host, display_port = get_display_host_and_port()
    if running_in_container() and get_network_mode() == 'bridge':
        print(f"• Web Interface - Browse to http://{display_host}:{display_port} on the host machine")
        print("• Container Port - Flask listens on the internal container port configured by FIREFLY_PORT")
    else:
        print(f"• Web Interface - Browse to http://{display_host}:{display_port} after startup")
    print("")
    print("📋 PAGES AVAILABLE:")
    print("• Chat - Send/receive messages with node overview")
    print("• Nodes - Comprehensive list of discovered mesh nodes")
    print("• Profiles - Manage your Meshtastic configurations")


def _register_shutdown_hooks(udp_server, packet_receiver=None):
    def cleanup():
        global _SHUTDOWN_IN_PROGRESS
        if _SHUTDOWN_IN_PROGRESS:
            return
        _SHUTDOWN_IN_PROGRESS = True
        try:
            if udp_server:
                udp_server.stop()
        except Exception as exc:
            print(f"[SHUTDOWN] Error while stopping UDP runtime: {exc}")
        try:
            if packet_receiver:
                packet_receiver.stop()
        except Exception as exc:
            print(f"[SHUTDOWN] Error while stopping packet receiver: {exc}")

    def handle_signal(signum, _frame):
        signal_name = signal.Signals(signum).name
        print(f"\n[SHUTDOWN] Received {signal_name}, stopping Firefly...")
        cleanup()
        # Force process termination so Werkzeug/background threads cannot keep the HTTP port bound.
        os._exit(0)

    atexit.register(cleanup)
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)


def main():
    """Main startup routine"""
    print_banner()
    udp_server = None

    try:
        ensure_runtime_python()

        # Initialize database
        db = initialize_database()

        # Show features
        show_features()

        print(f"\n🗄️  Database: {os.getenv('FIREFLY_DATABASE_FILE', 'firefly.db')}")
        print_access_summary()
        print("\n" + "=" * 50)
        print("Starting Flask application...")
        print("=" * 50)

        # Import and run the main application
        from app import app, socketio, shared_packet_receiver, udp_server
        _register_shutdown_hooks(udp_server, shared_packet_receiver)

        print("✓ Meshtastic packet receiver started")
        print("✓ Virtual node transport will activate per profile when needed")
        # Get port from environment variable, default to 5011
        port = int(os.getenv('FIREFLY_PORT', 5011))
        host = os.getenv('FIREFLY_HOST', '0.0.0.0')
        debug = os.getenv('FIREFLY_DEBUG', 'false').lower() == 'true'
        
        if running_in_container() and get_network_mode() == 'bridge':
            bind_host, bind_port = get_bind_host_and_port()
            print(f"🌐 Starting Flask server inside container on http://{bind_host}:{bind_port}")
        else:
            display_host, display_port = get_display_host_and_port()
            print(f"🌐 Starting Flask server on http://{display_host}:{display_port}")

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
        try:
            if udp_server:
                udp_server.stop()
        except Exception:
            pass
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error starting application: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
