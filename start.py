#!/usr/bin/env python3
"""
Enhanced MUDP Chat Application with Node Discovery
Startup script with database initialization and feature overview
"""

import sys
import os
from database import Database


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
    print(f"• Web Interface - Browse to http://localhost:{display_port} after startup")
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
        # Get port from environment for display
        display_port = int(os.getenv('FIREFLY_PORT', 5011))
        print(f"🌐 Web interface: http://localhost:{display_port}")
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
        
        print(f"🌐 Starting Flask server on http://localhost:{port}")

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
