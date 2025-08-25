#!/usr/bin/env python3
"""
Simple runner script for the UDP Chat Application
"""

import sys
import subprocess
import os

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 7):
        print("Error: Python 3.7 or higher is required")
        print(f"Current version: {sys.version}")
        return False
    return True

def check_dependencies():
    """Check if required dependencies are installed"""
    required_packages = ['flask', 'flask_socketio']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print("Error: Missing required packages:")
        for package in missing_packages:
            print(f"  - {package}")
        print("\nPlease install dependencies with: pip install -r requirements.txt")
        return False
    
    return True

def main():
    """Main entry point"""
    print("UDP Chat Application")
    print("=" * 50)
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    print("Starting UDP Chat Application...")
    print("Access the application at: http://localhost:5007")
    print("Press Ctrl+C to stop the application")
    print("-" * 50)
    
    try:
        # Import and run the app
        from app import app, socketio, udp_server, SOCKETIO_CLIENT_VERSION, _sio_ver
        
        print(f"[STARTUP] Will load client JS @{SOCKETIO_CLIENT_VERSION} for python-socketio {_sio_ver}")
        
        # Start UDP server
        if udp_server.start():
            print("UDP Chat Server started successfully")
            socketio.run(app, host='0.0.0.0', port=5007, debug=True, use_reloader=True, allow_unsafe_werkzeug=True)
        else:
            print("Failed to start UDP server")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        print(f"Error starting application: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
