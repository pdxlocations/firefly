#!/usr/bin/env python3
"""
MUDP Chat Startup Script with Virtual Environment
This script ensures the virtual environment is activated before starting the app
"""

import os
import sys
import subprocess

def check_virtual_env():
    """Check if we're running in the virtual environment"""
    venv_path = os.path.join(os.path.dirname(__file__), '.venv')
    
    if not os.path.exists(venv_path):
        print("❌ Virtual environment (.venv) not found!")
        print("Please create it with: python3 -m venv .venv")
        print("Then install dependencies: pip install -r requirements.txt")
        return False
    
    # Check if we're already in the virtual environment
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("✅ Virtual environment is active")
        return True
    else:
        print("⚠️  Virtual environment not activated")
        return False

def start_in_venv():
    """Start the application within the virtual environment"""
    venv_path = os.path.join(os.path.dirname(__file__), '.venv')
    python_path = os.path.join(venv_path, 'bin', 'python3')
    
    if not os.path.exists(python_path):
        print(f"❌ Python interpreter not found at {python_path}")
        return False
    
    print("🚀 Starting MUDP Chat with virtual environment...")
    print("=" * 60)
    
    # Start the application using the virtual environment's Python
    try:
        start_script = os.path.join(os.path.dirname(__file__), 'start.py')
        subprocess.run([python_path, start_script], check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to start application: {e}")
        return False
    except KeyboardInterrupt:
        print("\n👋 Application stopped by user")
        return True

def main():
    print("🌐 MUDP Chat - Virtual Environment Startup")
    print("=" * 50)
    
    if check_virtual_env():
        # Already in virtual environment, import and run directly
        try:
            from start import main as start_main
            start_main()
        except ImportError as e:
            print(f"❌ Failed to import start module: {e}")
            print("Make sure all dependencies are installed in the virtual environment")
            sys.exit(1)
    else:
        # Not in virtual environment, start with proper environment
        if not start_in_venv():
            print("❌ Failed to start with virtual environment")
            print("\n💡 Manual activation:")
            print("1. source .venv/bin/activate")
            print("2. python3 start.py")
            sys.exit(1)

if __name__ == "__main__":
    main()