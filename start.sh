#!/bin/bash

# MUDP Chat Startup Script with Virtual Environment
echo "🌐 MUDP Chat - Starting with Virtual Environment"
echo "=================================================="

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "❌ Virtual environment (.venv) not found!"
    echo "Please create it first:"
    echo "  python3 -m venv .venv"
    echo "  source .venv/bin/activate"
    echo "  pip install -r requirements.txt"
    exit 1
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source .venv/bin/activate

# Check if activation was successful
if [ "$VIRTUAL_ENV" = "" ]; then
    echo "❌ Failed to activate virtual environment"
    exit 1
fi

echo "✅ Virtual environment activated: $VIRTUAL_ENV"
echo ""

# Show installed packages
echo "📦 Key dependencies:"
python3 -c "
try:
    import flask; print('   ✓ Flask')
except ImportError: print('   ❌ Flask')
try:
    import flask_socketio; print('   ✓ Flask-SocketIO') 
except ImportError: print('   ❌ Flask-SocketIO')
try:
    import meshtastic; print('   ✓ Meshtastic')
except ImportError: print('   ❌ Meshtastic')
try:
    import mudp; print('   ✓ MUDP')
except ImportError: print('   ❌ MUDP')
"

echo ""
echo "🚀 Starting MUDP Chat Application..."
echo "=================================================="

# Start the application
python3 start.py