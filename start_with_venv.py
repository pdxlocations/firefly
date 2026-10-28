#!/usr/bin/env python3
"""
MUDP Chat Startup Script with Virtual Environment
This script validates the current interpreter before starting the app
"""

import sys
import importlib
from local_deps import bootstrap_local_dependency, ensure_dependency_version

bootstrap_local_dependency("meshdb")
bootstrap_local_dependency("mudp")
bootstrap_local_dependency("vnode")
ensure_dependency_version("meshdb", "0.3.0")
ensure_dependency_version("mudp", "1.6.0")
ensure_dependency_version("vnode", "0.2.3")

REQUIRED_RUNTIME_MODULES = ("meshdb", "mudp", "vnode")


def has_required_modules():
    """Check whether the active interpreter has the runtime dependencies we need."""
    missing = []
    for name in REQUIRED_RUNTIME_MODULES:
        try:
            importlib.import_module(name)
        except ImportError:
            missing.append(name)
    return (len(missing) == 0, missing)

def validate_current_environment():
    """Validate the currently selected interpreter instead of switching environments."""
    ok, missing = has_required_modules()
    if ok:
        print(f"✅ Using interpreter: {sys.executable}")
        return True

    print(f"❌ Current interpreter is missing dependencies: {', '.join(missing)}")
    print(f"Interpreter: {sys.executable}")
    print("Install dependencies in this environment before launching Firefly.")
    return False

def main():
    print("🌐 MUDP Chat - Environment Startup")
    print("=" * 50)

    if not validate_current_environment():
        sys.exit(1)

    try:
        from start import main as start_main
        start_main()
    except ImportError as e:
        print(f"❌ Failed to import start module: {e}")
        print("Make sure all dependencies are installed in the active environment")
        sys.exit(1)

if __name__ == "__main__":
    main()
