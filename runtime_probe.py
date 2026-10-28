#!/usr/bin/env python3

import importlib
import os
import sys
from local_deps import bootstrap_local_dependency, ensure_dependency_version

bootstrap_local_dependency("meshdb")
bootstrap_local_dependency("mudp")
bootstrap_local_dependency("vnode")
ensure_dependency_version("meshdb", "0.3.0")
ensure_dependency_version("mudp", "1.6.0")
ensure_dependency_version("vnode", "0.2.3")


REQUIRED_RUNTIME_MODULES = ("meshdb", "mudp", "vnode")


def main() -> int:
    for name in REQUIRED_RUNTIME_MODULES:
        module = importlib.import_module(name)
        print(f"{name}: {module.__file__}", flush=True)
    importlib.import_module("mesh_runtime")
    return 0


if __name__ == "__main__":
    sys.exit(main())
