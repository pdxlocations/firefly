#!/usr/bin/env python3

import importlib.util
import os
import sys
from local_deps import bootstrap_local_dependency, ensure_dependency_version

bootstrap_local_dependency("meshdb")
bootstrap_local_dependency("mudp")
bootstrap_local_dependency("vnode")
ensure_dependency_version("meshdb", "0.2.0")
ensure_dependency_version("mudp", "1.5.7")
ensure_dependency_version("vnode", "0.1.10")


REQUIRED_RUNTIME_MODULES = ("meshdb", "mudp", "vnode")


def main() -> int:
    missing = [name for name in REQUIRED_RUNTIME_MODULES if importlib.util.find_spec(name) is None]
    return 0 if not missing else 1


if __name__ == "__main__":
    os._exit(main())
