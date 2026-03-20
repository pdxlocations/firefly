#!/usr/bin/env python3

import importlib.util
import os
import sys


REQUIRED_RUNTIME_MODULES = ("meshdb", "vnode")


def main() -> int:
    missing = [name for name in REQUIRED_RUNTIME_MODULES if importlib.util.find_spec(name) is None]
    return 0 if not missing else 1


if __name__ == "__main__":
    os._exit(main())
