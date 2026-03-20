#!/usr/bin/env python3

import os
import socket
import sys


def apply_mudp_multicast_patch() -> None:
    from mudp.connection import Connection

    if getattr(Connection, "_firefly_multicast_patch_applied", False):
        return

    def setup_multicast(self, group: str, port: int):
        self.host = group
        self.port = port

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except (AttributeError, OSError):
            pass

        bind_mode = os.getenv("FIREFLY_MUDP_BIND_MODE", "auto").strip().lower()
        if bind_mode == "group":
            bind_targets = [(group, port)]
        elif bind_mode == "any":
            bind_targets = [("", port)]
        elif sys.platform.startswith("linux"):
            # When another multicast listener already owns the group socket on Linux,
            # binding to the group address is usually the compatible choice.
            bind_targets = [(group, port), ("", port)]
        else:
            bind_targets = [("", port), (group, port)]

        last_error = None
        for bind_host, bind_port in bind_targets:
            try:
                sock.bind((bind_host, bind_port))
                break
            except OSError as exc:
                last_error = exc
        else:
            sock.close()
            raise last_error

        mreq = socket.inet_aton(group) + socket.inet_aton("0.0.0.0")
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        self.socket = sock
        self.sock = sock

    Connection.setup_multicast = setup_multicast
    Connection._firefly_multicast_patch_applied = True
