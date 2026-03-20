#!/usr/bin/env python3

import os
import socket
import sys


def apply_mudp_multicast_patch() -> None:
    from mudp.connection import Connection
    import mudp.encryption as mudp_encryption
    import mudp.reliability as mudp_reliability
    import mudp.rx_message_handler as mudp_rx_message_handler
    from meshtastic.protobuf import mesh_pb2

    if getattr(Connection, "_firefly_firefly_patch_applied", False):
        return

    original_encrypt_packet = mudp_encryption.encrypt_packet
    original_decrypt_packet = mudp_encryption.decrypt_packet
    original_generate_hash = mudp_encryption.generate_hash

    def expand_short_psk(key: str) -> str:
        if key == "AQ==":
            return "1PG7OiApB1nwvP+rz05pAQ=="
        if key == "BQ==":
            return "1PG7OiApB1nwvP+rz05pBQ=="
        return key

    def encrypt_packet(channel: str, key: str, mp, encoded_message):
        return original_encrypt_packet(channel, expand_short_psk(key), mp, encoded_message)

    def decrypt_packet(mp, key: str):
        return original_decrypt_packet(mp, expand_short_psk(key))

    def generate_hash(name: str, key: str) -> int:
        return original_generate_hash(name, expand_short_psk(key))

    def is_ack(packet) -> bool:
        routing = mudp_reliability.parse_routing(packet)
        if routing is None or not packet.HasField("decoded"):
            return False
        request_id = int(getattr(packet.decoded, "request_id", 0) or 0)
        if request_id <= 0:
            return False
        variant = routing.WhichOneof("variant")
        if variant is None:
            return True
        return variant == "error_reason" and routing.error_reason == mesh_pb2.Routing.Error.NONE

    def is_nak(packet) -> bool:
        routing = mudp_reliability.parse_routing(packet)
        if routing is None or not packet.HasField("decoded"):
            return False
        request_id = int(getattr(packet.decoded, "request_id", 0) or 0)
        if request_id <= 0:
            return False
        variant = routing.WhichOneof("variant")
        return variant == "error_reason" and routing.error_reason != mesh_pb2.Routing.Error.NONE

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

    mudp_encryption.encrypt_packet = encrypt_packet
    mudp_encryption.decrypt_packet = decrypt_packet
    mudp_encryption.generate_hash = generate_hash
    mudp_reliability.is_ack = is_ack
    mudp_reliability.is_nak = is_nak
    mudp_rx_message_handler.is_ack = is_ack
    mudp_rx_message_handler.is_nak = is_nak
    Connection.setup_multicast = setup_multicast
    Connection._firefly_firefly_patch_applied = True
