"""Minimal standard-library mDNS discovery for Alfen ACE chargers."""

import select
import socket
import struct
import time


SERVICE_TYPE = "_alfen._tcp.local"
MDNS_ADDRESS = "224.0.0.251"
MDNS_PORT = 5353


def _encode_name(name):
    labels = name.rstrip(".").split(".")
    return b"".join(bytes((len(label),)) + label.encode("ascii") for label in labels) + b"\x00"


def _read_name(packet, offset):
    labels = []
    cursor = offset
    resume_at = None
    visited = set()
    while True:
        if cursor >= len(packet):
            raise ValueError("truncated DNS name")
        length = packet[cursor]
        if length & 0xC0 == 0xC0:
            if cursor + 1 >= len(packet):
                raise ValueError("truncated DNS compression pointer")
            pointer = ((length & 0x3F) << 8) | packet[cursor + 1]
            if pointer in visited or pointer >= len(packet):
                raise ValueError("invalid DNS compression pointer")
            visited.add(pointer)
            if resume_at is None:
                resume_at = cursor + 2
            cursor = pointer
            continue
        if length & 0xC0:
            raise ValueError("unsupported DNS label encoding")
        cursor += 1
        if length == 0:
            return ".".join(labels).lower(), resume_at if resume_at is not None else cursor
        if length > 63 or cursor + length > len(packet):
            raise ValueError("invalid DNS label")
        labels.append(packet[cursor:cursor + length].decode("ascii", errors="strict"))
        cursor += length


def _parse_txt(data):
    values = {}
    cursor = 0
    while cursor < len(data):
        size = data[cursor]
        cursor += 1
        if cursor + size > len(data):
            raise ValueError("truncated DNS TXT string")
        text = data[cursor:cursor + size].decode("utf-8", errors="replace")
        cursor += size
        key, separator, value = text.partition("=")
        if separator:
            values[key.casefold()] = value
    return values


def _parse_packet(packet):
    if len(packet) < 12:
        return []
    _, flags, question_count, answer_count, authority_count, additional_count = struct.unpack_from(
        "!6H", packet, 0
    )
    if not flags & 0x8000:
        return []
    cursor = 12
    for _ in range(question_count):
        _, cursor = _read_name(packet, cursor)
        cursor += 4
        if cursor > len(packet):
            raise ValueError("truncated DNS question")

    records = []
    total_records = answer_count + authority_count + additional_count
    for _ in range(total_records):
        name, cursor = _read_name(packet, cursor)
        if cursor + 10 > len(packet):
            raise ValueError("truncated DNS resource record")
        record_type, _, ttl, data_length = struct.unpack_from("!HHIH", packet, cursor)
        cursor += 10
        data_end = cursor + data_length
        if data_end > len(packet):
            raise ValueError("truncated DNS resource data")
        record = {"name": name, "type": record_type, "ttl": ttl}
        if record_type == 12:
            record["target"], _ = _read_name(packet, cursor)
        elif record_type == 33 and data_length >= 7:
            priority, weight, port = struct.unpack_from("!HHH", packet, cursor)
            record["priority"] = priority
            record["weight"] = weight
            record["port"] = port
            record["target"], _ = _read_name(packet, cursor + 6)
        elif record_type == 16:
            record["text"] = _parse_txt(packet[cursor:data_end])
        elif record_type == 1 and data_length == 4:
            record["address"] = socket.inet_ntoa(packet[cursor:data_end])
        records.append(record)
        cursor = data_end
    return records


def parse_alfen_packets(packets):
    """Return Alfen service records decoded from mDNS response packets."""
    records = []
    for packet in packets:
        try:
            records.extend(_parse_packet(packet))
        except (UnicodeError, ValueError, struct.error):
            continue

    instances = {record["target"] for record in records
                 if record["type"] == 12 and record["name"] == SERVICE_TYPE}
    srv_records = {record["name"]: record for record in records if record["type"] == 33}
    txt_records = {}
    for record in records:
        if record["type"] == 16:
            txt_records.setdefault(record["name"], {}).update(record["text"])
    addresses = {}
    for record in records:
        if record["type"] == 1:
            addresses.setdefault(record["name"], set()).add(record["address"])

    services = {}
    for instance in instances:
        srv = srv_records.get(instance)
        txt = txt_records.get(instance, {})
        identity = txt.get("identity", "").strip()
        if srv is None or not identity or not 1 <= srv["port"] <= 65535:
            continue
        for address in addresses.get(srv["target"], ()):
            endpoint = "https://{}:{}".format(address, srv["port"])
            services[(identity.casefold(), address, srv["port"])] = {
                "identity": identity,
                "hostname": srv["target"],
                "address": address,
                "port": srv["port"],
                "endpoint": endpoint,
                "content_type": txt.get("contenttype", ""),
                "firmware_version": txt.get("fwversion", ""),
            }
    return sorted(services.values(), key=lambda service: (service["identity"].casefold(), service["address"]))


def _query_packet():
    header = struct.pack("!6H", 0, 0, 1, 0, 0, 0)
    question = _encode_name(SERVICE_TYPE) + struct.pack("!HH", 12, 1)
    return header + question


def _default_interface_address():
    """Return the IPv4 address on the lowest-metric Linux default route."""
    try:
        import fcntl

        with open("/proc/net/route", "r") as routes_file:
            routes = routes_file.readlines()[1:]
        candidates = []
        for line in routes:
            columns = line.split()
            if len(columns) < 8 or columns[1] != "00000000":
                continue
            flags = int(columns[3], 16)
            if flags & 1:
                candidates.append((int(columns[6]), columns[0]))
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            for _, interface_name in sorted(candidates):
                request = struct.pack("256s", interface_name.encode("ascii")[:15])
                try:
                    result = fcntl.ioctl(sock.fileno(), 0x8915, request)
                    address = socket.inet_ntoa(result[20:24])
                except OSError:
                    continue
                if not address.startswith("127.") and not address.startswith("169.254."):
                    return address
    except (ImportError, OSError, ValueError):
        pass
    return ""


def discover_alfen(timeout=2.5, interface_address=None):
    """Query local mDNS for ``_alfen._tcp.local`` and resolve advertised IPv4 endpoints."""
    timeout = max(0.1, float(timeout))
    query = _query_packet()
    packets = []
    interface_address = interface_address or _default_interface_address()
    if interface_address:
        interface = socket.inet_aton(interface_address)
    else:
        interface = socket.inet_aton("0.0.0.0")
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass
        sock.bind(("", MDNS_PORT))
        membership = socket.inet_aton(MDNS_ADDRESS) + interface
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
        if interface_address:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, interface)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        sock.setblocking(False)

        deadline = time.monotonic() + timeout
        next_query = 0.0
        while True:
            now = time.monotonic()
            if now >= deadline:
                break
            if now >= next_query:
                sock.sendto(query, (MDNS_ADDRESS, MDNS_PORT))
                next_query = now + 0.5
            wait = min(deadline - now, max(0.0, next_query - time.monotonic()))
            readable, _, _ = select.select([sock], [], [], wait)
            if readable:
                try:
                    packet, _ = sock.recvfrom(9000)
                    packets.append(packet)
                except BlockingIOError:
                    pass
    return parse_alfen_packets(packets)