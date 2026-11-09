import socket
import struct
import sys
import unittest
from unittest.mock import mock_open, patch

from ..lib.alfeneve.discovery import (
    _default_interface_address,
    _encode_name,
    _read_name,
    parse_alfen_packets,
)


def _resource_record(name, record_type, data):
    owner = _encode_name(name)
    fixed = struct.pack("!HHIH", record_type, 1, 120, len(data))
    return owner + fixed + data


def _response(records):
    header = struct.pack("!6H", 0, 0x8400, 0, len(records), 0, 0)
    return header + b"".join(records)


def _txt_record(*values):
    return b"".join(bytes((len(value.encode("utf-8")),)) + value.encode("utf-8") for value in values)


def _alfen_response(identity="DIE_16628", address="192.168.0.146", port=443):
    instance = "ng910-60023-ace0562528._alfen._tcp.local"
    host = "ng910-60023-ace0562528.local"
    records = [
        _resource_record("_alfen._tcp.local", 12, _encode_name(instance)),
        _resource_record(instance, 33, struct.pack("!HHH", 0, 0, port) + _encode_name(host)),
        _resource_record(instance, 16, _txt_record(
            "ContentType=alfen-ace-v1/json",
            "Identity={}".format(identity),
            "Type=1.1.1",
            "SCNNetwork=",
            "FWVersion=6.4.0-4210",
        )),
        _resource_record(host, 1, socket.inet_aton(address)),
    ]
    return _response(records)


class AlfenMdnsParserTest(unittest.TestCase):
    def test_linux_default_route_selects_its_ipv4_interface(self):
        routes = (
            "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\n"
            "eth0 00000000 0100000A 0003 0 0 10 00000000 0 0 0\n"
        )
        ioctl_result = b"\x00" * 20 + socket.inet_aton("192.168.0.100") + b"\x00" * 12
        mock_socket = unittest.mock.MagicMock()
        mock_socket.__enter__.return_value = mock_socket
        mock_socket.fileno.return_value = 1
        fcntl = unittest.mock.MagicMock()
        fcntl.ioctl.return_value = ioctl_result

        with patch("builtins.open", mock_open(read_data=routes)), \
                patch("alfeneve.lib.alfeneve.discovery.socket.socket", return_value=mock_socket), \
                patch.dict(sys.modules, {"fcntl": fcntl}):
            address = _default_interface_address()

        self.assertEqual(address, "192.168.0.100")

    def test_decodes_service_identity_endpoint_and_txt_metadata(self):
        services = parse_alfen_packets([_alfen_response()])

        self.assertEqual(len(services), 1)
        self.assertEqual(services[0]["identity"], "DIE_16628")
        self.assertEqual(services[0]["address"], "192.168.0.146")
        self.assertEqual(services[0]["port"], 443)
        self.assertEqual(services[0]["endpoint"], "https://192.168.0.146:443")
        self.assertEqual(services[0]["content_type"], "alfen-ace-v1/json")
        self.assertEqual(services[0]["firmware_version"], "6.4.0-4210")

    def test_merges_records_split_across_response_packets(self):
        instance = "ng910-60023-ace0562528._alfen._tcp.local"
        host = "ng910-60023-ace0562528.local"
        ptr = _resource_record("_alfen._tcp.local", 12, _encode_name(instance))
        srv = _resource_record(instance, 33, struct.pack("!HHH", 0, 0, 443) + _encode_name(host))
        txt = _resource_record(instance, 16, _txt_record("Identity=DIE_16628"))
        address = _resource_record(host, 1, socket.inet_aton("192.168.0.146"))
        services = parse_alfen_packets([_response([ptr, srv]), _response([txt, address])])

        self.assertEqual(services[0]["identity"], "DIE_16628")

    def test_decodes_compressed_dns_name(self):
        encoded = _encode_name("ng910-60023-ace0562528.local")
        packet = encoded + b"\xc0\x00"

        name, next_offset = _read_name(packet, len(encoded))

        self.assertEqual(name, "ng910-60023-ace0562528.local")
        self.assertEqual(next_offset, len(encoded) + 2)

    def test_ignores_records_without_identity(self):
        services = parse_alfen_packets([_alfen_response(identity="")])

        self.assertEqual(services, [])

    def test_malformed_packets_are_ignored(self):
        self.assertEqual(parse_alfen_packets([b"bad", b"\x00" * 12]), [])


if __name__ == "__main__":
    unittest.main()
