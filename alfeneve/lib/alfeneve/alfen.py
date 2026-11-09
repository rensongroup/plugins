# Copyright (c) 2019 Nick Douma <n.douma@nekoconeko.nl>
#
# This file is part of alfeneve .
#
# Licensed under the terms of the MIT license, see the
# LICENSE file in the root of the project.

from datetime import datetime, timezone
from urllib.parse import urljoin
import json
import re
import ssl
import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3 import disable_warnings
except ImportError:
    from requests.packages.urllib3 import disable_warnings

try:
    import iso8601
except ImportError:  # pragma: no cover - optional dependency for log parsing only
    iso8601 = None


class _LegacyTLSAdapter(HTTPAdapter):
    """HTTPAdapter that relaxes OpenSSL 3.x security defaults.

    Older Alfen firmware negotiates TLS handshakes (weak DH params, legacy
    renegotiation) that OpenSSL 3.x rejects at its default security level.
    """

    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.set_ciphers("DEFAULT:@SECLEVEL=0")
        if hasattr(ssl, "OP_LEGACY_SERVER_CONNECT"):
            ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)

    def cert_verify(self, conn, url, verify, cert):
        super().cert_verify(conn, url, verify=False, cert=cert)


class Alfen:
    def __init__(self, endpoint, credentials, verify=False, tls_legacy=False,
                 timeout=None):
        self.session = requests.Session()
        self.base_url = endpoint
        self.credentials = credentials
        self.timeout = timeout

        # By default we need to disable SSL verification because the API does
        # not have a valid certificate (expired in my case).
        self.session.verify = verify
        if verify is False:
            disable_warnings()

        if tls_legacy:
            adapter = _LegacyTLSAdapter()
            self.session.mount("https://", adapter)

    def _build_url(self, endpoint):
        return urljoin(self.base_url, endpoint)

    @staticmethod
    def _decode_json(text):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # Work around misspelled NaN values and trailing commas emitted
            # by some firmware versions (both are invalid JSON).
            text = text.replace(":nan,", ":NaN,")
            text = re.sub(r",\s*([}\]])", r"\1", text)
            return json.loads(text)

    def _post(self, endpoint, payload):
        resp = self.session.post(
            self._build_url(endpoint),
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=self.timeout)
        resp.raise_for_status()
        if len(resp.text) > 0:
            return self._decode_json(resp.text)

    def _put(self, endpoint, payload):
        resp = self.session.put(
            self._build_url(endpoint),
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=self.timeout)
        resp.raise_for_status()
        if len(resp.text) > 0:
            return self._decode_json(resp.text)

    def _get(self, endpoint, params=None, decode_json=True, try_login=True):
        resp = self.session.get(
            self._build_url(endpoint),
            params=params,
            timeout=self.timeout)
        if resp.status_code == 403 and try_login:
            self.login()
            return self._get(endpoint, params, decode_json, try_login=False)
        resp.raise_for_status()
        resp = resp.text
        if decode_json:
            try:
                return self._decode_json(resp)
            except json.JSONDecodeError:
                # Try to work around misspelled NaN values
                resp = resp.replace(":nan,", ":NaN,")
                return json.loads(resp)
        return resp

    def login(self):
        del self.session.cookies["session"]
        self._post(
            "/api/login",
            {"username": self.credentials[0],
             "password": self.credentials[1],
             "displayname": "python-alfeneve"})

    def logout(self):
        del self.session.cookies["session"]
        self._post("/api/logout", None)

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.logout()

    def categories(self):
        return [cat
                for cat in self._get("/api/categories")['categories']
                if cat]

    def properties(self, category=None, ids=None):
        if ids:
            params = {"ids": ids}
        elif category:
            params = {"cat": category}
        else:
            raise RuntimeError("Specify a property category or "
                               "at least one property id.")

        response = self._get("/api/prop", params=params)

        version = response['version']
        if version == 1:
            del response['count']
            del response['version']

            for name, r in response.items():
                yield AlfenProperty(name=name, **r)
        elif version == 2:
            properties = response['properties']
            total_items = response['total']
            offset = 0
            while len(properties) < total_items:
                offset = len(properties)
                params['offset'] = offset
                response = self._get("/api/prop", params=params)
                properties.extend(response['properties'])

            for property in properties:
                pid = property['id']
                pname = ALFEN_PROPERTY_ID_MAPPING.get(
                    pid, "prop_{}".format(pid))
                yield AlfenProperty(name=pname, **property)
        else:
            raise RuntimeError("Property version {} not supported."
                               .format(version))

    def _property_id_by_name(self, name):
        for pid, pname in ALFEN_PROPERTY_ID_MAPPING.items():
            if pname == name:
                return pid
        return None

    def set_property(self, name_or_id, value):
        """Write a property value, e.g. set_property('OD_...', 19).

        Confirmed via captured traffic: POST /api/prop with body
        {"<id>": {"id": "<id>", "value": <value>}}.
        """
        pid = self._property_id_by_name(name_or_id) or name_or_id
        payload = {pid: {"id": pid, "value": value}}
        return self._post("/api/prop", payload)

    def whitelist(self, index):
        for entry in self._get("/api/whitelist",
                               params={"index": index})['whitelist']:
            if not entry:
                continue
            ed = entry['expiryDate']
            if ed:
                ed = datetime.fromtimestamp(ed).replace(tzinfo=timezone.utc)
            entry['expiryDate'] = ed
            yield entry

    def transactions(self):
        offset = 0
        while True:
            resp = self._get("/api/transactions",
                             params={"offset": offset},
                             decode_json=False)
            for line in resp.splitlines():
                yield line

                try:
                    tid = line.split(" ", 2)[0]
                    ttype = tid.split("_", 2)[1][:-1]
                    tid = tid.split("_", 2)[0]
                except IndexError:
                    break

            offset = int(tid)

            if ttype == "txstop":
                break

    def logs(self, since=None):
        offset = 0
        stop = False
        while not stop:
            resp = self._get("/api/log",
                             params={"offset": offset},
                             decode_json=False)
            for line in reversed(resp.splitlines()):
                try:
                    lid = int(line.split("_", 1)[0])
                    rest = line.split("_", 1)[1]
                    if iso8601 is not None:
                        timestamp = iso8601.parse_date(rest[:24])
                    else:
                        timestamp = datetime.fromisoformat(rest[:24].replace("Z", "+00:00"))
                    rest = rest[25:]
                    level, filename, linenum, line = rest.split(":", 3)

                    if since and lid <= since:
                        stop = True
                        break
                    yield (lid, timestamp, level, filename, linenum, line)
                except IndexError:
                    stop = True
                    break

            if stop or lid == 0:
                break

            offset += 32


class AlfenProperty:
    def __init__(self, name, id, value, **kwargs):
        self.name = name
        self.id = id
        self.value = value
        self.properties = ["name", "value", "id", "cat"]
        self.all_properties = ["name", "value", "id", "cat"]
        if kwargs:
            for k, v in kwargs.items():
                self.all_properties.append(k)
                setattr(self, k, v)
            self.all_properties = set(self.all_properties)

    def to_dict(self, all=False):
        if all:
            return {k: getattr(self, k) for k in self.all_properties}
        return {k: getattr(self, k) for k in self.properties}

    def __str__(self):
        return "<{}({})>".format(
            self.__class__.__name__,
            ", ".join(["{}={}".format(k, getattr(self, k))
                       for k in self.properties]))

    __repr__ = __str__


ALFEN_PROPERTY_TYPES = {
    2: int,
    # 3: minutes?
    5: int,  # or bool?
    8: float,
    9: str,
    # 27: unixtime in milliseconds ?
}

ALFEN_PROPERTY_ID_MAPPING = {
    '1008_0': 'OD_manufacturerDeviceName',
    '1009_0': 'OD_manufacturerHardwareVersion',
    '100A_0': 'OD_manufacturerSoftwareVersion',
    '2005_0': 'OD_ODversion',
    '204D_1': 'hw_controllerRevision',
    '204D_2': 'hw_controllerAssy',
    '204D_3': 'hw_powerRevision',
    '204D_4': 'hw_powerAssy',
    '204F_0': 'OD_sysChargePointConfiguration',
    '2050_0': 'OD_sysChargePointModel',
    '2051_0': 'OD_sysChargePointSerialNumber',
    '2052_1': 'OD_sysBoardSerial_value',
    '2052_2': 'OD_sysBoardSerial_isFixed',
    '2053_0': 'OD_sysChargeBoxIdentity',
    '2054_0': 'OD_sysFirmwareVersion',
    '2055_0': 'OD_sysChargePointVendor',
    '2056_0': 'OD_sysBootCount',
    '2057_0': 'OD_sysBootReason',
    '2059_0': 'OD_sysDateTime',
    '205B_0': 'OD_sysDaylightSavings',
    '205C_1': 'OD_sysPosition_latitude',
    '205C_2': 'OD_sysPosition_longitude',
    '205D_0': 'OD_sysLanguage',
    '205E_0': 'OD_sysNumSockets',
    '205F_0': 'OD_sysDisabledSockets',
    '2060_0': 'OD_sysUpTime',
    '2061_1': 'OD_sysIntensity_auto',
    '2061_2': 'OD_sysIntensity_intensity',
    '2062_0': 'OD_sysMaxStationCurrent',
    '2063_0': 'OD_sysPlugAndChargeIdentifier',
    '2064_0': 'OD_sysLoadBalancingMode',
    '2065_1': 'OD_sysChargingProfileSettings_enabled',
    '2066_0': 'OD_sysStatusFlags',
    '2067_0': 'OD_sysMaxSmartMeterCurrent',
    '2068_0': 'OD_sysActiveLoadBalancingSafeCurrent',
    '2069_0': 'OD_sysActiveLoadBalancingPhaseConnection',
    '206A_0': 'OD_sysMinimumChameleonCurrent',
    '206B_1': 'OD_sysNuvveSupport.active',
    '206B_2': 'OD_sysNuvveSupport.interval',
    '206B_3': 'OD_sysNuvveSupport.threshold',
    '206C_1': 'OD_sysOCPP15SmartCharging_smartChargingType',
    '206E_0': 'OD_sysTimeZoneMinutes',
    '206F_0': 'OD_sysSmartMeterIncludesEV',
    '2070_0': 'OD_sysRcdActionImmediate',
    '2071_1': 'OD_commBackOfficeURLwired_serverDomainAndPort',
    '2071_2': 'OD_commBackOfficeURLwired_serverPath',
    '2072_1': 'OD_commDHCPaddress1_value',
    '2072_2': 'OD_commDHCPaddress1_isFixed',
    '2073_1': 'OD_commNetMask1_value',
    '2074_1': 'OD_commGWaddress1_value',
    '2075_1': 'OD_commIPaddress1_value',
    '2075_2': 'OD_commIPaddress1_isFixed',
    '2076_0': 'OD_commBackOfficeShortName',
    '2077_0': 'OD_commConnectMethod',
    '2078_1': 'OD_commBackOfficeURL_serverDomainAndPort',
    '2078_2': 'OD_commBackOfficeURL_serverPath',
    '2079_1': 'OD_commDNS1_1_value',
    '2079_2': 'OD_commDNS1_1_isFixed',
    '207A_1': 'OD_commDHCPaddress2_value',
    '207A_2': 'OD_commDHCPaddress2_isFixed',
    '207B_1': 'OD_commNetMask2_value',
    '207C_1': 'OD_commGWaddress2_value',
    '207D_1': 'OD_commIPaddress2_value',
    '207D_2': 'OD_commIPaddress2_isFixed',
    '207E_1': 'OD_commDNS2_1_value',
    '207E_2': 'OD_commDNS2_1_isFixed',
    '207F_1': 'OD_commDNS2_2_value',
    '207F_2': 'OD_commDNS2_2_isFixed',
    '2080_1': 'OD_commDNS1_2_value',
    '2080_2': 'OD_commDNS1_2_isFixed',
    '2081_0': 'OD_commProtocolName',
    '2082_0': 'OD_commProtocolVersion',
    '2086_0': 'OD_commActualHeartBeatInterval',
    '2087_0': 'OD_commMeteringInterval',
    '2088_0': 'OD_commMeteringTransmissionMode',
    '2089_0': 'OD_commMeteringAlignment',
    '208A_0': 'OD_commPingPongInterval',
    '208B_1': 'OD_commWebSocketTimeout[ODA_commWebSocketTimeout_wired]',
    '208B_2': 'OD_commWebSocketTimeout[ODA_commWebSocketTimeout_gprs]',
    '208D_1': 'OD_commRpcConnectTimeout[ODA_commRpcConnectTimeout_wired]',
    '208D_2': 'OD_commRpcConnectTimeout[ODA_commRpcConnectTimeout_gprs]',
    '208E_1': 'OD_commRpcReplyTimeout[ODA_commRpcReplyTimeout_wired]',
    '208E_2': 'OD_commRpcReplyTimeout[ODA_commRpcReplyTimeout_gprs]',
    '208F_0': 'OD_commCentralMeteringInterval',
    '2093_0': 'OD_commSendStationStatus',
    '2094_0': 'OD_commConcurrentTxAction',
    '2095_0': 'OD_commStopTransactionOnInvalidId',
    '2096_0': 'OD_commTransactionMessageAttempts',
    '2097_0': 'OD_commTransactionMessageRetryInterval',
    '2098_1': 'SampledData_1',
    '2098_2': 'SampledData_2',
    '2098_3': 'SampledData_3',
    '2098_4': 'SampledData_4',
    '2098_5': 'SampledData_5',
    '2098_6': 'SampledData_6',
    '2098_7': 'SampledData_7',
    '2098_8': 'SampledData_8',
    '2098_9': 'SampledData_9',
    '2099_1': 'AlignedData_1',
    '2099_2': 'AlignedData_2',
    '2099_3': 'AlignedData_3',
    '2099_4': 'AlignedData_4',
    '2099_5': 'AlignedData_5',
    '2099_6': 'AlignedData_6',
    '2099_7': 'AlignedData_7',
    '2099_8': 'AlignedData_8',
    '2099_9': 'AlignedData_9',
    '209A_0': 'OD_commClockAlignedDataInterval',
    '209B_0': 'OD_commAuthorizeRemoteTxRequests',
    '209C_0': 'OD_commStatusNotificationMethod',
    '209D_0': 'OD_commForceHeartBeats',
    '2100_0': 'OD_gprsAPNname',
    '2101_0': 'OD_gprsAPNuser',
    '2102_0': 'OD_gprsAPNpassword',
    '2103_0': 'OD_gprsSIMpin',
    '2104_0': 'OD_gprsSIMimsi',
    '2105_0': 'OD_gprsSIMiccid',
    '2106_0': 'OD_gprsConnectDelay',
    '2107_0': 'OD_gprsConnectAttempts',
    '2108_0': 'OD_gprsConnectRetryBehaviour',
    '2109_0': 'OD_gprsBand',
    '2110_0': 'OD_gprsSignalStrength',
    '2111_0': 'OD_gprsWeakSignalThreshold',
    '2112_0': 'OD_gprsProvider',
    '2113_0': 'MobileNetworkSelection',
    '2114_0': 'MobileNetworkPreference',
    '2115_0': 'ProxyAddressAndPort',
    '2116_0': 'ProxyUsername',
    '2117_0': 'ProxyEnable',
    '2125_0': 'OD_mainSocketType',
    '2126_0': 'OD_mainAuthorizationMethod',
    '2127_0': 'OD_mainOfflineNFCAuthorization',
    '2128_0': 'OD_mainStartMaxCurrent',
    '2129_0': 'OD_mainNormalMaxCurrent',
    '212A_0': 'OD_mainExternalMaxCurrent',
    '212B_0': 'OD_mainStaticLoadBalancingMaxCurrent',
    '212C_0': 'OD_mainActiveMaxCurrent',
    '212D_0': 'OD_mainActiveLoadBalancingMaxCurrent',
    '212E_1': 'OD_mainP1Status_readStatus',
    '212E_2': 'OD_mainP1Status_version',
    '212E_3': 'OD_mainP1Status_timestamp',
    '212F_1': 'OD_mainP1Measurements_1',
    '212F_2': 'OD_mainP1Measurements_2',
    '212F_3': 'OD_mainP1Measurements_3',
    '2130_0': 'OD_mainSimplifiedMaxCurrent',
    '2131_0': 'OD_mainStartupDelay',
    '2132_0': 'OD_mainSwitchErrorTime',
    '2133_0': 'OD_mainCurrentErrorTime',
    '2134_0': 'OD_mainHFContactorSwitchTime',
    '2135_0': 'OD_mainEVConnectTimeout',
    '2136_0': 'OD_mainEVDisconnectTimeout',
    '2137_0': 'OD_mainEVDisconnectAction',
    '2138_0': 'OD_mainNFCreaderType',
    '2139_0': 'OD_mainNFCreaderScanInterval',
    '213A_0': 'OD_mainNFCreaderModel',
    '213B_0': 'OD_mainWhiteListEnabled',
    '213C_0': 'OD_mainOnlineNFCAuthorization',
    '213D_0': 'OD_mainLocalListEnabled',
    '213E_0': 'OD_mainLocalAuthorizeOfflineEnabled',
    '2140_0': 'OD_mainNFCreaderTagDelay',
    '2153_0': 'OD_mainChargingMaxTimeToOpenS2',
    '2159_0': 'OD_mainZEready',
    '215D_0': 'OD_mainDisableOvercurrentCheck105',
    '215E_0': 'OD_mainRestartAfterPowerOutage',
    '215F_0': 'OD_mainStationExternalMaxCurrent',
    '2160_0': 'OD_mainExternalMinCurrent',
    '2161_0': 'OD_mainStationActiveMaxCurrent',
    '2165_1': 'DSC_Station_status',
    '2165_2': 'DSC_Station_safeCurrent',
    '2165_3': 'DSC_Station_maxCurrent',
    '2165_4': 'DSC_Station_validTo',
    '2166_1': 'DSC_Socket1_status',
    '2166_2': 'DSC_Socket1_safeCurrent',
    '2166_3': 'DSC_Socket1_maxCurrent',
    '2166_4': 'DSC_Socket1_validTo',
    '2167_1': 'DSC_Socket2_status',
    '2167_2': 'DSC_Socket2_safeCurrent',
    '2167_3': 'DSC_Socket2_maxCurrent',
    '2167_4': 'DSC_Socket2_validTo',
    '2168_0': 'OD_mainAutoStopTransactionTime',
    '2169_0': 'OD_mainMaxAllowedOutageDuration',
    '216A_0': 'OD_mainSignedDataEnabled',
    '216B_0': 'OD_mainSignedMeterValueUpdates',
    '216C_0': 'OD_mainP1PortUse',
    '216D_0': 'OD_mainQRCodeDisplayTime',
    '216E_0': 'OD_mainQRCodeURL',
    '216F_0': 'OD_mainSignedStartStopMeterValue',
    '2171_0': 'OD_mainLEDHeartBeatMode',
    '2172_0': 'OD_mainLEDHeartBeatIntensity',
    '2173_0': 'OD_mainTypeEMaxCurrent1',
    '2174_0': 'MaxImbalanceCurrent',
    '2175_0': 'OD_mainSupportsExternalModbus',
    '2182_0': 'OD_mainMaxTxMeterValueRandomisationTime',
    '2183_0': 'OD_mainCoverLockEnabled',
    '2184_0': 'OD_mainNonChargingReportThreshold',
    '2185_0': 'EnablePhaseSwitching',
    '21A0_0': 'OD_sysFeatureUniqueId',
    '21A1_0': 'OD_sysFeatureCode',
    '21A2_0': 'OD_sysFeatureEnabled',
    '21B0_0': 'OD_sysPEDETEnabled',
    '2200_0': 'OD_sensTemperatureMode',
    '2201_0': 'OD_sensTemperatureValue',
    '2202_0': 'OD_sensTemperatureAlarmLow',
    '2203_0': 'OD_sensTemperatureAlarmHigh',
    '2204_0': 'OD_sensTemperatureCheckInterval',
    '2205_0': 'OD_sensTemperatureLogInterval',
    '2206_0': 'OD_sensAccelerometerMode',
    '2207_0': 'OD_sensAccelerometerValueX',
    '2208_0': 'OD_sensAccelerometerValueY',
    '2209_0': 'OD_sensAccelerometerValueZ',
    '2210_0': 'OD_sensAccelerometerSetpointX',
    '2211_0': 'OD_sensAccelerometerSetpointY',
    '2212_0': 'OD_sensAccelerometerSetpointZ',
    '2213_0': 'OD_sensAccelerometerMarginX',
    '2214_0': 'OD_sensAccelerometerMarginY',
    '2215_0': 'OD_sensAccelerometerMarginZ',
    '2216_0': 'OD_sensAccelerometerLogInterval',
    '2218_0': 'OD_sensEnergyMeterType',
    '2219_0': 'OD_sensEnergyMeterPulsesPerkWh',
    '2221_10': 'meter1_cosPhiL3',
    '2221_11': 'meter1_cosPhiSum',
    '2221_12': 'meter1_frequency',
    '2221_13': 'meter1_powerRealL1',
    '2221_14': 'meter1_powerRealL2',
    '2221_15': 'meter1_powerRealL3',
    '2221_16': 'meter1_powerRealSum',
    '2221_17': 'meter1_powerApparentL1',
    '2221_18': 'meter1_powerApparentL2',
    '2221_19': 'meter1_powerApparentL3',
    '2221_1A': 'meter1_powerApparentSum',
    '2221_1B': 'meter1_powerReactiveL1',
    '2221_1C': 'meter1_powerReactiveL2',
    '2221_1D': 'meter1_powerReactiveL3',
    '2221_1E': 'meter1_powerReactiveSum',
    '2221_1F': 'meter1_energyRealDeliveredL1',
    '2221_20': 'meter1_energyRealDeliveredL2',
    '2221_21': 'meter1_energyRealDeliveredL3',
    '2221_22': 'meter1_energyRealDeliveredSum',
    '2221_23': 'meter1_energyRealConsumedL1',
    '2221_24': 'meter1_energyRealConsumedL2',
    '2221_25': 'meter1_energyRealConsumedL3',
    '2221_26': 'meter1_energyRealConsumedSum',
    '2221_27': 'meter1_energyApparentL1',
    '2221_28': 'meter1_energyApparentL2',
    '2221_29': 'meter1_energyApparentL3',
    '2221_2A': 'meter1_energyApparentSum',
    '2221_2B': 'meter1_energyReactiveL1',
    '2221_2C': 'meter1_energyReactiveL2',
    '2221_2D': 'meter1_energyReactiveL3',
    '2221_2E': 'meter1_energyReactivesSum',
    '2221_3': 'meter1_voltageL1N',
    '2221_4': 'meter1_voltageL2N',
    '2221_5': 'meter1_voltageL3N',
    '2221_6': 'meter1_voltageL1L2',
    '2221_7': 'meter1_voltageL2L3',
    '2221_8': 'meter1_voltageL3L1',
    '2221_9': 'meter1_currentN',
    '2221_A': 'meter1_currentL1',
    '2221_B': 'meter1_currentL2',
    '2221_C': 'meter1_currentL3',
    '2221_D': 'meter1_currentSum',
    '2221_E': 'meter1_cosPhiL1',
    '2221_F': 'meter1_cosPhiL2',
    '2224_0': 'OD_sensADCVoltageCP1High',
    '2225_0': 'OD_sensADCVoltageCP1Low',
    '2228_0': 'OD_sensADCVoltageCP2High',
    '2229_0': 'OD_sensADCVoltageCP2Low',
    '2231_0': 'OD_sensADCResistancePP1',
    '2233_0': 'OD_sensADCResistancePP2',
    '2300_0': 'OD_ledsStateUnknown',
    '2301_0': 'OD_ledsStateOff',
    '2302_0': 'OD_ledsStateBooting',
    '2303_0': 'OD_ledsStateBootingCheckMains',
    '2304_0': 'OD_ledsStateAvailable',
    '2305_0': 'OD_ledsStatePrepAuthorising',
    '2306_0': 'OD_ledsStatePrepAuthorised',
    '2307_0': 'OD_ledsStatePrepCableConnected',
    '2308_0': 'OD_ledsStatePrepEVConnected',
    '2309_0': 'OD_ledsStateChargingPreparing',
    '2310_0': 'OD_ledsStateChargingWaitVehicle',
    '2311_0': 'OD_ledsStateChargingActiveNormal',
    '2312_0': 'OD_ledsStateChargingActiveSimplified',
    '2313_0': 'OD_ledsStateChargingSuspendedOverCurrent',
    '2314_0': 'OD_ledsStateChargingSuspendedHFSwitching',
    '2315_0': 'OD_ledsStateChargingSuspendedEVDisconnected',
    '2316_0': 'OD_ledsStateFinishWaitVehicle',
    '2317_0': 'OD_ledsStateFinishWaitDisconnect',
    '2318_0': 'OD_ledsStateErrorProtectiveEarth',
    '2319_0': 'OD_ledsStateErrorPowerlineFault',
    '2320_0': 'OD_ledsStateErrorContactorFault',
    '2321_0': 'OD_ledsStateErrorCharging',
    '2322_0': 'OD_ledsStateErrorPowerFailure',
    '2323_0': 'OD_ledsStateErrorTemperature',
    '2324_0': 'OD_ledsStateErrorIllegalCPValue',
    '2325_0': 'OD_ledsStateErrorIllegalPPValue',
    '2326_0': 'OD_ledsStateErrorTooManyRestarts',
    '2327_0': 'OD_ledsStateError',
    '2328_0': 'OD_ledsStateErrorMessage',
    '2329_0': 'OD_ledsStateErrorMessageNotAuthorised',
    '2330_0': 'OD_ledsStateErrorMessageCableNotSupported',
    '2331_0': 'OD_ledsStateErrorMessageS2NotOpened',
    '2332_0': 'OD_ledsStateErrorMessageTimeOut',
    '2333_0': 'OD_ledsStateReserved',
    '2334_0': 'OD_ledsStateInOperative',
    '2335_0': 'OD_ledsStateLoadBalancingLimited',
    '2336_0': 'OD_ledsStateLoadBalancingForcedOff',
    '2338_0': 'OD_ledsStateNonCharging',
    '2350_0': 'OD_ledsHeartBeatParameters',
    '2400_1': 'OD_masterTagVars.isEnabled',
    '2400_2': 'OD_masterTagVars.tag',
    '2501_1': 'socket1_StateMain',
    '2501_2': 'socket1_StateLeds',
    '2501_3': 'socket1_StateSocket',
    '2501_4': 'socket1_StateMode3',
    '2502_1': 'socket2_StateMain',
    '2502_2': 'socket2_StateLeds',
    '2502_3': 'socket2_StateSocket',
    '2502_4': 'socket2_StateMode3',
    '2511_0': 'socket1_CP_PP_CPhigh',
    '2511_1': 'socket1_CP_PP_CPlow',
    '2511_2': 'socket1_CP_PP_PP',
    '2511_3': 'socket1_CP_PP_DC',
    '2512_0': 'socket2_CP_PP_CPhigh',
    '2512_1': 'socket2_CP_PP_CPlow',
    '2512_2': 'socket2_CP_PP_PP',
    '2512_3': 'socket2_CP_PP_DC',
    '2522_1': 'MbusTCP1_enabled',
    '2522_2': 'MbusTCP1_SlaveType',
    '2522_3': 'MbusTCP1_ConnectionType',
    '2522_4': 'MbusTCP1_IPaddress',
    '2522_6': 'MbusTCP1_SlaveAddress',
    '2523_1': 'MbusTCP2_enabled',
    '2523_2': 'MbusTCP2_SlaveType',
    '2523_3': 'MbusTCP2_ConnectionType',
    '2523_4': 'MbusTCP2_IPaddress',
    '2523_6': 'MbusTCP2_SlaveAddress',
    '2530_1': 'OD_modbusTCPIPSlave.options',
    '2530_2': 'OD_modbusTCPIPSlave.socketEnable',
    '2530_3': 'OD_modbusTCPIPSlave.SCNEnable',
    '2530_4': 'OD_modbusTCPIPSlave.validityTime',
    '2540_0': 'OD_modbusSlave1ConnectionState',
    '2541_0': 'OD_energyMeterPublicKey1',
    '2560_0': 'ModbusTCP1_Measurand',
    '2561_0': 'ModbusTCP1_Register',
    '2562_0': 'ModbusTCP1_Datatype',
    '2563_0': 'ModbusTCP1_Scalefactor',
    '2570_0': 'ModbusTCP2_Measurand',
    '2571_0': 'ModbusTCP2_Register',
    '2572_0': 'ModbusTCP2_Datatype',
    '2573_0': 'ModbusTCP2_Scalefactor',
    '2703_0': 'OD_securityFeatures',
    '2720_0': 'CertificateSignedMaxChain',
    '2721_0': 'CertificateStoreMaxLength',
    '2722_0': 'CpoName',
    '2723_0': 'SecurityProfile',
    '2911_0': 'OD_fileFirmwareUpdateStatus',
    '3125_0': 'OD_mainSocketType2',
    '3129_0': 'OD_mainNormalMaxCurrent2',
    '312A_0': 'OD_mainExternalMaxCurrent2',
    '312B_0': 'OD_mainStaticLoadBalancingMaxCurrent2',
    '312C_0': 'OD_mainActiveMaxCurrent2',
    '312D_0': 'OD_mainActiveLoadBalancingMaxCurrent2',
    '312E_0': 'OD_mainMaxNrPhases1',
    '312F_0': 'OD_mainMaxNrPhases2',
    '3160_0': 'OD_mainExternalMinCurrent2',
    '3173_0': 'OD_mainTypeEMaxCurrent2',
    '3180_0': 'NFC-Version1',
    '3181_0': 'NFC-Version2',
    '3190_1': 'UI-State-1',
    '3190_2': 'UI-ErrorNumber-1',
    '3191_1': 'UI-State-2',
    '3191_2': 'UI-ErrorNumber-2',
    '3260_1': 'disp_width',
    '3260_2': 'disp_height',
    '3260_3': 'disp_maxLogoWidth',
    '3260_4': 'disp_maxLogoHeight',
    '3260_5': 'disp_maxResourceSize',
    '3261_0': 'disp_itemsEnabled',
    '3262_1': 'Pricing-Currency',
    '3262_2': 'Pricing-StartPrice',
    '3262_3': 'Pricing-EnergyPrice',
    '3262_5': 'Pricing-ShowDisclaimer',
    '3600_1': 'ocpp_bootNotificationState',
    '3600_2': 'ocpp_bootNotificationLastSendTime',
    '3600_3': 'ocpp_bootNotificationAcceptTime',
    '3600_4': 'ocpp_bootNotificationDelay',
    '3600_5': 'ocpp_rpcConnected',
    '3600_6': 'ocpp_heartbeatLastReceived',
    '3600_7': 'ocpp_heartbeatLastFailed',
    '3600_8': 'ocpp_heartbeatLastSent',
    '5217_0': 'OD_sensOptionalEnergyMeter4',
    '5218_0': 'OD_sensEnergyMeterType4',
    '5221_10': 'meter4_cosPhiL3',
    '5221_11': 'meter4_cosPhiSum',
    '5221_12': 'meter4_frequency',
    '5221_13': 'meter4_powerRealL1',
    '5221_14': 'meter4_powerRealL2',
    '5221_15': 'meter4_powerRealL3',
    '5221_16': 'meter4_powerRealSum',
    '5221_17': 'meter4_powerApparentL1',
    '5221_18': 'meter4_powerApparentL2',
    '5221_19': 'meter4_powerApparentL3',
    '5221_1A': 'meter4_powerApparentSum',
    '5221_1B': 'meter4_powerReactiveL1',
    '5221_1C': 'meter4_powerReactiveL2',
    '5221_1D': 'meter4_powerReactiveL3',
    '5221_1E': 'meter4_powerReactiveSum',
    '5221_1F': 'meter4_energyRealDeliveredL1',
    '5221_20': 'meter4_energyRealDeliveredL2',
    '5221_21': 'meter4_energyRealDeliveredL3',
    '5221_22': 'meter4_energyRealDeliveredSum',
    '5221_23': 'meter4_energyRealConsumedL1',
    '5221_24': 'meter4_energyRealConsumedL2',
    '5221_25': 'meter4_energyRealConsumedL3',
    '5221_26': 'meter4_energyRealConsumedSum',
    '5221_27': 'meter4_energyApparentL1',
    '5221_28': 'meter4_energyApparentL2',
    '5221_29': 'meter4_energyApparentL3',
    '5221_2A': 'meter4_energyApparentSum',
    '5221_2B': 'meter4_energyReactiveL1',
    '5221_2C': 'meter4_energyReactiveL2',
    '5221_2D': 'meter4_energyReactiveL3',
    '5221_2E': 'meter4_energyReactivesSum',
    '5221_3': 'meter4_voltageL1N',
    '5221_4': 'meter4_voltageL2N',
    '5221_5': 'meter4_voltageL3N',
    '5221_6': 'meter4_voltageL1L2',
    '5221_7': 'meter4_voltageL2L3',
    '5221_8': 'meter4_voltageL3L1',
    '5221_9': 'meter4_currentN',
    '5221_A': 'meter4_currentL1',
    '5221_B': 'meter4_currentL2',
    '5221_C': 'meter4_currentL3',
    '5221_D': 'meter4_currentSum',
    '5221_E': 'meter4_cosPhiL1',
    '5221_F': 'meter4_cosPhiL2'
}
