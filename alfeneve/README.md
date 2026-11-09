# AlfenEveControl Plugin

Version 0.2.7 controls a single-phase Alfen Eve charger using measured solar,
grid, home-battery and EV power, with Forecast.Solar informing when to defer
grid charging. See [PLAN.md](PLAN.md) for formulas, decisions and pending hardware checks.

## Status

The runtime policy is implemented in release 0.2.7. The user verified that
setting the current property to 0 pauses charging and restoring a positive value
resumes it. Live API writes have succeeded; actual delivered current still needs
to be confirmed from the vehicle or charger.

## Charging Policy

- **Night (22:00–07:00):** offer up to 20 A, subject to the grid cap.
- **Daytime before 16:00 with useful solar ahead:** solar-only; pause below the supported minimum.
- **Poor/unavailable forecast or after 16:00:** permit a grid contribution of 6 A
	in summer (May–September), 10 A in winter, plus available solar.
- **Always:** car gets priority over battery charging; battery discharge is not
	counted as solar. Reduce charging to target grid import below 6000 W with a
	200 W margin. If household demand alone is too high, the car is paused.

All times are local to `Europe/Brussels`, including daylight saving. Settings are
configurable. A good forecast means at least 2 kWh across 60 usable minutes in
the next four hours after subtracting an estimated 500 W household demand, with
enough surplus to exceed minimum charging power plus a 200 W restart margin.
This is not a forecast of the car's energy need or a departure-SOC guarantee.

Limits default to 6–20 A at nominal 230 V. Increases ramp gradually, reductions
are immediate at the next control decision (default 5 s). Pauses have a 120 s
restart hold. Grid control is reactive software, not electrical protection.

Signal freshness is split by safety role: grid and EV readings must be no older
than 120 seconds or control requests a pause. Solar and battery readings may be
up to 15 minutes old to accommodate FoxEss's 120-second polling and unchanged
values. Older auxiliary readings are excluded from solar-based charging; a
stale battery reading never contributes inferred solar surplus.

## Forecast

The public [Forecast.Solar API](https://doc.forecast.solar/account_models) requires
no API key. Configured installation: 6.4 kWp panels, 45° tilt, SSE (-23° API
azimuth), 5 kW inverter, and the supplied Belgian location. Review coordinates
and installation settings when using this plugin in another home. Forecast
requests send site coordinates and panel parameters to Forecast.Solar; the
plugin does not send meter readings or charger credentials.

Forecasts refresh every 30 minutes in an independent background task, remain
usable for at most two hours, and otherwise fall back to seasonal baseline.
Only future intervals count. Energy per interval is converted to average power
using its actual duration, including partial sunrise/sunset intervals.

Public quota is 12 calls per rolling hour per IP, shared with other integrations.
The plugin honours its configured refresh interval even on errors/HTTP 429 and
does not query while disabled. See the provider's current terms before deployment.

## Vendored Dependency

The file in lib/alfeneve/alfen.py is copied from:
https://gitlab.com/LordGaav/alfen-eve

See:
- NOTICE.txt
- LICENSE.txt

## Plugin Config Notes

The plugin requires these four mappings, each reading the gateway's realtime event `value`. Events identify counters by numeric gateway ID; external IDs are retained as a fallback alias.

The plugin subscribes to gateway `SENSOR_CHANGE` events, where `id` is the
sensor ID, not the measurement-counter ID. The confirmed IDs for this gateway:

| Signal | Counter external ID | Sensor ID | Source value normalization |
| --- | --- | ---: | --- |
| `solar_w` | `PLUGIN/FoxEss/foxess/solar` | 525 | FoxEss reports negative kW; multiply by -1000 to get positive generation W |
| `grid_w` | `EAN.3153414731313035313134323631` | 526 | Positive import, negative export; expected W |
| `battery_w` | `PLUGIN/FoxEss/foxess/battery` | 524 | Signed kW; positive charging, negative discharging; multiply by 1000 |
| `ev_w` | `EY220722000169.3` | 513 | Positive EV consumption; expected W |

`soc_percent` is optional and may map another field from the same battery counter.
Its exact total stored-energy field/unit still needs verification. After applying
`scale`, its input must be **kWh stored**, not lifetime energy throughput or
instantaneous power. Conversion uses `battery_capacity_wh` (default 10000 Wh).
No SOC mapping is enabled by default and SOC is not used to select the policy.

Realtime `value` is distinct from counter totals such as `total_consumed`; no
power-to-total fallback is performed. Multiple signals per counter are supported.
The internal `config_schema_version` is omitted from the UI config payload so
every returned setting has a matching description; UI saves retain the existing
schema version to avoid replaying migrations.
Older saved `realtime` mappings are migrated to the v2 event's `value` key;
0.2.3 measurement-counter IDs are migrated to sensor IDs.
Saved 0.2.1 scale-1 defaults for FoxEss solar and battery are migrated to
solar `-1000` and battery `1000`; scale choices are retained after schema v2.
Invalid values invalidate that signal; omitted fields do not update timestamps.
The stale-reading limit remains a conservative, configurable 120 seconds; sparse
logs are not evidence of the event delivery cadence. Runtime diagnostics count
callbacks and matched IDs, and show the last event's ID, field names and numeric
measurement fields. This distinguishes missing events from unit/sign/field
mismatches.
The numeric IDs above are gateway-specific: update them if counters differ on
another installation. Missing/stale required data requests pause.

Charger connection options:
- tls_legacy: enable for older Alfen firmware whose TLS handshake is rejected by OpenSSL 3.x defaults.
- request_timeout: HTTP read timeout in seconds.
- current_property: Alfen property name written for the current setpoint and pause (default OD_sysActiveLoadBalancingSafeCurrent, confirmed via captured device traffic; zero pauses and a positive value resumes the active session).
- verify_ssl: certificate verification; enabling tls_legacy relaxes TLS security
  and the legacy adapter disables certificate verification. Use only on a trusted LAN.

## Installation, Migration and Live Enable

1. Install the dependencies in [requirements.txt](requirements.txt) into the
	gateway's plugin Python environment; this file is not an automatic installer.
2. Deploy using the normal OpenMotics plugin process. Set connection details and
	confirm signal mappings. Old three-signal configurations are disabled with a
	migration error; replace their signal list and remove SOC-on-`realtime`.
3. Check `max_current`: new defaults use **20 A**, but saved explicit limits,
	including 16 A, are preserved. Change only after verifying circuit/charger limits.
4. Start with `enabled=true`, `dry_run=true`. Inspect `get_runtime_state()` for
	inputs, signal ages, decision mode, power allowances, forecast/error status and
	simulated commands. Config changes clear cached readings and decisions.
5. The user verified that writing `0` to `current_property` pauses an active
	session and restoring a positive current resumes it. Confirm the vehicle's
	timer remains respected during the supervised live plugin test.
6. Only after reviewing the configuration and being present for a supervised
	test should live writes (`dry_run=false`) be enabled. Pause writes `0 A` to
	`current_property`; positive targets write the desired current. Missing
	critical readings request pause.

Normal commands are retried up to three times. Exhausted/partial failures clear
cached command acknowledgement and expose the failing phase, allowing retries
on the next decision. Unchanged commands are reasserted every 60 s for recovery
from charger restart/external changes. There is no charger readback verification
yet; a successful API call is not proof of actual charging.

Disabling stops issuing commands; it **does not restore the previous charger
configuration**. Explicitly pause or restore settings before relinquishing control.

## Safety Defaults

- enabled: false
- dry_run: true

So it will not write to your charger until explicitly configured.
