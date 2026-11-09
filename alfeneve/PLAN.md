# AlfenEve Plugin Plan

## Goal
Maintain the production OpenMotics plugin that controls Alfen charger current from measured solar, grid, battery and EV power.

## Phase 1: Alfen API validation (complete)
1. Vendor the Alfen client with its license and source attribution.
2. Verify property reads and current-property writes against the charger API.

Deliverable: the licensed Alfen client used by the production plugin.

## Phase 2: Measurement Counter Mapping
1. Define how each measurement counter maps to:
   - solar power (W),
   - grid power (W),
   - load power (W),
   - battery SOC (%).
2. Decide data source field for each signal (`realtime`, `total_consumed`, `total_injected`) and sign convention.
3. Add safeguards for stale/missing values.

Deliverable: stable data contract usable by the plugin.

## Phase 3: Plugin Implementation
1. Create plugin package (`main.py`, `info.json`, docs, vendored library).
2. Add plugin config:
   - charger endpoint/user/password,
   - counter external IDs,
   - min/max current,
   - update interval, deadband, hold time.
3. Subscribe to measurement counter status events and update internal signal state.
4. Run a background loop that computes and applies current using the Alfen client.

Deliverable: deployable plugin package.

## Phase 4: Hardening
1. Retry and backoff for charger API failures.
2. Input validation and config checker rules.
3. Logging and observability (inputs, current decisions, API result).
4. Unit tests for controller logic.

Deliverable: robust plugin ready for real gateway tests.

## Historical Progress
- Step 1 complete: vendored code + notice.
- Step 2 complete: the Alfen client's legacy TLS handshake and `POST /api/prop` write format were validated against a real charger.
- The production plugin implements config, sensor-event subscription, background control, and charger writes. Confirmed external IDs:
  - solar_w: `PLUGIN/FoxEss/foxess/solar`
   - soc_percent: the battery counter's total stored-energy value supplies SOC via battery capacity; the old implementation incorrectly read `realtime` for this conversion. The exact total-energy payload field still needs verification.
  - grid_w: `EAN.3153414731313035313134323631`
   - load_w dropped as a dedicated meter. The earlier `solar_w + grid_w` estimate does not isolate household consumption when the battery and car are active; use the power balance below.
- Confirmed: grid_w sign convention is positive = import, negative = export — matches the summer excess-solar calculation as implemented.
- Confirmed during gateway debug (2026-11-02): the dummy publisher calls `report_realtime_state(...)`; its measurement-counter v2 subscription is syntactically valid, but the live gateway does not forward that event type. Realtime reports are published as `SENSOR_VALUE`, then `SensorController` emits `SENSOR_CHANGE`; meanwhile the measurement-counter connector subscribes to `MEASUREMENT_COUNTER_CHANGE`, which is not in `GatewayEvent.STATUS_EVENT_TYPES`. Version 0.2.4 uses the supported sensor connector with sensor IDs 525/526/524/513 and migrates saved counter IDs 16/17/15/5. FoxEss logs are change/hour throttled and are not evidence of callback cadence.
- `realtime` supplies instantaneous power, including battery charge/discharge. The battery counter's total stored-energy value supplies SOC, not its `realtime` field. Convert stored energy to percentage using compatible units and battery capacity; never convert instantaneous power to SOC.
- Basic retries, validation and unit tests existed. The policy revision below replaces the original SOC/season-only algorithm.

## Agreed Control Direction

Implemented in plugin version 0.2.7. It uses the gateway's sensor-status forwarding path. The user verified that writing 0 A to the current property pauses an active session and restoring a positive limit resumes it. Live API writes have succeeded; actual delivered current still requires vehicle or charger readback. The UI config response omits internal `config_schema_version` and preserves the current migration version on save.

### Confirmed Measurements and Priorities

| Signal | Counter external ID | Sensor ID | Event field/scaling | Result |
| --- | --- | ---: | --- | --- |
| EV consumption (`ev_w`) | `EY220722000169.3` | 513 | `value`, x1 | W consumed by the charger |
| Battery power (`battery_w`) | `PLUGIN/FoxEss/foxess/battery` | 524 | `value`, signed kW x1000 | W; positive charging, negative discharging |
| Battery SOC (`soc_percent`) | `PLUGIN/FoxEss/foxess/battery` | 524 | Sensor status is realtime-only; no cumulative field | Optional/unmapped |
| Grid power (`grid_w`) | `EAN.3153414731313035313134323631` | 526 | `value`, x1 | W; positive import, negative export |
| Solar generation (`solar_w`) | `PLUGIN/FoxEss/foxess/solar` | 525 | `value`, signed kW x-1000 | W generated, normalized positive |

- The home is single-phase. Use nominal 230 V for initial W-to-A conversion; measured voltage can improve accuracy if available.
- The car has priority over charging the home battery: battery-charging power may be redirected to the car.
- No reliable connected-car state, departure SOC or required charging duration is assumed. Zero EV consumption can mean absent, full, or waiting on the car's schedule.
- Battery power and SOC come from different fields of the same counter. Multiple signal mappings per counter ID are supported. SOC is optional and omitted from defaults until its payload field/unit is verified; it does not gate charging.
- The total stored-energy reading is not a lifetime cumulative charge/discharge throughput counter. Verify its exact payload key and unit before wiring it to SOC conversion. If it is kWh, `soc_percent = stored_energy_kwh * 1000 / battery_capacity_wh * 100`.

### Power Budget

With battery power positive for charging and negative for discharge:

`household_w = solar_w + grid_w - battery_w - ev_w`

`solar_available_for_ev_w = max(0, ev_w - grid_w + battery_w)`

The solar budget includes the car's existing consumption, exported power and battery charging, while subtracting battery discharge. It estimates solar remaining after other household loads, subject to meter alignment and conversion losses. Solar generation alone or grid export alone is not the available EV budget.

The grid-import ceiling is a separate, always-active constraint. For a configurable ceiling initially proposed as 6000 W:

`ev_grid_allowance_w = max(0, ev_w + grid_import_limit_w - grid_w - safety_margin_w)`

Clamp the policy target to this allowance and the charger/circuit maximum. This assumes other loads remain approximately unchanged over the response interval; battery/inverter reactions require live testing. Reduce quickly on excess import and increase more slowly. Household overload with the car stopped cannot be corrected by this controller, and software control does not replace electrical protection.

### Scenarios to Implement and Test

1. **Sunny noon:** solar-only charging, including solar currently charging the battery; no sustained intentional grid import or battery discharge for the car.
2. **Late evening/night:** allow grid charging up to the configured charger limit, constrained by total grid import. No departure-energy guarantee is possible without knowing vehicle demand.
3. **Summer scheduled 09:00 start:** the car controls its own start time; the plugin already offers the daytime solar policy and follows surplus when consumption begins. Include restart-from-paused behaviour in tests.
4. **Cloudy/autumn/rainy day:** permit useful grid charging rather than waiting indefinitely for solar; seasonal baselines and a forecast can inform the allowance.
5. **Grid import above the ceiling:** reduce EV demand irrespective of season, forecast or charging mode; pause if the remaining allowance is below the supported charging minimum.

### Implemented Algorithm and Initial Configurable Defaults

1. Require finite grid and EV power no older than **120 s**; missing/invalid/stale critical readings request **pause**, not continued operation at the last limit. Solar and battery readings may be up to **900 s** old to tolerate FoxEss's 120 s polling and unchanged values. Older auxiliary data is excluded from solar-based charging, and stale battery power never contributes inferred solar surplus. Decision interval: 5 s.
2. Select local-time policy in `Europe/Brussels`, including daylight saving:
   - **22:00–07:00:** offer up to **20 A**, regardless of forecast, still subject to the grid cap and gradual increases.
   - **07:00–16:00 with useful solar ahead:** solar-only; pause if insufficient surplus.
   - **Poor/missing/stale forecast, or from 16:00 until night:** solar plus a seasonal permitted grid contribution: **6 A summer, 10 A winter**. Summer is May–September, winter the remaining months; season can be overridden.
   - These baselines are grid contributions, not a clamp on total current. Unexpected actual solar is always used in every mode.
3. Use the public Forecast.Solar API with a 45° tilt, SSE azimuth rounded to -23°, **6.4 kWp** panels and **5 kW** inverter. Location is configurable; configured site coordinates are rounded to API precision. Refresh every 30 minutes in a separate task, cache for at most 2 hours and fall back to seasonal baseline when unavailable. No API key required; public quota is shared per IP.
4. Determine whether waiting is worthwhile from **future intervals in the next 4 hours**, not whole-day totals or cloud labels. Subtract an initial estimated household demand of **500 W**. Count intervals with surplus at least the minimum charging power plus **200 W** restart margin. Defer only if these intervals provide at least **2 kWh** and **60 minutes** of usable charging. Thresholds and demand estimate are configurable; useful minutes need not be consecutive.
5. Compute solar budget and grid allowance using the formulas above. Policy target is solar budget plus permitted grid contribution, constrained by **6000 W import minus 200 W margin**, and **20 A maximum**. The previously configured 16 A default was wrong for this installation. Preserve any explicit saved lower limit during migration; never silently raise a saved installation limit.
6. Convert W to A using configured nominal **230 V**, rounding down to 0.1 A. Assume a supported minimum of **6 A** until hardware validation. Below minimum: request pause; never round up through the grid cap. A new solar-only start additionally requires 200 W surplus margin. Hold a pause for at least **120 s** before restarting.
7. Smooth the solar budget (EMA, **35%** new sample), but always take the smaller of raw and smoothed surplus so reductions are not delayed. Grid cap uses raw readings. Start at the minimum, increase by at most **1 A every 30 s**, and apply reductions without hold/deadband delay. Increases use a **0.5 A** deadband. Reassert commands every **60 s** to recover from external changes/charger restart.
8. Write the target directly to `OD_sysActiveLoadBalancingSafeCurrent`: zero pauses and a positive limit resumes. Retry up to **3 attempts**, with linear 2 s/4 s retry waits. On failure, invalidate cached command state and retry on the next decision. HTTP success is not proof of actual delivered current.
9. Expose decisions, signal ages, forecast age/status, successful command timestamp and errors via `get_runtime_state()`. Do not infer delivered energy or connection state from requested current.

### Live-Operation Gate and Migration

- Defaults remain `enabled=false`, `dry_run=true`. No charger writes or weather requests occur while disabled.
- The user verified that setting `OD_sysActiveLoadBalancingSafeCurrent` to 0 through the API pauses an active session and restoring a positive limit resumes it. The plugin now uses that same property for pause/resume; a supervised live plugin test must still verify the car's schedule and end-to-end behavior.
- Dry-run can exercise all policy decisions without live charger writes. Config changes clear simulated/current/measurement/forecast caches; fresh readings are required before normal control resumes. The controller requests pause while critical readings are incomplete.
- Saved legacy configurations lacking EV/battery-power mappings are disabled with a migration error. Replace the old signal list; old SOC mappings on `realtime` are rejected. Keep the existing connection credentials and explicitly check the saved maximum.
- Disabling the plugin stops issuing commands; it does not automatically undo its last charger setting. Pause or restore charger controls explicitly before handing control back.

### Remaining Hardware and Integration Work

- Compare requested current against vehicle/charger readback under supervision, including restart with a car-side 09:00 timer.
- Verify battery total stored-energy payload key/unit, then optionally add `soc_percent` mapping; no SOC mapping is guessed.
- Validate grid/battery/EV meter timing and sign conventions and battery inverter reaction under load. Software cap is reactive, cannot prevent every transient, and cannot fix household-only overload.
- Compare forecasts to actual generation and tune household-demand estimate, deferral thresholds, seasonal baselines and time windows.


