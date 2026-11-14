# otgw2mqtt

A bridge between [otmonitor](https://otgw.tclcode.com/otmonitor.html) (driving an OpenTherm Gateway connected to an
**Itho HP-M 25i** heat pump) and the rest of the home automation:

- **Sanitizes** everything otmonitor publishes over MQTT (type checks, range checks, spike filter, de-duplication).
- **Re-publishes** the sanitized values over MQTT (rate limited to one update per 10 s, using time-weighted averages)
  and writes every reading to **InfluxDB 1.x**.
- Reads the **raw OpenTherm stream** to capture the *thermostat's own* control setpoint (ID 1, `tchsetpoint`), which
  otmonitor stops reporting while a control setpoint override is active.
- Supports a **Control Setpoint (CS) override** of the flow temperature: an absolute value, or relative to what the
  thermostat requests.
- Integrates with **Home Assistant** via MQTT discovery, creating entities only for values the boiler actually reports.
- Forwards an external **outdoor temperature** sensor (in my case from rtl_433) to otmonitor.
- Runs as a systemd `Type=notify` service with **watchdog** support.

```
otmonitor MQTT events ---+               +--> InfluxDB (every changed reading)
OTGW raw stream (T..01) -+--> sanitizer --+
                                          +--> rate limiter (time-weighted avg) --> OUT_TOPIC/<name> --> Home Assistant
outdoor sensor (MQTT) -------> sanitizer --> otmonitor action 'outside'
Home Assistant switch/number ------------> otmonitor action 'ctrlsetpt' (OTGW CS=)
```

## Requirements

- Working OTGW & otmonitor setup
- Only tested with a Itho HP-M 25i & spider thermostat
- MQTT broker

## Background

I used to run a loose collection of scripts before adding Home Assistant on top. I have different hardware in different 
places, so MQTT is a convenient way to loosely couple ot all together.

The Itho HP-M 25i is a little wierd in how it is controlled though.

My end goal with the remote control is to make thermostat schedules in HA and eventually optimize running the heatpump
at the lowest possible electricity cost, given expected outdoor temperatures and dynamic electricity rates.

I started with my own script which didn't work very well, so i let Claude use that for inspiration to build something
better :)

## Installation

Requires Python 3.11+.

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp otgw2mqtt.env.example otgw2mqtt.env && chmod 600 otgw2mqtt.env   # then edit it
.venv/bin/python otgw2mqtt.py -e otgw2mqtt.env -d -L DEBUG          # dry run first
```

### As a systemd service

```sh
sudo mkdir -p /opt/otgw2mqtt
sudo cp otgw2mqtt.py requirements.txt README.md /opt/otgw2mqtt/
sudo python3 -m venv /opt/otgw2mqtt/.venv
sudo /opt/otgw2mqtt/.venv/bin/python -m pip install -r /opt/otgw2mqtt/requirements.txt
sudo chown -R root:root /opt/otgw2mqtt && sudo chmod -R go+rX /opt/otgw2mqtt
sudo install -d -m 755 /etc/homeautomation
sudo install -m 600 -o root -g root otgw2mqtt.env /etc/homeautomation/otgw2mqtt.env
sudo cp otgw2mqtt.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now otgw2mqtt
journalctl -u otgw2mqtt -f
```

The service runs with hardening and minimal privileges.

## Command line

| Option | Meaning |
|---|---|
| `-e, --env-file PATH` | Optional env file to load (needs `python-dotenv`). Without it only the process environment is used. Real env vars take precedence. Not used under systemd. |
| `-L, --loglevel LEVEL` | `CRITICAL`/`ERROR`/`WARNING`/`INFO`/`DEBUG` (default `LOG_LEVEL` or `INFO`) |
| `-l, --logfile PATH` | Also log to a file (stdout/journal is always used) |
| `-d, --dryrun` | Connect and process everything, but do not publish to MQTT or write to InfluxDB; only log |
| `--purge` | Remove all retained MQTT messages of this bridge and exit (see *Removing the device from Home Assistant*). With `-d`, only list them |

## Configuration

All settings are environment variables, normally kept in `otgw2mqtt.env`. See
[otgw2mqtt.env.example](otgw2mqtt.env.example) for a commented template.

| Variable | Default | Description |
|---|---|---|
| `BOILER_NAME` | `HP-M_25_i` | InfluxDB tag `id`, HA device name, default `OUT_TOPIC` |
| `LOG_LEVEL` | `INFO` | Log level when `-L` is not given |
| `MQTT_HOST` | `localhost` | Broker host |
| `MQTT_PORT` | `1883` | |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | empty | |
| `MQTT_CLIENT_ID` | `otgw2mqtt-<hostname>` | Must be unique on the broker (a dry run next to the service needs a different one) |
| `OTMONITOR_EVENT_TOPIC` | `events/central_heating/otmonitor` | otmonitor event prefix (subscribed with `/#`) |
| `OTMONITOR_ACTION_TOPIC` | `actions/otmonitor` | otmonitor action prefix |
| `OTGW_HOST` / `OTGW_PORT` | empty / `7686` | Raw OTGW message stream; empty host disables `tchsetpoint` |
| `OUT_TOPIC` | `otgw2mqtt/<BOILER_NAME>` | Prefix for sanitized values; must not be under the otmonitor prefix |
| `HEARTBEAT` | `55` | Unchanged values are re-published / re-written at most this often (s); `0` = always |
| `OUTDOOR_TOPIC` | empty | External outdoor temperature topic; empty disables |
| `OUTDOOR_JSON_KEY` | empty | JSON key (dotted path allowed) holding the temperature; empty = plain number |
| `INFLUX_HOST` | empty | InfluxDB 1.x host; empty disables InfluxDB |
| `INFLUX_PORT` | `8086` | |
| `INFLUX_USERNAME` / `INFLUX_PASSWORD` | empty | |
| `INFLUX_DATABASE` | `sensors` | |
| `INFLUX_MEASUREMENT` | `environmental` | |
| `HA_ENABLED` | `true` | Publish Home Assistant discovery |
| `HA_DISCOVERY_PREFIX` | `homeassistant` | |
| `HA_UPDATE_INTERVAL` | `10` | Numeric values go to MQTT / HA at most once per N seconds (time-weighted average); `0` = no limit |
| `OVERRIDE_MIN` / `OVERRIDE_MAX` | `10` / `55` | Allowed override range (°C), in both modes. In relative mode a thermostat request at or below `OVERRIDE_MIN` counts as "off" and is passed through |
| `OVERRIDE_OFFSET_MAX` | `10` | Relative mode: offset range is `-N`..`+N` °C |
| `OVERRIDE_REFRESH` | `60` | Re-send the override every N seconds while active |
| `OVERRIDE_RELEASE_ON_EXIT` | `true` | Send `CS=0` (back to thermostat) on clean shutdown |

## MQTT topics

Published by otgw2mqtt:

| Topic | Retained | Payload |
|---|---|---|
| `OUT_TOPIC/status` | yes | `online` / `offline` (also the MQTT last will) |
| `OUT_TOPIC/<name>` | no | `{"value": <v>, "timestamp": <ms>}` for every sanitized value (see below and *Rate limiting*) |
| `OUT_TOPIC/override_mode` | no | `Off` / `Absolute` / `Relative` – override mode |
| `OUT_TOPIC/override_setpoint` | no | e.g. `35.0` – absolute override value |
| `OUT_TOPIC/override_offset` | no | e.g. `-2.0` – relative override offset |
| `HA_DISCOVERY_PREFIX/<component>/otgw2mqtt_<BOILER_NAME>/<name>/config` | yes | Home Assistant discovery |

Values are deliberately **not retained**, so no stale readings linger on the broker. Instead the latest value of
everything is re-published:

- when Home Assistant sends its birth message (`online` on `HA_DISCOVERY_PREFIX/status`, also after HA's own reconnect to
  the broker): discovery right away, the values 5 s later so HA has created the entities first;
- when otgw2mqtt (re)connects to the broker, because anything published while disconnected was lost.

Only the availability topic (so HA sees `offline` even when it starts after otgw2mqtt died) and discovery are retained.

Commands (subscribed; Home Assistant publishes these retained, so the last choice survives restarts):

| Topic | Payload |
|---|---|
| `OUT_TOPIC/override_mode/set` | `Off` / `Absolute` / `Relative` (case-insensitive; `Relative` needs `OTGW_HOST`) |
| `OUT_TOPIC/override_setpoint/set` | number in °C, clamped to `OVERRIDE_MIN`..`OVERRIDE_MAX` |
| `OUT_TOPIC/override_offset/set` | number in °C, clamped to `-OVERRIDE_OFFSET_MAX`..`+OVERRIDE_OFFSET_MAX` |

Sent to otmonitor: `OTMONITOR_ACTION_TOPIC/ctrlsetpt` (OTGW `CS=`) and `OTMONITOR_ACTION_TOPIC/outside` (OTGW `OT=`), with
payload `{"name":"temp","type":"float","value":<v>}`. The action names are fixed by otmonitor
([mqtt.tcl](https://github.com/hvxl/otmonitor/blob/master/otmonitor.vfs/mqtt.tcl)); otmonitor silently ignores unknown
actions. Its MQTT format must be one of the JSON formats except `json3`, which nests the value differently.

## Sanitization

Every value name is described in the `SPECS` table in [otgw2mqtt.py](otgw2mqtt.py) (type, range, spike limit,
Home Assistant metadata). Values not in the table are logged once and ignored.

- **Parsing**: otmonitor JSON (`value` + `timestamp` in ms) or a bare value. Timestamps that are missing, more than
  5 minutes in the future or older than 30 days are replaced with the current time. Older past timestamps are kept, so
  retained values keep the time of their last change.
- **Logging**: the first rejection of a value is logged at INFO, repeats at DEBUG until a valid value arrives again.
- **Types**: booleans accept `true/false/1/0/on/off/yes/no`; numbers must be finite; counters must be non-negative integers;
  strings must be short and printable.
- **Ranges**: out-of-range values are rejected (modulation values are clamped to 0–100 %).
- **Spike filter** (flow/return/DHW/room/outside temperature): a jump larger than the limit is held back and only
  accepted after 3 consecutive readings that agree with each other, so a real large change is never blocked forever.
- **De-duplication**: a value is published and written to InfluxDB when it changes, or at least every `HEARTBEAT` seconds
  (tracked separately for MQTT and InfluxDB).

| Kind | Names |
|---|---|
| Temperatures (°C) | `boilerwatertemperature` (flow), `returnwatertemperature`, `dhwtemperature`, `roomtemperature`, `outsidetemperature`, `setpoint`, `setpoint2`, `controlsetpoint`, `controlsetpoint2`, `tchsetpoint`, `chsetpoint`, `maxchsetpoint`, `dhwsetpoint` |
| Other numbers | `modulation`, `maxmodulation` (%), `chwaterpressure` (bar) |
| Flags | `chenable`, `dhwenable`, `coolingenable`, `ch2enable`, `otcstate`, `thermostat`, `fault`, `centralheating`, `hotwater`, `flame`, `cooling`, `centralheating2`, `diagnostic`, `electricity`, `override_active` |
| Counters | `burnerstarts`, `chpumpstarts`, `dhwpumpstarts`, `dhwburnerstarts`, `burnerhours`, `chpumphours`, `dhwpumphours`, `dhwburnerhours` |
| Strings | `powermode` |

`tchsetpoint` comes from the raw stream (thermostat `T` frames, WRITE-DATA, data-id 1), not from otmonitor.
`override_active` is derived: true when the gateway's `controlsetpoint` differs from the thermostat's `tchsetpoint`.

## Rate limiting (MQTT / Home Assistant)

Numeric (float) values are published to `OUT_TOPIC/<name>` at most once per `HA_UPDATE_INTERVAL` seconds (default 10):

- **Slow sensors pass through unmodified.** A reading that arrives at least `HA_UPDATE_INTERVAL` after the previous
  update (with nothing buffered) is published immediately, exactly as sanitized.
- **Fast sensors are averaged.** Readings that arrive sooner are buffered. Once the interval since the previous update has
  passed, their **time-weighted average** is published: each reading counts for as long as it was the current value,
  and the reading that was current at the start of the window counts until the first new reading. For example, with a
  10 s window and 20 °C at 0 s, 30 °C at 2 s and 40 °C at 6 s, the update is (20·2 + 30·4 + 40·4) / 10 = 32 °C.
  The payload timestamp is the end of the window.
- Flags, counters and strings are not rate limited, so state changes (e.g. `flame`) reach HA immediately.
- InfluxDB is not affected: it receives every changed reading with its original timestamp.

## InfluxDB

One point per value: measurement `INFLUX_MEASUREMENT`, tags `host` and `id` (= `BOILER_NAME`), field `<name>`, time from
otmonitor's timestamp. Writes are batched every 5 s and run off the event loop; on failures up to 10 000 points are
buffered and retried.

## Control setpoint override

Home Assistant gets a **CS override mode** select, a **CS override setpoint** number (absolute mode) and a
**CS override offset** number (relative mode). The override is sent to otmonitor as action `ctrlsetpt` (OTGW `CS=<value>`).

| Mode | Control setpoint sent |
|---|---|
| **Off** | `CS=0`: the thermostat is in control |
| **Absolute** | the setpoint number, clamped to `OVERRIDE_MIN`..`OVERRIDE_MAX` |
| **Relative** | the thermostat's own request (`tchsetpoint`, from the raw stream) + the offset, clamped to `OVERRIDE_MIN`..`OVERRIDE_MAX` |

On the HP-M 25i the control setpoint is what switches the heat pump on and off: the thermostat requests 10 °C for "off" and
about 30–35 °C when it wants heat. So in **relative** mode:

- A thermostat request **at or below `OVERRIDE_MIN`** means "no heat demand" and is passed through unchanged, without the
  offset; otherwise a positive offset would switch the heat pump on (10 + 3 = 13 °C) when the thermostat wants it off.
- Changes in the thermostat's request are followed within a second or two, but at most once per 5 s.
- If the thermostat's request is unknown or older than 120 s (raw stream down, or right after startup), control is handed
  back to the thermostat (`CS=0`) and a warning is logged, until the stream delivers it again.
- The selector only offers `Relative` when `OTGW_HOST` is set.

In all modes:

- The override is sent when anything changes, every `OVERRIDE_REFRESH` seconds while active, and after an MQTT reconnect.
  It is also re-sent if otmonitor reports a different control setpoint (e.g. after a gateway reboot).
- **Startup**: after a 5 s grace period for retained commands, the stored mode is applied. If it is `Off`, `CS=0` is sent
  once to clear an override left behind by a crash.
- **Shutdown**: with `OVERRIDE_RELEASE_ON_EXIT=true` an active override is released.
- The thermostat's own request remains visible as `tchsetpoint`; `controlsetpoint` shows what the heat pump gets.

**Upgrading from 2.2 or older**: the old *CS override* switch is replaced by the mode select. On connect otgw2mqtt removes
the switch from HA and clears its retained command, so after the upgrade the override starts as `Off`; select
`Absolute` to get the old behaviour back. The setpoint number keeps its value.

## Home Assistant

A single device **OpenTherm Gateway (BOILER_NAME)** is created with the override mode select and the setpoint and offset numbers. All entities use
`OUT_TOPIC/status` for availability. Discovery and the latest values are re-published whenever HA comes online
(`homeassistant/status`), see *MQTT topics*. Unique IDs are `otgw2mqtt_<BOILER_NAME>_<name>`.

**Entities are created on demand.** Every boiler/thermostat combination implements a different part of OpenTherm, so
instead of announcing every value in `SPECS` up front, a sensor/binary_sensor is announced the first time a valid
(sanitized) value for it is published. Values your system never reports, or only reports as garbage, never get an
entity. The state is re-sent 3 s after a new entity is announced, because HA misses the first one while it is still
setting up the entity. Discovery is retained, so the entities survive restarts of otgw2mqtt and HA. Values otmonitor
reports with a constant value (e.g. `cooling` = off) do get an entity; disable those in HA if you don't want them.

### Removing the device from Home Assistant

Discovery messages are retained, so the device stays in HA until they are removed from the broker:

```sh
sudo systemctl stop otgw2mqtt      # otherwise it re-creates the entities
sudo /opt/otgw2mqtt/.venv/bin/python /opt/otgw2mqtt/otgw2mqtt.py -e /etc/homeautomation/otgw2mqtt.env --purge -d   # list
sudo /opt/otgw2mqtt/.venv/bin/python /opt/otgw2mqtt/otgw2mqtt.py -e /etc/homeautomation/otgw2mqtt.env --purge      # remove
```

`--purge` subscribes to `OUT_TOPIC/#` and `HA_DISCOVERY_PREFIX/+/otgw2mqtt_<BOILER_NAME>/+/config`, collects the retained
messages (until 2 s pass without a new one), and clears each by publishing an empty retained message. That removes every
entity and with them the device from HA, plus `OUT_TOPIC/status` and the retained override commands, so the override starts
OFF next time. It connects with client id `MQTT_CLIENT_ID-purge` and warns if `OUT_TOPIC/status` is still `online`.
Start the service again to get a fresh device with only the entities that receive data.

## Development

```sh
.venv/bin/python -m unittest discover tests
.venv/bin/python -m pylint otgw2mqtt.py
```