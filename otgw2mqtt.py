#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""otgw2mqtt - sanitizing bridge between otmonitor (OpenTherm Gateway) and MQTT / InfluxDB / Home Assistant.

Data flow:
  otmonitor MQTT events  --+                +--> InfluxDB 1.x (every changed reading)
  OTGW raw stream (T..01)--+--> Sanitizer --+
                                            +--> rate limiter (time-weighted avg) --> MQTT OUT_TOPIC/<name> --> HA
  outdoor sensor (MQTT) ------> Sanitizer --> otmonitor action 'outside'
  Home Assistant commands ----> Control Setpoint override --> otmonitor action 'ctrlsetpt' (OTGW CS=)
                                (absolute value, or the thermostat's request from the raw stream + offset)

Configuration comes from environment variables (systemd EnvironmentFile=, or --env-file for manual runs).
"""
import argparse
import asyncio
import contextlib
import json
import logging
import math
import os
import re
import signal
import socket
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

import aiomqtt
from influxdb import InfluxDBClient

__version__ = "2.3.0"

SCRIPTNAME = "otgw2mqtt"
HOSTNAME = socket.gethostname()

logger = logging.getLogger(SCRIPTNAME)

# Timing constants (seconds)
STARTUP_GRACE = 5.0          # wait for retained HA commands before touching the override
HA_BIRTH_DELAY = 5.0         # after HA's 'online' birth message, wait for it to process discovery before sending states
HA_NEW_ENTITY_DELAY = 3.0    # after creating an entity, wait for HA to subscribe before re-sending its state
STREAM_READ_TIMEOUT = 120.0  # reconnect the OTGW stream when silent this long
CONNECT_TIMEOUT = 10.0
CS_RESEND_MIN_INTERVAL = 30.0  # re-send when otmonitor reports a different control setpoint, at most this often
CS_FOLLOW_MIN_INTERVAL = 5.0   # relative mode: follow thermostat changes at most this often
THERMOSTAT_MAX_AGE = 120.0     # relative mode: hand back to the thermostat when its setpoint is older than this
SHUTDOWN_TIMEOUT = 3.0
INFLUX_FLUSH_INTERVAL = 5.0
INFLUX_BATCH_SIZE = 100
INFLUX_MAX_BUFFER = 10000
INFLUX_MAX_WRITE = 5000      # points per write call
TIMESTAMP_MAX_SKEW = 300.0   # otmonitor timestamps further in the future than this are replaced with now
TIMESTAMP_MAX_AGE = 30 * 86400.0  # ... as are timestamps older than this
SPIKE_CONFIRM = 3            # consecutive consistent readings needed to accept a large jump
PURGE_IDLE = 2.0             # --purge: stop collecting retained messages after this long without a new one
PURGE_MAX = 30.0             # --purge: ... or after this long in total

# --------------------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------------------
class ConfigError(Exception):
  pass


def _env_str(env: Mapping[str, str], name: str, default: str | None = None, required: bool = False) -> str:
  value = env.get(name, default)
  if value is not None:
    value = value.strip()
  if required and not value:
    raise ConfigError(f"{name} is required")
  return value or ""


def _env_int(env: Mapping[str, str], name: str, default: int) -> int:
  raw = _env_str(env, name, str(default))
  try:
    return int(raw)
  except ValueError as e:
    raise ConfigError(f"{name} must be an integer, got {raw!r}") from e


def _env_float(env: Mapping[str, str], name: str, default: float) -> float:
  raw = _env_str(env, name, str(default))
  try:
    value = float(raw)
  except ValueError as e:
    raise ConfigError(f"{name} must be a number, got {raw!r}") from e
  if not math.isfinite(value):
    raise ConfigError(f"{name} must be finite, got {raw!r}")
  return value


def _env_bool(env: Mapping[str, str], name: str, default: bool) -> bool:
  raw = _env_str(env, name, "true" if default else "false").lower()
  if raw in ("1", "true", "yes", "on"):
    return True
  if raw in ("0", "false", "no", "off"):
    return False
  raise ConfigError(f"{name} must be true/false, got {raw!r}")


def _check_topic(name: str, topic: str, allow_empty: bool = False):
  if not topic:
    if allow_empty:
      return
    raise ConfigError(f"{name} must not be empty")
  if any(c in topic for c in "+#") or topic.startswith("/") or topic.endswith("/"):
    raise ConfigError(f"{name} must be a plain topic without wildcards or leading/trailing '/': {topic!r}")


@dataclass(frozen=True)
class Config:  # pylint: disable=too-many-instance-attributes
  boiler_name: str
  mqtt_host: str
  mqtt_port: int
  mqtt_username: str
  mqtt_password: str
  mqtt_client_id: str
  otmonitor_event_topic: str
  otmonitor_action_topic: str
  out_topic: str
  outdoor_topic: str
  outdoor_json_key: str
  otgw_host: str
  otgw_port: int
  influx_host: str
  influx_port: int
  influx_username: str
  influx_password: str
  influx_database: str
  influx_measurement: str
  ha_enabled: bool
  ha_discovery_prefix: str
  override_min: float
  override_max: float
  override_refresh: float
  override_offset_max: float
  override_release_on_exit: bool
  heartbeat: float
  ha_update_interval: float

  @classmethod
  def from_env(cls, env: Mapping[str, str] | None = None) -> "Config":
    env = os.environ if env is None else env
    boiler = _env_str(env, "BOILER_NAME", "HP-M_25_i", required=True)
    cfg = cls(
      boiler_name=boiler,
      mqtt_host=_env_str(env, "MQTT_HOST", required=True),
      mqtt_port=_env_int(env, "MQTT_PORT", 1883),
      mqtt_username=_env_str(env, "MQTT_USERNAME"),
      mqtt_password=_env_str(env, "MQTT_PASSWORD"),
      mqtt_client_id=_env_str(env, "MQTT_CLIENT_ID", f"{SCRIPTNAME}-{HOSTNAME}"),
      otmonitor_event_topic=_env_str(env, "OTMONITOR_EVENT_TOPIC", "events/central_heating/otmonitor"),
      otmonitor_action_topic=_env_str(env, "OTMONITOR_ACTION_TOPIC", "actions/otmonitor"),
      out_topic=_env_str(env, "OUT_TOPIC", f"{SCRIPTNAME}/{boiler}"),
      outdoor_topic=_env_str(env, "OUTDOOR_TOPIC"),
      outdoor_json_key=_env_str(env, "OUTDOOR_JSON_KEY"),
      otgw_host=_env_str(env, "OTGW_HOST"),
      otgw_port=_env_int(env, "OTGW_PORT", 7686),
      influx_host=_env_str(env, "INFLUXDB_HOST"),
      influx_port=_env_int(env, "INFLUXDB_PORT", 8086),
      influx_username=_env_str(env, "INFLUXDB_USERNAME"),
      influx_password=_env_str(env, "INFLUXDB_PASSWORD"),
      influx_database=_env_str(env, "INFLUXDB_DATABASE", "sensors"),
      influx_measurement=_env_str(env, "INFLUXDB_MEASUREMENT", "environmental"),
      ha_enabled=_env_bool(env, "HA_ENABLED", True),
      ha_discovery_prefix=_env_str(env, "HA_DISCOVERY_PREFIX", "homeassistant"),
      override_min=_env_float(env, "OVERRIDE_MIN", 10.0),
      override_max=_env_float(env, "OVERRIDE_MAX", 55.0),
      override_refresh=_env_float(env, "OVERRIDE_REFRESH", 60.0),
      override_offset_max=_env_float(env, "OVERRIDE_OFFSET_MAX", 10.0),
      override_release_on_exit=_env_bool(env, "OVERRIDE_RELEASE_ON_EXIT", True),
      heartbeat=_env_float(env, "HEARTBEAT", 60.0),
      ha_update_interval=_env_float(env, "HA_UPDATE_INTERVAL", 10.0),
    )
    cfg.validate()
    return cfg

  def validate(self):
    for name in ("otmonitor_event_topic", "otmonitor_action_topic", "out_topic", "ha_discovery_prefix"):
      _check_topic(name.upper(), getattr(self, name))
    _check_topic("OUTDOOR_TOPIC", self.outdoor_topic, allow_empty=True)
    if self.out_topic == self.otmonitor_event_topic or self.out_topic.startswith(self.otmonitor_event_topic + "/"):
      raise ConfigError("OUT_TOPIC must not be inside OTMONITOR_EVENT_TOPIC (it would receive its own messages)")
    if not 0 < self.override_min < self.override_max <= 90:
      raise ConfigError("Need 0 < OVERRIDE_MIN < OVERRIDE_MAX <= 90")
    if self.override_refresh < 5:
      raise ConfigError("OVERRIDE_REFRESH must be at least 5 seconds")
    if not 0 < self.override_offset_max <= 30:
      raise ConfigError("OVERRIDE_OFFSET_MAX must be between 0 and 30")
    if self.heartbeat < 0:
      raise ConfigError("HEARTBEAT must be >= 0")
    if not 0 <= self.ha_update_interval <= 3600:
      raise ConfigError("HA_UPDATE_INTERVAL must be between 0 (disabled) and 3600 seconds")
    for name in ("mqtt_port", "otgw_port", "influx_port"):
      if not 0 < getattr(self, name) < 65536:
        raise ConfigError(f"{name.upper()} must be a valid TCP port")


# --------------------------------------------------------------------------------------------------
# Value specifications
# --------------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class Spec:  # pylint: disable=too-many-instance-attributes
  kind: str                       # 'float' | 'bool' | 'int' | 'str'
  name: str                       # friendly name (Home Assistant)
  vmin: float | None = None
  vmax: float | None = None
  max_step: float | None = None   # spike filter: larger jumps need SPIKE_CONFIRM consistent readings
  clamp: bool = False             # clamp to [vmin, vmax] instead of rejecting
  source: str = "otmonitor"       # 'otmonitor' | 'stream' | 'derived'
  influx: bool = True
  device_class: str | None = None
  unit: str | None = None
  state_class: str | None = "measurement"
  icon: str | None = None


def _temp(name, vmin, vmax, max_step=None, source="otmonitor") -> Spec:
  return Spec("float", name, vmin, vmax, max_step, source=source, device_class="temperature", unit="°C")


def _bool(name, device_class=None, icon=None, source="otmonitor") -> Spec:
  return Spec("bool", name, source=source, device_class=device_class, state_class=None, icon=icon)


def _counter(name, hours=False) -> Spec:
  return Spec("int", name, 0, 10_000_000, state_class="total_increasing",
              device_class="duration" if hours else None, unit="h" if hours else None,
              icon=None if hours else "mdi:counter")


SPECS: dict[str, Spec] = {
  # Temperatures / setpoints
  "boilerwatertemperature": _temp("Flow temperature", 0, 90, 5),
  "returnwatertemperature": _temp("Return temperature", 0, 90, 5),
  "dhwtemperature":         _temp("DHW temperature", 0, 90, 5),
  "roomtemperature":        _temp("Room temperature", 0, 40, 2),
  "outsidetemperature":     _temp("Outside temperature", -40, 50, 5),
  "setpoint":               _temp("Room setpoint", 5, 30),
  "setpoint2":              _temp("Room setpoint 2", 5, 30),
  "controlsetpoint":        _temp("Control setpoint", 0, 70),
  "controlsetpoint2":       _temp("Control setpoint 2", 0, 70),
  "tchsetpoint":            _temp("Thermostat control setpoint", 0, 70, source="stream"),
  "chsetpoint":             _temp("CH setpoint", 0, 90),
  "maxchsetpoint":          _temp("Max CH setpoint", 0, 90),
  "dhwsetpoint":            _temp("DHW setpoint", 0, 90),
  # Other measurements
  "modulation":             Spec("float", "Relative modulation", 0, 100, clamp=True, unit="%", icon="mdi:percent"),
  "maxmodulation":          Spec("float", "Max modulation", 0, 100, clamp=True, unit="%", icon="mdi:percent"),
  "chwaterpressure":        Spec("float", "CH water pressure", 0, 5, device_class="pressure", unit="bar"),
  # Status flags
  "chenable":               _bool("CH enable", icon="mdi:radiator"),
  "dhwenable":              _bool("DHW enable", icon="mdi:water-boiler"),
  "coolingenable":          _bool("Cooling enable", icon="mdi:snowflake"),
  "ch2enable":              _bool("CH2 enable", icon="mdi:radiator"),
  "otcstate":               _bool("Outside temperature compensation", icon="mdi:sun-thermometer"),
  "thermostat":             _bool("Thermostat connected", device_class="connectivity"),
  "fault":                  _bool("Fault", device_class="problem"),
  "centralheating":         _bool("Central heating", device_class="heat"),
  "hotwater":               _bool("Hot water", device_class="heat"),
  "flame":                  _bool("Flame / compressor", device_class="heat"),
  "cooling":                _bool("Cooling", device_class="cold"),
  "centralheating2":        _bool("Central heating 2", device_class="heat"),
  "diagnostic":             _bool("Diagnostic", device_class="problem"),
  "electricity":            _bool("Electricity production", device_class="power"),
  "override_active":        _bool("CS override active", icon="mdi:thermometer-alert", source="derived"),
  # Counters
  "burnerstarts":           _counter("Burner starts"),
  "chpumpstarts":           _counter("CH pump starts"),
  "dhwpumpstarts":          _counter("DHW pump starts"),
  "dhwburnerstarts":        _counter("DHW burner starts"),
  "burnerhours":            _counter("Burner hours", hours=True),
  "chpumphours":            _counter("CH pump hours", hours=True),
  "dhwpumphours":           _counter("DHW pump hours", hours=True),
  "dhwburnerhours":         _counter("DHW burner hours", hours=True),
  # Strings
  "powermode":              Spec("str", "Power mode", state_class=None, icon="mdi:flash"),
}

# Spec used for the external outdoor sensor before it is forwarded to otmonitor
OUTDOOR_SPEC = SPECS["outsidetemperature"]
OUTDOOR_TRACK = "_outdoorsensor"


# --------------------------------------------------------------------------------------------------
# Parsing / sanitizing
# --------------------------------------------------------------------------------------------------
def _sane_timestamp(raw: Any, now: float) -> float:
  """otmonitor timestamps are in ms; fall back to now when missing or implausible.

  Past timestamps are kept (retained messages carry the time of the last change, and writing them with
  that time keeps InfluxDB truthful); only future timestamps or ones older than TIMESTAMP_MAX_AGE are replaced.
  """
  if isinstance(raw, bool):
    return now
  try:
    ts = float(raw) / 1000.0
  except (TypeError, ValueError):
    return now
  if not math.isfinite(ts) or ts > now + TIMESTAMP_MAX_SKEW or ts < now - TIMESTAMP_MAX_AGE:
    return now
  return ts


def parse_otmonitor_payload(payload: bytes | bytearray | str, now: float) -> tuple[Any, float]:
  """Return (raw value, timestamp in s). Accepts otmonitor JSON or a bare value. Raises ValueError."""
  text = payload.decode("utf-8") if isinstance(payload, (bytes, bytearray)) else payload
  text = text.strip()
  if not text:
    raise ValueError("empty payload")
  try:
    obj = json.loads(text)
  except ValueError:
    return text, now
  if isinstance(obj, dict):
    if "value" not in obj:
      raise ValueError("JSON payload has no 'value'")
    return obj["value"], _sane_timestamp(obj.get("timestamp"), now)
  if isinstance(obj, (list, type(None))):
    raise ValueError(f"unsupported JSON payload: {text[:60]!r}")
  return obj, now


_TRUE = ("1", "true", "on", "yes")
_FALSE = ("0", "false", "off", "no")


def _coerce_bool(raw: Any) -> bool:
  if isinstance(raw, bool):
    return raw
  if isinstance(raw, (int, float)) and raw in (0, 1):
    return bool(raw)
  s = raw.strip().lower() if isinstance(raw, str) else None
  if s in _TRUE or s in _FALSE:
    return s in _TRUE
  raise ValueError(f"not a boolean: {raw!r}")


def _coerce_number(spec: Spec, raw: Any) -> float | int:
  if isinstance(raw, bool):
    raise ValueError(f"boolean where number expected: {raw!r}")
  try:
    value = float(raw)
  except (TypeError, ValueError) as e:
    raise ValueError(f"not a number: {raw!r}") from e
  if not math.isfinite(value):
    raise ValueError(f"not finite: {raw!r}")
  lo = -math.inf if spec.vmin is None else spec.vmin
  hi = math.inf if spec.vmax is None else spec.vmax
  if spec.clamp:
    value = min(max(value, lo), hi)
  elif not lo <= value <= hi:
    raise ValueError(f"out of range [{spec.vmin}, {spec.vmax}]: {value}")
  if spec.kind == "int":
    if value != int(value):
      raise ValueError(f"not an integer: {raw!r}")
    return int(value)
  return round(value, 2)


def _coerce_str(raw: Any) -> str:
  if not isinstance(raw, (str, int, float)) or isinstance(raw, bool):
    raise ValueError(f"not a string: {raw!r}")
  s = str(raw).strip()
  if not s or len(s) > 64 or not s.isprintable():
    raise ValueError(f"invalid string: {s[:70]!r}")
  return s


def coerce(spec: Spec, raw: Any) -> Any:
  """Convert a raw value into the spec's type and range. Raises ValueError when invalid."""
  if spec.kind == "bool":
    return _coerce_bool(raw)
  if spec.kind in ("float", "int"):
    return _coerce_number(spec, raw)
  if spec.kind == "str":
    return _coerce_str(raw)
  raise ValueError(f"unknown spec kind {spec.kind}")


@dataclass
class _Track:
  value: Any = None
  emitted_value: Any = None
  emitted_at: float | None = None
  pending: list = field(default_factory=list)
  rejecting: bool = False   # only the first rejection of a streak is logged at INFO


class Sanitizer:
  """Type/range checks, spike filtering and change/heartbeat de-duplication per value name."""

  def __init__(self, heartbeat: float, clock: Callable[[], float] = time.time, spike_confirm: int = SPIKE_CONFIRM):
    self._heartbeat = heartbeat
    self._clock = clock
    self._confirm = spike_confirm
    self._tracks: dict[str, _Track] = {}

  def last(self, name: str) -> Any:
    track = self._tracks.get(name)
    return None if track is None else track.value

  def accept(self, name: str, spec: Spec, raw: Any) -> Any:
    """Return the sanitized value, or None when rejected / held back as a possible spike."""
    track = self._tracks.setdefault(name, _Track())
    try:
      value = coerce(spec, raw)
    except ValueError as e:
      self._log_reject(track, "Rejected %s: %s", name, e)
      return None
    if spec.max_step is not None and track.value is not None and abs(value - track.value) > spec.max_step:
      track.pending.append(value)
      track.pending = track.pending[-self._confirm:]
      if len(track.pending) >= self._confirm and max(track.pending) - min(track.pending) <= spec.max_step:
        logger.info("Accepting confirmed jump of %s from %s to %s", name, track.value, value)
      else:
        self._log_reject(track, "Holding back possible spike of %s from %s to %s", name, track.value, value)
        return None
    if track.rejecting:
      logger.info("Accepting %s again: %s", name, value)
      track.rejecting = False
    track.pending.clear()
    track.value = value
    return value

  @staticmethod
  def _log_reject(track: _Track, msg: str, *args):
    logger.log(logging.DEBUG if track.rejecting else logging.INFO, msg, *args)
    track.rejecting = True

  def should_emit(self, name: str, value: Any) -> bool:
    track = self._tracks.setdefault(name, _Track())
    now = self._clock()
    if track.emitted_at is not None and track.emitted_value == value and now - track.emitted_at < self._heartbeat:
      return False
    track.emitted_value = value
    track.emitted_at = now
    return True


@dataclass
class _Window:
  hold: float                 # last raw reading; it holds until the next reading (sample-and-hold)
  published_at: float         # start of the current averaging window
  samples: list = field(default_factory=list)   # [(arrival time, value)] since published_at


class TimeWeightedLimiter:
  """Limit numeric updates to one per `interval` seconds, combining readings into a time-weighted average.

  - A reading arriving at least `interval` after the previous update (and with nothing buffered) is passed through
    unmodified, so slow sensors are never averaged or delayed.
  - Faster readings are buffered; once `interval` has passed since the previous update, `due()` returns the
    time-weighted average over that window. Every reading is assumed to hold until the next one, and the reading that
    was current at the start of the window counts for the time until the first new reading.
  """

  def __init__(self, interval: float):
    self.interval = interval
    self._windows: dict[str, _Window] = {}

  def add(self, name: str, value: float, now: float) -> float | None:
    """Register a reading. Returns the value to publish immediately, or None when it was buffered."""
    w = self._windows.get(name)
    if self.interval <= 0 or w is None or (not w.samples and now - w.published_at >= self.interval):
      self._windows[name] = _Window(hold=value, published_at=now)
      return value
    w.samples.append((now, value))
    return None

  def due(self, now: float) -> list[tuple[str, float]]:
    """Return [(name, time-weighted average)] for all windows that are complete at `now`."""
    out = []
    for name, w in self._windows.items():
      if not w.samples or now - w.published_at < self.interval:
        continue
      total = weighted = 0.0
      start, current = w.published_at, w.hold
      for t, v in w.samples:
        dt = max(t - start, 0.0)
        weighted += current * dt
        total += dt
        start, current = max(t, start), v
      dt = max(now - start, 0.0)
      weighted += current * dt
      total += dt
      out.append((name, round(weighted / total, 2) if total > 0 else current))
      w.hold, w.published_at, w.samples = current, now, []
    return out


# --------------------------------------------------------------------------------------------------
# systemd integration (sd_notify protocol, no dependency needed)
# --------------------------------------------------------------------------------------------------
class SystemdNotifier:
  """Minimal sd_notify(3): READY/STATUS/STOPPING/WATCHDOG messages over $NOTIFY_SOCKET. A no-op outside systemd."""

  def __init__(self, env: Mapping[str, str] | None = None):
    env = os.environ if env is None else env
    self._sock: socket.socket | None = None
    self._addr: str | None = None
    addr = env.get("NOTIFY_SOCKET", "")
    if addr.startswith(("/", "@")):
      self._addr = "\0" + addr[1:] if addr.startswith("@") else addr
      self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    self.watchdog_interval: float | None = None
    try:
      usec = int(env.get("WATCHDOG_USEC", "0"))
      pid = int(env.get("WATCHDOG_PID", "0"))
    except ValueError:
      usec, pid = 0, 0
    if self._sock and usec > 0 and pid in (0, os.getpid()):
      self.watchdog_interval = usec / 1e6 / 2   # sd_watchdog_enabled(3): ping at half the timeout

  @property
  def enabled(self) -> bool:
    return self._sock is not None

  def notify(self, *messages: str) -> bool:
    if self._sock is None:
      return False
    try:
      self._sock.sendto("\n".join(messages).encode(), self._addr)
    except OSError as e:
      logger.debug("sd_notify failed: %s", e)
      return False
    return True

  def close(self):
    if self._sock is not None:
      self._sock.close()
      self._sock = None

  async def watchdog_task(self):
    """Ping the systemd watchdog. Pings stop if the event loop hangs, after which systemd restarts the service."""
    if self.watchdog_interval is None:
      return
    logger.info("systemd watchdog enabled, pinging every %.1fs", self.watchdog_interval)
    while True:
      self.notify("WATCHDOG=1")
      await asyncio.sleep(self.watchdog_interval)


# --------------------------------------------------------------------------------------------------
# OpenTherm frames from the OTGW raw stream
# --------------------------------------------------------------------------------------------------
OT_FRAME_RE = re.compile(r"^([TBRA])([0-9A-F]{2})([0-9A-F]{2})([0-9A-F]{4})$")
OT_WRITE_DATA = 1
OT_ID_CONTROL_SETPOINT = 0x01


def parse_ot_frame(line: str) -> tuple[str, int, int, int] | None:
  """Parse e.g. 'T10012D00' into (source, msg_type, data_id, data_value)."""
  m = OT_FRAME_RE.match(line.strip().upper())
  if not m:
    return None
  source, b1, data_id, data_value = m.group(1), int(m.group(2), 16), int(m.group(3), 16), int(m.group(4), 16)
  return source, (b1 >> 4) & 0x07, data_id, data_value


def decode_f88(data_value: int) -> float:
  """Signed fixed point 8.8, as used by OpenTherm temperatures."""
  if data_value & 0x8000:
    data_value -= 0x10000
  return data_value / 256.0


def thermostat_control_setpoint(line: str) -> float | None:
  """Return the control setpoint the thermostat itself requests (T WRITE-DATA, ID 1), else None."""
  frame = parse_ot_frame(line)
  if frame is None:
    return None
  source, msg_type, data_id, data_value = frame
  if source != "T" or msg_type != OT_WRITE_DATA or data_id != OT_ID_CONTROL_SETPOINT:
    return None
  return decode_f88(data_value)


# --------------------------------------------------------------------------------------------------
# Control setpoint override
# --------------------------------------------------------------------------------------------------
MODE_OFF, MODE_ABSOLUTE, MODE_RELATIVE = "Off", "Absolute", "Relative"
OVERRIDE_MODES = (MODE_OFF, MODE_ABSOLUTE, MODE_RELATIVE)


def _parse_number(payload: str, what: str) -> float:
  try:
    value = float(payload.strip())
  except ValueError as e:
    raise ValueError(f"invalid {what} payload {payload!r}") from e
  if not math.isfinite(value):
    raise ValueError(f"invalid {what} payload {payload!r}")
  return value


class Override:
  """Control setpoint override state. Absolute: send a fixed value. Relative: thermostat's request + offset."""

  def __init__(self, vmin: float, vmax: float, offset_max: float):
    self.vmin = vmin
    self.vmax = vmax
    self.offset_max = offset_max
    self.mode = MODE_OFF
    self.value = self.clamp(35.0)
    self.offset = 0.0

  @property
  def enabled(self) -> bool:
    return self.mode != MODE_OFF

  def clamp(self, value: float) -> float:
    return round(min(max(value, self.vmin), self.vmax), 1)

  @staticmethod
  def parse_mode(payload: str) -> str:
    s = payload.strip().lower()
    for mode in OVERRIDE_MODES:
      if s == mode.lower():
        return mode
    raise ValueError(f"invalid mode payload {payload!r}, expected one of {', '.join(OVERRIDE_MODES)}")

  def parse_value(self, payload: str) -> float:
    return self.clamp(_parse_number(payload, "setpoint"))

  def parse_offset(self, payload: str) -> float:
    return round(min(max(_parse_number(payload, "offset"), -self.offset_max), self.offset_max), 1)

  def target(self, thermostat: float | None) -> float | None:
    """The control setpoint to send, or None to hand control back to the thermostat (CS=0).

    Relative mode needs the thermostat's own request; None when it is unknown. A request at or below
    OVERRIDE_MIN means "no heat demand" (this thermostat sends 10 for off) and is passed through unchanged:
    adding a positive offset to it would switch the heat pump on.
    """
    if self.mode == MODE_ABSOLUTE:
      return self.value
    if self.mode == MODE_RELATIVE and thermostat is not None:
      if thermostat <= self.vmin:
        return round(thermostat, 1)
      return self.clamp(thermostat + self.offset)
    return None


# --------------------------------------------------------------------------------------------------
# InfluxDB 1.x writer
# --------------------------------------------------------------------------------------------------
class InfluxWriter:
  def __init__(self, cfg: Config, dryrun: bool):
    self.enabled = bool(cfg.influx_host)
    self._dryrun = dryrun
    self._measurement = cfg.influx_measurement
    self._tags = {"host": HOSTNAME, "id": cfg.boiler_name}
    self._buffer: deque[dict] = deque(maxlen=INFLUX_MAX_BUFFER)
    self._wake = asyncio.Event()
    self._client = None
    if self.enabled and not dryrun:
      self._client = InfluxDBClient(host=cfg.influx_host, port=cfg.influx_port, username=cfg.influx_username or None,
                                    password=cfg.influx_password or None, database=cfg.influx_database, timeout=10)

  def add(self, name: str, value: Any, ts: float):
    if not self.enabled:
      return
    if len(self._buffer) == self._buffer.maxlen:
      logger.warning("InfluxDB buffer full, dropping oldest point")
    self._buffer.append({"measurement": self._measurement, "tags": self._tags, "fields": {name: value}, "time": int(ts * 1000)})
    if len(self._buffer) >= INFLUX_BATCH_SIZE:
      self._wake.set()

  async def flush(self):
    if not self._buffer:
      return
    batch = [self._buffer[i] for i in range(min(len(self._buffer), INFLUX_MAX_WRITE))]
    if self._dryrun:
      for point in batch:
        logger.debug("DRYRUN would write to InfluxDB: %s", point)
    else:
      try:
        await asyncio.to_thread(self._client.write_points, batch, time_precision="ms")
      except Exception as e:  # pylint: disable=broad-exception-caught  # requests/influx raise many types
        logger.warning("InfluxDB write of %d points failed (%d buffered): %s", len(batch), len(self._buffer), e)
        return
      logger.debug("Wrote %d points to InfluxDB", len(batch))
    for _ in batch:
      self._buffer.popleft()

  async def run(self):
    if not self.enabled:
      logger.info("InfluxDB disabled (INFLUXDB_HOST empty)")
      return
    while True:
      with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(self._wake.wait(), INFLUX_FLUSH_INTERVAL)
      self._wake.clear()
      await self.flush()


# --------------------------------------------------------------------------------------------------
# Home Assistant discovery
# --------------------------------------------------------------------------------------------------
def _slug(s: str) -> str:
  return re.sub(r"[^A-Za-z0-9_-]", "_", s)


def ha_node_id(cfg: Config) -> str:
  return f"{SCRIPTNAME}_{_slug(cfg.boiler_name)}"


def _ha_common(cfg: Config) -> dict:
  node = ha_node_id(cfg)
  return {
    "availability_topic": f"{cfg.out_topic}/status",
    "device": {
      "identifiers": [node],
      "name": f"OpenTherm Gateway ({cfg.boiler_name})",
      "manufacturer": "Itho Daalderop",
      "model": "HP-M 25i via OpenTherm Gateway",
      "sw_version": f"{SCRIPTNAME} {__version__}",
    },
  }


def sensor_discovery(cfg: Config, key: str) -> tuple[str, dict]:
  """(config topic, payload) for the Home Assistant sensor / binary_sensor of SPECS[key]."""
  spec = SPECS[key]
  node = ha_node_id(cfg)
  component = "binary_sensor" if spec.kind == "bool" else "sensor"
  conf = {"name": spec.name, "unique_id": f"{node}_{key}", "state_topic": f"{cfg.out_topic}/{key}", **_ha_common(cfg)}
  if spec.kind == "bool":
    conf["value_template"] = "{{ 'ON' if value_json.value else 'OFF' }}"
  else:
    conf["value_template"] = "{{ value_json.value }}"
  for attr, ha_key in (("device_class", "device_class"), ("unit", "unit_of_measurement"),
                       ("state_class", "state_class"), ("icon", "icon")):
    if getattr(spec, attr):
      conf[ha_key] = getattr(spec, attr)
  return f"{cfg.ha_discovery_prefix}/{component}/{node}/{key}/config", conf


def control_discovery(cfg: Config) -> list[tuple[str, dict]]:
  """(config topic, payload) for the override controls; these always exist."""
  node = ha_node_id(cfg)
  out = cfg.out_topic
  common = _ha_common(cfg)
  # Relative mode follows the thermostat's request from the raw stream, so it needs OTGW_HOST
  modes = list(OVERRIDE_MODES) if cfg.otgw_host else [MODE_OFF, MODE_ABSOLUTE]
  return [
    (f"{cfg.ha_discovery_prefix}/select/{node}/override_mode/config", {
      "name": "CS override mode", "unique_id": f"{node}_override_mode", "icon": "mdi:thermometer-alert",
      "command_topic": f"{out}/override_mode/set", "state_topic": f"{out}/override_mode",
      "options": modes, "retain": True, **common,
    }),
    (f"{cfg.ha_discovery_prefix}/number/{node}/override_setpoint/config", {
      "name": "CS override setpoint", "unique_id": f"{node}_override_setpoint", "icon": "mdi:thermometer-lines",
      "command_topic": f"{out}/override_setpoint/set", "state_topic": f"{out}/override_setpoint",
      "min": cfg.override_min, "max": cfg.override_max, "step": 0.5, "mode": "box",
      "device_class": "temperature", "unit_of_measurement": "°C", "retain": True, **common,
    }),
    (f"{cfg.ha_discovery_prefix}/number/{node}/override_offset/config", {
      "name": "CS override offset", "unique_id": f"{node}_override_offset", "icon": "mdi:plus-minus-variant",
      "command_topic": f"{out}/override_offset/set", "state_topic": f"{out}/override_offset",
      "min": -cfg.override_offset_max, "max": cfg.override_offset_max, "step": 0.5, "mode": "box",
      "unit_of_measurement": "°C", "retain": True, **common,
    }),
  ]


def legacy_retained_topics(cfg: Config) -> list[str]:
  """Retained topics from older versions that are cleared on connect: the CS override switch (replaced by the
  mode select in 2.3.0) and its retained command."""
  return [f"{cfg.ha_discovery_prefix}/switch/{ha_node_id(cfg)}/override/config", f"{cfg.out_topic}/override/set"]


def build_discovery(cfg: Config, keys: Iterable[str] | None = None) -> list[tuple[str, dict]]:
  """[(config topic, payload)] for the controls plus the sensors in keys (default: every known value)."""
  return control_discovery(cfg) + [sensor_discovery(cfg, key) for key in (SPECS if keys is None else keys)]


def purge_filters(cfg: Config) -> list[str]:
  """Subscriptions that cover every retained message this bridge (or Home Assistant on its behalf) creates."""
  return [f"{cfg.out_topic}/#", f"{cfg.ha_discovery_prefix}/+/{ha_node_id(cfg)}/+/config"]


# --------------------------------------------------------------------------------------------------
# Bridge
# --------------------------------------------------------------------------------------------------
class Backoff:
  def __init__(self, start: float = 1.0, maximum: float = 60.0):
    self._start = start
    self._max = maximum
    self._next = start

  def reset(self):
    self._next = self._start

  def next(self) -> float:
    delay = self._next
    self._next = min(self._next * 2, self._max)
    return delay


def _compact(obj: Any) -> str:
  return json.dumps(obj, separators=(",", ":"))


class Bridge:  # pylint: disable=too-many-instance-attributes,too-many-public-methods
  def __init__(self, cfg: Config, influx: InfluxWriter, dryrun: bool = False, clock: Callable[[], float] = time.time,
               notifier: SystemdNotifier | None = None):
    self.cfg = cfg
    self.influx = influx
    self.dryrun = dryrun
    self.clock = clock
    self.notifier = notifier
    self.client: aiomqtt.Client | None = None
    self.sanitizer = Sanitizer(cfg.heartbeat, clock)
    self.limiter = TimeWeightedLimiter(cfg.ha_update_interval)
    self._status = {"MQTT": "connecting", "OTGW stream": "connecting" if cfg.otgw_host else "disabled"}
    self.override = Override(cfg.override_min, cfg.override_max, cfg.override_offset_max)
    self._override_kick = asyncio.Event()
    self._cs_sent: float | None = None   # control setpoint last sent (0 = released); None = unknown, send again
    self._cs_sent_at = 0.0
    self._thermostat_at = 0.0             # when the thermostat's own setpoint was last seen in the raw stream
    self._thermostat_lost = False
    self._connected = asyncio.Event()
    self._unknown: set[str] = set()
    self._event_prefix = cfg.otmonitor_event_topic + "/"
    self._cmd_override_mode = f"{cfg.out_topic}/override_mode/set"
    self._cmd_override_sp = f"{cfg.out_topic}/override_setpoint/set"
    self._cmd_override_offset = f"{cfg.out_topic}/override_offset/set"
    self._ha_status = f"{cfg.ha_discovery_prefix}/status"
    # Values are published without retain; remember the latest so they can be re-sent when HA restarts
    self._last_published: dict[str, tuple[Any, float]] = {}
    self._republish_task: asyncio.Task | None = None
    # HA entities are created on the first valid value, so values this boiler never reports get no entity
    self._discovered: set[str] = set()
    self._new_entities: set[str] = set()
    self._new_entity_task: asyncio.Task | None = None

  # ---- publishing -------------------------------------------------------------------------------
  async def publish(self, topic: str, payload: Any, retain: bool = False, qos: int = 0) -> bool:
    if isinstance(payload, (dict, list)):
      payload = _compact(payload)
    if self.dryrun:
      logger.debug("DRYRUN would publish %s%s: %s", topic, " (retained)" if retain else "", payload)
      return True
    client = self.client
    if client is None:
      logger.debug("MQTT not connected, dropping publish to %s", topic)
      return False
    try:
      await client.publish(topic, payload, qos=qos, retain=retain)
    except aiomqtt.MqttError as e:
      logger.warning("Publish to %s failed: %s", topic, e)
      return False
    return True

  async def publish_value(self, name: str, value: Any, ts: float):
    """Publish a sanitized value to OUT_TOPIC/<name> (what Home Assistant reads), unless it is an unchanged repeat."""
    if self.sanitizer.should_emit(f"mqtt:{name}", value):
      self._last_published[name] = (value, ts)
      if self.cfg.ha_enabled and name not in self._discovered:
        await self._create_entity(name)
      await self._publish_state(name, value, ts)

  async def _create_entity(self, name: str):
    topic, conf = sensor_discovery(self.cfg, name)
    if not await self.publish(topic, conf, retain=True, qos=1):
      return   # not connected; publish_discovery() on (re)connect covers it
    self._discovered.add(name)
    logger.info("Created Home Assistant entity for %s", name)
    # HA subscribes to the state topic only after processing the config, so the state sent right now is
    # probably missed (states are not retained). Re-send it shortly; batch entities created in a burst.
    self._new_entities.add(name)
    if self._new_entity_task is None or self._new_entity_task.done():
      self._new_entity_task = asyncio.create_task(self._publish_new_entity_states(), name="ha-new-entities")

  async def _publish_new_entity_states(self):
    await asyncio.sleep(HA_NEW_ENTITY_DELAY)
    names, self._new_entities = self._new_entities, set()
    for name in sorted(names):
      if name in self._last_published:
        await self._publish_state(name, *self._last_published[name])

  async def _publish_state(self, name: str, value: Any, ts: float):
    await self.publish(f"{self.cfg.out_topic}/{name}", {"value": value, "timestamp": int(ts * 1000)})

  async def republish_states(self):
    """Re-send the latest value of everything (states are not retained on the broker)."""
    for name, (value, ts) in list(self._last_published.items()):
      await self._publish_state(name, value, ts)
    await self.publish_override_state()
    logger.info("Re-published %d values for Home Assistant", len(self._last_published))

  async def _republish_after_birth(self):
    await self.publish_discovery()
    await asyncio.sleep(HA_BIRTH_DELAY)
    await self.republish_states()

  async def handle_ha_status(self, payload: bytes):
    if payload.strip().lower() != b"online":
      return
    logger.info("Home Assistant came online, re-publishing discovery and states")
    if self._republish_task is not None:
      self._republish_task.cancel()   # restart the delay when several birth messages arrive in a row
    self._republish_task = asyncio.create_task(self._republish_after_birth(), name="ha-republish")

  async def flush_averages(self):
    now = self.clock()
    for name, value in self.limiter.due(now):
      await self.publish_value(name, value, now)

  def set_status(self, key: str, state: str):
    if self._status.get(key) == state:
      return
    self._status[key] = state
    if self.notifier:
      self.notifier.notify("STATUS=" + ", ".join(f"{k} {v}" for k, v in self._status.items()))

  async def publish_discovery(self):
    if not self.cfg.ha_enabled:
      return
    keys = sorted(self._last_published)   # only values that have actually been seen
    for topic, conf in build_discovery(self.cfg, keys):
      await self.publish(topic, conf, retain=True, qos=1)
    self._discovered.update(keys)
    logger.info("Published Home Assistant discovery (controls + %d sensors)", len(keys))

  async def publish_override_state(self):
    out = self.cfg.out_topic
    await self.publish(f"{out}/override_mode", self.override.mode)
    await self.publish(f"{out}/override_setpoint", f"{self.override.value:.1f}")
    await self.publish(f"{out}/override_offset", f"{self.override.offset:.1f}")

  async def clear_legacy_topics(self):
    """Remove retained topics of older versions (an empty retained message deletes them; a no-op when absent)."""
    for topic in legacy_retained_topics(self.cfg):
      await self.publish(topic, b"", retain=True, qos=1)

  async def send_control_setpoint(self, value: float) -> bool:
    topic = f"{self.cfg.otmonitor_action_topic}/ctrlsetpt"
    # A release is sent as the plain integer 0 (OTGW "CS=0"), as documented for the gateway
    ok = await self.publish(topic, {"name": "temp", "type": "float", "value": round(value, 2) if value else 0})
    if ok:
      self._cs_sent = value
      self._cs_sent_at = self.clock()
      logger.info("%s control setpoint %s to otmonitor%s", "DRYRUN would send" if self.dryrun else "Sent",
                  f"{value:.1f}", " (release to thermostat)" if value == 0 else "")
    return ok

  # ---- value pipeline ---------------------------------------------------------------------------
  async def handle_value(self, name: str, raw: Any, ts: float):
    spec = SPECS.get(name)
    if spec is None:
      if name not in self._unknown:
        self._unknown.add(name)
        logger.info("Ignoring unknown value %r (raw %r)", name, raw)
      return
    value = self.sanitizer.accept(name, spec, raw)
    if value is None:
      return
    # InfluxDB: every changed reading (plus a heartbeat), at full resolution
    if spec.influx and self.sanitizer.should_emit(name, value):
      self.influx.add(name, value, ts)
    # MQTT / Home Assistant: numeric values rate limited with a time-weighted average, the rest directly
    if spec.kind == "float":
      direct = self.limiter.add(name, value, self.clock())
      if direct is not None:
        await self.publish_value(name, direct, ts)
    else:
      await self.publish_value(name, value, ts)
    if name in ("controlsetpoint", "tchsetpoint"):
      await self._update_override_active(ts)
    if name == "tchsetpoint":
      self._thermostat_at = self.clock()
      self._follow_thermostat()
    elif name == "controlsetpoint" and self._cs_sent and abs(value - self._cs_sent) > 0.5 \
        and self.clock() - self._cs_sent_at > CS_RESEND_MIN_INTERVAL:
      logger.info("Gateway reports control setpoint %s while the override sent %s, re-sending", value, self._cs_sent)
      self._cs_sent = None
      self._override_kick.set()

  def thermostat_setpoint(self) -> float | None:
    """The thermostat's own control setpoint from the raw stream, or None when unknown or stale."""
    value = self.sanitizer.last("tchsetpoint")
    if value is None or self.clock() - self._thermostat_at > THERMOSTAT_MAX_AGE:
      return None
    return value

  def override_target(self) -> float:
    """The control setpoint that should be in effect now; 0 means hand control back to the thermostat."""
    target = self.override.target(self.thermostat_setpoint())
    return 0.0 if target is None else target

  def _follow_thermostat(self):
    """Relative mode: re-send when the thermostat's request changes the target (the thermostat repeats its request
    about every second, so a change that comes too soon after the last send is picked up by a later repeat)."""
    if self.override.mode == MODE_RELATIVE and self.override_target() != self._cs_sent \
        and self.clock() - self._cs_sent_at >= CS_FOLLOW_MIN_INTERVAL:
      self._override_kick.set()

  async def apply_override(self, refresh: bool = False):
    """Send the target control setpoint if it changed (or, with refresh, re-send an active override)."""
    target = self.override_target()
    lost = self.override.mode == MODE_RELATIVE and target == 0
    if lost and not self._thermostat_lost:
      logger.warning("Relative override: thermostat setpoint unknown or older than %.0fs (raw stream down?), "
                     "handing control back to the thermostat", THERMOSTAT_MAX_AGE)
    elif self._thermostat_lost and not lost:
      logger.info("Relative override: thermostat setpoint available again")
    self._thermostat_lost = lost
    if target == self._cs_sent and not (refresh and target):
      return
    if target and self.override.mode == MODE_RELATIVE:
      logger.info("Relative override: thermostat requests %.1f, offset %+.1f", self.thermostat_setpoint(),
                  self.override.offset)
    await self.send_control_setpoint(target)

  async def _update_override_active(self, ts: float):
    cs = self.sanitizer.last("controlsetpoint")
    tcs = self.sanitizer.last("tchsetpoint")
    if cs is not None and tcs is not None:
      await self.handle_value("override_active", abs(cs - tcs) > 0.25, ts)

  async def handle_otmonitor(self, name: str, payload: bytes):
    if "/" in name or (name in SPECS and SPECS[name].source != "otmonitor"):
      return
    now = self.clock()
    try:
      raw, ts = parse_otmonitor_payload(payload, now)
    except ValueError as e:
      logger.info("Unparseable otmonitor payload on %s: %s", name, e)
      return
    await self.handle_value(name, raw, ts)

  async def handle_outdoor(self, payload: bytes):
    try:
      text = payload.decode("utf-8")
      raw: Any = text
      if self.cfg.outdoor_json_key:
        raw = json.loads(text)
        for part in self.cfg.outdoor_json_key.split("."):
          raw = raw[part]
    except (ValueError, KeyError, TypeError, IndexError) as e:
      logger.info("Could not extract outdoor temperature from %r: %s", payload[:100], e)
      return
    value = self.sanitizer.accept(OUTDOOR_TRACK, OUTDOOR_SPEC, raw)
    if value is None:
      return
    await self.publish(f"{self.cfg.otmonitor_action_topic}/outside", {"name": "temp", "type": "float", "value": value})
    logger.debug("Forwarded outdoor temperature %s to otmonitor", value)

  async def handle_stream_line(self, line: str):
    value = thermostat_control_setpoint(line)
    if value is not None:
      logger.debug("Thermostat requests control setpoint %.2f", value)
      await self.handle_value("tchsetpoint", value, self.clock())

  async def handle_override_mode_command(self, payload: bytes):
    try:
      mode = Override.parse_mode(payload.decode("utf-8"))
    except ValueError as e:
      logger.warning("Override mode rejected: %s", e)
      return
    if mode == MODE_RELATIVE and not self.cfg.otgw_host:
      logger.warning("Override mode %s rejected: it needs the raw OTGW stream (OTGW_HOST)", mode)
    elif mode != self.override.mode:
      logger.info("Control setpoint override mode %s", mode)
      self.override.mode = mode
      self._override_kick.set()
    await self.publish_override_state()

  async def handle_override_setpoint_command(self, payload: bytes):
    try:
      value = self.override.parse_value(payload.decode("utf-8"))
    except ValueError as e:
      logger.warning("Override setpoint rejected: %s", e)
      return
    if value != self.override.value:
      logger.info("Control setpoint override value set to %.1f", value)
      self.override.value = value
      self._override_kick.set()
    await self.publish_override_state()

  async def handle_override_offset_command(self, payload: bytes):
    try:
      offset = self.override.parse_offset(payload.decode("utf-8"))
    except ValueError as e:
      logger.warning("Override offset rejected: %s", e)
      return
    if offset != self.override.offset:
      logger.info("Control setpoint override offset set to %+.1f", offset)
      self.override.offset = offset
      self._override_kick.set()
    await self.publish_override_state()

  async def dispatch(self, message: aiomqtt.Message):
    topic = message.topic.value
    payload = message.payload if isinstance(message.payload, (bytes, bytearray)) else str(message.payload).encode()
    try:
      if topic.startswith(self._event_prefix):
        await self.handle_otmonitor(topic[len(self._event_prefix):], payload)
      elif self.cfg.outdoor_topic and topic == self.cfg.outdoor_topic:
        await self.handle_outdoor(payload)
      elif topic == self._cmd_override_mode:
        await self.handle_override_mode_command(payload)
      elif topic == self._cmd_override_sp:
        await self.handle_override_setpoint_command(payload)
      elif topic == self._cmd_override_offset:
        await self.handle_override_offset_command(payload)
      elif topic == self._ha_status:
        await self.handle_ha_status(payload)
    except Exception:  # pylint: disable=broad-exception-caught  # one bad message must not stop the bridge
      logger.exception("Error handling message on %s: %r", topic, payload[:200])

  # ---- tasks ------------------------------------------------------------------------------------
  async def _on_connect(self):
    await self.publish(f"{self.cfg.out_topic}/status", "online", retain=True, qos=1)
    subscriptions = [f"{self.cfg.otmonitor_event_topic}/#", self._cmd_override_mode, self._cmd_override_sp,
                     self._cmd_override_offset]
    if self.cfg.outdoor_topic:
      subscriptions.append(self.cfg.outdoor_topic)
    if self.cfg.ha_enabled:
      subscriptions.append(self._ha_status)
    for sub in subscriptions:
      await self.client.subscribe(sub, qos=1)
    logger.info("Subscribed to %s", ", ".join(subscriptions))
    await self.clear_legacy_topics()
    await self.publish_discovery()
    await self.republish_states()   # anything published while disconnected was lost (no retain)
    self._connected.set()
    self._cs_sent = None        # re-assert the override (or the release) after a reconnect
    self._override_kick.set()

  async def _on_shutdown(self, client: aiomqtt.Client):
    try:
      async with asyncio.timeout(SHUTDOWN_TIMEOUT):
        if self.cfg.override_release_on_exit and self.override.enabled:
          logger.info("Releasing control setpoint override on exit")
          await self.send_control_setpoint(0)
        if not self.dryrun:
          await client.publish(f"{self.cfg.out_topic}/status", "offline", qos=1, retain=True)
    except (aiomqtt.MqttError, TimeoutError) as e:
      logger.warning("Clean MQTT shutdown failed: %s", e)

  async def mqtt_task(self):
    backoff = Backoff()
    will = None if self.dryrun else aiomqtt.Will(f"{self.cfg.out_topic}/status", "offline", qos=1, retain=True)
    while True:
      try:
        async with aiomqtt.Client(hostname=self.cfg.mqtt_host, port=self.cfg.mqtt_port,
                                  username=self.cfg.mqtt_username or None, password=self.cfg.mqtt_password or None,
                                  identifier=self.cfg.mqtt_client_id, will=will, timeout=CONNECT_TIMEOUT,
                                  logger=logging.getLogger(f"{SCRIPTNAME}.mqtt")) as client:
          self.client = client
          logger.info("Connected to MQTT broker %s:%s", self.cfg.mqtt_host, self.cfg.mqtt_port)
          self.set_status("MQTT", "connected")
          backoff.reset()
          try:
            await self._on_connect()
            async for message in client.messages:
              await self.dispatch(message)
          except asyncio.CancelledError:
            await self._on_shutdown(client)
            raise
          finally:
            self.client = None
            self._connected.clear()
            if self._republish_task is not None:
              self._republish_task.cancel()   # the reconnect re-publishes everything anyway
      except aiomqtt.MqttError as e:
        delay = backoff.next()
        logger.warning("MQTT connection error: %s; reconnecting in %.0fs", e, delay)
        self.set_status("MQTT", "disconnected")
        await asyncio.sleep(delay)

  async def otgw_stream_task(self):
    if not self.cfg.otgw_host:
      logger.info("OTGW raw stream disabled (OTGW_HOST empty); tchsetpoint will not be available")
      return
    backoff = Backoff()
    while True:
      writer = None
      try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(self.cfg.otgw_host, self.cfg.otgw_port), CONNECT_TIMEOUT)
        logger.info("Connected to OTGW stream %s:%s", self.cfg.otgw_host, self.cfg.otgw_port)
        self.set_status("OTGW stream", "connected")
        backoff.reset()
        while True:
          line = await asyncio.wait_for(reader.readline(), STREAM_READ_TIMEOUT)
          if not line:
            raise ConnectionError("closed by peer")
          await self.handle_stream_line(line.decode("ascii", errors="replace"))
      except (OSError, ValueError) as e:  # OSError covers ConnectionError and TimeoutError; ValueError: line too long
        delay = backoff.next()
        logger.warning("OTGW stream error: %s; reconnecting in %.0fs", e or type(e).__name__, delay)
        self.set_status("OTGW stream", "disconnected")
        await asyncio.sleep(delay)
      finally:
        if writer is not None:
          writer.close()
          with contextlib.suppress(Exception):
            await asyncio.wait_for(writer.wait_closed(), 2)

  async def rate_limit_task(self):
    """Publish time-weighted averages of buffered numeric readings once their window is complete."""
    if self.cfg.ha_update_interval <= 0:
      return
    tick = min(1.0, self.cfg.ha_update_interval / 4)
    while True:
      await asyncio.sleep(tick)
      await self.flush_averages()

  async def override_task(self):
    if self.dryrun:
      logger.info("DRYRUN: override commands are tracked but not sent")
    await self._connected.wait()
    await asyncio.sleep(STARTUP_GRACE)   # let retained HA commands arrive first
    self._override_kick.clear()
    if not self.override.enabled:
      logger.info("Override off at startup, clearing any stale override")
    self._cs_sent = None   # always send once at startup; when off, CS=0 clears an override left by a crash
    await self.apply_override()
    while True:
      refresh = False
      try:
        await asyncio.wait_for(self._override_kick.wait(), self.cfg.override_refresh)
      except TimeoutError:
        refresh = True     # periodic re-send of an active override
      self._override_kick.clear()
      await self.apply_override(refresh)


# --------------------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------------------
async def run(cfg: Config, dryrun: bool) -> int:
  notifier = SystemdNotifier()
  influx = InfluxWriter(cfg, dryrun)
  bridge = Bridge(cfg, influx, dryrun, notifier=notifier)
  main_task = asyncio.current_task()
  loop = asyncio.get_running_loop()
  for sig in (signal.SIGTERM, signal.SIGINT):
    loop.add_signal_handler(sig, main_task.cancel)

  failed = False
  try:
    try:
      async with asyncio.TaskGroup() as tg:
        tg.create_task(bridge.mqtt_task(), name="mqtt")
        tg.create_task(bridge.otgw_stream_task(), name="otgw-stream")
        tg.create_task(bridge.override_task(), name="override")
        tg.create_task(bridge.rate_limit_task(), name="rate-limit")
        tg.create_task(influx.run(), name="influx")
        tg.create_task(notifier.watchdog_task(), name="watchdog")
        # Ready as soon as everything runs; connections are retried in the background and reported via STATUS
        notifier.notify("READY=1", "STATUS=Started, connecting")
    except* Exception as eg:  # pylint: disable=broad-exception-caught
      for exc in eg.exceptions:
        logger.critical("Task crashed, exiting", exc_info=exc)
      failed = True
  except asyncio.CancelledError:
    logger.info("Shutting down")
  finally:
    notifier.notify("STOPPING=1", "STATUS=Shutting down")
    with contextlib.suppress(Exception):
      await asyncio.wait_for(influx.flush(), SHUTDOWN_TIMEOUT)
    notifier.close()
  return 1 if failed else 0


async def collect_retained(client: aiomqtt.Client, filters: list[str]) -> dict[str, bytes]:
  """Subscribe to filters and return {topic: payload} of the retained messages the broker sends."""
  for sub in filters:
    await client.subscribe(sub, qos=1)
  found: dict[str, bytes] = {}
  messages = client.messages
  deadline = time.monotonic() + PURGE_MAX
  while time.monotonic() < deadline:
    try:
      message = await asyncio.wait_for(anext(messages), PURGE_IDLE)
    except TimeoutError:
      break
    if message.retain and message.payload:   # live (non-retained) traffic from a running instance is ignored
      payload = message.payload
      found[message.topic.value] = payload if isinstance(payload, (bytes, bytearray)) else str(payload).encode()
  return found


async def purge_retained(cfg: Config, dryrun: bool) -> int:
  """Remove every retained message of this bridge: HA discovery (removes the device), status, override commands."""
  filters = purge_filters(cfg)
  try:
    # A separate client id, so a still-running instance is not kicked off the broker
    async with aiomqtt.Client(hostname=cfg.mqtt_host, port=cfg.mqtt_port, username=cfg.mqtt_username or None,
                              password=cfg.mqtt_password or None, identifier=f"{cfg.mqtt_client_id}-purge",
                              timeout=CONNECT_TIMEOUT, logger=logging.getLogger(f"{SCRIPTNAME}.mqtt")) as client:
      logger.info("Collecting retained messages on %s", ", ".join(filters))
      found = await collect_retained(client, filters)
      if not found:
        logger.info("No retained messages found, nothing to do")
        return 0
      if found.get(f"{cfg.out_topic}/status", b"").strip() == b"online":
        logger.warning("%s/status is 'online': if otgw2mqtt is still running it will re-create its entities. "
                       "Stop it first (systemctl stop otgw2mqtt)", cfg.out_topic)
      for topic in sorted(found):
        if dryrun:
          logger.info("DRYRUN would remove %s: %s", topic, found[topic][:80].decode("utf-8", errors="replace"))
        else:
          await client.publish(topic, b"", qos=1, retain=True)
          logger.info("Removed %s", topic)
  except aiomqtt.MqttError as e:
    logger.error("MQTT error: %s", e)
    return 1
  logger.info("%s %d retained messages", "DRYRUN: would have removed" if dryrun else "Removed", len(found))
  return 0


def setup_logging(level: str, logfile: str | None):
  under_journald = bool(os.environ.get("JOURNAL_STREAM"))
  fmt = "%(levelname)s %(name)s: %(message)s" if under_journald else "%(asctime)s %(levelname)s %(name)s: %(message)s"
  handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
  if logfile:
    handlers.append(logging.FileHandler(logfile))
  logging.basicConfig(level=level, format=fmt, handlers=handlers, force=True)
  if level != "DEBUG":
    logging.getLogger(f"{SCRIPTNAME}.mqtt").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def main(argv: list[str] | None = None) -> int:
  parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
  parser.add_argument("-e", "--env-file", help="Load settings from this env file (optional; real environment variables "
                                                "take precedence). Under systemd use EnvironmentFile= instead.")
  parser.add_argument("-l", "--logfile", help="Also log to this file")
  parser.add_argument("-L", "--loglevel", choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
                      help="Logging level (default: LOG_LEVEL from env, else INFO)")
  parser.add_argument("-d", "--dryrun", action="store_true", help="Do not publish to MQTT or write to InfluxDB, only log")
  parser.add_argument("--purge", action="store_true",
                      help="Remove all retained MQTT messages of this bridge (Home Assistant discovery, which removes the "
                           "device and its entities; status; override commands) and exit. Stop the service first. "
                           "With --dryrun, only list them.")
  args = parser.parse_args(argv)

  if args.env_file:
    if not os.path.isfile(args.env_file):
      print(f"Env file {args.env_file} not found", file=sys.stderr)
      return 2
    try:
      from dotenv import load_dotenv  # pylint: disable=import-outside-toplevel  # only needed for --env-file
    except ImportError:
      print("--env-file requires the python-dotenv package", file=sys.stderr)
      return 2
    load_dotenv(args.env_file, override=False)   # real environment variables take precedence

  level = (args.loglevel or os.environ.get("LOG_LEVEL", "INFO")).upper()
  if level not in logging.getLevelNamesMapping():
    print(f"Unknown log level {level}", file=sys.stderr)
    return 2
  setup_logging(level, args.logfile)

  if args.env_file:
    logger.info("Loaded settings from %s", os.path.abspath(args.env_file))
  else:
    logger.info("No --env-file given, using the process environment")

  try:
    cfg = Config.from_env()
  except ConfigError as e:
    logger.error("Configuration error: %s", e)
    return 2

  if args.purge:
    return asyncio.run(purge_retained(cfg, args.dryrun))
  logger.info("*** %s %s starting%s ***", SCRIPTNAME, __version__, " (DRY RUN)" if args.dryrun else "")
  return asyncio.run(run(cfg, args.dryrun))


if __name__ == "__main__":
  sys.exit(main())
