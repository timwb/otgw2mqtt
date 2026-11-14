import asyncio
import json
import os
import socket
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import otgw2mqtt as og  # noqa: E402  pylint: disable=wrong-import-position,import-error

BASE_ENV = {"MQTT_HOST": "broker", "BOILER_NAME": "HP-M_25_i"}


def make_cfg(**overrides) -> og.Config:
  return og.Config.from_env({**BASE_ENV, **overrides})


class FakeClock:  # pylint: disable=too-few-public-methods
  def __init__(self, t=1_700_000_000.0):
    self.t = t

  def __call__(self):
    return self.t


class TestOtFrames(unittest.TestCase):
  def test_thermostat_write_control_setpoint(self):
    self.assertEqual(og.thermostat_control_setpoint("T10012D00"), 45.0)
    self.assertEqual(og.thermostat_control_setpoint("T10012D80\r\n"), 45.5)

  def test_parity_bit_ignored(self):
    self.assertEqual(og.thermostat_control_setpoint("T90011E00"), 30.0)

  def test_rejects_other_frames(self):
    for line in ("R10012D00", "B40012D00", "A10012D00", "T00010000", "T10102D00",
                 "T10012D0", "garbage", "", "Error 01", "T1001ZZ00"):
      self.assertIsNone(og.thermostat_control_setpoint(line), line)

  def test_captured_relay_stream(self):
    # Captured from otmonitor relay port 7686
    captured = ("T00110000\r\nB40116400\r\nT80380000\r\nBF0380007\r\nA40385000\r\nT80190000\r\nBC0191A18\r\n"
                "T90012300\r\nB50012300\r\nT80000100\r\nB40000103\r\n")
    values = [og.thermostat_control_setpoint(l) for l in captured.split("\r\n")]
    self.assertEqual([v for v in values if v is not None], [35.0])

  def test_negative_f88(self):
    self.assertEqual(og.decode_f88(0xFF00), -1.0)
    self.assertEqual(og.decode_f88(0x0080), 0.5)


class TestPayload(unittest.TestCase):
  NOW = 1_700_000_000.0

  def test_json_with_timestamp(self):
    raw, ts = og.parse_otmonitor_payload(b'{"name":"temp","type":"float","value":21.5,"timestamp":1700000010000}', self.NOW)
    self.assertEqual((raw, ts), (21.5, 1_700_000_010.0))

  def test_implausible_timestamp_replaced(self):
    _, ts = og.parse_otmonitor_payload(b'{"value":1,"timestamp":99999999999999}', self.NOW)
    self.assertEqual(ts, self.NOW)
    _, ts = og.parse_otmonitor_payload(b'{"value":1,"timestamp":1000}', self.NOW)
    self.assertEqual(ts, self.NOW)
    _, ts = og.parse_otmonitor_payload(b'{"value":1,"timestamp":"abc"}', self.NOW)
    self.assertEqual(ts, self.NOW)

  def test_old_retained_timestamp_kept(self):
    _, ts = og.parse_otmonitor_payload(b'{"value":1,"timestamp":1699990000000}', self.NOW)
    self.assertEqual(ts, 1_699_990_000.0)

  def test_plain_payload(self):
    self.assertEqual(og.parse_otmonitor_payload(b"on", self.NOW), ("on", self.NOW))
    self.assertEqual(og.parse_otmonitor_payload(b"21.5", self.NOW), (21.5, self.NOW))

  def test_invalid(self):
    for payload in (b"", b'{"timestamp":1}', b"[1,2]", b"null", b"\xff\xfe"):
      with self.assertRaises(ValueError, msg=payload):
        og.parse_otmonitor_payload(payload, self.NOW)


class TestCoerce(unittest.TestCase):
  def test_bool(self):
    spec = og.SPECS["flame"]
    for raw, expected in ((True, True), ("on", True), ("0", False), (1, True), ("FALSE", False)):
      self.assertEqual(og.coerce(spec, raw), expected)
    for raw in ("maybe", 2, None):
      with self.assertRaises(ValueError):
        og.coerce(spec, raw)

  def test_float_range_and_round(self):
    spec = og.SPECS["controlsetpoint"]
    self.assertEqual(og.coerce(spec, "45.123"), 45.12)
    for raw in (-1, 71, "nan", "inf", True, "x"):
      with self.assertRaises(ValueError):
        og.coerce(spec, raw)

  def test_clamp(self):
    spec = og.SPECS["modulation"]
    self.assertEqual(og.coerce(spec, 120), 100.0)
    self.assertEqual(og.coerce(spec, -3), 0.0)

  def test_int_counter(self):
    spec = og.SPECS["burnerstarts"]
    self.assertEqual(og.coerce(spec, 1234.0), 1234)
    for raw in (-1, 1.5):
      with self.assertRaises(ValueError):
        og.coerce(spec, raw)

  def test_str(self):
    spec = og.SPECS["powermode"]
    self.assertEqual(og.coerce(spec, " normal "), "normal")
    for raw in ("", "x" * 65, "a\nb", None, ["a"]):
      with self.assertRaises(ValueError):
        og.coerce(spec, raw)


class TestSanitizer(unittest.TestCase):
  def setUp(self):
    self.clock = FakeClock()
    self.san = og.Sanitizer(heartbeat=60, clock=self.clock)
    self.spec = og.SPECS["boilerwatertemperature"]   # max_step 5

  def test_small_steps_accepted(self):
    self.assertEqual(self.san.accept("t", self.spec, 30), 30)
    self.assertEqual(self.san.accept("t", self.spec, 34), 34)

  def test_single_spike_rejected(self):
    self.san.accept("t", self.spec, 30)
    self.assertIsNone(self.san.accept("t", self.spec, 80))
    self.assertEqual(self.san.accept("t", self.spec, 31), 31)
    self.assertEqual(self.san.last("t"), 31)

  def test_confirmed_jump_accepted(self):
    self.san.accept("t", self.spec, 30)
    self.assertIsNone(self.san.accept("t", self.spec, 45))
    self.assertIsNone(self.san.accept("t", self.spec, 45.5))
    self.assertEqual(self.san.accept("t", self.spec, 46), 46)
    self.assertEqual(self.san.accept("t", self.spec, 47), 47)

  def test_inconsistent_spikes_not_accepted(self):
    self.san.accept("t", self.spec, 30)
    for v in (60, 80, 0.5, 70):
      self.assertIsNone(self.san.accept("t", self.spec, v))

  def test_reject_streak_logged_once(self):
    spec = og.SPECS["setpoint2"]
    with self.assertLogs("otgw2mqtt", level="DEBUG") as cm:
      for _ in range(3):
        self.assertIsNone(self.san.accept("s2", spec, -12.5))
      self.assertEqual(self.san.accept("s2", spec, 20), 20)
    levels = [r.levelname for r in cm.records]
    self.assertEqual(levels, ["INFO", "DEBUG", "DEBUG", "INFO"])

  def test_dedupe_and_heartbeat(self):
    self.assertTrue(self.san.should_emit("t", 30))
    self.assertFalse(self.san.should_emit("t", 30))
    self.assertTrue(self.san.should_emit("t", 31))
    self.clock.t += 61
    self.assertTrue(self.san.should_emit("t", 31))

  def test_heartbeat_zero_always_emits(self):
    san = og.Sanitizer(heartbeat=0, clock=self.clock)
    self.assertTrue(san.should_emit("t", 1))
    self.assertTrue(san.should_emit("t", 1))


class TestOverride(unittest.TestCase):
  def test_parse(self):
    ov = og.Override(10, 55, 10)
    self.assertEqual(ov.parse_mode("Absolute"), og.MODE_ABSOLUTE)
    self.assertEqual(ov.parse_mode(" relative "), og.MODE_RELATIVE)
    self.assertEqual(ov.parse_mode("OFF"), og.MODE_OFF)
    for bad in ("ON", "toggle", ""):
      with self.assertRaises(ValueError):
        ov.parse_mode(bad)
    self.assertEqual(ov.parse_value("70"), 55)
    self.assertEqual(ov.parse_value("2"), 10)
    self.assertEqual(ov.parse_value("35.25"), 35.2)
    for bad in ("abc", "nan", ""):
      with self.assertRaises(ValueError):
        ov.parse_value(bad)
    self.assertEqual(ov.parse_offset("-3"), -3.0)
    self.assertEqual(ov.parse_offset("+2.5"), 2.5)
    self.assertEqual(ov.parse_offset("25"), 10.0)
    self.assertEqual(ov.parse_offset("-25"), -10.0)
    with self.assertRaises(ValueError):
      ov.parse_offset("inf")

  def test_target(self):
    ov = og.Override(10, 35, 10)
    self.assertIsNone(ov.target(30))                  # off
    ov.mode = og.MODE_ABSOLUTE
    ov.value = 28.0
    self.assertEqual(ov.target(None), 28.0)           # absolute ignores the thermostat
    ov.mode = og.MODE_RELATIVE
    ov.offset = 3.0
    self.assertIsNone(ov.target(None))                # thermostat unknown: hand back
    self.assertEqual(ov.target(30.0), 33.0)
    self.assertEqual(ov.target(34.0), 35.0)           # clamped to OVERRIDE_MAX
    self.assertEqual(ov.target(10.0), 10.0)           # thermostat "off" stays off despite +3
    self.assertEqual(ov.target(0.0), 0.0)
    ov.offset = -5.0
    self.assertEqual(ov.target(12.0), 10.0)           # clamped to OVERRIDE_MIN
    self.assertEqual(ov.target(31.25), 26.2)


class TestConfig(unittest.TestCase):
  def test_defaults(self):
    cfg = make_cfg()
    self.assertEqual(cfg.out_topic, "otgw2mqtt/HP-M_25_i")
    self.assertEqual(cfg.mqtt_port, 1883)
    self.assertTrue(cfg.ha_enabled)

  def test_errors(self):
    bad = [
      {"MQTT_HOST": ""},
      {"MQTT_PORT": "abc"},
      {"OUT_TOPIC": "events/central_heating/otmonitor/x"},
      {"OUT_TOPIC": "a/#"},
      {"OVERRIDE_MIN": "60", "OVERRIDE_MAX": "50"},
      {"HA_ENABLED": "perhaps"},
      {"OVERRIDE_REFRESH": "1"},
      {"HA_UPDATE_INTERVAL": "-1"},
    ]
    for overrides in bad:
      with self.assertRaises(og.ConfigError, msg=overrides):
        make_cfg(**overrides)


class TestTimeWeightedLimiter(unittest.TestCase):
  def test_fast_readings_averaged(self):
    lim = og.TimeWeightedLimiter(10)
    self.assertEqual(lim.add("t", 20.0, 0), 20.0)      # first reading passes through
    self.assertIsNone(lim.add("t", 30.0, 2))
    self.assertIsNone(lim.add("t", 40.0, 6))
    self.assertEqual(lim.due(9.9), [])
    # 20 for 0-2 s, 30 for 2-6 s, 40 for 6-10 s
    self.assertEqual(lim.due(10), [("t", 32.0)])
    self.assertEqual(lim.due(11), [])                 # nothing buffered, nothing to publish

  def test_held_value_carries_into_next_window(self):
    lim = og.TimeWeightedLimiter(10)
    lim.add("t", 20.0, 0)
    lim.add("t", 30.0, 7)
    self.assertEqual(lim.due(10), [("t", 23.0)])      # 20*7 + 30*3
    self.assertIsNone(lim.add("t", 40.0, 14))
    self.assertEqual(lim.due(20), [("t", 36.0)])      # 30*4 + 40*6

  def test_slow_sensor_passed_through_unmodified(self):
    lim = og.TimeWeightedLimiter(10)
    for t, v in ((0, 21.37), (15, 21.41), (30.5, 21.12)):
      self.assertEqual(lim.add("room", v, t), v)
      self.assertEqual(lim.due(t + 0.5), [])

  def test_back_to_slow_after_fast_burst(self):
    lim = og.TimeWeightedLimiter(10)
    lim.add("t", 20.0, 0)
    lim.add("t", 22.0, 5)
    self.assertEqual(lim.due(10), [("t", 21.0)])
    self.assertEqual(lim.add("t", 25.0, 30), 25.0)    # >= interval since last update and nothing buffered

  def test_disabled(self):
    lim = og.TimeWeightedLimiter(0)
    self.assertEqual([lim.add("t", v, 0) for v in (1.0, 2.0, 3.0)], [1.0, 2.0, 3.0])
    self.assertEqual(lim.due(100), [])

  def test_names_independent(self):
    lim = og.TimeWeightedLimiter(10)
    lim.add("a", 1.0, 0)
    lim.add("a", 3.0, 5)
    self.assertEqual(lim.add("b", 7.0, 6), 7.0)
    self.assertEqual(lim.due(10), [("a", 2.0)])


class TestSystemdNotifier(unittest.TestCase):
  def test_noop_outside_systemd(self):
    n = og.SystemdNotifier({})
    self.assertFalse(n.enabled)
    self.assertIsNone(n.watchdog_interval)
    self.assertFalse(n.notify("READY=1"))

  def test_sends_to_socket(self):
    with tempfile.TemporaryDirectory() as d:
      path = os.path.join(d, "notify")
      server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
      server.bind(path)
      try:
        n = og.SystemdNotifier({"NOTIFY_SOCKET": path, "WATCHDOG_USEC": "30000000", "WATCHDOG_PID": str(os.getpid())})
        self.assertEqual(n.watchdog_interval, 15.0)
        self.assertTrue(n.notify("READY=1", "STATUS=ok"))
        self.assertEqual(server.recv(100), b"READY=1\nSTATUS=ok")
        n.close()
        self.assertFalse(n.notify("READY=1"))
      finally:
        server.close()

  def test_watchdog_for_other_pid_ignored(self):
    n = og.SystemdNotifier({"NOTIFY_SOCKET": "@x", "WATCHDOG_USEC": "30000000", "WATCHDOG_PID": "1"})
    self.assertIsNone(n.watchdog_interval)
    n.close()


class TestMain(unittest.TestCase):
  def test_missing_env_file(self):
    self.assertEqual(og.main(["-e", "/nonexistent/otgw2mqtt.env"]), 2)


class TestDiscovery(unittest.TestCase):
  def test_entities(self):
    cfg = make_cfg()
    entities = og.build_discovery(cfg)
    ids = [conf["unique_id"] for _, conf in entities]
    self.assertEqual(len(ids), len(set(ids)))
    self.assertEqual(len(entities), len(og.SPECS) + 3)
    topics = dict(entities)
    sel = topics["homeassistant/select/otgw2mqtt_HP-M_25_i/override_mode/config"]
    self.assertEqual(sel["command_topic"], "otgw2mqtt/HP-M_25_i/override_mode/set")
    self.assertEqual(sel["options"], ["Off", "Absolute"])      # no raw stream configured: no relative mode
    self.assertTrue(sel["retain"])
    num = topics["homeassistant/number/otgw2mqtt_HP-M_25_i/override_setpoint/config"]
    self.assertEqual((num["min"], num["max"]), (10.0, 55.0))
    off = topics["homeassistant/number/otgw2mqtt_HP-M_25_i/override_offset/config"]
    self.assertEqual((off["min"], off["max"], off["command_topic"]), (-10.0, 10.0, "otgw2mqtt/HP-M_25_i/override_offset/set"))
    sel = dict(og.build_discovery(make_cfg(OTGW_HOST="otgw")))["homeassistant/select/otgw2mqtt_HP-M_25_i/override_mode/config"]
    self.assertEqual(sel["options"], ["Off", "Absolute", "Relative"])
    flame = topics["homeassistant/binary_sensor/otgw2mqtt_HP-M_25_i/flame/config"]
    self.assertEqual(flame["state_topic"], "otgw2mqtt/HP-M_25_i/flame")

  def test_selected_keys_only(self):
    cfg = make_cfg()
    topics = [t for t, _ in og.build_discovery(cfg, ["flame"])]
    self.assertEqual(topics, ["homeassistant/select/otgw2mqtt_HP-M_25_i/override_mode/config",
                              "homeassistant/number/otgw2mqtt_HP-M_25_i/override_setpoint/config",
                              "homeassistant/number/otgw2mqtt_HP-M_25_i/override_offset/config",
                              "homeassistant/binary_sensor/otgw2mqtt_HP-M_25_i/flame/config"])

  def test_purge_filters_cover_everything_retained(self):
    cfg = make_cfg()
    filters = og.purge_filters(cfg)
    retained = [t for t, _ in og.build_discovery(cfg)] + og.legacy_retained_topics(cfg) + [
      "otgw2mqtt/HP-M_25_i/status", "otgw2mqtt/HP-M_25_i/override_mode/set", "otgw2mqtt/HP-M_25_i/override_setpoint/set",
      "otgw2mqtt/HP-M_25_i/override_offset/set"]
    for topic in retained:
      self.assertTrue(any(paho_matches(f, topic) for f in filters), topic)
    self.assertFalse(any(paho_matches(f, "homeassistant/sensor/other_device/x/config") for f in filters))


def paho_matches(sub, topic):
  import paho.mqtt.client as paho  # pylint: disable=import-outside-toplevel
  return paho.topic_matches_sub(sub, topic)


class FakeMessages:
  def __init__(self, messages):
    self._messages = list(messages)

  def __aiter__(self):
    return self

  async def __anext__(self):
    if self._messages:
      return self._messages.pop(0)
    await asyncio.Event().wait()   # like a quiet broker: block until cancelled
    raise AssertionError("unreachable")


class FakeClient:  # pylint: disable=too-few-public-methods
  def __init__(self, messages):
    self.messages = FakeMessages(messages)
    self.subscribed = []

  async def subscribe(self, topic, qos=0):  # pylint: disable=unused-argument
    self.subscribed.append(topic)


class TestPurge(unittest.IsolatedAsyncioTestCase):
  async def test_collects_only_retained_non_empty(self):
    msgs = [og.aiomqtt.Message("a/status", b"online", qos=1, retain=True, mid=1, properties=None),
            og.aiomqtt.Message("a/status", b"offline", qos=1, retain=False, mid=2, properties=None),   # live traffic
            og.aiomqtt.Message("a/x/config", b"{}", qos=1, retain=True, mid=3, properties=None),
            og.aiomqtt.Message("a/y/config", b"", qos=1, retain=True, mid=4, properties=None)]
    client = FakeClient(msgs)
    original = og.PURGE_IDLE
    og.PURGE_IDLE = 0.05
    try:
      found = await og.collect_retained(client, ["a/#"])
    finally:
      og.PURGE_IDLE = original
    self.assertEqual(client.subscribed, ["a/#"])
    self.assertEqual(found, {"a/status": b"online", "a/x/config": b"{}"})


class RecordingBridge(og.Bridge):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.sent: list[tuple[str, str, bool]] = []

  async def publish(self, topic, payload, retain=False, qos=0):  # pylint: disable=unused-argument
    if isinstance(payload, (dict, list)):
      payload = og._compact(payload)  # pylint: disable=protected-access
    self.sent.append((topic, payload, retain))
    return True

  def topics(self):
    return [t for t, _, _ in self.sent]

  def states(self):
    return [m for m in self.sent if not m[0].endswith("/config")]


class TestBridge(unittest.IsolatedAsyncioTestCase):
  async def asyncSetUp(self):
    self.clock = FakeClock()
    self.cfg = make_cfg(INFLUX_HOST="influx", OUTDOOR_TOPIC="sensors/out", OUTDOOR_JSON_KEY="temperature_C",
                        OTGW_HOST="otgw", OVERRIDE_MAX="35")
    self.influx = og.InfluxWriter(self.cfg, dryrun=True)
    self.bridge = RecordingBridge(self.cfg, self.influx, dryrun=True, clock=self.clock)

  async def test_otmonitor_value_emitted_and_buffered(self):
    payload = json.dumps({"value": 33.333, "timestamp": int(self.clock.t * 1000)}).encode()
    await self.bridge.handle_otmonitor("boilerwatertemperature", payload)
    self.assertEqual(self.bridge.sent[-1][0], "otgw2mqtt/HP-M_25_i/boilerwatertemperature")
    self.assertEqual(json.loads(self.bridge.sent[-1][1])["value"], 33.33)
    self.assertFalse(self.bridge.sent[-1][2])            # readings are not retained
    self.assertEqual(self.influx._buffer[-1]["fields"], {"boilerwatertemperature": 33.33})  # pylint: disable=protected-access
    # duplicate within heartbeat is not re-emitted
    await self.bridge.handle_otmonitor("boilerwatertemperature", payload)
    self.assertEqual(len(self.bridge.states()), 1)

  async def test_entity_created_on_first_value_only(self):
    await self.bridge.publish_discovery()
    configs = [t for t, _, r in self.bridge.sent if t.endswith("/config")]
    self.assertEqual(len(configs), 3)                     # only the override controls up front
    original = og.HA_NEW_ENTITY_DELAY
    og.HA_NEW_ENTITY_DELAY = 0
    try:
      self.bridge.sent.clear()
      await self.bridge.handle_otmonitor("dhwtemperature", b"45")
      self.assertEqual(self.bridge.sent[0][0], "homeassistant/sensor/otgw2mqtt_HP-M_25_i/dhwtemperature/config")
      self.assertTrue(self.bridge.sent[0][2])             # discovery is retained
      self.assertEqual(self.bridge.sent[1][0], "otgw2mqtt/HP-M_25_i/dhwtemperature")
      await self.bridge._new_entity_task  # pylint: disable=protected-access
      self.assertEqual([t for t, _, _ in self.bridge.sent].count("otgw2mqtt/HP-M_25_i/dhwtemperature"), 2)  # re-sent
      self.clock.t += 100
      await self.bridge.handle_otmonitor("dhwtemperature", b"46")
      self.assertEqual(len([t for t, _, _ in self.bridge.sent if t.endswith("/config")]), 1)   # created once
    finally:
      og.HA_NEW_ENTITY_DELAY = original
    # rejected values never create an entity
    await self.bridge.handle_otmonitor("chwaterpressure", b"99")
    self.assertNotIn("homeassistant/sensor/otgw2mqtt_HP-M_25_i/chwaterpressure/config", self.bridge.topics())
    # (re)connect / HA birth: discovery for everything seen so far
    self.bridge.sent.clear()
    await self.bridge.publish_discovery()
    self.assertIn("homeassistant/sensor/otgw2mqtt_HP-M_25_i/dhwtemperature/config", self.bridge.topics())
    self.assertEqual(len(self.bridge.sent), 4)

  async def test_ha_disabled_creates_no_entities(self):
    bridge = RecordingBridge(make_cfg(HA_ENABLED="false"), self.influx, dryrun=True, clock=self.clock)
    await bridge.handle_otmonitor("dhwtemperature", b"45")
    await bridge.publish_discovery()
    self.assertEqual(bridge.topics(), ["otgw2mqtt/HP-M_25_i/dhwtemperature"])

  async def test_fast_numeric_rate_limited_bool_direct(self):
    for i, v in enumerate((30.0, 31.0, 32.0)):
      self.clock.t = 1_700_000_000.0 + i * 2
      await self.bridge.handle_otmonitor("boilerwatertemperature", json.dumps({"value": v}).encode())
    temps = [json.loads(p)["value"] for t, p, _ in self.bridge.sent if t.endswith("/boilerwatertemperature")]
    self.assertEqual(temps, [30.0])                     # only the first reading went out directly
    fields = [p["fields"]["boilerwatertemperature"] for p in self.influx._buffer]  # pylint: disable=protected-access
    self.assertEqual(fields, [30.0, 31.0, 32.0])        # InfluxDB gets full resolution
    await self.bridge.handle_otmonitor("flame", b"true")
    self.assertTrue(self.bridge.sent[-1][0].endswith("/flame"))   # flags are not delayed
    self.clock.t = 1_700_000_010.0
    await self.bridge.flush_averages()
    temps = [json.loads(p)["value"] for t, p, _ in self.bridge.sent if t.endswith("/boilerwatertemperature")]
    self.assertEqual(temps, [30.0, 31.4])               # (30*2 + 31*2 + 32*6) / 10 s

  async def test_ha_birth_republishes_latest_values(self):
    await self.bridge.handle_otmonitor("boilerwatertemperature", b'{"value":30}')
    await self.bridge.handle_otmonitor("flame", b"true")
    await self.bridge.handle_otmonitor("flame", b"false")
    self.bridge.sent.clear()
    original = og.HA_BIRTH_DELAY
    og.HA_BIRTH_DELAY = 0
    try:
      await self.bridge.handle_ha_status(b"offline")
      self.assertIsNone(self.bridge._republish_task)  # pylint: disable=protected-access
      await self.bridge.handle_ha_status(b"online")
      await self.bridge._republish_task  # pylint: disable=protected-access
    finally:
      og.HA_BIRTH_DELAY = original
    retained = {t for t, _, r in self.bridge.sent if r}
    self.assertTrue(all(t.endswith("/config") for t in retained))   # only discovery is retained
    states = {t: p for t, p, _ in self.bridge.sent if not t.endswith("/config")}
    self.assertEqual(json.loads(states["otgw2mqtt/HP-M_25_i/boilerwatertemperature"])["value"], 30.0)
    self.assertFalse(json.loads(states["otgw2mqtt/HP-M_25_i/flame"])["value"])   # latest, not first
    self.assertEqual(states["otgw2mqtt/HP-M_25_i/override_mode"], "Off")
    self.assertEqual(states["otgw2mqtt/HP-M_25_i/override_offset"], "0.0")

  async def test_unknown_and_reserved_names_ignored(self):
    await self.bridge.handle_otmonitor("somethingnew", b"1")
    await self.bridge.handle_otmonitor("tchsetpoint", b"40")
    await self.bridge.handle_otmonitor("nested/topic", b"1")
    self.assertEqual(self.bridge.sent, [])

  async def test_stream_and_override_active(self):
    await self.bridge.handle_otmonitor("controlsetpoint", b'{"value":35}')
    await self.bridge.handle_stream_line("T10011E00")  # thermostat wants 30
    topics = self.bridge.topics()
    self.assertIn("otgw2mqtt/HP-M_25_i/tchsetpoint", topics)
    active = [json.loads(p)["value"] for t, p, _ in self.bridge.sent if t.endswith("/override_active")]
    self.assertEqual(active, [True])

  async def test_outdoor_forwarded(self):
    await self.bridge.handle_outdoor(b'{"temperature_C": 7.25, "id": 11}')
    self.assertEqual(self.bridge.sent[-1][0], "actions/otmonitor/outside")
    self.assertEqual(json.loads(self.bridge.sent[-1][1])["value"], 7.25)
    await self.bridge.handle_outdoor(b'{"humidity": 50}')
    await self.bridge.handle_outdoor(b'{"temperature_C": 99}')
    self.assertEqual(len(self.bridge.sent), 1)

  def cs_sent(self):
    return [json.loads(p)["value"] for t, p, _ in self.bridge.sent if t == "actions/otmonitor/ctrlsetpt"]

  async def test_override_commands(self):
    await self.bridge.handle_override_setpoint_command(b"32")
    self.assertEqual(self.bridge.override.value, 32.0)
    await self.bridge.handle_override_mode_command(b"Absolute")
    self.assertEqual(self.bridge.override.mode, og.MODE_ABSOLUTE)
    self.assertIn(("otgw2mqtt/HP-M_25_i/override_mode", "Absolute", False), self.bridge.sent)
    self.assertTrue(self.bridge._override_kick.is_set())  # pylint: disable=protected-access
    await self.bridge.apply_override()
    self.assertEqual(self.bridge.sent[-1], ("actions/otmonitor/ctrlsetpt", '{"name":"temp","type":"float","value":32.0}', False))
    await self.bridge.apply_override()                    # unchanged: nothing sent
    self.assertEqual(self.cs_sent(), [32.0])
    await self.bridge.apply_override(refresh=True)        # periodic refresh re-sends
    self.assertEqual(self.cs_sent(), [32.0, 32.0])
    await self.bridge.handle_override_offset_command(b"-2")
    self.assertEqual(self.bridge.override.offset, -2.0)
    self.assertIn(("otgw2mqtt/HP-M_25_i/override_offset", "-2.0", False), self.bridge.sent)
    await self.bridge.handle_override_mode_command(b"Off")
    await self.bridge.apply_override()
    await self.bridge.apply_override(refresh=True)        # a release is sent once, not refreshed
    self.assertEqual(self.cs_sent(), [32.0, 32.0, 0])
    await self.bridge.handle_override_mode_command(b"bogus")
    self.assertEqual(self.bridge.override.mode, og.MODE_OFF)

  async def test_relative_follows_thermostat(self):
    await self.bridge.handle_override_offset_command(b"3")
    await self.bridge.handle_override_mode_command(b"Relative")
    await self.bridge.apply_override()
    self.assertEqual(self.cs_sent(), [0])                 # thermostat not seen yet: hand back
    self.bridge._override_kick.clear()  # pylint: disable=protected-access
    self.clock.t += 10
    await self.bridge.handle_stream_line("T10011E00")     # thermostat wants 30
    self.assertTrue(self.bridge._override_kick.is_set())  # pylint: disable=protected-access
    await self.bridge.apply_override()
    self.assertEqual(self.cs_sent(), [0, 33.0])
    self.bridge._override_kick.clear()  # pylint: disable=protected-access
    await self.bridge.handle_stream_line("T10011E00")     # repeat, same target
    self.assertFalse(self.bridge._override_kick.is_set())  # pylint: disable=protected-access
    self.clock.t += 1
    await self.bridge.handle_stream_line("T10012300")     # 35 -> 38, clamped to 35, but too soon after the last send
    self.assertFalse(self.bridge._override_kick.is_set())  # pylint: disable=protected-access
    self.clock.t += og.CS_FOLLOW_MIN_INTERVAL
    await self.bridge.handle_stream_line("T10012300")     # the next repeat picks it up
    self.assertTrue(self.bridge._override_kick.is_set())  # pylint: disable=protected-access
    await self.bridge.apply_override()
    self.clock.t += 10
    await self.bridge.handle_stream_line("T10010A00")     # thermostat off (10): stays off despite +3
    await self.bridge.apply_override()
    self.assertEqual(self.cs_sent(), [0, 33.0, 35.0, 10.0])
    with self.assertLogs(og.logger, "WARNING"):
      self.clock.t += og.THERMOSTAT_MAX_AGE + 1           # raw stream silent: hand back to the thermostat
      await self.bridge.apply_override(refresh=True)
    self.assertEqual(self.cs_sent(), [0, 33.0, 35.0, 10.0, 0])

  async def test_relative_needs_raw_stream(self):
    bridge = RecordingBridge(make_cfg(), self.influx, dryrun=True, clock=self.clock)
    with self.assertLogs(og.logger, "WARNING"):
      await bridge.handle_override_mode_command(b"Relative")
    self.assertEqual(bridge.override.mode, og.MODE_OFF)

  async def test_legacy_switch_removed(self):
    await self.bridge.clear_legacy_topics()
    self.assertEqual(self.bridge.sent, [("homeassistant/switch/otgw2mqtt_HP-M_25_i/override/config", b"", True),
                                        ("otgw2mqtt/HP-M_25_i/override/set", b"", True)])

  async def test_deviation_triggers_resend(self):
    self.bridge.override.mode = og.MODE_ABSOLUTE
    self.bridge.override.value = 34.0
    await self.bridge.apply_override()
    self.bridge._override_kick.clear()  # pylint: disable=protected-access
    await self.bridge.handle_otmonitor("controlsetpoint", b"30")
    self.assertFalse(self.bridge._override_kick.is_set())  # pylint: disable=protected-access  # just sent
    self.clock.t += og.CS_RESEND_MIN_INTERVAL + 1
    await self.bridge.handle_otmonitor("controlsetpoint", b"30.5")
    self.assertTrue(self.bridge._override_kick.is_set())  # pylint: disable=protected-access
    await self.bridge.apply_override()
    self.assertEqual(self.cs_sent(), [34.0, 34.0])

  async def test_override_task_startup_releases(self):
    self.bridge._connected.set()  # pylint: disable=protected-access
    original = og.STARTUP_GRACE
    og.STARTUP_GRACE = 0
    try:
      task = asyncio.create_task(self.bridge.override_task())
      await asyncio.sleep(0.05)
      task.cancel()
      with self.assertRaises(asyncio.CancelledError):
        await task
    finally:
      og.STARTUP_GRACE = original
    self.assertIn(("actions/otmonitor/ctrlsetpt", '{"name":"temp","type":"float","value":0}', False), self.bridge.sent)


if __name__ == "__main__":
  unittest.main()
