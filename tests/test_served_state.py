"""Served points' values survive a platform driver restart through the agent's configuration store."""
from types import SimpleNamespace
from unittest import mock

import pytest

from volttron.driver.base.config import DeviceConfig, PointConfig

from platform_driver.config import PlatformDriverConfig
from platform_driver.equipment import EquipmentTree
from platform_driver.served_state import ServedStateStore

DEVICE = 'devices/campus/b/outstation'


class FakeConfigStore:
    def __init__(self):
        self.entries = {}
        self.calls = []

    def set(self, name, contents, trigger_callback=False, **_):
        self.calls.append(('set', name, contents, trigger_callback))
        self.entries[name] = contents

    def get(self, name):
        return self.entries[name]

    def delete(self, name, trigger_callback=False, **_):
        self.calls.append(('delete', name, trigger_callback))
        del self.entries[name]


class FakeCore:
    def __init__(self):
        self.later = []

    def spawn_later(self, seconds, func, *args):
        self.later.append((seconds, func, args))
        return object()

    def run_pending(self):
        pending, self.later = self.later, []
        for _, func, args in pending:
            func(*args)


@pytest.fixture
def agent():
    agent = SimpleNamespace(config=PlatformDriverConfig(), vip=SimpleNamespace(config=FakeConfigStore()), core=FakeCore(),
                            poll_schedulers={})
    agent.equipment_tree = EquipmentTree(agent)
    agent.served_state = ServedStateStore(agent, flush_interval=2.5)
    return agent


def _points():
    return [PointConfig(volttron_point_name='AI_2', data_source='server'),
            PointConfig(volttron_point_name='BO_0', data_source='server'),
            PointConfig(volttron_point_name='Polled')]                          # short_poll: not served


def test_recorded_values_are_written_once_per_flush_interval(agent):
    tree, store = agent.equipment_tree, agent.served_state
    tree.add_device(DEVICE, DeviceConfig(), mock.Mock(), _points())
    ai, bo, polled = (tree.get_node(f'{DEVICE}/{n}') for n in ('AI_2', 'BO_0', 'Polled'))
    ai.last_value, polled.last_value = 240.1, 7
    tree.record_pushed_value(ai)
    tree.record_pushed_value(polled)                                             # ignored: not a served point
    tree.record_pushed_value(ai)                                                 # coalesced
    assert store.dirty == {DEVICE} and len(agent.core.later) == 1 and agent.core.later[0][0] == 2.5
    agent.core.run_pending()
    assert agent.vip.config.calls == [('set', f'state/{DEVICE}', {'AI_2': 240.1}, False)]    # BO_0 has no value yet
    assert store.dirty == set() and store._flusher is None
    bo.last_value = True
    tree.record_pushed_value(bo)
    assert store.flush() == 1
    assert agent.vip.config.entries[f'state/{DEVICE}'] == {'AI_2': 240.1, 'BO_0': True}


def test_load_into_restores_served_points_without_values_before_registration(agent):
    tree, store = agent.equipment_tree, agent.served_state
    agent.vip.config.entries[f'state/{DEVICE}'] = {'AI_2': 231.0, 'BO_0': True, 'Polled': 3, 'Gone': 1}
    device = tree.add_device(DEVICE, DeviceConfig(), mock.Mock(), _points())
    tree.get_node(f'{DEVICE}/BO_0').last_value = False                           # already has a value: kept
    assert store.load_into(device) == 1
    assert tree.get_node(f'{DEVICE}/AI_2').last_value == 231.0 and tree.get_node(f'{DEVICE}/AI_2').last_updated is not None
    assert tree.get_node(f'{DEVICE}/BO_0').last_value is False
    assert tree.get_node(f'{DEVICE}/Polled').last_value is None                 # not a served point


def test_missing_or_bad_entries_are_harmless(agent):
    tree, store = agent.equipment_tree, agent.served_state
    device = tree.add_device(DEVICE, DeviceConfig(), mock.Mock(), _points())
    assert store.load_into(device) == 0
    agent.vip.config.entries[f'state/{DEVICE}'] = ['not', 'a', 'mapping']
    assert store.load_into(device) == 0


def test_forget_deletes_the_entry_and_a_removed_device_is_not_flushed(agent):
    tree, store = agent.equipment_tree, agent.served_state
    tree.add_device(DEVICE, DeviceConfig(), mock.Mock(), _points())
    ai = tree.get_node(f'{DEVICE}/AI_2')
    ai.last_value = 1
    tree.record_pushed_value(ai)
    store.forget(DEVICE)                                                         # nothing stored yet: no error
    store.forget(DEVICE)
    assert store.dirty == set()
    agent.vip.config.entries[f'state/{DEVICE}'] = {'AI_2': 1}
    store.forget(DEVICE)
    assert f'state/{DEVICE}' not in agent.vip.config.entries
    tree.record_pushed_value(ai)
    tree.remove_segment(DEVICE)
    assert store.flush() == 0                                                    # the device is gone: nothing written


def test_failed_writes_stay_dirty(agent):
    tree, store = agent.equipment_tree, agent.served_state
    tree.add_device(DEVICE, DeviceConfig(), mock.Mock(), _points())
    ai = tree.get_node(f'{DEVICE}/AI_2')
    ai.last_value = 1
    tree.record_pushed_value(ai)
    agent.vip.config.set = mock.Mock(side_effect=RuntimeError('store down'))
    assert store.flush() == 0 and store.dirty == {DEVICE}
