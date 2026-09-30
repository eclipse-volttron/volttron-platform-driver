"""Device-scoped point lookup in the equipment tree, for devices whose topics nest beneath other devices."""
from types import SimpleNamespace
from unittest import mock

import pytest

from volttron.driver.base.config import DeviceConfig, PointConfig

from platform_driver.config import PlatformDriverConfig
from platform_driver.equipment import EquipmentTree

BESS = 'devices/PNNL/SEB/BESS'
METER = f'{BESS}/METER'


def _points(*names):
    return [PointConfig(volttron_point_name=n) for n in names]


@pytest.fixture
def tree():
    agent = SimpleNamespace(config=PlatformDriverConfig(), vip=mock.Mock(), core=mock.Mock(),
                            poll_schedulers={})
    return EquipmentTree(agent)


@pytest.fixture
def nested(tree):
    """A BESS device with a METER device nested beneath its topic; each has its own remote."""
    bess_remote, meter_remote = mock.Mock(name='bess_remote'), mock.Mock(name='meter_remote')
    tree.add_device(BESS, DeviceConfig(), bess_remote, _points('SOC', 'Power'))
    tree.add_device(METER, DeviceConfig(), meter_remote, _points('Volts AN', 'Watts'))
    return tree, bess_remote, meter_remote


def test_points_returns_the_whole_subtree(nested):
    tree, *_ = nested
    assert {p.identifier for p in tree.points(BESS)} == {f'{BESS}/SOC', f'{BESS}/Power', f'{METER}/Volts AN',
                                                         f'{METER}/Watts'}


def test_device_points_stops_at_nested_devices(nested):
    tree, *_ = nested
    assert {p.identifier for p in tree.device_points(BESS)} == {f'{BESS}/SOC', f'{BESS}/Power'}
    assert {p.identifier for p in tree.device_points(METER)} == {f'{METER}/Volts AN', f'{METER}/Watts'}


def test_device_points_order_independent(tree):
    """Configuring the nested device first must not change what the enclosing device owns."""
    tree.add_device(METER, DeviceConfig(), mock.Mock(), _points('Watts'))
    tree.add_device(BESS, DeviceConfig(), mock.Mock(), _points('SOC'))
    assert tree.get_node(BESS).is_device and tree.get_node(METER).is_device
    assert [p.identifier for p in tree.device_points(BESS)] == [f'{BESS}/SOC']
    assert [p.identifier for p in tree.device_points(METER)] == [f'{METER}/Watts']


def test_stored_registry_contains_only_the_devices_own_points(nested):
    tree, *_ = nested
    tree.get_node(BESS).data['registry_name'] = 'bess.csv'
    tree.update_stored_registry_config(BESS)
    tree.agent.vip.config.set.assert_called_once()
    name, registry = tree.agent.vip.config.set.call_args.args
    assert name == 'bess.csv' and {r['volttron_point_name'] for r in registry} == {'SOC', 'Power'}


def test_updating_the_enclosing_device_leaves_nested_points_alone(nested):
    tree, bess_remote, _ = nested
    changed = tree.update_equipment(BESS, DeviceConfig(), bess_remote, _points('SOC', 'Power'))
    assert changed is False
    assert tree.get_node(f'{METER}/Volts AN') is not None and tree.get_node(f'{METER}/Watts') is not None


def test_removing_a_point_from_the_enclosing_device(nested):
    tree, bess_remote, _ = nested
    assert tree.update_equipment(BESS, DeviceConfig(), bess_remote, _points('SOC')) is True
    assert tree.get_node(f'{BESS}/Power') is None
    assert {p.identifier for p in tree.device_points(METER)} == {f'{METER}/Volts AN', f'{METER}/Watts'}


def test_nested_device_first_keeps_its_points_when_enclosing_device_arrives(tree):
    meter_remote = mock.Mock()
    tree.add_device(METER, DeviceConfig(), meter_remote, _points('Watts'))
    watts = tree.get_node(f'{METER}/Watts')
    assert not tree.get_node(BESS).is_concrete            # a bare segment created as METER's ancestor
    tree.add_device(BESS, DeviceConfig(), mock.Mock(), _points('SOC'))
    bess = tree.get_node(BESS)
    assert bess.is_device and tree.parent(METER).identifier == BESS and tree.parent(BESS).identifier == 'devices/PNNL/SEB'
    assert tree.get_node(f'{METER}/Watts') is watts       # same node objects: schedules holding them stay valid
    assert tree.get_device_node(f'{METER}/Watts').remote is meter_remote
    assert tree.get_device_node(f'{BESS}/SOC') is bess
    assert bess.data['topic'] == 'PNNL/SEB/BESS'


def test_segment_config_applies_to_an_existing_segment(tree):
    from volttron.driver.base.config import EquipmentConfig
    tree.add_device(METER, DeviceConfig(), mock.Mock(), _points('Watts'))
    tree.add_segment('devices/PNNL/SEB', EquipmentConfig(group='plant'))
    assert tree.get_node('devices/PNNL/SEB').group == 'plant'
    assert tree.get_group(f'{METER}/Watts') == 'plant'
