# -*- coding: utf-8 -*- {{{
# ===----------------------------------------------------------------------===
#
#                 Installable Component of Eclipse VOLTTRON
#
# ===----------------------------------------------------------------------===
#
# Copyright 2022 Battelle Memorial Institute
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# ===----------------------------------------------------------------------===
# }}}

import gevent
import logging

from datetime import datetime, timedelta
from importlib.metadata import distribution, PackageNotFoundError
from treelib.exceptions import DuplicatedNodeIdError
from typing import Any, cast, Iterable, Optional, TYPE_CHECKING, Union
from weakref import WeakValueDictionary

try:
    distribution('volttron-core')
    from volttron.client.known_identities import CONFIGURATION_STORE
    from volttron.driver.base.driver import DriverAgent
    from volttron.driver.base.config import DataSource, DeviceConfig, EquipmentConfig, PointConfig
    from volttron.utils import get_aware_utc_now, parse_json_config
except PackageNotFoundError:
    from volttron.platform.agent.known_identities import CONFIGURATION_STORE
    from volttron.driver.base.driver import DriverAgent
    from volttron.driver.base.config import DataSource, DeviceConfig, EquipmentConfig, PointConfig
    from volttron.platform.agent.utils import get_aware_utc_now, parse_json_config

from .overrides import OverrideError
from .reservations import ReservationLockError
from .topic_tree import TopicNode, TopicTree


_log = logging.getLogger(__name__)


class EquipmentNode(TopicNode):
    def __init__(self, config: EquipmentConfig = None, *args, **kwargs):
        super(EquipmentNode, self).__init__(*args, **kwargs)
        self.data['config']: EquipmentConfig = config if config is not None else EquipmentConfig()
        self.data['remote'] = None
        self.data['segment_type'] = 'TOPIC_SEGMENT'

    @property
    def active(self) -> bool:
        return self.data['config'].active

    @active.setter
    def active(self, value: bool):
        self.data['config'].active = value

    @property
    def config(self) -> EquipmentConfig:
        return self.data['config']

    @config.setter
    def config(self, value: dict):
        self.data['config'] = value

    @property
    def group(self) -> str:
        return self.data['config'].group

    @property
    def meta_data(self) -> dict:
        return self.data.get('meta_data')

    @meta_data.setter
    def meta_data(self, value: dict):
        self.data['meta_data'] = value

    @property
    def polling_interval(self) -> float:
        # TODO: Should this be a property that inherits from parents?
        return self.data['config'].polling_interval

    @polling_interval.setter
    def polling_interval(self, value: float):
        self.data['config'].polling_interval = value

    @property
    def is_point(self) -> bool:
        return True if self.segment_type == 'POINT' else False

    @property
    def is_device(self) -> bool:
        return True if self.segment_type == 'DEVICE' else False

    @property
    def is_concrete(self) -> bool:
        return False if self.segment_type == 'TOPIC_SEGMENT' else True
        
    @property
    def publish_single_depth(self) -> bool:
        return self.data['config'].publish_single_depth

    @property
    def publish_single_breadth(self) -> bool:
        return self.data['config'].publish_single_breadth

    @property
    def publish_multi_depth(self) -> bool:
        return self.data['config'].publish_multi_depth

    @property
    def publish_multi_breadth(self) -> bool:
        return self.data['config'].publish_multi_breadth
    
    @property
    def publish_all_depth(self) -> bool:
        return self.data['config'].publish_all_depth

    @property
    def publish_all_breadth(self) -> bool:
        return self.data['config'].publish_all_breadth

    @property
    def strict_all_publishes(self) -> bool:
        return self.data['config'].strict_all_publishes

    @property
    def reservation_required_for_write(self) -> bool:
        return self.data['config'].reservation_required_for_write

    @reservation_required_for_write.setter
    def reservation_required_for_write(self, value: bool):
        self.data['config'].reservation_required_for_write = value

    def wipe_configuration(self):
        # Wipe all data and reset segment_type to TOPIC_SEGMENT.
        self.data = {'segment_type': 'TOPIC_SEGMENT'}


class DeviceNode(EquipmentNode):
    def __init__(self, config, driver, *args, **kwargs):
        config = config.model_copy()
        super(DeviceNode, self).__init__(config, *args, **kwargs)
        self._remote: DriverAgent = driver
        self.data['registry_name'] = None
        self.data['segment_type'] = 'DEVICE'
        self.config_finished = False

    @property
    def all_publish_interval(self) -> float:
        return self.data['config'].all_publish_interval

    @property
    def remote(self) -> DriverAgent:
        return self._remote

    @property
    def registry_name(self) -> str:
        return self.data['registry_name']

    def stop_device(self):
        _log.info(f"Stopping driver: {self.identifier}")
        # TODO: This previously stopped the DriverAgent. Do we need to check if any polling instances need stopped?

class PointNode(EquipmentNode):
    def __init__(self, config, *args, **kwargs):
        super(PointNode, self).__init__(config, *args, **kwargs)
        self.data['last_value']: Any = None
        self.data['last_updated']: Optional[datetime] = None
        self.data['segment_type'] = 'POINT'
        # self._stale = True

    @property
    def data_source(self) -> DataSource:
        return self.data['config'].data_source

    @data_source.setter
    def data_source(self, value: Union[str, int, DataSource]):
        if isinstance(value, DataSource | str):
            self.data['config'].data_source = value
        else:
            raise ValueError(f'Data source must be a DataSource or a string in: {list(DataSource.__members__.keys())}.')

    @property
    def last_value(self) -> Any:
        return self.data['last_value']

    @last_value.setter
    def last_value(self, value: Any):
        self.data['last_value'] = value
        self.data['last_updated'] = get_aware_utc_now()

    @property
    def last_updated(self) -> datetime:
        return self.data['last_updated']

class EquipmentTree(TopicTree):
    def __init__(self, agent, *args, **kwargs):
        super(EquipmentTree, self).__init__(root_name=agent.config.depth_first_base, node_class=EquipmentNode,
                                            *args, **kwargs)
        self.agent = agent
        self.remotes = WeakValueDictionary()

        root_config = self[self.root].data['config']
        root_config.active = True
        root_config.group = 'default'
        root_config.polling_interval = agent.config.default_polling_interval
        root_config.publish_single_depth = agent.config.publish_single_depth
        root_config.publish_single_breadth = agent.config.publish_single_breadth
        root_config.publish_multi_depth = agent.config.publish_multi_depth
        root_config.publish_multi_breadth = agent.config.publish_multi_breadth
        root_config.publish_all_depth = agent.config.publish_all_depth
        root_config.publish_all_breadth = agent.config.publish_all_breadth
        root_config.stale_timeout_configured = agent.config.stale_timeout_configured
        root_config.stale_timeout_multiplier = agent.config.stale_timeout_multiplier
        root_config.strict_all_publishes = agent.config.strict_all_publishes

    if TYPE_CHECKING:
        def get_node(self, nid) -> EquipmentNode | DeviceNode | PointNode:
            ...

    def set_registry_name(self, nid):
        # TODO: This method should be unnecessary, if we can just get the registry_name in the config_store push.
        #  The registry name itself was not available at configuration time
        #   and is not returned by the self.config.get() method ( it is dereferenced, already).
        remote_conf = {}
        try:
            remote_conf_json = self.agent.vip.rpc.call(CONFIGURATION_STORE, 'get_config',self.agent.core.identity,
                                                        nid).get(timeout=5)
            remote_conf = parse_json_config(remote_conf_json)
        except (Exception, gevent.Timeout) as e:
            _log.warning(f'Unable to get registry_name for device: {nid} -- {e}')
        finally:
            reg_name = remote_conf.get('registry_config', '')
            # TODO: This should probably actually be checking if it is a string that startswith("config://").
            #  What if the registry is a json dictionary in the device config?
            return reg_name[len('config://'):] if len(reg_name) >= len('config://') else None

    def add_device(self, device_topic: str, dev_config: DeviceConfig, remote: DriverAgent,
                   registry_configs: Iterable[PointConfig]):
        """
        Add Device
        Adds a device node to the equipment tree. Also adds any necessary ancestor topic nodes and child point nodes.
        Returns a reference to the device node.
        """
        # Set up ancestor nodes.
        ancestral_topic = device_topic.split('/')
        device_name = ancestral_topic.pop()
        parent = self.add_segment('/'.join(ancestral_topic))

        # Set up the device node itself.
        try:
            # TODO: It would be possible to allow inheritance of dev_config properties from something set on parent,
            #  similar to how registry configs are handled.
            device_node = DeviceNode(config=dev_config, driver=remote, tag=device_name, identifier=device_topic)
            device_node.data['registry_name'] = self.set_registry_name(device_node.identifier)
            self.add_node(device_node, parent=parent)
        except DuplicatedNodeIdError:
            # TODO: If the node already exists, update it as necessary?
            device_node = self.get_node(device_topic)

        # Set up any point nodes which are children of this device.
        for point_config in registry_configs:
            try:
                node = PointNode(config=point_config, tag=point_config.volttron_point_name,
                                 identifier='/'.join([device_topic, point_config.volttron_point_name]))
                self.add_node(node, parent=device_topic)
            except DuplicatedNodeIdError:
                _log.warning(f'Duplicate Volttron Point Name "{point_config.volttron_point_name}" on {device_topic}.'
                             f'Duplicate register will not be created. Please update the configuration to ensure'
                             f' correct registers are created.')
        return device_node

    def update_equipment(self, nid: str, dev_config: DeviceConfig | None, remote: DriverAgent | None,
                         registry_config: list[PointConfig]) -> bool:
        changes = False
        dev_node: DeviceNode = self.get_node(nid)
        if dev_node and dev_config is not None:
            if dev_config != dev_node.config:
                changes = True
                dev_node.config = dev_config
            if remote is not None and dev_node.remote != remote:
                dev_node.data['remote'] = remote
                changes = True
        existing_points = {p.identifier for p in self.points(nid)}
        while registry_config:
            point_config = registry_config.pop()
            point_id = '/'.join([nid, point_config.volttron_point_name])
            existing = self.get_node(point_id)
            if point_id not in existing_points:
                new_point = PointNode(config=point_config, tag=point_config.volttron_point_name,
                                      identifier='/'.join([nid, point_config.volttron_point_name]))
                self.add_node(new_point, parent=nid)
                new_register = remote.interface.create_register(point_config)
                remote.interface.insert_register(new_register, nid)
                remote.update_metadata(point_id)
                changes = True
            else:
                if point_config != existing.config:
                    existing.config = point_config
                    new_register = remote.interface.create_register(point_config)
                    remote.interface.insert_register(new_register, nid)
                    remote.update_metadata(point_id)
                    changes = True
                existing_points.remove(point_id)
        for removed in existing_points:
            for poll_scheduler in self.agent.poll_schedulers.values():
                poll_scheduler.remove_from_schedule(self.get_node(removed), self)
            self.remove_segment(removed)
            changes = True
        return changes

    def add_segment(self, topic: str, config: EquipmentConfig = None):
        topic = topic.split('/')
        if topic[0] == self.root:
            topic.pop(0)
        parent = self.root
        nid, node = self.root, None
        # Set up node after setting up any missing ancestors.
        for segment in topic:
            nid = '/'.join([parent, segment])
            try:
                node = EquipmentNode(tag=segment, identifier=nid, config=EquipmentConfig())
                self.add_node(node, parent)  # TODO: This does raise the DuplicatedNodeIdError, not just replace, right?
            except DuplicatedNodeIdError:
                # TODO: How to handle updates if this node is the intended target?
                pass  # We are not creating nor updating this node, which already exists.
            parent = nid
        if node and config:
            node.config = config
        return nid

    def has_concrete_successors(self, nid: str) -> bool:
        children = self.children(nid)
        if any([c.is_concrete() for c in children]):
            return True
        else:
            for child in children:
                if self.has_concrete_successors(child.identifier):
                    return True
        return False

    def remove_segment(self, nid: str, leave_disconnected: bool = False) -> int:
        node = self.get_node(nid)
        if node.is_device:
            node.stop_device()
        elif node.is_point:
            self.get_remote(nid).interface.point_map.pop(nid)
        if leave_disconnected and self.has_concrete_successors(nid):
            # TODO: This may leave behind points which have no Device. Replace Fake Driver Interface with static points?
            #  How do we handle static points like this? What setup is required for this?  Should we do this at all?
            node.wipe_configuration()
            removed_node_count = 1
        else:
            for device in self.devices(nid):
                device.stop_device()
            removed_node_count = self.remove_node(node.identifier) # Removes node and the subtree below.
        return removed_node_count

    def points(self, nid: str = None) -> Iterable[PointNode]:
        if nid is None:
            points = [n for n in self._nodes.values() if n.is_point]
        else:
            points = [self[n] for n in self.expand_tree(nid) if self[n].is_point]
        return points

    def devices(self, nid: str = None) -> Iterable[DeviceNode]:
        if nid is None:
            devices = [n for n in self._nodes.values() if n.is_device]
        else:
            devices = [self[n] for n in self.expand_tree(nid) if self[n].is_device]
        return devices

    def find_points(self, topic_pattern: str = '', regex: str = None, exact_matches: Iterable = None
                    ) -> Iterable[PointNode]:
        return (p for p in self.resolve_query(topic_pattern, regex, exact_matches, return_leaves=True) if p.is_point)

    def raise_on_locks(self, node: EquipmentNode, requester: str):
        # TODO: Raising is expensive compared to just checking and handling it. Do we actually need to raise?
        reserved_by = self.agent.reservation_manager.reserved_by(node.identifier)
        if reserved_by and reserved_by != requester:
            raise ReservationLockError(f"Equipment {node.identifier} is reserved by another party."
                                       f" ({requester}) does not have permission to write at this time.")
        elif not reserved_by and any(self.rsearch(node.identifier, lambda n: n.reservation_required_for_write)):
            raise ReservationLockError(f'Caller ({requester}) does not have a reservation '
                                       f'for equipment {node.identifier}. A reservation is required to write.')
        elif self.get_device_node(node.identifier).identifier in self.agent.override_manager.devices:
            raise OverrideError(f"Cannot set point on {node.identifier} since global override is set")
        
    def get_device_node(self, nid: str) -> DeviceNode:
        return cast(DeviceNode, self.get_node(next(self.rsearch(nid, lambda n: n.is_device))))

    def get_remote(self, nid: str) -> DriverAgent:
        return self.get_device_node(nid).remote

    def get_group(self, nid: str) -> str:
        return self[next(self.rsearch(nid, lambda n: n.group is not None))].group

    def get_point_topics(self, nid: str) -> tuple[str, str]:
        return nid, '/'.join([self.agent.config.breadth_first_base] + list(reversed(nid.split('/')[1:])))

    def get_device_topics(self, nid: str) -> tuple[str, str]:
        return self.get_point_topics(self.get_device_node(nid).identifier)

    def get_polling_interval(self, nid: str) -> float:
        return self[next(self.rsearch(nid, lambda n: n.polling_interval is not None))].polling_interval

    def get_stale_timeout_configured(self, nid: str) -> float | None:
        node_with_value = next(self.rsearch(nid, lambda n: n.config.stale_timeout_configured is not None), None)
        return self[node_with_value].config.stale_timeout_configured if node_with_value is not None else None

    def get_stale_timeout_multiplier(self, nid: str) -> float:
        return self[next(self.rsearch(nid, lambda n: n.config.stale_timeout_multiplier is not None)
                         )].config.stale_timeout_multiplier

    def is_published_single_depth(self, nid: str) -> bool:
        return self[next(self.rsearch(nid, lambda n: n.publish_single_depth is not None))].publish_single_depth
    
    def is_published_single_breadth(self, nid: str) -> bool:
        return self[next(self.rsearch(nid, lambda n: n.publish_single_breadth is not None))].publish_single_breadth

    def is_published_multi_depth(self, nid: str) -> bool:
        return self[next(self.rsearch(nid, lambda n: n.publish_multi_depth is not None))].publish_multi_depth

    def is_published_multi_breadth(self, nid: str) -> bool:
        return self[next(self.rsearch(nid, lambda n: n.publish_multi_breadth is not None))].publish_multi_breadth

    def is_published_all_depth(self, nid: str) -> bool:
        return self[next(self.rsearch(nid, lambda n: n.publish_all_depth is not None))].publish_all_depth

    def is_published_all_breadth(self, nid: str) -> bool:
        return self[next(self.rsearch(nid, lambda n: n.publish_all_breadth is not None))].publish_all_breadth

    def strict_all_publishes(self, nid: str) -> bool:
        return self[next(self.rsearch(nid, lambda n: n.strict_all_publishes is not None))].strict_all_publishes

    def is_active(self, nid: str) -> bool:
        return self[next(self.rsearch(nid, lambda n: n.active is not None))].active

    def is_ready(self, nid: str) -> bool:
        return not any(p.last_updated is None for p in self.points(nid) if self.is_active(p.identifier))

    def is_stale(self, nid: str) -> bool:
        stale_timeout_configured = self.get_stale_timeout_configured(nid)
        stale_timeout_multiplier = self.get_stale_timeout_multiplier(nid)
        polling_interval = self.get_polling_interval(nid)

        if stale_timeout_configured is None and (polling_interval is None or stale_timeout_multiplier is None):
                stale_timeout = float('inf')
        else:
            stale_timeout = timedelta(seconds=(stale_timeout_configured
                                               if stale_timeout_configured is not None
                                               else polling_interval * stale_timeout_multiplier))
        return True if get_aware_utc_now() - self.get_node(nid).last_updated > stale_timeout else False

    def active_points(self, points: EquipmentNode | Iterable[EquipmentNode]) -> Iterable[PointNode]:
        points = self.points(points.identifier) if isinstance(points, EquipmentNode) else points
        return {p for p in points if self.is_active(p.identifier)}

    def ready_points(self, points: EquipmentNode | Iterable[EquipmentNode]) -> Iterable[PointNode]:
        points = self.points(points.identifier) if isinstance(points, EquipmentNode) else points
        return {p for p in points if p.last_updated is not None}

    def non_stale_points(self, points: EquipmentNode | Iterable[EquipmentNode]) -> Iterable[PointNode]:
        points = self.points(points.identifier) if isinstance(points, EquipmentNode) else points
        return {p for p in points if not self.is_stale(p.identifier)}

    def update_stored_registry_config(self, nid: str):
        # TODO: This updates the registry using JSON no matter what its original saved format was. This should be fine,
        #  and JSON should probably be preferred anyway, but it does not update the name, which is probably foo.csv....
        device_node = self.get_device_node(nid)
        registry = [p.config.model_dump() for p in self.points(device_node.identifier)]
        if device_node.registry_name:
            self.agent.vip.config.set(device_node.registry_name, registry)