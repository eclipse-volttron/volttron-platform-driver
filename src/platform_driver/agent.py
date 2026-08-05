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
import re
import subprocess
import sys

from collections import defaultdict
from datetime import datetime
from gevent.event import Event
from importlib.metadata import distribution, PackageNotFoundError
from pathlib import Path
from pkgutil import iter_modules
from pydantic import ValidationError
from typing import Any, Iterable, Sequence, Set

try:
    import tomllib  # Will not be available below 3.11. Need to pip install tomli.
except ModuleNotFoundError:
    import tomli as tomllib

try:
    distribution('volttron-core')
    from argparse import Namespace
    from contextlib import redirect_stdout
    from io import StringIO
    from volttron.client.commands.install_parser import install_lib_vctl
    from volttron.client.known_identities import PLATFORM_DRIVER
    from volttron.client.logs import setup_logging
    from volttron.client.messaging.health import STATUS_BAD
    from volttron.client.messaging.utils import normtopic
    from volttron.client.vip.agent import Agent, Core
    from volttron.client.vip.agent.subsystems.rpc import RPC
    from volttron.driver.base.driver import BaseInterface, DriverAgent
    from volttron.driver.base.driver_locks import configure_publish_lock, setup_socket_lock
    from volttron.driver.base.config import DeviceConfig, EquipmentConfig, PointConfig, RemoteConfig
    from volttron.driver.base.utils import publication_headers, publish_wrapper
    from volttron.utils import format_timestamp, get_aware_utc_now, load_config, vip_main
    from volttron.utils.jsonrpc import RemoteError
    from volttron.utils.scheduling import periodic
except PackageNotFoundError:
    from importlib.metadata import requires
    from volttron.platform.agent.known_identities import PLATFORM_DRIVER
    from volttron.platform.messaging.health import STATUS_BAD
    from volttron.platform.messaging.utils import normtopic
    from volttron.platform.vip.agent import Agent, Core
    from volttron.platform.vip.agent.subsystems.rpc import RPC
    from volttron.driver.base.driver import BaseInterface, DriverAgent
    from volttron.driver.base.driver_locks import configure_publish_lock, setup_socket_lock
    from volttron.driver.base.config import DeviceConfig, EquipmentConfig, PointConfig, RemoteConfig
    from volttron.driver.base.utils import publication_headers, publish_wrapper
    from volttron.platform.agent.utils import format_timestamp, get_aware_utc_now, load_config, setup_logging, vip_main
    from volttron.platform.jsonrpc import RemoteError
    from volttron.platform.scheduling import periodic

from .config import PlatformDriverConfig
from .constants import *
from .equipment import DeviceNode, EquipmentNode, EquipmentTree, PointNode
from .overrides import OverrideManager
from .poll_scheduler import PollScheduler
from .reservations import ReservationManager
from .scalability_testing import ScalabilityTester

try:
    distribution('volttron-core')
    setup_logging()
except PackageNotFoundError:
    setup_logging()
_log = logging.getLogger(__name__)
__version__ = '4.0'


class PlatformDriverAgent(Agent):

    def __init__(self, **kwargs):
        config_path = kwargs.pop('config_path', None)
        super(PlatformDriverAgent, self).__init__(**kwargs)
        self.config: PlatformDriverConfig = self._load_agent_config(load_config(config_path) if config_path else {})
        # Initialize internal data structures:
        self.equipment_tree = EquipmentTree(self)
        self.interface_classes = {}

        # Set up locations for helper objects:
        self.heartbeat_greenlet = None
        self.override_manager = None  # TODO: Should this initialize object here and call a load method on config?
        self.poll_schedulers = {}
        self.publishers = {}
        self.reservation_manager = None  # TODO: Should this use a default reservation manager?
        self.scalability_test = None
        self._stop_agent = Event()

        self.vip.config.set_default("config", self.config.model_dump())
        self.vip.config.subscribe(self.configure_main, actions=['NEW', 'UPDATE', 'DELETE'], pattern='config')
        self.vip.config.subscribe(self._configure_new_equipment, actions=['NEW'], pattern='devices/*')
        self.vip.config.subscribe(self._update_equipment, actions=['UPDATE'], pattern='devices/*')
        self.vip.config.subscribe(self._remove_equipment, actions='DELETE', pattern='devices/*')

    #########################
    # Configuration & Startup
    #########################

    @Core.receiver('onstop')
    def _on_stop(self, _, **__):
        # TODO: Integrate this with greenlets other than heartbeat.
        self._stop_agent.set()

    def _load_agent_config(self, config: dict) -> PlatformDriverConfig:
        try:
            return PlatformDriverConfig(**config)
        except ValidationError as e:
            _log.warning(f'Validation of platform driver configuration file failed. Using default values. --- {str(e)}')
            if self.core.connected:  # TODO: Is this a valid way to make sure we are ready to call subsystems?
                self.vip.health.set_status(STATUS_BAD, f'Error processing configuration: {e}')
            return PlatformDriverConfig()

    def configure_main(self, _, action: str, contents: dict):
        old_config = self.config.model_copy(deep=True)
        new_config = self._load_agent_config(contents)
        if action == "NEW":
            self.config = new_config
            self.equipment_tree = EquipmentTree(self)
            try:
                setup_socket_lock(self.config.max_open_sockets)
                configure_publish_lock(int(self.config.max_concurrent_publishes))
                self.scalability_test = (ScalabilityTester(self.config.scalability_test_iterations)
                                         if self.config.scalability_test else None)
            except ValueError as e:
                _log.error(f"ERROR PROCESSING STARTUP CRITICAL CONFIGURATION SETTINGS: {e}")
                _log.error("Platform driver SHUTTING DOWN")
                sys.exit(1)

        else:
            # Some settings cannot be changed while running. Warn and replace these with the old ones until restart.
            _log.info('Updated configuration received for Platform Driver.')
            if new_config.max_open_sockets != old_config['max_open_sockets']:
                new_config.max_open_sockets = old_config['max_open_sockets']
                _log.info('Restart Platform Driver for changes to the max_open_sockets setting to take effect')

            if new_config.max_concurrent_publishes != old_config['max_concurrent_publishes']:
                new_config.max_concurrent_publishes = old_config['max_concurrent_publishes']
                _log.info('Restart Platform Driver for changes to the max_concurrent_publishes setting to take effect')

            if new_config.scalability_test != old_config['scalability_test']:
                new_config.scalability_test = old_config['scalability_test']
                if not old_config.scalability_test:
                    _log.info('Restart Platform Driver with scalability_test set to true in order to run a test.')
                if old_config.scalability_test:
                    _log.info("A scalability test may not be interrupted. Restart the driver to stop the test.")
            try:
                if new_config.scalability_test_iterations != old_config['scalability_test_iterations'] and \
                        old_config.scalability_test:
                    new_config.scalability_test_iterations = old_config['scalability_test_iterations']
                    _log.info('The scalability_test_iterations setting cannot be changed without restarting the agent.')
            except ValueError:
                pass
            if old_config.scalability_test:
                _log.info("Running scalability test. Settings may not be changed without restart.")
                return
            self.config = new_config
        if self.override_manager is None:
            self.override_manager = OverrideManager(self)

        # Set up Reservation Manager:
        if self.reservation_manager is None:
            now = get_aware_utc_now()
            self.reservation_manager = ReservationManager(self, self.config.reservation_preempt_grace_time, now)
            self.reservation_manager.update(now)
        else:
            self.reservation_manager.set_grace_period(self.config.reservation_preempt_grace_time)

        # Set up heartbeat to devices:
        # TODO: Should this be globally uniform (here), by device (in remote), or globally scheduled (in poll scheduler)?
        # Only restart the heartbeat if it changes.
        if (self.config.remote_heartbeat_interval != old_config.remote_heartbeat_interval
                or action == "NEW" or self.heartbeat_greenlet is None):
            if self.heartbeat_greenlet is not None:
                self.heartbeat_greenlet.kill()
            self.heartbeat_greenlet = gevent.spawn(self.heart_beat)

        # Start subscriptions:
        current_subscriptions = {topic: subscribed for _, topic, subscribed in self.vip.pubsub.list('pubsub').get()}
        for topic, callback in [
            (GET_TOPIC, self.handle_get),
            (SET_TOPIC, self.handle_set),
            (RESERVATION_REQUEST_TOPIC, self.handle_reservation_request),
            (REVERT_POINT_TOPIC, self.handle_revert_point),
            (REVERT_DEVICE_TOPIC, self.handle_revert_device)
        ]:
            if not current_subscriptions.get(topic):
                self.vip.pubsub.subscribe('pubsub', topic, callback)

        # Load Equipment Tree:
        for c in self.vip.config.list():
            if 'devices/' in c[:8]:
                equipment_config = self.vip.config.get(c)
                self._configure_new_equipment(c, 'NEW', equipment_config, schedule_now=False)

        # Schedule Polling
        self.poll_schedulers = PollScheduler.setup(self.equipment_tree, self.config.groups)
        for poll_scheduler in self.poll_schedulers.values():
            poll_scheduler.schedule()

        # Set up All Publishes:
        self._start_all_publishes()

    def _separate_equipment_configs(self, config_dict: dict[str, Any]
                                    ) -> tuple[RemoteConfig, DeviceConfig | None, list[dict[str, Any]]]:
        # Separate remote_config and make adjustments for possible config version 1:
        remote_config_dict: dict[str, Any] = config_dict.pop('remote_config', config_dict.pop('driver_config', {}))
        remote_config_dict['driver_type'] = remote_config_dict.get('driver_type', config_dict.pop('driver_type', None))
        remote_config_dict['heart_beat_point'] = remote_config_dict.get('heart_beat_point',
                                                              config_dict.pop('heart_beat_point', None))
        remote_config: RemoteConfig = RemoteConfig(**remote_config_dict)
        if remote_config.driver_type:
            # Received new device node.
            registry_config = config_dict.pop('registry_config', [])
            registry_config = registry_config if registry_config is not None else []
            dev_config = DeviceConfig(**config_dict)

            point_configs = []
            # Set up any point nodes which are children of this device.
            for reg in registry_config:
                # If there are fields in device config for all registries, add them where they are not overridden:
                for k, v in dev_config.equipment_specific_fields.items():
                    if not reg.get(k):
                        reg[k] = v
                point_configs.append(reg)

        else:
            dev_config, point_configs = None, []
        return remote_config, dev_config, point_configs

    def _configure_new_equipment(self, equipment_name: str, _, contents: dict, schedule_now: bool = True) -> bool:
        existing_node = self.equipment_tree.get_node(equipment_name)
        if existing_node:
            if not existing_node.config_finished:
                existing_node.config_finished = True
                return False
            else:
                return self._update_equipment(equipment_name, 'UPDATE', contents)
        try:
            remote_config, dev_config, registry_configs = self._separate_equipment_configs(contents)
            if dev_config:
                # Received new device node.
                remote = self._get_or_create_remote(equipment_name, remote_config, dev_config.allow_duplicate_remotes)
                validated_reg_configs = (remote.interface.REGISTER_CONFIG_CLASS(**r) for r in registry_configs)
                device_node = self.equipment_tree.add_device(device_topic=equipment_name, dev_config=dev_config,
                                                             remote=remote, registry_configs=validated_reg_configs)
                remote.add_equipment(device_node)
            else: # Received new or updated segment node.
                equipment_config = EquipmentConfig(**contents)
                self.equipment_tree.add_segment(equipment_name, equipment_config)
            if schedule_now:
                points = self.equipment_tree.points(equipment_name)
                self._update_polling_schedules(points)
            return True
        except ValueError as e:
            _log.warning(f'Skipping configuration of equipment: {equipment_name} after encountering error --- {e}')
            return False

    def _get_or_create_remote(self, equipment_name: str, remote_config: RemoteConfig, allow_duplicate_remotes: bool,
                              is_update: bool = False):
        interface = self._get_configured_interface(remote_config)

        if isinstance(remote_config, RemoteConfig):
            interface_config_class = getattr(interface, 'INTERFACE_CONFIG_CLASS', RemoteConfig)
            default_config = getattr(interface, 'default_config', {})
            if not isinstance(default_config, dict):
                default_config = {}
            config_dump = remote_config.model_dump()
            if interface_config_class is not RemoteConfig:
                interface_config = interface_config_class(**(default_config | config_dump))
            else:
                interface_config = remote_config
        else:
            interface_config = remote_config

        allow_duplicate_remotes = True if (allow_duplicate_remotes or self.config.allow_duplicate_remotes) else False
        if not allow_duplicate_remotes:
            unique_remote_id = interface.unique_remote_id(equipment_name, interface_config)
        else:
            unique_remote_id = BaseInterface.unique_remote_id(equipment_name, interface_config)

        remote = self.equipment_tree.remotes.get(unique_remote_id)
        if not remote:
            remote = DriverAgent(interface_config, self.core, self.equipment_tree, self.scalability_test,
                                       self.config.timezone, unique_remote_id, self.vip)
            self.equipment_tree.remotes[unique_remote_id] = remote
        elif not is_update and remote.config != remote.interface.INTERFACE_CONFIG_CLASS(**interface_config.model_dump()):
            # TODO: Can we support some settings being different between two groupings on same remote?
            #       e.g., two groups with different cov_lifetime intervals?
            #       (This doesn't affect remote itself, but is an interface specific configuration.)
            #       Can this use case already be accomodated using groups somehow?
            _log.warning(f'Remote configuration for equipment "{equipment_name}" does not match configuration'
                         f' of shared remote "{unique_remote_id}. Check configurations for consistency or consider'
                         f' setting "allow_duplicate_remotes == True".')
        return remote

    def _get_configured_interface(self, remote_config: RemoteConfig):
        driver_type = 'fake' if remote_config.driver_type == 'fakedriver' else remote_config.driver_type
        interface = self.interface_classes.get(driver_type)
        if not interface:
            try:
                module = remote_config.module
                interface = BaseInterface.get_interface_subclass(driver_type, module)
                if interface.default_config is None:
                    try:
                        interface.default_config = self.vip.config.get(f'interfaces/{driver_type}')
                    except KeyError:  # TODO: Can this even raise KeyError? It will be None or and Attribute Error, no?
                        interface.default_config = {}
            except (AttributeError, ModuleNotFoundError, ValueError) as e:
                raise ValueError(f'Unable to configure driver with interface: {driver_type}.'
                                 f' This interface type is currently unknown or not installed.'
                                 f' Received exception: {e}')
            self.interface_classes[driver_type] = interface
        return interface

    def _update_equipment(self, config_name: str, _, contents: dict) -> bool:
        """Callback for updating equipment configuration."""
        remote_config, dev_config, registry_configs = self._separate_equipment_configs(contents)
        if dev_config:
            try:
                remote = self._get_or_create_remote(config_name, remote_config, dev_config.allow_duplicate_remotes, True)
            except ValueError as e:
                _log.warning(f'Skipping configuration of equipment: {config_name} after encountering error --- {e}')
                return False
            if remote.config != remote.interface.INTERFACE_CONFIG_CLASS(**remote_config.model_dump()):
                # TODO: How do we reconcile potential differences with other users of this remote?
                #       Currently, we are just going to change things out from underneath them.
                pass
        else:
            remote = None
        validated_reg_configs = [remote.interface.REGISTER_CONFIG_CLASS(**r) for r in registry_configs]
        is_changed = self.equipment_tree.update_equipment(config_name, dev_config, remote, validated_reg_configs)
        if is_changed:
            points = self.equipment_tree.points(config_name)
            self._update_polling_schedules(points)
        return is_changed

    def _update_polling_schedules(self, points):
        reschedules_required, new_groups = [], []
        for point in points:
            group = self.equipment_tree.get_group(point.identifier)
            if group not in self.poll_schedulers:
                new_groups.append(group)
            if PollScheduler.add_to_schedule(point, self.equipment_tree):
                reschedules_required.append(group)
        if new_groups:
            self.poll_schedulers.update(PollScheduler.create_poll_schedulers(self.equipment_tree, self.config.groups,
                                                                             new_groups, len(self.poll_schedulers)))
        for updated_group in reschedules_required:
            self.poll_schedulers[updated_group].schedule()

    def _remove_equipment(self, config_name: str, _, __, leave_disconnected=False) -> bool:
        """Callback to remove equipment configuration."""
        poll_schedulers = []
        for point in self.equipment_tree.points(config_name):
            group = self.equipment_tree.get_group(point.identifier)
            poll_schedulers.append(self.poll_schedulers.get(group))
        removed_count = self.equipment_tree.remove_segment(config_name, leave_disconnected)
        # TODO: Add reschedule_all_on_update option and reschedule all poll_schedulers when true.
        return True if removed_count > 0 else False

    def _start_all_publishes(self):
        # TODO: Can we just schedule and let the stale property work its magic?
        for device in self.equipment_tree.devices(self.equipment_tree.root):
            if (device.all_publish_interval and
                    (self.equipment_tree.is_published_all_depth(device.identifier) or
                     self.equipment_tree.is_published_all_breadth(device.identifier))):
                # Schedule first publish at end of first polling cycle to guarantee all points should have data.
                start_all_datatime = max(poller.start_all_datetime for poller in self.poll_schedulers.values())
                self.publishers[device] = self.core.schedule(
                    periodic(device.all_publish_interval, start=start_all_datatime), self._all_publish, device
                )

    def _all_publish(self, node):
        if not node:
            _log.warning(f'All publish running for a device node which no longer exists.')
        active_points = self.equipment_tree.active_points(node)
        if not ((ready_points := self.equipment_tree.ready_points(node))
                or (self.equipment_tree.strict_all_publishes(node.identifier))
                    and active_points != ready_points):
            _log.info(f'Skipping all publish of device: {node.identifier}. Data is not yet ready.')
            return
        if not ((non_stale_points := self.equipment_tree.non_stale_points(ready_points))
                or (self.equipment_tree.strict_all_publishes(node.identifier))
                    and active_points != non_stale_points):
            _log.warning(f'Skipping all publish of device: {node.identifier}. Data is stale.')
            return
        else:
            headers = publication_headers()
            depth_topic, breadth_topic = self.equipment_tree.get_device_topics(node.identifier)
            if self.equipment_tree.is_published_all_depth(node.identifier):
                publish_wrapper(self.vip, f'{depth_topic}/all', headers=headers, message=[
                    {p.identifier.rsplit('/', 1)[-1]: p.last_value for p in non_stale_points},
                    {p.identifier.rsplit('/', 1)[-1]: p.meta_data for p in non_stale_points}
                ])
            elif self.equipment_tree.is_published_all_breadth(node.identifier):
                publish_wrapper(self.vip, f'{breadth_topic}/all', headers=headers, message=[
                    {p.identifier.rsplit('/', 1)[-1]: p.last_value for p in non_stale_points},
                    {p.identifier.rsplit('/', 1)[-1]: p.meta_data for p in non_stale_points}
                ])

    ###############
    # Query Backend
    ###############

    def semantic_query(self, query):
        """ Resolve tags from tagging service. """
        try:
            return self.vip.rpc.call('platform.semantic', 'semantic_query', query).get(timeout=5)
        except gevent.Timeout as e:
            _log.warning(f'Semantic Interoperability Service timed out: {e.exception}')
            return {}

    def build_query_plan(self, topic: str | Sequence[str] | Set[str] = None,
                         regex: str = None) -> dict[DriverAgent, Set[PointNode]]:
        """ Find points to be queried and organize by remote."""
        exact_matches, topic = (topic, None) if isinstance(topic, list) or isinstance(topic, set) else ([], topic)
        query_plan = defaultdict(set)
        for p in self.equipment_tree.find_points(topic, regex, exact_matches):
            query_plan[self.equipment_tree.get_remote(p.identifier)].add(p)
        return query_plan

    ###############
    # RPC Interface
    ###############

    @RPC.export
    def get(self, topic: str | Sequence[str] | Set[str] = None, regex: str = None) -> tuple[dict, dict]:
        # Find set of points to query and organize by remote:
        query_plan = self.build_query_plan(topic, regex)
        return self._get(query_plan)

    @RPC.export
    def semantic_get(self, query: str) -> tuple[dict, dict]:
        exact_matches = self.semantic_query(query)
        query_plan = self.build_query_plan(exact_matches)
        return self._get(query_plan)

    def _get(self, query_plan: dict[DriverAgent, Set[PointNode]]):
        """Make query for selected points on each remote"""
        results, errors = {}, {}
        for (remote, point_set) in query_plan.items():
            q_return_values, q_return_errors = remote.get_multiple_points([p.identifier for p in point_set])
            for topic, val in q_return_values.items():
                node = self.equipment_tree.get_node(topic)
                if node:
                    node.last_value = val
            results.update(q_return_values)
            errors.update(q_return_errors)
        return results, errors

    @RPC.export
    def set(self, value: Any, topic: str | Sequence[str] | Set[str] = None, regex: str = None,
            confirm_values: bool = False, map_points: bool = False) -> tuple[dict, dict]:
        query_plan = self.build_query_plan(topic, regex)
        return self._set(value, query_plan, confirm_values, map_points)

    @RPC.export
    def semantic_set(self, value: Any, query: str, confirm_values: bool = False) -> tuple[dict, dict]:
        exact_matches = self.semantic_query(query)
        query_plan = self.build_query_plan(exact_matches)
        return self._set(value, query_plan, confirm_values)

    def _set(self, value: Any, query_plan: dict[DriverAgent, Set[PointNode]], confirm_values: bool, map_points=False
             ) -> tuple[dict, dict]:
        """Set selected points on each remote"""
        results, errors = {}, {}
        sender = self.vip.rpc.context.vip_message.peer
        for (remote, point_set) in query_plan.items():
            for point in point_set:
                try:
                    self.equipment_tree.raise_on_locks(point, sender)
                except:
                    pass # TODO: Handle this exception.
            # TODO: When map_points is True, all topics are sent to all remotes. This is probably wrong.
            point_value_tuples = list(value.items()) if map_points else [(p.identifier, value) for p in point_set]
            query_return_results, query_return_errors = remote.set_multiple_points(point_value_tuples)
            results.update(query_return_results)
            errors.update(query_return_errors)
            if confirm_values:
                # TODO: Should results contain the values read back from the device, or Booleans for success?
                results.update(remote.get_multiple_points([p.identifier for p in point_set])[0])
        return results, errors

    @RPC.export
    def revert(self, topic: str | Sequence[str] | Set[str] = None, regex: str = None) -> dict[str, str]:
              # confirm_values: bool = False) -> dict:
        query_plan = self.build_query_plan(topic, regex)
        return self._revert(query_plan)  # , confirm_values)

    @RPC.export
    def semantic_revert(self, query: str) -> dict[str, str]:  #, confirm_values: bool = False) -> dict:
        exact_matches = self.semantic_query(query)
        query_plan = self.build_query_plan(exact_matches)
        return self._revert(query_plan)  #, confirm_values)

    @staticmethod
    def _revert(query_plan) -> dict[str, str]:  #, confirm_values: bool) -> dict[str, str]:
        """
        Revert each point from query.
          If an exception is raised, return it in the error dict.
        """
        # TODO: If it is possible to check values, we may need to do that at the interface level.
        #  No functionality exists for this now.
        errors = {}
        for (remote, point_set) in query_plan.items():
            for point in point_set:
                try:
                    remote.revert_point(point.identifier)
                except Exception as e:
                    # TODO: revert_point may not raise. Does _set_point, typically?  If we make them raise,
                    #  we can return some errors, at least. It may not be possible to check success in all cases.
                    errors[point.identifier] = str(e)
        return errors

    @RPC.export
    def last(self, topic: str | Sequence[str] | Set[str] = None, regex: str = None,
             value: bool = True, updated: bool = True) -> dict:
        points = self.equipment_tree.find_points(topic, regex)
        return self._last(points, value, updated)

    @RPC.export
    def semantic_last(self, query: str, value: bool = True, updated: bool = True) -> dict:
        # 1. Get the list of topic strings
        exact_matches = self.semantic_query(query)

        # 2. Convert those topic strings into point node objects
        #    For example, if you have a helper method to do this:
        points = self.equipment_tree.find_points(exact_matches)

        # 3. Pass the point objects to _last
        return self._last(points, value, updated)

    @staticmethod
    def _last(points: Iterable[PointNode], value: bool, updated: bool):
        if value:
            if updated:
                return_dict = {p.topic: {
                    'value': p.last_value,
                    'updated': (p.last_updated.isoformat() if p.last_updated else None)} for p in points
                }
            else:
                return_dict = {p.topic: p.last_value for p in points}
        else:
            return_dict = {p.topic: (p.last_updated.isoformat() if p.last_updated else None) for p in points}
        return return_dict

    @RPC.export
    def call(self, method: str, topic: str | Sequence[str] | Set[str] = None, regex: str = None,
             *args, **kwargs) -> tuple[dict, dict]:
        # Find set of points to query and organize by remote:
        query_plan = self.build_query_plan(topic, regex)
        return self._call(query_plan, method, *args, **kwargs)

    @RPC.export
    def semantic_call(self, method: str, query: str, *args, **kwargs) -> tuple[dict, dict]:
        exact_matches = self.semantic_query(query)
        query_plan = self.build_query_plan(exact_matches)
        return self._call(query_plan, method, *args, **kwargs)

    @staticmethod
    def _call(query_plan: dict[DriverAgent, Set[PointNode]], method: str, *args, **kwargs):
        """Make query for selected points on each remote"""
        results, errors = {}, {}
        for (remote, point_set) in query_plan.items():
            q_return_values, q_return_errors = remote.call(method, [p.identifier for p in point_set], *args, **kwargs)
            results.update(q_return_values)
            errors.update(q_return_errors)
        return results, errors

    #-----------
    # UI Support
    #-----------
    @RPC.export
    def start(self, topic: str | Sequence[str] | Set[str] = None, regex: str = None) -> None:
        points = self.equipment_tree.find_points(topic, regex)
        self._start(points)

    @RPC.export
    def semantic_start(self, query: str) -> None:
        exact_matches = self.semantic_query(query)
        points = self.equipment_tree.find_points(exact_matches)
        self._start(points)

    def _start(self, points: Iterable[PointNode]) -> None:
        updates_required = []
        for p in points:
            if self.equipment_tree.is_active(p.identifier):
                continue
            else:
                p.active = True
                updates_required.append(p)
    # TODO: Add reschedule_all_on_update option and reschedule all poll_schedulers when true.
        if updates_required:
            self._update_polling_schedules(updates_required)

    @RPC.export
    def stop(self, topic: str | Sequence[str] | Set[str] = None, regex: str = None) -> None:
        points = self.equipment_tree.find_points(topic, regex)
        self._stop(points)

    @RPC.export
    def semantic_stop(self, query: str) -> None:
        topics = self.semantic_query(query)
        points = self.equipment_tree.find_points(topics)
        self._stop(points)

    def _stop(self, points: Iterable[PointNode]) -> None:
        for p in points:
            if not self.equipment_tree.is_active(p.identifier):
                continue
            else:
                p.active = False
                group = self.equipment_tree.get_group(p.identifier)
                self.poll_schedulers[group].remove_from_schedule(p, self.equipment_tree)
        # TODO: Add reschedule_all_on_update option and reschedule all poll_schedulers when true.

    @RPC.export
    def enable(self, topic: str | Sequence[str] | Set[str] = None, regex: str = None) -> None:
        nodes = self.equipment_tree.resolve_query(topic, regex)
        self._enable(nodes)

    @RPC.export
    def semantic_enable(self, query: str) -> None:
        topics = self.semantic_query(query)
        points = self.equipment_tree.resolve_query(topics)
        self._enable(points)

    def _enable(self, nodes: Iterable[DeviceNode | EquipmentNode | PointNode]):
        for node in nodes:
            node.config.active = True
            if not node.is_point:
                new_config = node.config.model_dump()
                if node.is_device:
                    self._add_fields_to_device_configuration_for_save(new_config, node)
                self.vip.config.set(node.identifier, new_config, trigger_callback=False)
            else:
                self.equipment_tree.update_stored_registry_config(node.identifier)

    @RPC.export
    def disable(self, topic: str | Sequence[str] | Set[str] = None, regex: str = None) -> None:
        nodes = self.equipment_tree.resolve_query(topic, regex)
        self._disable(nodes)

    @RPC.export
    def semantic_disable(self, query: str) -> None:
        topics = self.semantic_query(query)
        points = self.equipment_tree.resolve_query(topics)
        self._disable(points)

    def _disable(self, nodes: Iterable[DeviceNode | EquipmentNode | PointNode]) -> None:
        for node in nodes:
            node.config.active = False
            if not node.is_point:
                new_config = node.config.model_dump()
                if node.is_device:
                    self._add_fields_to_device_configuration_for_save(new_config, node)
                self.vip.config.set(node.identifier, new_config, trigger_callback=False)
            else:
                self.equipment_tree.update_stored_registry_config(node.identifier)

    def _add_fields_to_device_configuration_for_save(self, new_config, node):
        registry_name = node.registry_name
        if not registry_name or not (registry_name := self.equipment_tree.set_registry_name(node.identifier)):
            raise Exception(f'Unable to set configuration for device node {node.identifier}.'
                            f' Registry name is unknown and cannot be determined.')
        # TODO: This assumes that the registry was originally provided as a separate file.
        #  We should detect this and modify the file or dict that was originally configured.
        new_config['registry_config'] = f'config://{registry_name}'
        new_config['remote_config'] = self.equipment_tree.get_remote(node.identifier).config.model_dump()

    @RPC.export
    def status(self, topic: str | Sequence[str] | Set[str] = None, regex: str = None) -> dict:
        nodes = self.equipment_tree.find_points(topic, regex)
        return self._status(nodes)

    @RPC.export
    def semantic_status(self, query: str) -> dict:
        topics = self.semantic_query(query)
        points = self.equipment_tree.find_points(topics)
        return self._status(points)

    def _status(self, points: Iterable[PointNode]) -> dict:
        raise NotImplementedError('status is not yet implemented.')
        # # TODO: Implement _status()
        # return {'error': 'Status reporting is not yet implemented'}

    @RPC.export
    def add_node(self, node_topic: str, config: dict, update_schedule: bool = True) -> bool:
        # TODO: Need logic to determine if this is a point. Configure_new_equipment should not be used if it is.
        return self._configure_new_equipment(node_topic, 'NEW', contents=config, schedule_now=update_schedule)

    @RPC.export
    def remove_node(self, node_topic: str, leave_disconnected: bool = False) -> bool:
        return self._remove_equipment(node_topic, None, None, leave_disconnected)

    @RPC.export
    def add_interface(self, interface_name: str, force: bool = False, pre_release: bool = False) -> bool:
        # TODO: vdrv might want to get the absolute path before calling this, if it is a path.
        interface_path = Path(interface_name)
        if interface_path.is_dir():
            with open(interface_path / 'pyproject.toml', 'rb') as f:
                ppt = tomllib.load(f)
            package_name, install_path = ppt['tool']['poetry']['name'], str(interface_path)
        elif interface_path.is_file() and interface_path.suffix == ".whl":
            package_name, install_path = str(interface_path.stem).split('-')[0], str(interface_path)
        else:  # Use package name to get it from PyPI.
            package_name = install_path = self._interface_package_from_short_name(interface_name)
        try:
            distribution('volttron-core')
            from functools import partial
            # TODO: This method should probably be moved at least mostly into vctl,
            #  and it should call the control service directly.
            #  Otherwise, this may try to package a directory which is on the client here on the server.
            class Conn:
                call = partial(self.vip.rpc.call, 'platform.control')
            arguments = Namespace(connection=Conn(), install_path=install_path,
                                  force=force, pre_release=pre_release)
            try:
                with redirect_stdout(st_out := StringIO()):
                    install_lib_vctl(arguments)
                success = True if st_out.getvalue().startswith('Installed') else False
            except ValueError as e:
                _log.warning(f'Failed to install interface "{interface_name}": {e}')
                success = False
        except PackageNotFoundError:  # Monolithic does not have install-lib. Handle this manually.
            try:
                subprocess.run(['pip', 'install', '--no-deps', str(install_path)],
                               check=True, capture_output=True)
                exclude_packages = ['python', 'volttron-core', 'volttron-lib-base-driver']
                if deps := [d for d in requires(package_name) if d.split(' ')[0] not in exclude_packages]:
                    subprocess.run((['pip', 'install', *deps]), check=True, capture_output=True)
                success = True
            except subprocess.CalledProcessError as e:
                _log.warning(f'Failed to install interface "{interface_name}": {e.stderr}')
                success = False
        if success:
            _log.info(f'Successfully installed {interface_name} driver interface.')
        return success

    @RPC.export
    def list_interfaces(self) -> list[str]:
        """Return list of all installed driver interfaces."""
        try:
            from volttron.driver import interfaces
            return [i.name for i in iter_modules(interfaces.__path__)]
        except ImportError:
            return []

    @RPC.export
    def remove_interface(self, interface_name: str) -> bool:
        interface_package = self._interface_package_from_short_name(interface_name)
        try:
            distribution('volttron-core')
            subprocess.run(['poetry', '--directory', 'remove', interface_package], capture_output=True)
        except PackageNotFoundError:
            subprocess.run([sys.executable, '-m', 'pip', 'uninstall', interface_package, '--no-input'],
                           capture_output=True)
        sp_result = subprocess.run([sys.executable, '-m', 'pip', 'show', interface_package], capture_output=True)
        return True if sp_result.returncode else False

    @RPC.export
    def list_topics(self, topic: str, regex: str = None,
                    active: bool = False, enabled: bool = False) -> list[str]:
        # TODO: Semantic version?
        topic = topic.strip('/') if topic and topic.startswith(self.equipment_tree.root) else self.equipment_tree.root
        parent = topic if self.equipment_tree.get_node(topic) else topic.rsplit('/', 1)[0]
        children = self.equipment_tree.children(parent)
        regex = re.compile(regex) if regex else None
        if regex:
            children = [c for c in children if regex.search(c)]
        if active:
            children = [c for c in children if self.equipment_tree.is_active(c.identifier)]
        if enabled:
            children = [c for c in children if c.enabled]
        return [c.identifier for c in children]

    @RPC.export
    def get_poll_schedule(self, full_topics=False):
        return {group: scheduler.get_schedule(full_topics) for group, scheduler in self.poll_schedulers.items()}

    @RPC.export
    def export_equipment_tree(self):
        return self.equipment_tree.to_json(with_data=True)

    #-------------
    # Reservations
    #-------------
    # TODO: Improve the Reservations and Overrides API:
    # @RPC.export
    # def new_reservation(self, task_id: str, priority: str, requests: list) -> dict|None:
    #     """
    #     Reserve one or more blocks on time on one or more device.
    #
    #     :param task_id: An identifier for this reservation.
    #     :param priority: Priority of the task. Must be either "HIGH", "LOW",
    #     or "LOW_PREEMPT"
    #     :param requests: A list of time slot requests in the format
    #     described in `Device Schedule`_.
    #     """
    #     rpc_peer = self.vip.rpc.context.vip_message.peer
    #     return self.reservation_manager.new_task(rpc_peer, task_id, priority, requests)  #, publish_result=False)
    #
    # @RPC.export
    # def cancel_reservation(self, task_id: str) -> dict|None:
    #     """
    #     Requests the cancellation of the specified task id.
    #     :param task_id: Task name.
    #     """
    #     rpc_peer = self.vip.rpc.context.vip_message.peer
    #     return self.reservation_manager.cancel_task(rpc_peer, task_id)  # , publish_result=False)

    #----------
    # Overrides
    #----------
    @RPC.export
    def set_override_on(self, pattern: str, duration: float = 0.0,
                        failsafe_revert: bool = True, staggered_revert: bool = False):
        """RPC method

        Turn on override condition on all the devices matching the pattern.
        :param pattern: Override pattern to be applied. For example,
            If pattern is campus/building1/* - Override condition is applied for all the devices under
            campus/building1/.
            If pattern is campus/building1/ahu1 - Override condition is applied for only campus/building1/ahu1
            The pattern matching is based on bash style filename matching semantics.
        :type pattern: str
        :param duration: Time duration for the override in seconds. If duration <= 0.0, it implies as indefinite
        duration.
        :type duration: float
        :param failsafe_revert: Flag to indicate if all the devices falling under the override condition has to be set
         to its default state/value immediately.
        :type failsafe_revert: boolean
        :param staggered_revert: If this flag is set, reverting of devices will be staggered.
        :type staggered_revert: boolean
        """
        self.override_manager.set_on(pattern, duration, failsafe_revert, staggered_revert)

    @RPC.export
    def set_override_off(self, pattern: str):
        """RPC method

        Turn off override condition on all the devices matching the pattern. The pattern matching is based on bash style
        filename matching semantics.
        :param pattern: Pattern on which override condition has to be removed.
        :type pattern: str
        """
        return self.override_manager.set_off(pattern)

    # Get a list of all the devices with override condition.
    @RPC.export
    def get_override_devices(self):
        """RPC method

        Get a list of all the devices with override condition.
        """
        return list(self.override_manager.devices)

    @RPC.export
    def clear_overrides(self):
        """RPC method

        Clear all overrides.
        """
        self.override_manager.clear()

    @RPC.export
    def get_override_patterns(self):
        """RPC method

        Get a list of all the override patterns.
        """
        return list(self.override_manager.patterns)

    #-------------------
    # Legacy RPC Methods
    #-------------------
    @RPC.export
    def get_point(self, path: str = None, point_name: str = None, **kwargs) -> Any:
        """
        RPC method

        Gets up-to-date value of a specific point on a device.
        Does not require the device be scheduled.

        :param path: The topic of the point to grab in the
                      format <device topic>/<point name>

                      Only the <device topic> if point is specified.
        :param point_name: Point on the device. Assumes topic includes point name if omitted.
        :param kwargs: Any driver specific parameters
        :type path: str
        :returns: point value
        :rtype: any base python type"""

        # Support for old-actuator-style keyword arguments.
        path = path if path else kwargs.get('topic', None)
        point_name = point_name if point_name else kwargs.get('point', None)
        if path is None:
            # DEPRECATED: Only allows topic to be None to permit use of old-actuator-style keyword argument "topic".
            raise TypeError('Argument "path" is required.')

        point_name = self._equipment_id(path, point_name)
        node = self.equipment_tree.get_node(point_name)
        if not node:
            raise ValueError(f'No equipment found for topic: {point_name}')
        remote = self.equipment_tree.get_remote(node.identifier)
        if not remote:
            raise ValueError(f'No remote found for topic: {point_name}')
        return remote.get_point(point_name, **kwargs)

    @RPC.export
    def set_point(self, path: str, point_name: str | None, value: Any, *args, **kwargs) -> Any:
        """RPC method

        Sets the value of a specific point on a device.
        Requires the device be scheduled by the calling agent.

        :param path: The topic of the point to set in the
                      format <device topic>/<point name>
                      Only the <device topic> if point is specified.
        :param value: Value to set point to.
        :param point_name: Point on the device.
        :param kwargs: Any driver specific parameters
        :type path: str
        :type value: any basic python type
        :type point_name: str
        :returns: value point was actually set to. Usually invalid values
                cause an error but some drivers (MODBUS) will return a
                different
                value with what the value was actually set to.
        :rtype: any base python type

        .. warning:: Calling will raise a ReservationLockError if another agent has already scheduled
        this device for the present time."""

        sender = self.vip.rpc.context.vip_message.peer

        # Support for old-actuator-style arguments.
        topic = kwargs.get('topic')
        if topic:
            path = topic
        elif path == sender or len(args) > 0:
            # Function was likely called with actuator-style positional arguments. Reassign variables to match.
            _log.info('Deprecated actuator-style positional arguments detected in set_point().'
                      ' Please consider converting code to use set() method.')
            path, point_name = (point_name, args[0]) if len(args) >= 1 else point_name, None
        point_name = point_name if point_name else kwargs.get('point', None)

        point_name = self._equipment_id(path, point_name)
        return self._set_point(point_name, value, sender, **kwargs)

    def _set_point(self, topic, value, sender, **kwargs):
        node: EquipmentNode = self.equipment_tree.get_node(topic)
        if not node:
            raise ValueError(f'No equipment found for topic: {topic}')
        self.equipment_tree.raise_on_locks(node, sender)
        remote = self.equipment_tree.get_remote(node.identifier)
        if not remote:
            raise ValueError(f'No remote found for topic: {topic}')
        result = remote.set_point(topic, value, **kwargs)
        headers = self._get_headers(sender)
        self._push_result_topic_pair(WRITE_ATTEMPT_PREFIX, topic, headers, value)
        self._push_result_topic_pair(VALUE_RESPONSE_PREFIX, topic, headers, result)
        return result

    @RPC.export
    def scrape_all(self, topic: str) -> dict:
        """RPC method

        Get all points from a device.

        :param topic: Device topic
        :returns: Dictionary of points to values
        """
        _log.info('Call to deprecated RPC method "scrape_all". This method has been superseded by the "get" method'
                  ' and will be removed in a future version. Please update to the newer method.')
        path = self._equipment_id(topic, None)
        return self.get(topic=path)[0]

    @RPC.export
    def get_multiple_points(self, path: str | Sequence[str | Sequence] = None, point_names = None,
                            **kwargs) -> tuple[dict, dict]:
        """RPC method

        Get multiple points on multiple devices. Makes a single
        RPC call to the platform driver per device.

        :param path: A topic (with or without point names), a list of full topics (with point names),
         or a list of [device, point] pairs.
        :param point_names: A Sequence of point names associated with the given path.
        :param kwargs: Any driver specific parameters

        :returns: Dictionary of points to values and dictionary of points to errors

        .. warning:: This method does not require that all points be returned
                     successfully. Check that the error dictionary is empty.
        """

        # Support for actuator-style keyword arguments.
        topics = path if path else kwargs.get('topics', None)
        if topics is None:
            # path is allowed to be None to permit use of old-actuator-style keyword argument "topics".
            raise TypeError('Argument "path" is required.')

        errors = {}
        devices = set()
        if isinstance(topics, str):
            if not point_names:
                devices.add(topics)
            else:
                for point in point_names:
                    devices.add(self._equipment_id(topics, point))
        elif isinstance(topics, Sequence):
            for topic in topics:
                if isinstance(topic, str):
                    devices.add(self._equipment_id(topic))
                elif isinstance(topic, Sequence) and len(topic) == 2:
                    devices.add(self._equipment_id(*topic))
                else:
                    e = ValueError("Invalid topic: {}".format(topic))
                    errors[repr(topic)] = repr(e)

        results, query_errors = self.get(devices)
        errors.update(query_errors)
        return results, errors

    @RPC.export
    def set_multiple_points(self, path: str, point_names_values: list[tuple[str, Any]], **kwargs) -> dict:
        """RPC method

        Set values on multiple set points at once. If global override is condition is set,raise OverrideError exception.
        :param path: device path
        :type path: str
        :param point_names_values: list of points and corresponding values
        :type point_names_values: list of tuples
        :param kwargs: additional arguments for the device
        :type kwargs: arguments pointer
        """
        errors = {}
        topic_value_map = {}
        sender = self.vip.rpc.context.vip_message.peer
        # Support for old-actuator-style positional arguments so long as sender matches rpc peer.
        topics_values = kwargs.get('topics_values')
        if path == sender or topics_values is not None:  # Method was called with old-actuator-style arguments.
            topics_values = topics_values if topics_values else point_names_values
            for topic, value in topics_values:
                if isinstance(topic, str):
                    topic_value_map[self._equipment_id(topic, None)] = value
                elif isinstance(topic, Sequence) and len(topic) == 1:
                    topic_value_map[self._equipment_id(*topic)] = value
                else:
                    e = ValueError("Invalid topic: {}".format(topic))
                    errors[str(topic)] = repr(e)
        else:  # Assume method was called with old-driver-style arguments.
            for point, value in point_names_values:
                topic_value_map[self._equipment_id(path, point)] = value

        _, ret_errors = self.set(topic_value_map, map_points=True, **kwargs)
        errors.update(ret_errors)
        return errors

    def heart_beat(self):
        """
        Sends heartbeat to all devices
        """
        # TODO: Move this into the PollScheduler with configurable (per device) set of points and intervals (per-point).
        # TODO: config.heart_beat_point should be a set for each remote.
        while True:
            if not self.equipment_tree.remotes:
                gevent.sleep(self.config.remote_heartbeat_interval)
                continue
            for remote in self.equipment_tree.remotes.values():
                if self._stop_agent.is_set():
                    return
                try:
                    remote.heart_beat()
                except (Exception, gevent.Timeout) as e:
                    _log.warning(f'Failed to set heart_beat point on remote: {remote.unique_id} -- {e}.')
                finally:
                    gevent.sleep(self.config.remote_heartbeat_interval / len(self.equipment_tree.remotes))

    @RPC.export
    def revert_point(self, path: str, point_name: str, **kwargs):
        """RPC method

        Revert the set point to default state/value.
        If global override is condition is set, raise OverrideError exception.
        If topic has been reserved by another user
        or if it is not reserved but reservations are required,
         raise ReservationLockError exception.
        :param path: device path
        :type path: str
        :param point_name: set point to revert
        :type point_name: str
        :param kwargs: additional arguments for the device
        :type kwargs: arguments pointer
        """
        sender = self.vip.rpc.context.vip_message.peer

        # Support for old-actuator-style arguments.
        topic = kwargs.get('topic')
        if topic:
            path, point_name = topic, None
        elif path == sender:
            # Function was likely called with actuator-style positional arguments. Reassign variables to match.
            _log.info('Deprecated actuator-style positional arguments detected in revert_point().'
                      ' Please consider converting code to use revert() method.')
            path, point_name = point_name, None

        equip_id = self._equipment_id(path, point_name)
        node = self.equipment_tree.get_node(equip_id)
        if not node:
            raise ValueError(f'No equipment found for topic: {equip_id}')
        self.equipment_tree.raise_on_locks(node, sender)
        remote = self.equipment_tree.get_remote(node.identifier)
        remote.revert_point(equip_id, **kwargs)

        headers = self._get_headers(sender)
        self._push_result_topic_pair(REVERT_POINT_RESPONSE_PREFIX, equip_id, headers, None)

    @RPC.export
    def revert_device(self, path: str, *args, **kwargs):
        """RPC method

        Revert all the set point values of the device to default state/values. If global override is condition is set,
        raise OverrideError exception.
        :param path: device path
        :type path: str
        :param kwargs: additional arguments for the device
        :type kwargs: arguments pointer
        """
        sender = self.vip.rpc.context.vip_message.peer

        # Support for old-actuator-style arguments.
        topic = kwargs.get('topic')
        if topic:
            path = topic
        elif path == sender and len(args) > 0:
            # Function was likely called with actuator-style positional arguments. Reassign variables to match.
            _log.info('Deprecated actuator-style positional arguments detected in revert_device().'
                      ' Please consider converting code to use revert() method.')
            path = args[0]

        self.revert(self._equipment_id(path, None))

        headers = self._get_headers(sender)
        self._push_result_topic_pair(REVERT_DEVICE_RESPONSE_PREFIX, path, headers, None)

    @RPC.export
    def request_new_schedule(self, _, task_id: str, priority: str,
                             requests: list[list[str]] | list[str], **__) -> dict:
        """
        RPC method

        Requests one or more blocks on time on one or more device.

        :param _: formerly requester_id -- now ignored, VIP Identity used internally
        :param task_id: Task name.
        :param priority: Priority of the task. Must be either "HIGH", "LOW",
        or "LOW_PREEMPT"
        :param requests: A list of time slot requests in the format
        described in `Device Schedule`_.

        :type priority: str
        :returns: Request result
        :rtype: dict

        :return Values:

            The return values are described in `New Task Response`_.
        """
        # _log.info('Call to deprecated RPC method "request_new_schedule". '
        #            'This method provides compatability with the actuator API, but has been superseded '
        #            'by "new_reservation". Please update to the newer method.')
        rpc_peer = self.vip.rpc.context.vip_message.peer
        return self.reservation_manager.new_task(rpc_peer, task_id, priority, requests)

    @RPC.export
    def request_cancel_schedule(self, _, task_id: str, **__) -> dict:
        """RPC method

        Requests the cancellation of the specified task id.

        :param _: formerly requester_id -- now ignored, VIP Identity used internally
        :param task_id: Task name.

        :returns: Request result
        :rtype: dict

        :return Values:

        The return values are described in `Cancel Task Response`_.

        """
        # _log.info('Call to deprecated RPC method "request_cancel_schedule". '
        #            'This method provides compatability with the actuator API, but has been superseded '
        #            'by "cancel_reservation". Please update to the newer method.')
        rpc_peer = self.vip.rpc.context.vip_message.peer
        return self.reservation_manager.cancel_task(rpc_peer, task_id)

    ##################
    # PubSub Interface
    ##################

    def handle_get(self, _, sender: str, __, topic: str, ___, ____):
        """
        Requests up-to-date value of a point.

        To request a value publish a message to the following topic:

        ``devices/actuators/get/<device path>/<actuation point>``

        with the fallowing header:

        .. code-block:: python

            {
                'requesterID': <Ignored, VIP Identity used internally>
            }

        The ActuatorAgent will reply on the **value** topic
        for the actuator:

        ``devices/actuators/value/<full device path>/<actuation point>``

        with the message set to the value the point.

        """
        point = topic.replace(GET_TOPIC + '/', '', 1)
        headers = self._get_headers(sender)
        try:
            value = self.get_point(point)
            self._push_result_topic_pair(VALUE_RESPONSE_PREFIX, point, headers, value)
        except Exception as ex:
            self._handle_error(ex, point, headers)

    def handle_set(self, _, sender: str, __, topic: str, ___, message: Any):
        """
        Set the value of a point.

        To set a value publish a message to the following topic:

        ``devices/actuators/set/<device path>/<actuation point>``

        with the fallowing header:

        .. code-block:: python

            {
                'requesterID': <Ignored, VIP Identity used internally>
            }

        The ActuatorAgent will reply on the **value** topic
        for the actuator:

        ``devices/actuators/value/<full device path>/<actuation point>``

        with the message set to the value the point.

        Errors will be published on

        ``devices/actuators/error/<full device path>/<actuation point>``

        with the same header as the request.

        """
        point = topic.replace(SET_TOPIC + '/', '', 1)
        headers = self._get_headers(sender)
        if not message:
            error = {'type': 'ValueError', 'value': 'missing argument'}
            _log.debug('ValueError: ' + str(error))
            self._push_result_topic_pair(ERROR_RESPONSE_PREFIX, point, headers, error)
            return

        try:
            equip_id = self._equipment_id(point)
            self._set_point(equip_id, message, sender, **{})
        except Exception as ex:
            self._handle_error(ex, point, headers)

    def handle_revert_point(self, _, sender: str, __, topic: str, ___, ____):
        """
        Revert the value of a point.

        To revert a value publish a message to the following topic:

        ``actuators/revert/point/<device path>/<actuation point>``

        with the following header:

        .. code-block:: python

            {
                'requesterID': <Ignored, VIP Identity used internally>
            }

        The ActuatorAgent will reply on

        ``devices/actuators/reverted/point/<full device path>/<actuation point>``

        to indicate that a point was reverted.

        Errors will be published on

        ``devices/actuators/error/<full device path>/<actuation point>``

        with the same header as the request.
        """
        topic = self._equipment_id(topic.replace(REVERT_POINT_TOPIC + '/', '', 1), None)
        headers = self._get_headers(sender)
        try:
            node = self.equipment_tree.get_node(topic)
            if not node:
                raise ValueError(f"No point node found for topic: {topic}")

            self.equipment_tree.raise_on_locks(node, sender)
            remote = self.equipment_tree.get_remote(node.identifier)
            if not remote:
                raise ValueError(f"No remote found for point: {topic}")

            remote.revert_point(topic)

            self._push_result_topic_pair(REVERT_POINT_RESPONSE_PREFIX, topic, headers, None)
        except Exception as ex:
            self._handle_error(ex, topic, headers)

    def handle_revert_device(self, _, sender: str, __, topic: str, ___, ____):
        """
        Revert all the writable values on a device.

        To revert a device publish a message to the following topic:

        ``devices/actuators/revert/device/<device path>``

        with the following header:

        .. code-block:: python

            {
                'requesterID': <Ignored, VIP Identity used internally>
            }

        The ActuatorAgent will reply on

        ``devices/actuators/reverted/device/<full device path>``

        to indicate that a device was reverted.

        Errors will be published on

        ``devices/actuators/error/<full device path>/<actuation point>``

        with the same header as the request.
        """
        topic = topic.replace(REVERT_DEVICE_TOPIC + '/', '', 1)
        topic = self._equipment_id(topic, None)
        headers = self._get_headers(sender)
        try:
            device_node = self.equipment_tree.get_node(topic)
            if not device_node:
                raise ValueError(f"No device node found for topic: {topic}")

            self.equipment_tree.raise_on_locks(device_node, sender)
            self.revert(device_node.identifier)

            self._push_result_topic_pair(REVERT_DEVICE_RESPONSE_PREFIX, topic, headers, None)

        except Exception as ex:
            self._handle_error(ex, topic, headers)

    def handle_reservation_request(self, _, sender: str, __, topic: str, headers: dict,
                                   message: list[list[str]] | list[str]):
        """
        Schedule request pub/sub handler

        An agent can request a task schedule by publishing to the
        ``devices/actuators/schedule/request`` topic with the following header:

        .. code-block:: python

            {
                'type': 'NEW_SCHEDULE',
                'requesterID': <Ignored, VIP Identity used internally>,
                'taskID': <unique task ID>, #The desired task ID for this
                task. It must be unique among all scheduled tasks.
                'priority': <task priority>, #The desired task priority,
                must be 'HIGH', 'LOW', or 'LOW_PREEMPT'
            }

        The message must describe the blocks of time using the format
        described in `Device Schedule`_.

        A task may be canceled by publishing to the
        ``devices/actuators/schedule/request`` topic with the following header:

        .. code-block:: python

            {
                'type': 'CANCEL_SCHEDULE',
                'requesterID': <Ignored, VIP Identity used internally>,
                'taskID': <unique task ID>, #The task ID for the canceled Task.
            }

        requesterID
            The name of the requesting agent. Automatically replaced with VIP id.
        taskID
            The desired task ID for this task. It must be unique among all
            scheduled tasks.
        priority
            The desired task priority, must be 'HIGH', 'LOW', or 'LOW_PREEMPT'

        No message is required to cancel a schedule.

        """
        request_type = headers.get('type')
        _log.debug(f'handle_schedule_request: {topic}, {headers}, {message}')

        task_id = headers.get('taskID')
        priority = headers.get('priority')

        now = get_aware_utc_now()
        if request_type == RESERVATION_ACTION_NEW or request_type == LEGACY_RESERVATION_ACTION_NEW:
            try:
                requests = message[0] if len(message) == 1 else message
                headers = self._get_headers(sender, now, task_id, RESERVATION_ACTION_NEW)
                result = self.reservation_manager.new_task(sender, task_id, priority, requests, now)
            except Exception as ex:
                return self._handle_unknown_reservation_error(ex, headers, message)
            # Dealing with success and other first world problems.
            if result.success:
                for preempted_task in result.data:
                    preempt_headers = self._get_headers(preempted_task[0], task_id=preempted_task[1],
                                                        action_type=RESERVATION_ACTION_CANCEL)
                    self.vip.pubsub.publish('pubsub',
                                            topic=RESERVATION_RESULT_TOPIC,
                                            headers=preempt_headers,
                                            message={
                                                'result': RESERVATION_CANCEL_PREEMPTED,
                                                'info': '',
                                                'data': {
                                                    'agentID': sender,
                                                    'taskID': task_id
                                                }
                                            })
            results = {'result': (RESERVATION_RESPONSE_SUCCESS if result.success else RESERVATION_RESPONSE_FAILURE),
                       'data': (result.data if not result.success else {}),
                       'info': result.info_string}
            self.vip.pubsub.publish('pubsub', topic=RESERVATION_RESULT_TOPIC, headers=headers, message=results)


        elif request_type == RESERVATION_ACTION_CANCEL or request_type == LEGACY_RESERVATION_ACTION_CANCEL:
            try:
                result = self.reservation_manager.cancel_reservation(sender, task_id)
                message = {
                    'result': (RESERVATION_RESPONSE_SUCCESS if result.success else RESERVATION_RESPONSE_FAILURE),
                    'info': result.info_string,
                    'data': {}
                }
                topic = RESERVATION_RESULT_TOPIC
                headers = self._get_headers(sender, now, task_id, RESERVATION_ACTION_CANCEL)
                self.vip.pubsub.publish('pubsub', topic, headers=headers, message=message)

            except Exception as ex:
                return self._handle_unknown_reservation_error(ex, headers, message)
        else:
            _log.debug('handle-schedule_request, invalid request type')
            self.vip.pubsub.publish('pubsub', RESERVATION_RESULT_TOPIC, headers, {
                'result': RESERVATION_RESPONSE_FAILURE,
                'info': 'INVALID_REQUEST_TYPE',
                'data': {}
            })

    ################
    # Helper Methods
    ################

    def _equipment_id(self, path: str, point: str = None) -> str:
        path = path.strip('/')
        if point is not None:
            path = '/'.join([path, point])
        # If path already starts with "devices/", skip prefixing
        if not path.startswith(self.equipment_tree.root + '/'):
            path = '/'.join([self.equipment_tree.root, path])
        return path

    @staticmethod
    def _get_headers(requester: str, time: datetime = None, task_id: str = None, action_type: str = None):
        # TODO: This method should have a better name that reflects its actuator-specific usage.
        headers = {'time': format_timestamp(time) if time else format_timestamp(get_aware_utc_now())}
        if requester is not None:
            headers['requesterID'] = requester
        if task_id is not None:
            headers['taskID'] = task_id
        if action_type is not None:
            headers['type'] = action_type
        return headers

    def _handle_error(self, ex: BaseException, point: str, headers: dict):
        if isinstance(ex, RemoteError):
            try:
                exc_type = ex.exc_info['exc_type']
                exc_args = ex.exc_info['exc_args']
            except KeyError:
                exc_type = "RemoteError"
                exc_args = ex.message
            error = {'type': exc_type, 'value': str(exc_args)}
        else:
            error = {'type': ex.__class__.__name__, 'value': str(ex)}
        self._push_result_topic_pair(ERROR_RESPONSE_PREFIX, point, headers, error)
        _log.warning('Error handling subscription: ' + str(error))

    def _handle_unknown_reservation_error(self, ex: BaseException, headers: dict, message: list[list[str]] | list[str]):
        _log.warning(f'bad request: {headers}, {message}, {str(ex)}')
        results = {
            'result': "FAILURE",
            'data': {},
            'info': 'MALFORMED_REQUEST: ' + ex.__class__.__name__ + ': ' + str(ex)
        }
        self.vip.pubsub.publish('pubsub', RESERVATION_RESULT_TOPIC, headers=headers, message=results)
        return results

    @staticmethod
    def _interface_package_from_short_name(interface_name):
        if interface_name.startswith('volttron-lib-') and interface_name.endswith('-driver'):
            return interface_name
        else:
            return f'volttron-lib-{interface_name}-driver'

    def _push_result_topic_pair(self, prefix: str, point: str, headers: dict, value: Any):
        topic = normtopic('/'.join([prefix, point]))
        self.vip.pubsub.publish('pubsub', topic, headers, message=value)

    def _split_topic(self, topic: str, point: str = None) -> tuple[str, str]:
        """Convert actuator-style optional point names to (path, point) pair."""
        topic = topic.strip('/')
        if not topic.startswith(self.equipment_tree.root):
            topic = '/'.join([self.equipment_tree.root, topic])
        path, point_name = (topic, point) if point is not None else topic.rsplit('/', 1)
        return path, point_name

    @RPC.export
    def forward_bacnet_cov_value(self, remote_id, topic, point_values):
        """
        Called by the BACnet Proxy to pass the COV value to the driver agent
        for publishing
        :param remote_id: The unique addressable identifier of the remote.
        :param topic: name of the point in the COV notification
        :param point_values: dictionary of updated values sent by the device
        """
        self.equipment_tree.remotes[remote_id].publish_cov_value(topic, point_values)


def main():
    """Main method called to start the agent."""
    vip_main(PlatformDriverAgent, identity=PLATFORM_DRIVER, version=__version__)


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
