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

import abc
import gevent
import importlib  # TODO: Look into using "get_module", "get_class", "get_subclasses" from volttron.utils.dynamic_helper
import logging

from collections import defaultdict
from datetime import datetime, timedelta
from importlib.metadata import distribution, PackageNotFoundError
from functools import reduce
from math import floor, gcd, lcm
from typing import Any
from weakref import WeakKeyDictionary, WeakValueDictionary

try:
    distribution('volttron-core')
    # noinspection PyProtectedMember
    from volttron.client.vip.agent.core import ScheduledEvent
    from volttron.driver.base.driver import DriverAgent
    from volttron.utils import get_aware_utc_now
except PackageNotFoundError:
    # noinspection PyProtectedMember
    from volttron.platform.vip.agent.core import ScheduledEvent
    from volttron.driver.base.driver import DriverAgent
    from volttron.platform.agent.utils import get_aware_utc_now

from .config import GroupConfig
from .equipment import EquipmentTree, PointNode

_log = logging.getLogger(__name__)


class PollSet:
    def __init__(self, data_model: EquipmentTree, remote: DriverAgent,
                 points: WeakValueDictionary[str, PointNode] = None,
                 single_depth: set[str] = None, single_breadth: set[tuple[str, str]] = None,
                 multi_depth: dict[str, set[str]] = None, multi_breadth: dict[str, set[str]] = None):
        self.data_model: EquipmentTree = data_model
        self.points: WeakValueDictionary[str, PointNode] = points if points else WeakValueDictionary()
        self.single_depth: set[str] = single_depth if single_depth else set()
        self.single_breadth: set[tuple[str, str]] = single_breadth if single_breadth else set()
        self.multi_depth: dict[str, set[str]] = multi_depth if multi_depth else defaultdict(set)
        self.multi_breadth: dict[str, set[str]] = multi_breadth if multi_breadth else defaultdict(set)
        self.remote = remote

    def add(self, item: PointNode):
        self.points[item.identifier] = item
        self._add_to_publish_setup(item)

    def remove(self, item: PointNode):
        success = self.points.pop(item.identifier, None)
        self._remove_from_publish_setup(item)
        return success

    def _add_to_publish_setup(self, point: PointNode):
        point_depth, point_breadth = self.data_model.get_point_topics(point.identifier)
        device_depth, device_breadth = self.data_model.get_device_topics(point.identifier)

        if self.data_model.is_published_single_depth(point.identifier):
            self.single_depth.add(point_depth)

        if self.data_model.is_published_single_breadth(point.identifier):
            self.single_breadth.add((point_depth, point_breadth))

        if self.data_model.is_published_multi_depth(point.identifier):
            self.multi_depth[device_depth].add(point_depth)

        if self.data_model.is_published_multi_breadth(point.identifier):
            self.multi_breadth[device_breadth].add(point.identifier)

    def  _remove_from_publish_setup(self, point: PointNode):
        point_depth, point_breadth = self.data_model.get_point_topics(point.identifier)
        device_depth, device_breadth = self.data_model.get_device_topics(point.identifier)
        self.single_depth.discard(point_depth)
        self.single_breadth.discard((point_depth, point_breadth))
        self.multi_depth[device_depth].discard(point_depth)
        if not self.multi_depth[device_depth]:
            self.multi_depth.pop(device_depth, None)
        self.multi_breadth[device_breadth].discard(point.identifier)
        if not self.multi_breadth[device_breadth]:
            self.multi_breadth.pop(device_breadth, None)

    def __or__(self, other):
        if self.data_model is not other.data_model:
            raise ValueError(f'Cannot combine PollSets based on different data models:'
                             f' {self.data_model}, {other.data_model}.')
        # TODO: Why can't we have polling schedules for multiple remotes -- e.g., a serial bus with many remotes.
        if self.remote is not other.remote:
            raise ValueError(f'Cannot combine PollSets based on different remotes:'
                             f' {self.remote.unique_id}, {other.remote.unique_id}.')
        return PollSet(
            self.data_model,
            self.remote,
            points=self.points | other.points,
            single_depth=self.single_depth | other.single_depth,
            single_breadth=self.single_breadth | other.single_breadth,
            multi_depth={k: self.multi_depth.get(k, set()) | other.multi_depth.get(k, set())
                    for k in self.multi_depth.keys() | other.multi_depth.keys()},
            multi_breadth={k: self.multi_breadth.get(k, set()) | other.multi_breadth.get(k, set())
                    for k in self.multi_breadth.keys() | other.multi_breadth.keys()}
        )

    def __bool__(self):
        return bool(self.points)


class PollScheduler(metaclass=abc.ABCMeta):
    poll_sets: dict[str, WeakKeyDictionary[DriverAgent, dict[float, PollSet]]] = defaultdict(WeakKeyDictionary)

    def __init__(self, data_model: EquipmentTree, group: str, group_config: GroupConfig, **kwargs):
        self.data_model: EquipmentTree = data_model
        self.group: str = group
        self.group_config: GroupConfig = group_config

        self.start_all_datetime: datetime = get_aware_utc_now()
        self.pollers: dict[Any, ScheduledEvent] = {}

    def schedule(self):
        self._prepare_to_schedule()
        self._schedule_polling()

    @classmethod
    def setup(cls, data_model: EquipmentTree, group_configs: dict[str, GroupConfig]):
        """
        Sort points from each of the remote's EquipmentNodes by interval:
            Build cls.poll_sets  as: {group: {remote: {interval: WeakSet(points)}}}
        """
        cls._build_poll_sets(data_model)
        poll_schedulers = cls.create_poll_schedulers(data_model, group_configs)
        return poll_schedulers

    @classmethod
    def create_poll_schedulers(cls, data_model: EquipmentTree, group_configs,
                               specific_groups: list[str] = None, existing_group_count: int = 0):
        poll_schedulers = {}
        groups = specific_groups if specific_groups else cls.poll_sets
        for i, group in enumerate(groups):
            group_config = group_configs.get(group)
            if group_config is None:
                # Create a config for the group with default settings and mimic the old offset multiplier behavior.
                group_config: GroupConfig = GroupConfig()
                # TODO: Should start_offset instead be either a default offset * i or the specified start_offset if it is there?
                group_config.start_offset = group_config.start_offset * (i + existing_group_count)
                group_configs[group] = group_config  # Add this new config back to the agent settings.
                # TODO: Save the agent settings afterwards so this group gets the same config next time?
            poll_scheduler_module = importlib.import_module(group_config.poll_scheduler_module)
            poll_scheduler_class = getattr(poll_scheduler_module, group_config.poll_scheduler_class_name)
            poll_schedulers[group] = poll_scheduler_class(data_model, group, group_config)
        return poll_schedulers

    @classmethod
    def _build_poll_sets(cls, data_model: EquipmentTree):
        _log.debug(f'@@@@@@ IN BUILD POLL SETS @@@@@')
        for remote in data_model.remotes.values():
            _log.debug(f'@@@@@@ REMOTE: {remote.unique_id}')
            poll_sets = defaultdict(dict)
            groups = set()
            for point in remote.point_set:
                if data_model.is_active(point.identifier):
                    group = data_model.get_group(point.identifier)
                    interval = data_model.get_polling_interval(point.identifier)
                    if interval not in poll_sets[group]:
                        poll_sets[group][interval] = PollSet(data_model, remote)
                    poll_sets[group][interval].add(point)
                    groups.add(group)
            _log.debug(f'@@@@@@ POLL_SETS BEFORE GROUP LOOP IS: {poll_sets}')
            for group in poll_sets:
                if remote not in cls.poll_sets[group]:
                    cls.poll_sets[group][remote] = defaultdict(lambda: PollSet(data_model, remote))
                for interval in poll_sets[group]:
                    cls.poll_sets[group][remote][interval] = poll_sets[group][interval]
        _log.debug("@@@@@@ CLS.POLL_SETS AFTER ALL LOOPS IS: {}".format({k0: {k1.unique_id: [k2 for k2 in v1.keys()] for k1, v1 in v0.items()} for k0, v0 in cls.poll_sets.items()}))

    @staticmethod
    def find_starting_datetime(now: datetime, interval: timedelta, group_delay: timedelta = None):
        group_delay = timedelta(seconds=0.0) if not isinstance(group_delay, timedelta) else group_delay
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_from_midnight = (now - midnight)
        offset = seconds_from_midnight % interval
        if not offset:
            return now + interval + group_delay
        next_from_midnight = seconds_from_midnight - offset + interval
        return midnight + next_from_midnight + group_delay
    #
    # @staticmethod
    # def find_starting_datetime(now: datetime, interval: timedelta, group_delay: timedelta = None):
    #     group_delay = timedelta(seconds=0.0) if not isinstance(group_delay, timedelta) else group_delay
    #     midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    #     seconds_from_midnight = (now - midnight)
    #     offset = seconds_from_midnight % interval
    #     if True:
    #         return now + group_delay
    #         return now + interval + group_delay
    #     next_from_midnight = seconds_from_midnight - offset + interval
    #     return midnight + next_from_midnight + group_delay

    @classmethod
    def add_to_schedule(cls, point: PointNode, data_model: EquipmentTree):
        """Add a poll to the schedule, without complete rescheduling if possible"""
        group = data_model.get_group(point.identifier)
        remote = data_model.get_remote(point.identifier)
        interval = data_model.get_polling_interval(point.identifier)
        reschedule_required = (group not in cls.poll_sets
                               or remote not in cls.poll_sets[group]
                               or interval not in cls.poll_sets[group][remote])
        if remote not in cls.poll_sets[group].keys():
            cls.poll_sets[group][remote] = defaultdict(lambda: PollSet(data_model, remote))
        cls.poll_sets[group][remote][interval].add(point)
        return reschedule_required

    @classmethod
    def remove_from_schedule(cls, point: PointNode, data_model: EquipmentTree):
        """Remove a poll from the schedule without rescheduling."""
        group = data_model.get_group(point.identifier)
        remote = data_model.get_remote(point.identifier)
        interval = data_model.get_polling_interval(point.identifier)
        success = cls.poll_sets[group][remote][interval].remove(point)
        cls._prune_poll_sets(group, interval, remote)
        return True if success else False

    @classmethod
    def _prune_poll_sets(cls, group, interval, remote):
        if not cls.poll_sets[group][remote][interval].points:
            cls.poll_sets[group][remote].pop(interval)
            if not cls.poll_sets[group][remote]:
                cls.poll_sets[group].pop(remote)
                if not cls.poll_sets[group]:
                    cls.poll_sets.pop(group)

    @abc.abstractmethod
    def _prepare_to_schedule(self):
        pass

    @abc.abstractmethod
    def _schedule_polling(self):
        pass

    @abc.abstractmethod
    def get_schedule(self):
        pass


class StaticCyclicPollScheduler(PollScheduler):
    def __init__(self, *args, **kwargs):
        super(StaticCyclicPollScheduler, self).__init__(*args, **kwargs)
        # Slot Plans has: {remote: {hyperperiod: {slot: WeakSet(points)}}}
        self.slot_plans: list[dict[timedelta, dict[timedelta, list[PollSet]]]] = []

    def get_schedule(self, full_topics=False):
        """Return the calculated schedules to the user."""
        return_dict = defaultdict(lambda: defaultdict(dict))
        for slot_plan in self.slot_plans:
            for hyperperiod, plan in slot_plan.items():
                for slot, poll_sets in plan.items():
                    poll_set = reduce(lambda ps1, ps2: ps1 | ps2, poll_sets)
                    remote = str(poll_set.remote.unique_id)
                    if full_topics:
                        return_dict[str(hyperperiod)][str(slot)][remote] = [p for p in poll_set.points.keys()]
                    else:
                        return_dict[str(hyperperiod)][str(slot)][remote] = [p.split("/")[-1] for p in poll_set.points.keys()]
        return return_dict

    @staticmethod
    def calculate_hyperperiod(intervals, minimum_polling_interval):
        return lcm(*[floor(i / minimum_polling_interval) for i in intervals]) * minimum_polling_interval

    @staticmethod
    def _separate_coprimes(intervals):
        # TODO: The math in _separate_coprimes and calculate_hyperperiod needs to be done with integers, not floats.
        #  How to handle sub-second intervals?  Should we do this using milliseconds?
        separated = []
        unseparated = list(intervals)
        unseparated.sort(reverse=True)
        while len(unseparated) > 0:
            non_coprime, coprime = [], []
            first = unseparated.pop(0)
            non_coprime.append(first)
            for i in unseparated:
                if gcd(first, i) == 1 and first != 1 and i != 1:
                    coprime.append(i)
                else:
                    non_coprime.append(i)
            unseparated = coprime
            separated.append(non_coprime)
        return separated

    def _find_slots(self, input_dict: dict[float, dict[DriverAgent, list[PollSet]]], parallel_remote_index: int = 0):
        coprime_interval_sets = self._separate_coprimes(input_dict.keys())
        slot_plan: dict[timedelta, dict[timedelta, list[PollSet]]] = defaultdict(lambda: defaultdict(list))
        parallel_offset = parallel_remote_index * self.group_config.minimum_polling_interval
        min_spread = self.group_config.minimum_polling_interval
        all_remotes = {k for i in input_dict for k in input_dict[i].keys()}
        min_interval = min(input_dict.keys())
        min_remote_offset = min_interval / len(all_remotes)
        if self.group_config.parallel_subgroups and min_remote_offset < min_spread:
            _log.warning(f'There are {len(all_remotes)} scheduled sequentially with a smallest interval of'
                         f' {min_interval}. This only allows {min_remote_offset} between polls --- less than'
                         f' the group {self.group} minimum_polling_interval of {min_spread}. The resulting schedule is'
                         f' likely to result in unexpected behavior and potential loss of data if these remotes share'
                         f' a collision domain. If the minimum polling interval cannot be lowered, consider polling'
                         f' less frequently.')
        remote_offsets = {r: i * min_remote_offset for i, r in enumerate(all_remotes)}
        for interval_set in coprime_interval_sets:
            hyperperiod = self.calculate_hyperperiod(interval_set, min(interval_set))
            for interval in interval_set:
                s_count = int(hyperperiod / interval)
                remote_spread = interval / len(input_dict[interval].keys())
                spread = min_spread if self.group_config.parallel_subgroups else max(min_spread, remote_spread)
                for slot, remote in [((interval * i) + (spread * r) + remote_offsets[remote] + parallel_offset , remote)
                                     for i in range(s_count) for r, remote in enumerate(input_dict[interval].keys())]:
                    slot_plan[timedelta(seconds=hyperperiod)][timedelta(seconds=slot)].extend(input_dict[interval][remote])
        # noinspection PyTypeChecker
        return {hyperperiod: dict(sorted(sp.items())) for hyperperiod, sp in slot_plan.items()}

    @staticmethod
    def get_poll_generator(hyperperiod_start: datetime, hyperperiod: timedelta, slot_plan: dict[timedelta, list[PollSet]]):
        def get_polls(start_time):
            # Union of points and publish_setups is here to get any changes to self.poll_sets at start of hyperperiod.
            return ((start_time + slot, reduce(lambda d1, d2: d1 | d2, poll_sets)
                     ) for slot, poll_sets in slot_plan.items())
        polls = get_polls(hyperperiod_start)
        while True:
            try:
                p = next(polls)
            except StopIteration:
                hyperperiod_start += hyperperiod
                polls = get_polls(hyperperiod_start)
                p = next(polls)
            yield p

    def _prepare_to_schedule(self):
        group_poll_sets = self.poll_sets[self.group]
        if self.group_config.parallel_subgroups:
            for parallel_index, (remote, remote_poll_sets) in enumerate(group_poll_sets.items()):
                input_dict = defaultdict(lambda: defaultdict(list))
                for interval, poll_set in remote_poll_sets.items():
                    input_dict[interval][remote].append(poll_set)
                self.slot_plans.append(self._find_slots(input_dict, parallel_index))
        else:
            input_dict = defaultdict(lambda: defaultdict(list))
            for remote, remote_poll_sets in group_poll_sets.items():
                for interval, poll_set in remote_poll_sets.items():
                    input_dict[interval][remote].append(poll_set)
            self.slot_plans.append(self._find_slots(input_dict))

    def _schedule_polling(self):
        # TODO: How to fully ensure min_polling_interval? Nothing yet prevents collisions between individual polls in
        #  separate schedules. Is it worth keeping these apart if it requires a check for each slot at schedule time?
        #  Or, create global lock oscillating at min_poll_interval - check on poll for the next allowed start time?
        _log.debug('In _schedule_polling, poll sets is: ')
        _log.debug(self.slot_plans)
        for slot_plan in self.slot_plans:
            for hyperperiod, plan in slot_plan.items():
                initial_start = self.find_starting_datetime(get_aware_utc_now(), hyperperiod,
                                                            self.group_config.start_offset)
                self.start_all_datetime = max(self.start_all_datetime, initial_start + hyperperiod)
                poll_generator = self.get_poll_generator(initial_start, hyperperiod, plan)
                start, poll_set = next(poll_generator)
                _log.info(f'Scheduled polling for {self.group}--{hyperperiod} starts at {start.time()} (datetime: {start})')
                # TODO: Is hyperperiod a sufficient index for the pollers?
                self.pollers[hyperperiod] = self.data_model.agent.core.schedule(start, self._operate_polling,
                                                                                hyperperiod, poll_generator, poll_set)

    def _operate_polling(self, poller_id, poll_generator, current_poll_set):
        next_start, next_poll_set = next(poll_generator)

        # Find the current and next polls where the next poll is the first to still be in the future
        #  (This assures that if the host has gone to sleep, the poll will still be the most up to date):
        now = get_aware_utc_now()
        while next_start <= now:
            # TODO: If this takes too long for long pauses, call get_poll_generator again, instead.
            _log.warning(f'Skipping polls from {next_start} to {now} to catch up to the current time.')
            current_poll_set = next_poll_set
            next_start, next_poll_set = next(poll_generator)

        # Schedule next poll:
        if next_poll_set.points:
            self.pollers[poller_id] = self.data_model.agent.core.schedule(next_start, self._operate_polling, poller_id,
                                                                          poll_generator, next_poll_set)
        else:
            _log.info(f'Stopping polling loop of {poller_id} points on {next_poll_set.remote.unique_id}.'
                      f' There are no points in this request set to poll.')
        current_poll_set.remote.poll_data(current_poll_set)


class SerialPollScheduler(PollScheduler):
    def get_schedule(self):
        pass

    def _prepare_to_schedule(self):
        pass

    def __init__(self, agent, sleep_duration, **kwargs):
        super(SerialPollScheduler, self).__init__(agent, **kwargs)
        self.sleep_duration = sleep_duration

        self.status = {}

    # TODO: Serial Poll Scheduler (schedule a single job to poll each item of poll set after the return or failure
    #  of the previous):
    #  Create timeouts such that enough time is available to address each item in series before the next cycle.
    def _schedule_polling(self):
        pass

    # TODO: If there is not sufficient difference in the prepare and schedule methods,
    #  this could be a separate operate method in the StaticCyclicPollScheduler.
    def _operate_polling(self,  remote, poller_id, poll_generator):
        while True:  # TODO: This should probably check whether agent is stopping.
            start, points = next(poll_generator)
            poll = gevent.spawn(remote.poll_data, points)
            # Wait for poll to finish.
            while not poll.ready():
                gevent.sleep(self.sleep_duration)
            # Track whether this poller_id has been successful.
            # TODO: Would it be more helpful if the poll_data method returned the time (when it is successful) or None?
            self.status[poller_id] = poll.get(timeout=1)
