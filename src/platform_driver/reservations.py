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

import bisect
import logging
import pickle

from base64 import b64encode
from collections import defaultdict, namedtuple
from copy import deepcopy
from datetime import timedelta
from importlib.metadata import distribution, PackageNotFoundError
from pickle import dumps, loads
from tzlocal import get_localzone

try:
    distribution('volttron-core')
    from volttron.client.messaging import topics
    from volttron.utils import get_aware_utc_now, format_timestamp, parse_timestamp_string
except PackageNotFoundError:
    from volttron.platform.messaging import topics
    from volttron.platform.agent.utils import get_aware_utc_now, format_timestamp, parse_timestamp_string

PRIORITY_HIGH = 'HIGH'
PRIORITY_LOW = 'LOW'
PRIORITY_LOW_PREEMPT = 'LOW_PREEMPT'
ALL_PRIORITIES = {PRIORITY_HIGH, PRIORITY_LOW, PRIORITY_LOW_PREEMPT}

# TODO: Update volttron.client.messaging.topics?
ACTUATOR_RESERVATION_ANNOUNCE_RAW = topics.ACTUATOR_SCHEDULE_ANNOUNCE_RAW

# RequestResult - Result of a reservation request returned from the reservation
# manager.
RequestResult = namedtuple('RequestResult', ['success', 'data', 'info_string'])
DeviceState = namedtuple('DeviceState', ['agent_id', 'task_id', 'time_remaining'])
_log = logging.getLogger(__name__)


class TimeSlice(object):

    def __init__(self, start=None, end=None):
        if end is None:
            end = start
        if start is not None:
            if end < start:
                raise ValueError('Invalid start and end values.')
        self._start = start
        self._end = end

    def __repr__(self):
        return 'TimeSlice({start!r},{end!r})'.format(start=self.start, end=self.end)

    def __str__(self):
        return '({start} <-> {end})'.format(start=self.start, end=self.end)

    @property
    def end(self):
        return self._end

    @property
    def start(self):
        return self._start

    def __cmp__(self, other):
        if self.start >= other.end:
            return 1
        if self.end <= other.start:
            return -1
        return 0

    # def __ne__(self, other):
    #     return self.__cmp__(other) != 0
    #
    # def __gt__(self, other):
    #     return self.__cmp__(other) > 0

    def __lt__(self, other):
        return self.__cmp__(other) < 0

    # def __ge__(self, other):
    #     return self.__cmp__(other) >= 0
    #
    # def __le__(self, other):
    #     return self.__cmp__(other) <= 0

    def __contains__(self, other):
        return self.start < other < self.end

    def stretch_to_include(self, time_slice):
        if self.start is None or time_slice.start < self.start:
            self._start = time_slice.start
        if self.end is None or time_slice.end > self.end:
            self._end = time_slice.end

    def contains_include_start(self, other):
        """Similar to == or "in" but includes time == self.start"""
        return other in self or other == self.start


class Task(object):
    STATE_PRE_RUN = 'PRE_RUN'
    STATE_RUNNING = 'RUNNING'
    STATE_PREEMPTED = 'PREEMPTED'
    STATE_FINISHED = 'FINISHED'

    def __init__(self, agent_id, priority, requests):
        self.agent_id = agent_id
        self.priority = priority
        self.time_slice = TimeSlice()
        self.devices = defaultdict(Reservation)
        self.state = Task.STATE_PRE_RUN
        self.populate_reservation(requests)

    def change_state(self, new_state):
        if self.state == new_state:
            return

        # TODO: We can put code here for managing state changes.

        self.state = new_state

    def populate_reservation(self, requests):
        for request in requests:
            device, start, end = request

            time_slice = TimeSlice(start, end)
            if not isinstance(device, str):
                raise ValueError('Device not string.')
            self.devices[device].reserve_slot(time_slice)
            self.time_slice.stretch_to_include(time_slice)

    def make_current(self, now):
        if self.state == Task.STATE_FINISHED:
            self.devices.clear()
            return

        for device, reservation in list(self.devices.items()):
            if reservation.finished(now):
                del self.devices[device]

        if self.time_slice.contains_include_start(now):
            if self.state != Task.STATE_PREEMPTED:
                self.change_state(Task.STATE_RUNNING)

        elif self.time_slice > TimeSlice(now):
            self.change_state(Task.STATE_PRE_RUN)

        elif self.time_slice < TimeSlice(now):
            self.change_state(Task.STATE_FINISHED)

    def get_current_slots(self, now):
        result = {}
        for device, reservation in self.devices.items():
            time_slot = reservation.get_current_slot(now)
            if time_slot is not None:
                result[device] = time_slot

        return result

    def get_conflicts(self, other):
        results = []
        for device, reservation in self.devices.items():
            if device in other.devices:
                conflicts = other.devices[device].get_conflicts(reservation)
                results.extend([device, str(x.start), str(x.end)] for x in conflicts)

        return results

    def check_can_preempt_other(self, other):
        if self.priority != PRIORITY_HIGH:
            return False

        if other.priority == PRIORITY_HIGH:
            return False

        if other.state == Task.STATE_RUNNING and other.priority != \
                PRIORITY_LOW_PREEMPT:
            return False

        return True

    def preempt(self, grace_time, now):
        """Return true if there are time slots that have a grace period left"""
        self.make_current(now)
        if self.state == Task.STATE_PREEMPTED:
            return True
        if self.state == Task.STATE_FINISHED:
            return False

        current_time_slots = []
        for reservation in self.devices.values():
            current_time_slots.extend(reservation.prune_to_current(grace_time, now))

        self.change_state(Task.STATE_FINISHED if not current_time_slots else Task.STATE_PREEMPTED)

        if self.state == Task.STATE_PREEMPTED:
            self.time_slice = TimeSlice(now, now + grace_time)
            return True

        return False

    def get_next_event_time(self, now):
        device_reservations = (x.get_next_event_time(now) for x in self.devices.values())
        events = [x for x in device_reservations if x is not None]

        if events:
            return min(events)

        return None

class LockError(Exception):
    # Superclass of ReservationLockError for backwards compatability to old Actuator Agent.
    pass

class ReservationLockError(LockError):
    """Error raised when the user does not have a device scheduled
    and tries to use methods that require exclusive access."""
    pass

class ReservationSchedulingError(Exception):
    pass


class Reservation(object):

    def __init__(self):
        self.time_slots = []

    def check_availability(self, time_slot):
        start_slice = bisect.bisect_left(self.time_slots, time_slot)
        end_slice = bisect.bisect_right(self.time_slots, time_slot)
        return set(self.time_slots[start_slice:end_slice])

    def make_current(self, now):
        """Should be called before working with a reservation.
        Updates the state to the reservation to eliminate stuff in the past."""
        now_slice = bisect.bisect_left(self.time_slots, TimeSlice(now))
        _log.debug("now_slice in make_current {}".format(now_slice))
        if now_slice > 0:
            del self.time_slots[:now_slice]

    def reserve_slot(self, time_slot):
        if self.check_availability(time_slot):
            raise ReservationSchedulingError('DERP! We messed up the scheduling!')

        bisect.insort(self.time_slots, time_slot)

    def get_next_event_time(self, now):
        """Run this to know when to the next state change is going to happen
        with this reservation"""
        self.make_current(now)
        if not self.time_slots:
            return None
        _log.debug(f"in reservation get_next_event_time timeslots {self.time_slots[0]} now {now}")
        next_time = self.time_slots[0].end if self.time_slots[0].contains_include_start(
            now) else self.time_slots[0].start
        # Round to the next second to fix timer goofiness in agent timers.
        if next_time.microsecond:
            next_time = next_time.replace(microsecond=0) + timedelta(seconds=1)

        return next_time

    def get_current_slot(self, now):
        """
        Determines if "now" falls within any scheduled time slots and returns the current active slot, or None if no slot is active
        """
        self.make_current(now)
        if not self.time_slots:
            return None

        if self.time_slots[0].contains_include_start(now):
            return self.time_slots[0]

        return None

    def prune_to_current(self, grace_time, now):
        """Use this to prune a reservation due to preemption."""
        current_slot = self.get_current_slot(now)
        if current_slot is not None:
            latest_end = now + grace_time
            if current_slot.end > latest_end:
                current_slot = TimeSlice(current_slot.start, latest_end)
            self.time_slots = [current_slot]
        else:
            self.time_slots = []

        return self.time_slots

    def get_conflicts(self, other):
        """Returns a list of our time_slices that conflict with the other
        reservation"""
        return [x for x in self.time_slots if other.check_availability(x)]

    def finished(self, now):
        self.make_current(now)
        return not bool(self.time_slots)

    def get_reservation(self):
        return deepcopy(self.time_slots)

    def __len__(self):
        return len(self.time_slots)

    def __repr__(self):
        pass


class ReservationManager(object):

    def __init__(self, parent, grace_time, now=None):
        self.agent = parent
        self.tasks = {}
        self.running_tasks = set()
        self.preempted_tasks = set()
        self.grace_time = timedelta(grace_time)

        self._device_states = {}
        self.reservation_state_file = "_reservation_state"
        self._update_event = None
        self._update_event_time = None

        try:
            initial_state_string = self.agent.vip.config.get(self.reservation_state_file)
        except KeyError:
            initial_state_string = None
        now = now if now else get_aware_utc_now()
        self.load_state(now, initial_state_string)

    def reserved_by(self, topic):
        return self._device_states.get(topic)

    def update(self, now, device_only=None, publish=True):
        # Sanity check now.
        # This is specifically for when this is running in a VM that gets
        # suspended and then resumed.
        # If we don't make this check a resumed VM will publish one event
        # per minute of
        # time the VM was suspended for.

        test_now = get_aware_utc_now()
        if test_now - now > timedelta(minutes=3):
            now = test_now
        self._device_states = self.get_reservation_state(now)

        # device_only and publish tells us if we were called by a reservation change.
        # If we are being called as part of a regularly scheduled publish
        # we ignore our previous publication time.
        if device_only is None and publish:
            self._update_event_time = None

        next_reservation_event_time = self.get_next_event_time(now)
        new_update_event_time = self._get_adjusted_next_event_time(now, next_reservation_event_time,
                                                                   self._update_event_time)

        if publish:
            device_states = []
            if device_only is not None:
                # TODO: What if there are multiple devices in a newly scheduled task? Seems like this should loop.
                if device_only in self._device_states:
                    device_states.append((device_only, self._device_states[device_only]))
            else:
                device_states = iter(self._device_states.items())

            for device, state in device_states:
                _log.debug("device, state -  {}, {}".format(device, state))
                headers = {
                    'time': format_timestamp(now),
                    'requesterID': state.agent_id,
                    'taskID': state.task_id,
                    'window': state.time_remaining
                }
                topic = ACTUATOR_RESERVATION_ANNOUNCE_RAW.replace('{device}', device)
                self.agent.vip.pubsub.publish('pubsub', topic, headers=headers)

        if self._update_event is not None:
            # This won't hurt anything if we are canceling ourselves.
            self._update_event.cancel()
        self._update_event_time = new_update_event_time
        self._update_event = self.agent.core.schedule(new_update_event_time, self.update,
                                                      new_update_event_time)

    # # TODO: Is this function necessary?
    # def _update_reservation_state(self, now):
    #     self.update(now)

    def _get_adjusted_next_event_time(self, now, next_event_time, previously_reserved_time):
        latest_next = now + timedelta(seconds=self.agent.config.reservation_publish_interval)
        # Round to the next second to fix timer goofiness in agent timers.
        # TODO: Improved Reservation Manager should no longer require this.
        if latest_next.microsecond:
            latest_next = latest_next.replace(microsecond=0) + timedelta(seconds=1)

        result = latest_next
        if next_event_time is not None and result > next_event_time:
            result = next_event_time

        if previously_reserved_time is not None and result > previously_reserved_time:
            result = previously_reserved_time

        return result

    def set_grace_period(self, seconds):
        self.grace_time = timedelta(seconds=seconds)

    def load_state(self, now, initial_state_string):
        if initial_state_string is None:
            return

        try:
            self.tasks = loads(initial_state_string)
            self._cleanup(now)
        except pickle.PickleError as pe:
            self.tasks = {}
            _log.error(f'Pickle error {pe}')
        except Exception as e:
            self.tasks = {}
            _log.error(f'Reservation Manager state file corrupted! Exception {e}')

    def save_state(self, now):
        try:
            self._cleanup(now)
            _log.debug(f"Saving {len(self.tasks)} task(s)")
            self.agent.vip.config.set(self.reservation_state_file, b64encode(dumps(self.tasks)).decode("utf-8"), send_update=False)

        except Exception as e:
            _log.error(f'Failed to save Reservation Manager state! Error: {e}')

    def new_task(self, sender, task_id, priority, requests, now=None):
        priority = priority.upper() if priority is not None else None
        local_tz = get_localzone()
        if requests and isinstance(requests[0], str):
            requests = [requests]

        tmp_requests = requests
        requests = []
        for r in tmp_requests:
            device, start, end = r

            device = device.strip('/')
            start = parse_timestamp_string(start)
            end = parse_timestamp_string(end)
            if start.tzinfo is None:
                start = start.replace(tzinfo=local_tz)
            if end.tzinfo is None:
                end = end.replace(tzinfo=local_tz)
            requests.append([device, start, end])
        _log.debug(f"Got new reservation request: {sender}, {task_id}, {priority}, {requests}")
        
        now = now if now is not None else get_aware_utc_now()
        self._cleanup(now)

        if task_id in self.tasks:
            return RequestResult(False, {}, 'TASK_ID_ALREADY_EXISTS')
        if task_id is None:
            return RequestResult(False, {}, 'MISSING_TASK_ID')
        if priority is None:
            return RequestResult(False, {}, 'MISSING_PRIORITY')
        if priority not in ALL_PRIORITIES:
            return RequestResult(False, {}, 'INVALID_PRIORITY')
        if sender is None:
            return RequestResult(False, {}, 'MISSING_AGENT_ID')
        if requests is None or not requests:
            return RequestResult(False, {}, 'MALFORMED_REQUEST_EMPTY')
        if not isinstance(sender, str) or not sender:
            return RequestResult(
                False, {}, 'MALFORMED_REQUEST: TypeError: agent_id must '
                'be a nonempty string')
        if not isinstance(task_id, str) or not task_id:
            return RequestResult(
                False, {}, 'MALFORMED_REQUEST: TypeError: taskid must '
                'be a nonempty string')

        try:
            new_task = Task(sender, priority, requests)
        except ReservationSchedulingError:
            return RequestResult(False, {}, 'REQUEST_CONFLICTS_WITH_SELF')
        except Exception as ex:
            return RequestResult(False, {},
                                 'MALFORMED_REQUEST: ' + ex.__class__.__name__ + ': ' + str(ex))

        conflicts = defaultdict(dict)
        preempted_tasks = set()

        for t_id, task in self.tasks.items():
            conflict_list = new_task.get_conflicts(task)
            sender = task.agent_id
            if conflict_list:
                if not new_task.check_can_preempt_other(task):
                    conflicts[sender][t_id] = conflict_list
                else:
                    preempted_tasks.add((sender, t_id))
        if conflicts:
            return RequestResult(False, conflicts, 'CONFLICTS_WITH_EXISTING_RESERVATIONS')
        self.tasks[task_id] = new_task
        _log.debug(f"Task added. Total tasks now: {len(self.tasks)}")

        # By this point we know that any remaining conflicts can be preempted and the request will succeed.
        for _, t_id in preempted_tasks:
            task = self.tasks[t_id]
            task.preempt(self.grace_time, now)
        self.save_state(now)

        if preempted_tasks:
            result = RequestResult(True, list(preempted_tasks), 'TASKS_WERE_PREEMPTED')
        else:
            return RequestResult(True, {}, '')
        if result.success:
            # TODO: This implies a single device, but the loop above implies potentially multiple.
            #  The update() method sends a publish on the announce/full/device/path topic, implying all devices.
            #  Neither here nor update() actually have logic for multiple devices, though.
            #  This is a carryover bug from the Actuator Agent. Investigate the format of the request.
            self.update(now, device_only=device)
        return result

    def cancel_task(self, sender, task_id):
        now = get_aware_utc_now()
        if task_id not in self.tasks:
            return RequestResult(False, {}, 'TASK_ID_DOES_NOT_EXIST')
        task = self.tasks[task_id]
        if task.agent_id != sender:
            return RequestResult(False, {}, 'AGENT_ID_TASK_ID_MISMATCH')
        del self.tasks[task_id]
        _log.debug(f"Task {task_id} successfully cancelled.")
        self.save_state(now)
        result = RequestResult(True, {}, '')
        if result.success:
            self.update(now, publish=False)
        return result

    def get_reservation_state(self, now):
        self._cleanup(now)
        running_results = {}
        preempted_results = {}
        for task_id in self.running_tasks:
            task = self.tasks[task_id]
            agent_id = task.agent_id
            current_task_slots = task.get_current_slots(now)
            _log.debug("current_task_slots {}".format(current_task_slots))
            for device, time_slot in current_task_slots.items():
                assert (device not in running_results)
                running_results[device] = DeviceState(agent_id, task_id,
                                                      (time_slot.end - now).total_seconds())

        for task_id in self.preempted_tasks:
            task = self.tasks[task_id]
            agent_id = task.agent_id
            current_task_slots = task.get_current_slots(now)
            for device, time_slot in current_task_slots.items():
                assert (device not in preempted_results)
                preempted_results[device] = DeviceState(agent_id, task_id,
                                                        (time_slot.end - now).total_seconds())

        running_results.update(preempted_results)
        return running_results

    def get_next_event_time(self, now):
        task_times = (x.get_next_event_time(now) for x in self.tasks.values())
        events = [x for x in task_times if x is not None]

        if events:
            return min(events)

        return None

    def _cleanup(self, now):
        """Cleans up self and contained tasks to reflect the current time.
        Should be called:
        1. Before serializing to disk.
        2. After reading from disk.
        3. Before handling a reservation submission request.
        4. After handling a reservation submission request.
        5. Before handling a state request."""
        # Reset the running tasks.
        self.running_tasks = set()
        self.preempted_tasks = set()

        for task_id in list(self.tasks.keys()):
            task = self.tasks[task_id]
            task.make_current(now)
            if task.state == Task.STATE_FINISHED:
                _log.debug(f"Removing task '{task_id}' because it is finished.")
                del self.tasks[task_id]

            elif task.state == Task.STATE_RUNNING:
                self.running_tasks.add(task_id)

            elif task.state == Task.STATE_PREEMPTED:
                self.preempted_tasks.add(task_id)

    def __repr__(self):
        pass
