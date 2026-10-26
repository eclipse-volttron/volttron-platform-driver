import pickle
import pytest
from unittest.mock import MagicMock, Mock
from platform_driver.agent import PlatformDriverAgent
from platform_driver.reservations import ReservationManager, Task, TimeSlice, Reservation
from pickle import dumps
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from volttron.utils import get_aware_utc_now
from unittest.mock import patch


class TestTimeSliceStretchToInclude:
    now = get_aware_utc_now()

    def test_start_is_none(self):
        """Tests calling stretch_to_include with start as None in time slice 1"""
        ts1 = TimeSlice(start=None, end=self.now + timedelta(hours=2))
        ts2 = TimeSlice(start=self.now + timedelta(hours=1), end=self.now + timedelta(hours=3))
        ts1.stretch_to_include(ts2)
        assert ts1.start == ts2.start, "start should be updated to ts2's start"

    def test_end_is_none(self):
        """Tests calling stretch_to_include with end as None in time slice 1"""
        ts1 = TimeSlice(start=self.now, end=None)
        ts2 = TimeSlice(start=self.now - timedelta(hours=1), end=self.now + timedelta(hours=1))
        ts1.stretch_to_include(ts2)
        assert ts1.end == ts2.end, "end should be updated to ts2's end"

    def test_extend_before_start(self):
        """Tests that stretch_to_include correctly updates the start time to the earlier TimeSlice."""
        ts = TimeSlice(start=self.now + timedelta(hours=2), end=self.now + timedelta(hours=3))
        time_slice = TimeSlice(start=self.now + timedelta(hours=1),
                               end=self.now + timedelta(hours=2))
        ts.stretch_to_include(time_slice)
        assert ts.start == time_slice.start, "start should be updated to time_slice's start"

    def test_extend_after_end(self):
        """Tests that stretch_to_include correctly updates the end time to the later TimeSlice."""
        ts = TimeSlice(start=self.now - timedelta(hours=2), end=self.now)    # ended now
        time_slice = TimeSlice(start=self.now - timedelta(hours=2),
                               end=self.now + timedelta(hours=2))
        ts.stretch_to_include(time_slice)
        assert ts.end == time_slice.end, "end should be updated to time_slice's end"

    def test_within_current_timeslice(self):
        """Tests a timeslice within another time slice not changing either"""
        ts = TimeSlice(start=self.now, end=self.now + timedelta(hours=4))
        new_ts = TimeSlice(start=self.now + timedelta(hours=1), end=self.now + timedelta(hours=2))
        ts.stretch_to_include(new_ts)
        assert ts.start == self.now, "start should remain the same"
        assert ts.end == self.now + timedelta(hours=4), "end should remain the same"


class TestTaskPopulateReservation:
    requests = [["device1", datetime(2022, 1, 1, 12, 0),
                 datetime(2022, 1, 1, 13, 0)],
                ["device2", datetime(2022, 1, 1, 14, 0),
                 datetime(2022, 1, 1, 15, 0)]]

    @pytest.fixture
    def task(self):
        return Task(agent_id="test_agent", priority="HIGH", requests=[])

    def test_populate_reservation_with_valid_inputs(self, task):
        task.populate_reservation(self.requests)
        assert "device1" in task.devices
        assert len(task.devices["device1"].time_slots) == 1
        assert task.time_slice.start == datetime(2022, 1, 1, 12, 0)
        assert task.time_slice.end == datetime(2022, 1, 1, 15, 0)

    def test_populate_reservation_device_not_string(self, task):
        """ Tests calling populate reservation with device as non string"""
        requests = [
        # device is a bool instead of a true
            [True, datetime(2022, 1, 1, 12, 0),
             datetime(2022, 1, 1, 13, 0)],
            ["device2", datetime(2022, 1, 1, 14, 0),
             datetime(2022, 1, 1, 15, 0)]
        ]
        with pytest.raises(ValueError, match="Device not string."):
            task.populate_reservation(requests)


class TestTaskMakeCurrent:

    @pytest.fixture
    def task(self):
        # Create a Task instance with mock reservations and a set time slice.
        task = Task(agent_id="test_agent", priority="HIGH", requests=[])
        # Mocking device reservations within the task
        task.devices = {'device1': MagicMock(), 'device2': MagicMock()}
        # our task is active starting NOW for 1 hour
        start_time = get_aware_utc_now()
        end_time = get_aware_utc_now() + timedelta(hours=1)
        task.time_slice = TimeSlice(start_time, end_time)
        return task

    def test_task_already_finished(self, task):
        """set task state to finished, which clears devices, then we check that task.devices is empty"""
        task.state = Task.STATE_FINISHED
        task.make_current(get_aware_utc_now())
        assert not task.devices, "Devices should be cleared when the task is finished."

    def test_remove_finished_reservations(self, task):
        """tests automatic removal of a device when task is finished and keeping of a non finished device"""
        now = get_aware_utc_now()
        # Set one reservation to be finished
        task.devices['device1'].finished.return_value = True
        task.devices['device2'].finished.return_value = False
        task.make_current(now)
        assert 'device1' not in task.devices, "device1 should be removed from task.devices."
        assert 'device2' in task.devices, "device2 should be in task.devices"

    def test_state_transition_to_pre_run(self, task):
        """Tests calling make current with a time before the task is set to start"""
        past_time = get_aware_utc_now() - timedelta(hours=1)    # one hour before task starts
        task.make_current(past_time)
        assert task.state == Task.STATE_PRE_RUN, "task state should be in pre run"

    def test_state_transition_running(self, task):
        """Tests calling make current 30 minutes after it has started """
        within_time = get_aware_utc_now() + timedelta(
            minutes=30)    # 30 minutes after task has started
        task.make_current(within_time)
        assert task.state == Task.STATE_RUNNING, "task state should be running"

    def test_state_transition_finished(self, task):
        """Tests calling make current with a time after the task is finished """
        past_time = get_aware_utc_now() + timedelta(hours=2)    # 1 hr after task was set to end
        task.make_current(past_time)
        assert task.state == Task.STATE_FINISHED, "task state should be finished"


class TestTaskGetCurrentSlot:

    @pytest.fixture
    def task(self):
        # Create a Task instance with mock reservations and a set time slice.
        task = Task(agent_id="test_agent", priority="HIGH", requests=[])
        # Mocking device reservations within the task
        task.devices = {'device1': MagicMock(), 'device2': MagicMock()}
        return task

    def test_get_current_slots_during_active_time(self, task):
        """Tests return when two slots are active"""
        now = get_aware_utc_now()
        task.devices['device1'].get_current_slot.return_value = TimeSlice(
            now, now + timedelta(minutes=30))
        task.devices['device2'].get_current_slot.return_value = TimeSlice(
            now, now + timedelta(minutes=45))

        current_slots = task.get_current_slots(now)

        assert len(current_slots) == 2
        assert 'device1' in current_slots
        assert 'device2' in current_slots

    def test_get_current_slots_with_no_active_slots(self, task):
        """Tests return when two slots are none"""
        now = get_aware_utc_now()
        task.devices['device1'].get_current_slot.return_value = None
        task.devices['device2'].get_current_slot.return_value = None

        current_slots = task.get_current_slots(now)

        assert current_slots == {}

    def test_get_current_slots_with_mixed_active_and_inactive_slots(self, task):
        """Tests that get current slots returns correct slots when mixed"""
        now = get_aware_utc_now()
        task.devices['device1'].get_current_slot.return_value = TimeSlice(
            now, now + timedelta(minutes=30))
        task.devices['device2'].get_current_slot.return_value = None

        current_slots = task.get_current_slots(now)

        assert len(current_slots) == 1
        assert 'device1' in current_slots
        assert 'device2' not in current_slots


class TestTaskGetConflicts:

    @pytest.fixture
    def task(self):
        now = get_aware_utc_now()

        task = Task(agent_id="agent1", priority="HIGH", requests=[])
        reservation1 = Reservation()
        # starts now for 1 hour
        reservation1.time_slots.append(TimeSlice(start=now, end=now + timedelta(hours=1)))
        # starts in two hours and lasts for 1
        reservation1.time_slots.append(
            TimeSlice(start=now + timedelta(hours=2), end=now + timedelta(hours=3)))
        task.devices = {
            'device1': reservation1,    # two time slots
            'device2': Reservation()    # empty reservation
        }
        return task

    def test_no_conflicts(self, task):
        """Tests no conflicts returned when checking"""
        other_task = Task(agent_id="agent2", priority="LOW", requests=[])
        other_task.devices['device1'] = Reservation()
        other_task.devices['device1'].time_slots.append(
        # starts in 4hrs and lasts for 1hr
            TimeSlice(start=get_aware_utc_now() + timedelta(hours=4),
                      end=get_aware_utc_now() + timedelta(hours=5)))

        conflicts = task.get_conflicts(other_task)
        assert conflicts == [], "There should be no conflicts."

    def test_partial_conflicts(self, task):
        """Tests partial conflicts returned"""
        other_task = Task(agent_id="agent2", priority="LOW", requests=[])
        other_task.devices['device1'] = Reservation()
        other_task.devices['device1'].time_slots.append(
        # starts in 30 minutes (conflict with fixture) lasts for 1.5hrs
            TimeSlice(start=get_aware_utc_now() + timedelta(minutes=30),
                      end=get_aware_utc_now() + timedelta(hours=1, minutes=30)))

        conflicts = task.get_conflicts(other_task)
        assert len(conflicts) == 1, "There should be one conflict."

    def test_complete_conflicts(self, task):
        """Tests complete conflicts returned"""
        other_task = Task(agent_id="agent2", priority="LOW", requests=[])
        other_task.devices['device1'] = Reservation()
        other_task.devices['device1'].time_slots.append(
        # starts now and ends in 1 hour
            TimeSlice(start=get_aware_utc_now(), end=get_aware_utc_now() + timedelta(hours=1)))
        # starts in two hours and lasts for 1 hour
        other_task.devices['device1'].time_slots.append(
            TimeSlice(start=get_aware_utc_now() + timedelta(hours=2),
                      end=get_aware_utc_now() + timedelta(hours=3)))

        conflicts = task.get_conflicts(other_task)
        assert len(conflicts) == 2, "There should be two conflicts"


class TestTaskCheckCanPreemptOther:

    @pytest.fixture
    def high_priority_task(self):
        # Create a high priority task object in PRE_RUN state for basic setup.
        return Task(agent_id="agent1", priority='HIGH', requests=[])

    @pytest.fixture
    def low_priority_task(self):
        # Create a low priority task.
        return Task(agent_id="agent2", priority='LOW', requests=[])

    @pytest.fixture
    def preemptable_task(self):
        # Create a task that is running but is preemptable.
        task = Task(agent_id="agent3", priority='LOW_PREEMPT', requests=[])
        task.state = Task.STATE_RUNNING
        return task

    @pytest.fixture
    def non_preemptable_high_priority_task(self):
        # Create a high priority task that is currently running.
        task = Task(agent_id="agent4", priority='HIGH', requests=[])
        task.state = Task.STATE_RUNNING
        return task

    def test_preemption_high_vs_high(self, high_priority_task, non_preemptable_high_priority_task):
        """tests a high priority task trying to preept another high priority task"""
        result = high_priority_task.check_can_preempt_other(non_preemptable_high_priority_task)
        assert result == False, "High priority cannot preempt another high priority task."

    def test_preemption_low_priority(self, low_priority_task, non_preemptable_high_priority_task):
        """Tests a low priority task trying to preempt a high priority task"""
        result = low_priority_task.check_can_preempt_other(non_preemptable_high_priority_task)
        assert result == False, "Low priority task cannot preempt any task."

    def test_preemption_running_preemptable(self, high_priority_task, preemptable_task):
        """ tests a high priority task trying to preempt a preemptable low priority running task"""
        result = high_priority_task.check_can_preempt_other(preemptable_task)
        assert result == True, "High priority task should be able to preempt a low preemptable running task."

    def test_preemption_of_low_priority_pre_run_task(self, high_priority_task, low_priority_task):
        """Tests a high priorty task trying to preempt a low priority pre run task"""
        low_priority_task.state = Task.STATE_PRE_RUN
        result = high_priority_task.check_can_preempt_other(low_priority_task)
        assert result == True, "High priority should preempt low priority in PRE_RUN state."

    def test_preemption_of_low_priority_finished_task(self, high_priority_task, low_priority_task):
        """Tests a high priorty task trying to preempt a low priority finished task"""
        low_priority_task.state = Task.STATE_FINISHED
        result = high_priority_task.check_can_preempt_other(low_priority_task)
        assert result == True, "High priority should preempt low priority in FINISHED state."


class TestTaskPreempt:

    @pytest.fixture
    def task(self):
        requests = [["device1", get_aware_utc_now(), get_aware_utc_now() + timedelta(hours=1)]]
        return Task(agent_id="test_agent", priority="HIGH", requests=requests)

    @pytest.fixture
    def reservation(self):
        reservation = Reservation()
        reservation.reserve_slot(
            TimeSlice(get_aware_utc_now(),
                      get_aware_utc_now() + timedelta(hours=1)))
        return reservation

    def test_preempt_already_preempted(self, task):
        """Tests if the task state is already preempted"""
        task.state = Task.STATE_PREEMPTED
        result = task.preempt(grace_time=timedelta(minutes=10), now=get_aware_utc_now())
        assert result == True
        assert task.state == Task.STATE_PREEMPTED

    def test_preempt_finished(self, task):
        """Tests if the task state is already finished"""
        task.state = Task.STATE_FINISHED
        result = task.preempt(grace_time=timedelta(minutes=10), now=get_aware_utc_now())
        assert result == False

    def test_preempt_active_time_slots(self, task, reservation):
        """Tests running with tasks that qualify for preemption"""
        task.devices['device1'] = reservation
        now = get_aware_utc_now()
        result = task.preempt(grace_time=timedelta(minutes=30), now=now)
        assert result == True
        assert task.state == Task.STATE_PREEMPTED    # preempt method converted
        assert task.time_slice.start == now
        assert task.time_slice.end == now + timedelta(minutes=30)

    def test_preempt_no_remaining_time_slots(self, task, reservation):
        """ Set the current time after the end of the reservation"""
        now = get_aware_utc_now() + timedelta(hours=2)
        task.devices['device1'] = reservation
        result = task.preempt(grace_time=timedelta(minutes=30), now=now)
        assert result == False
        assert task.state == Task.STATE_FINISHED

    def test_grace_period_extension(self, task, reservation):
        """Tests extedning time slot"""
        task.devices['device1'] = reservation
        now = get_aware_utc_now()
        result = task.preempt(grace_time=timedelta(minutes=30), now=now)
        assert result == True
        assert task.time_slice.start == now
        assert task.time_slice.end == now + timedelta(minutes=30)


class TestTaskGetNextEventTime:

    @pytest.fixture
    def task(self):
        task = Task(agent_id="test_agent", priority="HIGH", requests=[])
        return task

    def test_no_reservations(self, task):
        """Test get_next_event_time returns None when there are no reservations."""
        now = get_aware_utc_now()
        assert task.get_next_event_time(now) is None

    def test_single_reservation(self, task):
        """Test with a single reservation."""
        now = get_aware_utc_now()
        reservation = Mock()
        # our reservation object is created from now to 10 mins from now.
        reservation.get_next_event_time.return_value = now + timedelta(minutes=10)
        task.devices['device1'] = reservation    # assign our new reservation to device1

        assert task.get_next_event_time(now) == now + timedelta(minutes=10)
        assert 'device1' in task.devices

    def test_multiple_reservations(self, task):
        """ Test with multiple reservations which should return the earliest event time"""
        now = get_aware_utc_now()
        reservation1 = Mock()
        reservation1.get_next_event_time.return_value = now + timedelta(minutes=10)
        reservation2 = Mock()
        reservation2.get_next_event_time.return_value = now + timedelta(minutes=20)

        task.devices['device1'] = reservation1
        task.devices['device2'] = reservation2

        assert task.get_next_event_time(now) == now + timedelta(minutes=10)

    def test_mixed_null_and_valid_times(self, task):
        """Test with mixed null and valid next event times."""
        now = get_aware_utc_now()
        reservation1 = Mock()
        # one reservation object returns none
        reservation1.get_next_event_time.return_value = None
        reservation2 = Mock()
        reservation2.get_next_event_time.return_value = now + timedelta(minutes=20)

        task.devices['device1'] = reservation1
        task.devices['device2'] = reservation2

        assert task.get_next_event_time(now) == now + timedelta(minutes=20)


class TestReservationCheckAvailability:

    @pytest.fixture
    def reservation(self):
        return Reservation()

    def test_empty_reservation_list(self, reservation):
        """Tests that empty reservation list returns empty set"""
        now = get_aware_utc_now()
        time_slot = TimeSlice(now, now + timedelta(hours=1))
        assert reservation.check_availability(
            time_slot) == set(), "Should return an empty set for no conflicts."

    def test_single_overlap(self, reservation):
        """ Tests that check availability correctly returns time slots affected by new overlapping times"""
        start_time = get_aware_utc_now()
        end_time = start_time + timedelta(hours=2)
        existing_time_slot = TimeSlice(start_time, end_time)
        reservation.time_slots.append(existing_time_slot)

        # Overlapping time slot
        overlap_start = start_time + timedelta(hours=1)    #starts one hour into the existing slot
        overlap_end = overlap_start + timedelta(hours=1)    # Ends one hour later
        new_time_slot = TimeSlice(overlap_start, overlap_end)

        available_slots = reservation.check_availability(new_time_slot)

        # check availability will return the time slots affected by the new time
        assert available_slots == {existing_time_slot
                                   }, "Should detect the overlap with the existing time slot."


class TestReservationMakeCurrent:

    @pytest.fixture
    def reservation(self):
        return Reservation()

    def test_make_current_no_time_slots(self, reservation):
        """Test making calling with no time slots"""
        now = get_aware_utc_now()
        reservation.make_current(now)
        assert len(reservation.time_slots) == 0, "No time slots should remain if none were added."

    def test_make_current_future_time_slots_only(self, reservation):
        """Test calling make_current with a future time slot which should remain unchanged."""
        now = get_aware_utc_now()
        future_time_slot = TimeSlice(start=now + timedelta(hours=1), end=now + timedelta(hours=2))
        reservation.time_slots.append(future_time_slot)    # add times to time_slots
        reservation.make_current(now)
        assert len(reservation.time_slots) == 1, "Future time slots should not be removed."

    def test_make_current_past_time_slots_only(self, reservation):
        """Test calling reservation make_current with past time slot which should be removed"""
        now = get_aware_utc_now()
        past_time_slot = TimeSlice(start=now - timedelta(hours=2), end=now - timedelta(hours=1))
        reservation.time_slots.append(past_time_slot)
        reservation.make_current(now)
        assert len(reservation.time_slots) == 0, "Past time slots should be removed."

    def test_make_current_mixed_time_slots(self, reservation):
        """Tests callimg make current with 1 old, and one future task"""
        now = get_aware_utc_now()
        past_time_slot = TimeSlice(start=now - timedelta(hours=2), end=now - timedelta(hours=1))
        future_time_slot = TimeSlice(start=now + timedelta(hours=1), end=now + timedelta(hours=2))
        reservation.time_slots.extend([past_time_slot, future_time_slot])
        reservation.make_current(now)
        # there should only be one time slot after running make current
        # and that time slot should be the future time
        assert len(reservation.time_slots) == 1 and reservation.time_slots[
            0] == future_time_slot, "Only past time slots should be removed."


class TestReservationReserveSlot:

    @pytest.fixture
    def reservation(self):
        res = Reservation()
        res.check_availability = MagicMock(return_value=set())
        return res

    def test_reserve_slot(self, reservation):
        """tests that reserve slot calls check avilability with the time slot"""
        now = get_aware_utc_now()
        time_slot = TimeSlice(now)
        reservation.reserve_slot(time_slot)

        reservation.check_availability.assert_called_once_with(time_slot), \
            "check_avilability should be called with time_slot as argument"
        assert time_slot in reservation.time_slots, "time slot should be in time_slots"


class TestReservationGetNextEventTime:

    @pytest.fixture
    def reservation(self):
        return Reservation()

    def test_get_next_event_time_no_slots(self, reservation):
        now = get_aware_utc_now()
        assert reservation.get_next_event_time(
            now) is None, "Should return None when there are no time slots."

    def test_get_next_event_time_future_slots(self, reservation):
        now = get_aware_utc_now()
        future_start = now + timedelta(hours=1)
        future_end = now + timedelta(hours=2)
        reservation.time_slots.append(TimeSlice(start=future_start, end=future_end))

        # adjusting the expected time for rounding similar to get_next_event_time
        expected_time = future_start.replace(microsecond=0) + timedelta(seconds=1)

        assert reservation.get_next_event_time(now) == expected_time, \
            "Should return the start of the next future slot after rounding to the next second."


class TestReservationGetCurrentSlot:

    @pytest.fixture
    def reservation(self):
        res = Reservation()
        now = get_aware_utc_now()
        res.time_slots.append(
            TimeSlice(start=now - timedelta(hours=1),
                      end=now + timedelta(hours=1)))    # Active slot
        res.time_slots.append(
            TimeSlice(start=now + timedelta(hours=2),
                      end=now + timedelta(hours=3)))    # Future slot
        return res

    def test_get_current_slot_inside_slot(self, reservation):
        """Tests getting a timeslot """
        now = get_aware_utc_now()
        current_slot = reservation.get_current_slot(now)
        assert current_slot != None, "Should return a current time slot"
        assert current_slot.start <= now, "Now should be after the start of the returned time slot."
        assert now <= current_slot.end, "Now should be before the end of the returned time slot."

    def test_get_current_slot_outside_slot(self, reservation):
        """Tets trying to get current slot outside any current time slot"""
        now = get_aware_utc_now() + timedelta(hours=4)    # Outside any defined slots
        current_slot = reservation.get_current_slot(now)
        assert current_slot == None, "Should return None when now is outside any slot."

    def test_get_current_slot_no_slots(self):
        """Calling with just reservation that has no slots added"""
        reservation = Reservation()    # no slots added
        now = get_aware_utc_now()
        current_slot = reservation.get_current_slot(now)
        assert current_slot == None, "Should return none when there are no time slots"


class TestReservationPruneToCurrent:

    @pytest.fixture
    def reservation(self):
        res = Reservation()
        now = get_aware_utc_now()
        res.time_slots.append(
            TimeSlice(start=now - timedelta(hours=1),
                      end=now + timedelta(hours=1)))    # Past to future
        res.time_slots.append(
            TimeSlice(start=now + timedelta(hours=2),
                      end=now + timedelta(hours=3)))    # Future slot
        return res

    def test_prune_to_current_no_active_slot(self, reservation):
        """Test that all slots are cleared if no current slot is active."""
        now = get_aware_utc_now() + timedelta(hours=4)    # Time beyond all slots
        grace_time = timedelta(minutes=30)
        reservation.prune_to_current(grace_time, now)

        assert len(reservation.time_slots) == 0, "No slots should remain."

    def test_prune_to_current_active_slot_extending_beyond_grace_period(self, reservation):
        """Test that an active slot extending beyond the grace period is pruned correctly."""
        now = get_aware_utc_now() + timedelta(minutes=10)    # 10 minutes from now
        grace_time = timedelta(minutes=20)    # 20 minutes from now
        reservation.prune_to_current(grace_time, now)
        expected_end_time = now + grace_time    # should extend time
        assert len(reservation.time_slots) == 1, "Only one slot should remain."
        assert reservation.time_slots[
            0].end == expected_end_time, "Slot should end at the grace period end."


class TestReservationGetConflicts:

    @pytest.fixture
    def reservation(self):
        res = Reservation()
        now = get_aware_utc_now()
        # Setup predefined time slots
        res.time_slots.append(TimeSlice(start=now, end=now + timedelta(hours=1)))
        res.time_slots.append(
            TimeSlice(start=now + timedelta(hours=2), end=now + timedelta(hours=3)))
        return res

    def test_no_conflicts(self, reservation):
        """Tests no conflicts returned when checking  """
        other_reservation = Reservation()
        other_reservation.time_slots.append(
            TimeSlice(start=get_aware_utc_now() + timedelta(hours=4),
                      end=get_aware_utc_now() + timedelta(hours=5)))
        conflicts = reservation.get_conflicts(other_reservation)
        assert len(conflicts) == 0, "There should be no conflicts."

    def test_partial_conflicts(self, reservation):
        """Tests partial conflicts returned"""
        other_reservation = Reservation()
        other_reservation.time_slots.append(
            TimeSlice(start=get_aware_utc_now() + timedelta(hours=4),
                      end=get_aware_utc_now() + timedelta(hours=5)))
        other_reservation.time_slots.append(
            TimeSlice(start=get_aware_utc_now() + timedelta(minutes=30),
                      end=get_aware_utc_now() + timedelta(hours=1, minutes=30)))
        conflicts = reservation.get_conflicts(other_reservation)
        assert len(conflicts) == 1, "There should be one conflict."

    def test_complete_conflicts(self, reservation):
        """Tests conflicts with our fixture and appended times"""
        other_reservation = Reservation()
        other_reservation.time_slots.append(
            TimeSlice(start=get_aware_utc_now(), end=get_aware_utc_now() + timedelta(hours=1)))
        other_reservation.time_slots.append(
            TimeSlice(start=get_aware_utc_now() + timedelta(hours=2),
                      end=get_aware_utc_now() + timedelta(hours=3)))
        conflicts = reservation.get_conflicts(other_reservation)
        assert len(conflicts) == 2, "There should be two conflicts."


class TestReservationManagerUpdate:

    @pytest.fixture
    def reservation_manager(self):
        parent = Mock()
        parent.vip = Mock()
        parent.vip.pubsub.publish = MagicMock()
        parent.core = Mock()
        parent.core.schedule = MagicMock()
        parent.vip.config.get = MagicMock(return_value=b'')
        parent.vip.config.set = MagicMock()
        parent.config = Mock()
        parent.config.reservation_publish_interval = 60
        grace_time = 10

        rm = ReservationManager(parent, grace_time)
        rm._cleanup = MagicMock()
        rm.save_state = MagicMock()
        rm.get_reservation_state = MagicMock()
        rm.get_next_event_time = MagicMock()
        rm._get_adjusted_next_event_time = MagicMock()
        rm._device_states = {}
        rm._update_event_time = None
        rm._update_event = None

        return rm

    def test_update_adjusts_time_correctly(self, reservation_manager):
        """Test that 'update' adjusts the event time correctly and schedules the next event properly."""
        mock_now = get_aware_utc_now()
        future_time = mock_now + timedelta(minutes=5)

        reservation_manager.get_reservation_state.return_value = {}
        reservation_manager.get_next_event_time.return_value = future_time
        reservation_manager._get_adjusted_next_event_time.return_value = future_time

        reservation_manager.update(now=mock_now)

        reservation_manager.get_reservation_state.assert_called_once_with(mock_now)
        reservation_manager.get_next_event_time.assert_called_once_with(mock_now)
        reservation_manager._get_adjusted_next_event_time.assert_called_once_with(
            mock_now, future_time, None)
        assert reservation_manager._update_event_time == future_time, "Event time should be updated to future_time"
        reservation_manager.agent.core.schedule.assert_called_once_with(
            future_time, reservation_manager.update, future_time)

    def test_update_with_stale_time(self, reservation_manager):
        """Test that 'update' adjusts for stale time when the system resumes from suspension."""
        # Replace 'platform_driver.reservations' with the actual module where 'get_aware_utc_now' is used
        with patch('platform_driver.reservations.get_aware_utc_now') as mock_get_now:
            # Use a fixed datetime to avoid microsecond differences
            test_now = get_aware_utc_now()
            test_now = test_now.replace(microsecond=0)
            stale_now = test_now - timedelta(minutes=5)    # Simulate system time lag
            future_time = test_now + timedelta(minutes=5)

            mock_get_now.return_value = test_now

            reservation_manager.get_reservation_state.return_value = {}
            reservation_manager.get_next_event_time.return_value = future_time
            reservation_manager._get_adjusted_next_event_time.return_value = future_time

            reservation_manager.update(now=stale_now)

            reservation_manager.get_reservation_state.assert_called_once_with(test_now)
            reservation_manager.get_next_event_time.assert_called_once_with(test_now)
            reservation_manager._get_adjusted_next_event_time.assert_called_once_with(
                test_now, future_time, None)
            reservation_manager.agent.core.schedule.assert_called_once_with(
                future_time, reservation_manager.update, future_time)

    def test_update_with_device_only(self, reservation_manager):
        """Test that 'update' publishes only for the specified device when 'device_only' is provided."""
        mock_now = get_aware_utc_now()
        future_time = mock_now + timedelta(minutes=5)
        device_state = Mock(agent_id='agent1', task_id='task1', time_remaining=60)
        reservation_manager.get_reservation_state.return_value = {'device1': device_state}
        reservation_manager.get_next_event_time.return_value = future_time
        reservation_manager._get_adjusted_next_event_time.return_value = future_time

        # Mock ACTUATOR_RESERVATION_ANNOUNCE_RAW
        with patch('platform_driver.reservations.ACTUATOR_RESERVATION_ANNOUNCE_RAW',
                   '/actuator/reservation/announce/{device}'):
            reservation_manager.update(now=mock_now, device_only='device1')

        reservation_manager.agent.vip.pubsub.publish.assert_called_once()
        args, kwargs = reservation_manager.agent.vip.pubsub.publish.call_args
        assert 'device1' in args[1], "The topic should include 'device1'"

    def test_update_no_publish(self, reservation_manager):
        """Test that 'update' does not publish when 'publish' is set to False."""
        mock_now = get_aware_utc_now()
        future_time = mock_now + timedelta(minutes=5)

        reservation_manager.get_reservation_state.return_value = {}
        reservation_manager.get_next_event_time.return_value = future_time
        reservation_manager._get_adjusted_next_event_time.return_value = future_time

        reservation_manager.update(now=mock_now, publish=False)

        reservation_manager.agent.vip.pubsub.publish.assert_not_called()
        reservation_manager.agent.core.schedule.assert_called_once_with(
            future_time, reservation_manager.update, future_time)

    def test_update_with_empty_device_states(self, reservation_manager):
        """Test that 'update' handles empty device states correctly."""
        mock_now = get_aware_utc_now()
        future_time = mock_now + timedelta(minutes=5)

        reservation_manager._device_states = {}
        reservation_manager.get_reservation_state.return_value = {}
        reservation_manager.get_next_event_time.return_value = future_time
        reservation_manager._get_adjusted_next_event_time.return_value = future_time

        reservation_manager.update(now=mock_now)

        reservation_manager.agent.vip.pubsub.publish.assert_not_called()
        reservation_manager.agent.core.schedule.assert_called_once_with(
            future_time, reservation_manager.update, future_time)


class TestReservationManagerGetAdjustedNextEventTime:
    now = get_aware_utc_now()

    @pytest.fixture
    def reservation_manager(self):
        parent = Mock()
        parent.vip = Mock()
        parent.vip.config.get = MagicMock(return_value=pickle.dumps({}))
        parent.vip.config.set = MagicMock()
        parent.config = Mock()
        parent.config.reservation_publish_interval = 60
        grace_time = 10

        rm = ReservationManager(parent, grace_time)
        rm._cleanup = MagicMock()
        rm.save_state = MagicMock()
        return rm

    def test_get_adjusted_next_event_time(self, reservation_manager):
        """Tests that it returns the next event time when next event is before previously reserved time"""
        next_event_time = self.now + timedelta(minutes=1)    # 60 seconds ahead
        previously_reserved_time = self.now + timedelta(minutes=2)    # 120 seconds ahead

        adjusted_time = reservation_manager._get_adjusted_next_event_time(
            self.now, next_event_time, previously_reserved_time)
        assert adjusted_time == next_event_time, "The adjusted time should be the next event time"

    def test_get_adjusted_next_event_time_previously_returned(self, reservation_manager):
        """Tests that it returns the previously reserved time when next event is after previously reserved time"""
        next_event_time = self.now + timedelta(minutes=2)    # 120 seconds ahead
        previously_reserved_time = self.now + timedelta(minutes=1)    # 60 seconds ahead

        adjusted_time = reservation_manager._get_adjusted_next_event_time(
            self.now, next_event_time, previously_reserved_time)
        assert adjusted_time == previously_reserved_time, "The adjusted time should be the previous event time"


class TestReservationManagerLoadState:

    @pytest.fixture
    def reservation_manager(self):
        parent = Mock()
        parent.config = Mock(reservation_publish_interval=60)
        rm = ReservationManager(parent, grace_time=10)
        rm._cleanup = MagicMock()
        return rm

    def test_load_state_none_initial_string(self, reservation_manager):
        """Tests loading state with None initial state string."""
        now = get_aware_utc_now()
        reservation_manager.load_state(now=now, initial_state_string=None)
        assert reservation_manager.tasks == {}, "Tasks should be empty when initial state string is None"

    def test_load_state_valid_initial_string(self, reservation_manager):
        """Tests loading state with a valid initial state string """
        now = get_aware_utc_now()
        reservation_manager.load_state(now=now,
                                       initial_state_string=pickle.dumps({'task1': 'data1'}))
        assert 'task1' in reservation_manager.tasks, "Tasks should contain the loaded data."

    def test_load_state_pickle_error(self, reservation_manager):
        """Test loading state with a pickle error."""
        now = get_aware_utc_now()
        reservation_manager.load_state(now=now, initial_state_string=b'not a pickle')
        assert reservation_manager.tasks == {}, "Tasks should be empty after a pickle error"

    def test_load_state_general_exception(self, reservation_manager):
        """Test loading state with a normal string"""
        now = get_aware_utc_now()
        reservation_manager.load_state(now=now, initial_state_string='unpickleable data')
        assert reservation_manager.tasks == {}, "Tasks should be empty after an exception."


class TestReservationManagerSaveState:
    now = get_aware_utc_now()

    @pytest.fixture
    def reservation_manager(self):
        parent = Mock()
        parent.vip = Mock()
        parent.vip.config = Mock()
        parent.vip.config.set = MagicMock()

        logger = Mock()
        logger.error = MagicMock()

        # Setting up the ReservationManager with mocked logger
        grace_time = 10
        rm = ReservationManager(parent, grace_time)
        rm._cleanup = MagicMock()
        rm._log = logger

        return rm

    def test_save_state_set_called_once(self, reservation_manager):
        expected_data = b64encode(dumps(reservation_manager.tasks)).decode("utf-8")

        reservation_manager.save_state(self.now)

        # Tests if our mocked object was called once, and with the correct args
        reservation_manager.agent.vip.config.set.assert_called_once_with(
            reservation_manager.reservation_state_file, expected_data,
            send_update=False), "save state should call parent.vip.config.set with correct data"

    def test_save_state_correct_file_name(self, reservation_manager):
        # make sure it's correct before
        assert reservation_manager.reservation_state_file == "_reservation_state", "file name should be _reservation_state before"
        reservation_manager.save_state(self.now)
        # and after calling save_state
        assert reservation_manager.reservation_state_file == "_reservation_state", "file name should be _reservation_state after"


class TestReservationManagerNewTask:
    sender = "test.agent"
    task_id = "test_task_id"
    requests = [['device1', '2022-01-01T00:00:00', '2022-01-01T01:00:00']]

    @pytest.fixture
    def reservation_manager(self):
        parent = Mock()
        parent.vip = Mock()
        parent.vip.config.get = MagicMock(return_value=pickle.dumps({}))
        parent.vip.config.set = MagicMock()
        parent.config = Mock()
        parent.config.reservation_publish_interval = 60    # Mock the interval for testing
        grace_time = 10

        rm = ReservationManager(parent, grace_time)
        rm._cleanup = MagicMock()
        rm.save_state = MagicMock()
        return rm

    def test_new_task_valid_inputs(self, reservation_manager):
        result = reservation_manager.new_task(self.sender,
                                              self.task_id,
                                              priority='HIGH',
                                              requests=self.requests)
        assert result.success

    def test_new_task_with_invalid_sender(self, reservation_manager):
        result = reservation_manager.new_task(sender="",
                                              task_id=self.task_id,
                                              priority="HIGH",
                                              requests=self.requests)
        assert result.info_string == 'MALFORMED_REQUEST: TypeError: agent_id must be a nonempty string' and not result.success

    def test_missing_agent_id(self, reservation_manager):
        result = reservation_manager.new_task(sender=None,
                                              task_id=self.task_id,
                                              priority="HIGH",
                                              requests=self.requests)
        assert result.info_string == 'MISSING_AGENT_ID' and not result.success

    def test_invalid_task_id(self, reservation_manager):
        """ Tests task request with missing task id, empty task id, and int task id"""
        result = reservation_manager.new_task(self.sender,
                                              task_id=None,
                                              priority="HIGH",
                                              requests=self.requests)
        assert result.info_string == 'MISSING_TASK_ID' and not result.success

        result = reservation_manager.new_task(self.sender,
                                              task_id="",
                                              priority="HIGH",
                                              requests=self.requests)
        assert result.info_string == 'MALFORMED_REQUEST: TypeError: taskid must be a nonempty string' and not result.success

        result = reservation_manager.new_task(self.sender,
                                              task_id=1234,
                                              priority="HIGH",
                                              requests=self.requests)
        assert result.info_string == 'MALFORMED_REQUEST: TypeError: taskid must be a nonempty string' and not result.success

    def test_requests_malformed(self, reservation_manager):
        """ Tests malformed request by creating new task with empty dates"""
        result = reservation_manager.new_task(self.sender,
                                              self.task_id,
                                              priority="HIGH",
                                              requests=[])
        assert result.info_string == 'MALFORMED_REQUEST_EMPTY' and not result.success

    def test_new_task_missing_priority(self, reservation_manager):
        result = reservation_manager.new_task(self.sender,
                                              self.task_id,
                                              priority=None,
                                              requests=self.requests)
        assert result.info_string == 'MISSING_PRIORITY' and not result.success

    def test_lowercase_priority(self, reservation_manager):
        result = reservation_manager.new_task(self.sender,
                                              self.task_id,
                                              priority="low",
                                              requests=self.requests)
        assert result.success

    def test_invalid_priority(self, reservation_manager):
        """ Tests an invalid priority (Medium priority does not exist)"""
        result = reservation_manager.new_task(self.sender,
                                              self.task_id,
                                              priority="MEDIUM",
                                              requests=self.requests)
        assert result.info_string == 'INVALID_PRIORITY' and not result.success

    def test_task_exists(self, reservation_manager):
        task_id = "test_task_id"
        mock_task = Mock()
        mock_task.make_current = Mock()    # add the make_current method to the mock task
        reservation_manager.tasks[task_id] = mock_task

        result = reservation_manager.new_task(self.sender,
                                              task_id,
                                              priority="HIGH",
                                              requests=self.requests)
        assert result.info_string == 'TASK_ID_ALREADY_EXISTS' and result.success == False

    def test_request_new_task_should_succeed_on_preempt_self(self, reservation_manager):
        """
        Test schedule preemption by a higher priority task from the same sender.
        """
        result = reservation_manager.new_task(self.sender,
                                              self.task_id,
                                              priority='LOW_PREEMPT',
                                              requests=self.requests)
        assert result.success
        result = reservation_manager.new_task(self.sender,
                                              "high_priority_task_id",
                                              priority='HIGH',
                                              requests=self.requests)
        assert result.success
        assert result.info_string == 'TASKS_WERE_PREEMPTED'

    def test_schedule_preempt_other(self, reservation_manager):
        """
        Test schedule preemption by a higher priority task from a different sender.
        """
        result = reservation_manager.new_task("agent1",
                                              self.task_id,
                                              priority='LOW_PREEMPT',
                                              requests=self.requests)
        assert result.success
        result = reservation_manager.new_task("agent2",
                                              "high_priority_task_id",
                                              priority='HIGH',
                                              requests=self.requests)
        assert result.success
        assert result.info_string == 'TASKS_WERE_PREEMPTED'

    def test_reservation_conflict(self, reservation_manager):
        """
        Test task conflict from different agents.
        """
        result = reservation_manager.new_task("agent1",
                                              self.task_id,
                                              priority='LOW',
                                              requests=self.requests)
        assert result.success
        result = reservation_manager.new_task("agent2",
                                              "different_task_id",
                                              priority='LOW',
                                              requests=self.requests)
        assert result.info_string == 'CONFLICTS_WITH_EXISTING_RESERVATIONS'

    def test_reservation_conflict_self(self, reservation_manager):
        """
        Test task conflict from one request.
        """
        # two tasks with same time frame
        requests = [['device1', '2022-01-01T00:00:00', '2022-01-01T01:00:00'],
                    ['device1', '2022-01-01T00:00:00', '2022-01-01T01:00:00']]
        result = reservation_manager.new_task("agent2",
                                              self.task_id,
                                              priority='LOW',
                                              requests=requests)
        assert result.info_string == 'REQUEST_CONFLICTS_WITH_SELF'

    def test_schedule_overlap(self, reservation_manager):
        """
        Test successful task when end time of one time slot is the same as
        start time of another slot.
        """
        time_1 = ['device1', '2022-01-01T00:00:00', '2022-01-01T01:00:00']
        time_2 = ['device2', '2022-01-01T01:00:00', '2022-01-02T01:00:00']
        result = reservation_manager.new_task("agent1",
                                              self.task_id,
                                              priority='LOW',
                                              requests=time_1)
        assert result.success
        result = reservation_manager.new_task("agent2",
                                              "different_task_id",
                                              priority='LOW',
                                              requests=time_2)
        assert result.success

    def test_cancel_error_invalid_task(self, reservation_manager):
        """
        Test invalid task id when trying to cancel a task.
        """
        # creating task with a task_id of "task_that_exists"
        result = reservation_manager.new_task(self.sender,
                                              task_id="task_that_exists",
                                              priority='LOW',
                                              requests=self.requests)
        assert result.success
        # trying to cancel a task with a task_id of "unexistent_task_id"
        result = reservation_manager.cancel_task(sender=self.sender, task_id="unexistent_task_id")
        assert result.info_string == 'TASK_ID_DOES_NOT_EXIST', "task id should not exist"


class TestReservationManagerCancelTask:
    sender = "test.agent"
    task_id = "test_task_id"

    @pytest.fixture
    def reservation_manager(self):
        parent = Mock(spec=PlatformDriverAgent)
        parent.vip = Mock()
        parent.vip.config = Mock()
        parent.vip.config.get = MagicMock(return_value=pickle.dumps({}))
        parent.config = Mock()
        parent.config.reservation_publish_interval = 10
        parent.core = Mock()
        grace_time = 10
        rm = ReservationManager(parent, grace_time)
        return rm

    def test_cancel_task_nonexistent_id(self, reservation_manager):
        result = reservation_manager.cancel_task(self.sender, self.task_id)
        assert result.success == False, "result should fail"
        assert result.info_string == 'TASK_ID_DOES_NOT_EXIST', "task should not exist"

    def test_cancel_task_agent_id_mismatch(self, reservation_manager):
        # Add a task with a different agent ID
        reservation_manager.tasks[self.task_id] = Mock(agent_id="different.agent")
        result = reservation_manager.cancel_task(self.sender, self.task_id)
        assert result.success == False, "result should fail"
        assert result.info_string == 'AGENT_ID_TASK_ID_MISMATCH', "info string should be agent id task id mismatch"

    def test_cancel_task_success(self, reservation_manager):
        # Add a task with the correct agent ID
        reservation_manager.tasks[self.task_id] = Mock(agent_id=self.sender)
        result = reservation_manager.cancel_task(self.sender, self.task_id)
        assert result.success == True, "result should succeed"
        assert self.task_id not in reservation_manager.tasks, "task id should no longer be in tasks"


class TestReservationManagerGetReservationState:

    @pytest.fixture
    def reservation_manager(self):
        parent = Mock()
        parent.vip.config.get = MagicMock()
        parent.vip.config.set = MagicMock()

        grace_time = 10
        rm = ReservationManager(parent, grace_time)

        rm._cleanup = MagicMock()
        return rm

    def test_get_reservation_state_multiple_running_tasks(self, reservation_manager):
        """Test get_reservation_state with multiple running tasks."""
        now = get_aware_utc_now()
        task1 = Mock()
        task1.agent_id = "agent1"
        task1.get_current_slots = MagicMock(
            return_value={"device1": Mock(end=now + timedelta(minutes=5))})
        task2 = Mock()
        task2.agent_id = "agent2"
        task2.get_current_slots = MagicMock(
            return_value={"device2": Mock(end=now + timedelta(minutes=10))})

        reservation_manager.tasks = {"task1": task1, "task2": task2}
        reservation_manager.running_tasks = {"task1", "task2"}
        reservation_manager.preempted_tasks = set()

        result = reservation_manager.get_reservation_state(now)

        reservation_manager._cleanup.assert_called_once_with(now)
        assert "device1" in result, "Device1 should be in reservation state"
        assert "device2" in result, "Device2 should be in reservation state"

        device_state1 = result["device1"]
        device_state2 = result["device2"]

        assert device_state1.agent_id == "agent1", "agent1 should be in agent_id for device1"
        assert device_state1.task_id == "task1", "task1 should be in task_id for device1"
        assert device_state1.time_remaining > 299, "Time remaining on device1 should be more than 299 seconds"

        assert device_state2.agent_id == "agent2", "agent2 should be in agent_id for device2"
        assert device_state2.task_id == "task2", "task2 should be in task_id for device2"
        assert device_state2.time_remaining > 599, "Time remaining on device2 should be more than 599 seconds"

    def test_get_reservation_state_with_preempted_tasks(self, reservation_manager):
        """Test get_reservation_state with preempted tasks."""
        now = get_aware_utc_now()
        task1 = Mock()
        task1.agent_id = "agent1"
        task1.get_current_slots = MagicMock(
            return_value={"device1": Mock(end=now + timedelta(minutes=5))})
        task2 = Mock()
        task2.agent_id = "agent2"
        task2.get_current_slots = MagicMock(
            return_value={"device2": Mock(end=now + timedelta(minutes=10))})

        reservation_manager.tasks = {"task1": task1, "task2": task2}
        reservation_manager.running_tasks = {"task1"}
        reservation_manager.preempted_tasks = {"task2"}

        result = reservation_manager.get_reservation_state(now)

        reservation_manager._cleanup.assert_called_once_with(now)
        assert "device1" in result, "Device1 should be in reservation state"
        assert "device2" in result, "Device2 should be in reservation state"

        device_state1 = result["device1"]
        device_state2 = result["device2"]

        assert device_state1.agent_id == "agent1", "agent1 should be in agent_id for device1"
        assert device_state1.task_id == "task1", "task1 should be in task_id for device1"
        assert device_state1.time_remaining > 299, "Time remaining on device1 should be more than 299 seconds"

        assert device_state2.agent_id == "agent2", "agent2 should be in agent_id for device2"
        assert device_state2.task_id == "task2", "task2 should be in task_id for device2"
        assert device_state2.time_remaining > 599, "Time remaining on device2 should be more than 599 seconds"

    def test_get_reservation_state_device_in_both_running_and_preempted(self, reservation_manager):
        """Test get_reservation_state when the same device is in both running and preempted tasks."""
        now = get_aware_utc_now()
        task1 = Mock()
        task1.agent_id = "agent1"
        task1.get_current_slots = MagicMock(
            return_value={"device1": Mock(end=now + timedelta(minutes=5))})
        task2 = Mock()
        task2.agent_id = "agent2"
        task2.get_current_slots = MagicMock(
            return_value={"device1": Mock(end=now + timedelta(minutes=10))})

        reservation_manager.tasks = {"task1": task1, "task2": task2}
        reservation_manager.running_tasks = {"task1"}
        reservation_manager.preempted_tasks = {"task2"}

        result = reservation_manager.get_reservation_state(now)

        reservation_manager._cleanup.assert_called_once_with(now)
        assert "device1" in result, "Device1 should be in reservation state"

        device_state = result["device1"]

        # Since preempted_results overwrite running_results, the device_state should come from preempted task
        assert device_state.agent_id == "agent2", "agent_id should be agent2 from preempted task"
        assert device_state.task_id == "task2", "task2 should be task_id from preempted task"
        assert device_state.time_remaining > 599, "Time remaining should be from preempted task"

    def test_get_reservation_state_with_expired_task(self, reservation_manager):
        """Test get_reservation_state with a task that has already expired."""
        now = get_aware_utc_now()
        task1 = Mock()
        task1.agent_id = "agent1"
        task1.get_current_slots = MagicMock(
            return_value={"device1": Mock(end=now - timedelta(minutes=1))})    # Ended 1 minute ago

        reservation_manager.tasks = {"task1": task1}
        reservation_manager.running_tasks = {"task1"}
        reservation_manager.preempted_tasks = set()

        # Simulate _cleanup removing expired tasks
        reservation_manager._cleanup.side_effect = lambda n: reservation_manager.running_tasks.discard(
            "task1")

        result = reservation_manager.get_reservation_state(now)

        reservation_manager._cleanup.assert_called_once_with(now)
        assert "device1" not in result, "Device1 should not be in reservation state as the task has expired"

    def test_get_reservation_state_with_no_tasks(self, reservation_manager):
        """Test get_reservation_state when there are no tasks."""
        now = get_aware_utc_now()
        reservation_manager.tasks = {}
        reservation_manager.running_tasks = set()
        reservation_manager.preempted_tasks = set()

        result = reservation_manager.get_reservation_state(now)

        reservation_manager._cleanup.assert_called_once_with(now)
        assert result == {}, "Reservation state should be empty when there are no tasks"

    def test_get_reservation_state_device_in_multiple_running_tasks(self, reservation_manager):
        """Test get_reservation_state when the same device is in multiple running tasks."""
        now = get_aware_utc_now()
        task1 = Mock()
        task1.agent_id = "agent1"
        task1.get_current_slots = MagicMock(
            return_value={"device1": Mock(end=now + timedelta(minutes=5))})
        task2 = Mock()
        task2.agent_id = "agent2"
        task2.get_current_slots = MagicMock(
            return_value={"device1": Mock(end=now + timedelta(minutes=10))})

        reservation_manager.tasks = {"task1": task1, "task2": task2}
        reservation_manager.running_tasks = {"task1", "task2"}
        reservation_manager.preempted_tasks = set()

        with pytest.raises(AssertionError):
            reservation_manager.get_reservation_state(now)

    def test_get_reservation_state_device_in_multiple_preempted_tasks(self, reservation_manager):
        """Test get_reservation_state when the same device is in multiple preempted tasks."""
        now = get_aware_utc_now()
        task1 = Mock()
        task1.agent_id = "agent1"
        task1.get_current_slots = MagicMock(
            return_value={"device1": Mock(end=now + timedelta(minutes=5))})
        task2 = Mock()
        task2.agent_id = "agent2"
        task2.get_current_slots = MagicMock(
            return_value={"device1": Mock(end=now + timedelta(minutes=10))})

        reservation_manager.tasks = {"task1": task1, "task2": task2}
        reservation_manager.running_tasks = set()
        reservation_manager.preempted_tasks = {"task1", "task2"}

        with pytest.raises(AssertionError):
            reservation_manager.get_reservation_state(now)

    def test_get_reservation_state_task_with_no_slots(self, reservation_manager):
        """Test get_reservation_state when a task has no current slots."""
        now = get_aware_utc_now()
        task1 = Mock()
        task1.agent_id = "agent1"
        task1.get_current_slots = MagicMock(return_value={})    # No current slots

        reservation_manager.tasks = {"task1": task1}
        reservation_manager.running_tasks = {"task1"}
        reservation_manager.preempted_tasks = set()

        result = reservation_manager.get_reservation_state(now)

        reservation_manager._cleanup.assert_called_once_with(now)
        assert result == {}, "Reservation state should be empty when tasks have no current slots"


class TestReservationGetNextEventTime:

    @pytest.fixture
    def reservation(self):
        return Reservation()

    def test_get_next_event_time_no_slots(self, reservation):
        now = get_aware_utc_now()
        assert reservation.get_next_event_time(
            now) is None, "Should return None when there are no time slots."

    def test_get_next_event_time_future_slots(self, reservation):
        now = get_aware_utc_now()
        future_start = now + timedelta(hours=1)
        future_end = now + timedelta(hours=2)
        reservation.time_slots.append(TimeSlice(start=future_start, end=future_end))

        # adjusting the expected time for rounding similar to get_next_event_time
        expected_time = future_start.replace(microsecond=0) + timedelta(seconds=1)

        assert reservation.get_next_event_time(now) == expected_time, \
            "Should return the start of the next future slot after rounding to the next second"

    def test_get_next_event_time_during_active_slot(self, reservation):
        """Tests get_next_event_time returns the most recent end time, indicating the next even time"""
        now = get_aware_utc_now()
        active_start = now - timedelta(minutes=30)    # 30 minutes ago
        active_end = now + timedelta(minutes=30)    # 30 minutes from now
        reservation.time_slots.append(TimeSlice(start=active_start, end=active_end))

        expected_time = active_end.replace(microsecond=0) + timedelta(
            seconds=1)    # Adjusting for the rounding in the method
        assert reservation.get_next_event_time(
            now) == expected_time, "Should return the end of the current active slot."


class TestReservationManagerCleanup:
    now = get_aware_utc_now()

    @pytest.fixture
    def reservation_manager(self):
        parent = Mock()
        parent.vip.config.get = MagicMock()
        parent.vip.config.set = MagicMock()

        grace_time = 10
        rm = ReservationManager(parent, grace_time)

        # mock the task states
        rm.task_finished = Mock(spec=Task)
        rm.task_finished.state = Task.STATE_FINISHED

        rm.task_running = Mock(spec=Task)
        rm.task_running.state = Task.STATE_RUNNING

        rm.task_preempted = Mock(spec=Task)
        rm.task_preempted.state = Task.STATE_PREEMPTED

        # Set up the manager with our mock tasks.
        rm.tasks = {
            "finished": rm.task_finished,
            "running": rm.task_running,
            "preempted": rm.task_preempted
        }

        return rm

    def test_cleanup_finished_task(self, reservation_manager):
        """Tests that cleanup removed finished tasks"""
        assert "finished" in reservation_manager.tasks
        assert reservation_manager.task_finished.state == Task.STATE_FINISHED

        reservation_manager._cleanup(self.now)

        assert "finished" not in reservation_manager.tasks, "finished tasks should have been removed"
        assert "finished" not in reservation_manager.running_tasks, "finished tasks should have been removed"
        assert "finished" not in reservation_manager.preempted_tasks, "finished tasks should have been removed"

    def test_cleanup_running_task(self, reservation_manager):
        """Tests that _cleanup correctly added running tasks based on task state"""
        reservation_manager._cleanup(self.now)

        assert "running" in reservation_manager.running_tasks, "running tasks should remain"
        assert "running" not in reservation_manager.preempted_tasks, "running task should not be in preempted tasks"
        assert "running" in reservation_manager.tasks, "running should be in tasks"

    def test_cleanup_preempted_task(self, reservation_manager):
        """Tests that _cleanup correctly added the preempted tasks based on task state"""
        reservation_manager._cleanup(self.now)

        assert "preempted" in reservation_manager.preempted_tasks, "preempted task should stay"
        assert "preempted" not in reservation_manager.running_tasks, "preempted task should not be set in running tasks"
        assert "preempted" in reservation_manager.tasks, "preempted should exist in tasks"


if __name__ == '__main__':
    pytest.main()


class TestReservationManagerNaiveTimes:
    """Naive request times are read in the system's local zone. tzlocal 5 returns zoneinfo zones, which have no
    localize(); the conversion must work with those as well as with the pytz-style zones of tzlocal 4."""

    @pytest.fixture
    def reservation_manager(self):
        parent = Mock()
        parent.vip = Mock()
        parent.vip.config.get = MagicMock(return_value=pickle.dumps({}))
        parent.vip.config.set = MagicMock()
        parent.config = Mock()
        parent.config.reservation_publish_interval = 60
        rm = ReservationManager(parent, 10)
        rm._cleanup = MagicMock()
        rm.save_state = MagicMock()
        return rm

    def test_naive_times_are_read_in_the_local_zoneinfo_zone(self, reservation_manager):
        from zoneinfo import ZoneInfo
        zone = ZoneInfo("America/Los_Angeles")
        with patch("platform_driver.reservations.get_localzone", return_value=zone):
            result = reservation_manager.new_task("test.agent", "naive", "HIGH",
                                                  [['device1', '2022-07-01T12:00:00', '2022-07-01T13:00:00']])
        assert result.success, result.info_string
        task = reservation_manager.tasks["naive"]
        assert task.time_slice.start == datetime(2022, 7, 1, 12, tzinfo=zone)
        assert task.time_slice.start.utcoffset() == timedelta(hours=-7)
