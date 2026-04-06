# Platform Driver Agent

![Passing?](https://github.com/eclipse-volttron/volttron-platform-driver/actions/workflows/run-tests.yml/badge.svg)
[![pypi version](https://img.shields.io/pypi/v/volttron-platform-driver.svg)](https://pypi.org/project/volttron-platform-driver/)


The Platform Driver agent is a special purpose agent a user can install on the platform to manage communication of the platform with devices. The Platform driver features a number of endpoints for collecting data and sending control signals using the message bus and automatically publishes data to the bus on a specified interval.

## Pre-requisite

Before installing this agent, VOLTTRON (>=11.0.0rc0) should be installed and running.  Its virtual environment should be active.
Information on how to install of the VOLTTRON platform can be found
[here](https://github.com/eclipse-volttron/volttron-core/tree/v10)

## Automatically installed dependencies

- volttron-lib-base-driver >= 2.0.0rc0

# Documentation
More detailed documentation can be found on [ReadTheDocs](https://eclipse-volttron.readthedocs.io/en/latest/external-docs/volttron-platform-driver/index.html). The RST source
of the documentation for this component is located in the "docs" directory of this repository.
This documentation is current up to version 1.x of the Platform Driver Agent and will be updated
to reflect new features and behaviors in the course of RC releases. Most existing configurations
and behaviors remain valid, however, so this remains a good source of information.

#### New Polling Features
The Platform Driver version 2.0 does introduce multiple new capabilities.
A detailed table showing current state of completion of features to be included in
the full 2.0.0 release can be found [here](driver_status_2.0.0rc0.png).
In addition to an expanded API, as summarized below, the new driver contains several
features intended to make polling more scalable, flexible, and efficient:

* Poll rates may now be set on individual points by adding a 'polling_interval' colunn
  to the registry.  Any point which does not have a polling interval set will fall back
  to the default interval defined in the device configuration.
* In the case where multiple devices within VOLTTRON correspond to a single remote on the
  network (that is the driver_configs dictionaries are the same), these will by default be
  polled in a single request, when possible, to the remote. No additional configuration
  is required to make use of this feature, but it can be disabled, if desired, by
  setting the "allow_duplicate_remotes" setting to True in either the agent configuration
  (as the default for all devices) or separately in the configuration for each device which
  should not share its remote with other devices.
* Last known values and times at which these were obtained are stored
  in a data model and can be retrieved with the "last" RPC method.

#### Publication Changes
As the direct result of the new capability for points to have different polling rates,
all points are no longer guaranteed to be obtained on every poll. In the previous driver
implementation, an all publish would be sent at the completion of each poll with the data
obtained from all configured points on the device. In the new design, however, only a subset
of points is guaranteed to be new. For this reason, the default behavior is now to publish
on a topic ending in "/multi". The multi-style publish is formatted identically to the all-style
publishes, except that it does not necessarily always contain every point on the device.

For applications which require all points to be available in a single publish, an all-style
publish may still be configured.  This can be done by setting the "publish_all_depth" key to True
in the device configuration file for any devices which should be published in this manner.
(The key worded "publish_depth_first_all" will continue to work as well for the same purpose.)
Additionally, an "all_publish_interval" should be provided as a number of seconds between publishes.
These settings may also be set in the agent configuration if the same behavior is desired for all
configured devices.

All-type publishes will begin once a first round of polling has completed for all points, and will contain
the last known value for each point at the time of the poll. A "stale_timeout" setting may
be configured for the entire device or in the registry on a point-by-point basis. All publishes
will only occur if all points have not become stale. The default state_timeout is 3 times the
length of the polling interval for any point.

#### Expanded API
The RPC API for the driver has been expanded both for the addition of new features and also as the result
of merging the functionality of the former Actuator Agent into the Platform Driver Agent itself.

The following new methods are provided for more flexible queries.

```
get(topic: str | Sequence | Set = None, regex: str = None) -> (dict, dict)
```
```
set(value: any, topic: str | Sequence | Set = None, regex: str = None) -> (dict, dict)
```

```
revert(topic: str | Sequence | Set = None, regex: str = None) -> (dict, dict)
```
```
last(topic: str | Sequence | Set = None, regex: str = None, value: bool = True, updated:bool = True) -> dict
```

Astute observers will notice that these methods largely share common arguments,
which are described below. Set also takes a value or mapping of values which will be
written to points. Last can also be configured to return values, update times, or both
(using the value and updated boolean arguments).

They each return two dicts --- the first for results and the second for errors.
The last method, which returns last known values and/or updated times for a set of points,
does not need to make a network request to obtain its data, it provides only a results dictionary.


* **topic**: This can be one or more topics. 
  * Where the topic describes more than a -single point, all points corresponding to the topic will be returned.
  * The '-' character may also be used as a wildcard to replace any complete segment in a topic string. For instance:
    `devices/Campus/Building1/-/ZoneTemperatureSetPoint` would match all devices in Building1
    with a point called "ZoneTemperatureSetPoint".
* **regex**: The set of points obtained from the topic argument may be further refined using
  a regular expression. If no topic is provided at all, the regular expression will be applied
  to all topics known to the driver.

The following additional methods have been added:

```
list_interfaces() -> list[str]
```
* List interfaces provides all the protocol interfaces which have been installed.  Additional methods will be provided
  in later releases for managing these.

```
list_topics(topic: str, regex: str = None, active: bool = False, enabled: bool = False) -> list[str]
```
* List topics provides a list of available topics. This can be filtered
  using the same arguments as the new query methods and also by whether these are
  active (currently being polled) or enabled (configured to be active).

```
get_poll_schedule()
```
* Get poll schedule provides a description of the polling schedule which has been
  built for polling devices. This can be useful in understanding and refining polling configurations.

All existing RPC methods from both the Platform Driver and Actuator agents continue to work as before.
In cases where both agents had methods with the same name, effort has been made to preserve the ability to
use either style of arguments to these functions. The one corner case which is known to not work is if
the caller passed a string for the requester_id when setting or reverting points using actuator-style arguments.
This will only work if the string passed was the vip-identity of the agent. If this argument is left out, however,
or the vip-identity is used, then it should continue to work as expected. The arguments shown here are the
old-driver-style arguments. Agents written to use this argument style will always work.


```
get_point(path: str = None, point_name: str = None, **kwargs) -> any
```

```
set_point(path: str, point_name: str | None, value: any, *args, **kwargs) -> any
```

```
scrape_all(topic: str) -> dict
```

```
get_multiple_points(path: str | Sequence[str | Sequence] = None, point_names = None, **kwargs) -> (dict, dict)
```

```
set_multiple_points(path: str, point_names_values: list[tuple[str, any]], **kwargs) -> dict
```

```
revert_point(path: str, point_name: str, **kwargs)
```

```
revert_device(path: str, *args, **kwargs)
```

# Installation

Install the volttron-platform-driver.

```shell
vctl install volttron-platform-driver --vip-identity platform.driver
```

View the status of the installed agent

```shell
vctl status
```

To communicate with devices, one or more driver interfaces will also need to be installed.
Each interface is distributed as a library and may be installed separately using `vctl install-lib`.
In the current RC version of the driver, only two interfaces are fully supported:

* A Fake Driver (which returns data from a csv file):
    ```shell
    vctl install-lib volttron-lib-fake-driver
    ```
* BACnet:
    ```shell
    vctl install-lib volttron-lib-bacnet-driver
    ```

Libraries may be removed later with `vctl remove-lib <library name>`.

Additional interfaces will be available in later RC releases.

# Configuration

Existing configuration files should generally continue to work as expected.
The following is the full set of possible confiurations (and their defaults)
for the Platform Driver Agent. Note that if no changes are made to the defaults,
no configuration file is needed for the agent. "Alias" names are provided for backwards
compatability and will work the same as the name in the key of this dictionary.

```json
{
  "allow_duplicate_remotes":false, # Setting True will cause every device to make separate network requests.
  "allow_no_lock_write":true,  # Allow writes to devices without first making a reservation.
  "allow_reschedule":true,  # Allow the polling schedule to be updated when new devices are added.
  "breadth_first_base":"points",  # Topics published breadth first will begin with this segment -- e.g., points/rest/of/topic
  "default_polling_interval":60.0,  # The default interval for polling where it is not specified by a device or registry configuration.
  "depth_first_base":"devices",  # Topics published breadth first will begin with this segment -- e.g., devices/rest/of/topic,
  "groups":{
    # Groups should be configured here (each as its own dict). 
    #   The key will be used to identify the group in device configurations.
    #   Groups may have any name (not just integers as in the old driver).,
    # NOTE -- Additional named groups will default to "parallel_subgroups" = false,
    #   and "minimum_polling_interval" = 1.0 if these are not specified.
    "default":{  # The default group for devices that do not specify one is "default".
      "minimum_polling_interval":0.02,  # The shortest time allowed between polls (in seconds).
      "start_offset_seconds":0.0,  # The first poll of the group will be delayed this many seconds.
      "poll_scheduler_class_name":"StaticCyclicPollScheduler",  # Allows specification of a different PollScehduler class. (overrides the agent config for this group.)
      "poll_scheduler_module":"platform_driver.poll_scheduler",  # Module for a specified poll scheduler class.
      "poll_scheduler_configs":null,  # Future use. Allows configurations to be passed to a Poll Scheduler.
      "parallel_subgroups":true  # Whether remotes within the group will be polled concurrently.
    }
  },
  "group_offset_interval":0.0,  # The default used for groups that do not spefify an offset interval.
  "max_concurrent_publishes":10000,  # The most publishes allowed at once.
  "max_open_sockets":null,  # The largest allowed number of sockets which may be opened.
  "minimum_polling_interval":0.02,  # (alias='driver_scrape_interval') The shortest time allowed between polls (in seconds).
  "poll_scheduler_class_name":"StaticCyclicPollScheduler",  # Allows specification of a different PollScehduler class.
  "poll_scheduler_configs":null,  # Future use. Allows configurations to be passed to a Poll Scheduler.
  "poll_scheduler_module":"platform_driver.poll_scheduler",  # Module for a specified poll scheduler class.
  "publish_single_depth":false,  # (alias='publish_depth_first_single') Default setting for whether points will be published individually with depth-first topics.
  "publish_single_breadth":false,  # (alias='publish_breadth_first_single') Default setting for whether points will be published individually with breadth-first topics.
  "publish_all_depth":false,  # (alias='publish_depth_first_all') Default setting for whether devices will be published with all-type publishes and with depth-first topics.
  "publish_all_breadth":false,  # (alias='publish_breadth_first_all') Default setting for whether devices will be published with all-type publishes and with breadth-first topics.
  "publish_multi_depth":true,  # (alias='publish_depth_first_multi') Default setting for whether devices will be published with multi-type publishes and with depth-first topics.
  "publish_multi_breadth":false,  # (alias='publish_breadth_first_multi') Default setting for whether devices will be published with multi-type publishes and with breadth-first topics.
  "remote_heartbeat_interval":60.0,  # The default interval on which to send a heartbeat to remotes configured with a heart_beat_point.
  "reservation_preempt_grace_time":60.0,  # Length of time before reservation preemption takes effect. 
  "reservation_publish_interval":60.0,  # The interval on which to make publishes of reservation statistics.
  "reservation_required_for_write":false,  # Require a reservation before making writes.
  "scalability_test":false,  # Whether to start a scalability test.
  "scalability_test_iterations":3,  # Number of iterations for scalabilty tests.
  "timezone":"UTC",  # The timezone to use in pulbish meta-data.
}
```

# Development

Please see the following for contributing guidelines [contributing](https://github.com/eclipse-volttron/volttron-core/blob/develop/CONTRIBUTING.md).

Please see the following helpful guide about [developing modular VOLTTRON agents](https://github.com/eclipse-volttron/volttron-core/blob/develop/DEVELOPING_ON_MODULAR.md)


# Disclaimer Notice

This material was prepared as an account of work sponsored by an agency of the
United States Government.  Neither the United States Government nor the United
States Department of Energy, nor Battelle, nor any of their employees, nor any
jurisdiction or organization that has cooperated in the development of these
materials, makes any warranty, express or implied, or assumes any legal
liability or responsibility for the accuracy, completeness, or usefulness or any
information, apparatus, product, software, or process disclosed, or represents
that its use would not infringe privately owned rights.

Reference herein to any specific commercial product, process, or service by
trade name, trademark, manufacturer, or otherwise does not necessarily
constitute or imply its endorsement, recommendation, or favoring by the United
States Government or any agency thereof, or Battelle Memorial Institute. The
views and opinions of authors expressed herein do not necessarily state or
reflect those of the United States Government or any agency thereof.
