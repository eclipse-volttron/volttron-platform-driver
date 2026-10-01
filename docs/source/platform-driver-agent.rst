.. _Platform-Driver-Agent:

===============
Platform Driver
===============

The Platform Driver Agent manages all device communication. This agent automatically collects data from
configured devices and publishes it to the message bus, generally on a specified interval.
It also features a number of RPC endpoints for getting and setting points on devices.

The Platform Driver uses :ref:`device configuration files <Device-Configuration-File>` in the platform driver's
configuration store to set up driver instances for each unique device controller on the network.
The agents also builds a polling schedule on which to retrieve data from the device and publish it to the message bus.
The driver additionally handles ad-hoc read and write requests issued by RPC.

.. note::

    Earlier versions of VOLTTRON referred to all data collection from a device as "scraping". The current documentation
    has been update to use the more precise language. The word "pull" or "query" are used for the activity of discretely
    requesting data from a remote, while the word "polling" is used where the data is requested in a periodic loop. The
    words "poll" and "scape", as used here, may be considered interchangeable.

Actual communication with devices is handled using protocol-specific implementations of standard logic logic defined
in an "Interface" class. Information on developing new driver interfaces can be found on
:ref:`the driver development page <Driver-Development>`.


.. _Platform-Driver-Configuration:

Configuration and Installation
==============================

Installation
------------

The platform driver agent may be installed using vctl:

.. code-block:: bash

    vctl install volttron-platform-driver --tag driver

Additionally, to communicate with any devices, it will be necessary to install one or more interface libraries.
These may be installed from pypi using vctl:

.. code-block:: bash

    vctl install-lib volttron-lib-fake-driver

Officially maintained driver interfaces (with corresponding package names) include:

* :ref:`Fake Driver <Fake-Driver>`: volttron-lib-fake-driver
* :ref:`BACnet Driver <BACnet-Driver>`: volttron-lib-bacnet-driver
* :ref:`DNP3 Driver <DNP3-Driver>`: volttron-lib-dnp3-driver
* :ref:`PyModbus Driver <Modbus-Driver>`: volttron-lib-modbus-driver
* :ref:`ModbusTk Driver <Modbus-TK-Driver>`: volttron-lib-modbustk-driver

Configuration
-------------

The Platform Driver requires use of the configuration store and expects three types of configuration file:

* **Platform Driver Agent Configuration (one for the agent):** global settings for all drivers.
* **Device Configuration (one per device):** settings for the driver to manage an individual device.
* **Registry (up to one per device):** contains the settings for each individual data point for a device or class of
  device. Some protocols (e.g, BACnet) may require one registry per individual device,
  but often with other protocols one registry may be shared by multiple devices of the same model.

Platform Driver Agent Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Platform Driver Agent configuration consists of general settings for all devices. The default values should be
sufficient for most users. If changes are made to any of the global settings, this configuration should be saved to the
configuration store with the name "config":

.. code-block:: bash

    vctl config store platform.driver config path/to/edited/config/file

The following example sets the driver_scrape_interval to 0.05 seconds or 20 devices per second:

.. code-block:: json

    {
        "driver_scrape_interval": 0.05,
        "publish_breadth_first_all": false,
        "publish_depth_first": false,
        "publish_breadth_first": false,
        "publish_depth_first_all": true,
        "group_offset_interval": 0.0
    }

* **minimum_polling_interval** (alias: driver_scrape_interval) - Sets the interval between devices polls.
  Defaults to 0.02 or 50 devices per second. This is useful for when the platform polls too many devices at once
  resulting in failed polls.

To spread polling of n devices evenly throughout a polling cycle, this may be set to:

    .. code-block::

        length_of_cycle_in_seconds / n

  If multiple groups are used, n would be the size of the largest group.

* **group_offset_interval** - Sets the delay between when each group of devices begins to be polled. There is no effect
  if all devices are in the same group. Group 0 will begin at ``t=0``. The first device of each subsequent group will
  be polled starting at

  .. code-block::

      t = group_number * group_offset_interval.

To improve the scalability of the platform unneeded device state publishes for all devices can be turned on and off.
Configured at the agent level, these settings will apply to all devices, but may also be overridden on a
device-by-device manner in the :ref:`Device Configuration <Device-Configuration-File>`.

.. note::

    * Depth first publishes have topics with the point name at the end (i.e., "campus/building/device/point").
    * Breadth first publishes have topics with the point name at the beginnning (i.e., "point/device/building/campus").
    * Historian agents subscribe only to the ``publish_depth_first_all`` version, so this must be true to archive data.

* **publish_depth_first_all** (default `True`) - Enable "depth first" publish of all points to a single topic.
* **publish_breadth_first_all** - (default `False`) Enable "breadth first" publish of all points to a single topic.
* **publish_depth_first** - (default `False`) Enable separate "depth first" device state publishes for each point.
* **publish_breadth_first** - (default `False`) Enable separate "breadth first" device state publishes for each point.

See :ref:`device scalability settings <Device-Scalability-Settings>` for more details on the effect of these settings.

An example `platform driver configuration file <https://raw.githubusercontent.com/eclipse-volttron/volttron-platform-driver/main/config>`_,
with default settings can be found in the volttron-platform-driver repository.

.. _Device-Configuration-File:

Device Configuration
^^^^^^^^^^^^^^^^^^^^

Each device must have a corresponding device configuration in the platform driver configuration store.
The topic used to reference the device is derived from the name of this device configuration. For instance,
to follow the common topic convention of ``{campus}/{building}/{unit}``, the device configuration file should be given
the name ``devices/{campus}/{building}/{unit}`` in the configuration store:

.. code-block:: bash

    vctl config store platform.driver devices/PNNL/Building1/AHU2 path/to/config/file.json

Each device configuration has the following form:

.. code-block:: json

    {
        "driver_config": {"device_address": "10.1.1.5",
                          "device_id": 500},
        "driver_type": "bacnet",
        "registry_config":"config://registry_configs/vav.csv",
        "interval": 60,
        "heart_beat_point": "heartbeat",
        "group": 0
    }

The following settings are required for all device configurations:

    - **driver_config** - Interface specific settings. See documentation for each interface type.
    - **driver_type** - Driver interface to use for this device: "bacnet", "modbus", "fake", etc.
    - **registry_config** - Reference to another file in the configuration store containing information regarding points
      on the device. See the `Registry-Configuration-File`_ section below.

These settings are optional:

    - **interval** - Period (in seconds) on which to poll the device and publish the results. Defaults to 60 seconds.
    - **heart_beat_point** - A Point on the device to toggle as a heartbeat. A point with this ``Volttron
      Point Name`` must exist in the registry.  If this setting is missing, the driver will not send a heart beat signal
      to the device.  Heart beats are triggered by the :ref:`Actuator Agent <Actuator-Agent>` which must be running to
      use this feature.
    - **group** - Group to which this device belongs. (Defaults to 0) --- See :ref:`Device Grouping <Device-Grouping>`.

Device Grouping
"""""""""""""""

Devices may be assigned to groups to separate them logically when they are polled. This is done by configuration of two
settings:

    1. Each device configuration may have a ``group`` setting, which should be an integer greater than or equal to 0.

    2. ``group_offset_interval``, in the platform driver agent configuration is the number of seconds to delay the start
       of each group after the start of the previous. When using this setting, assign devices only consecutive ``group``
       values starting from 0.

An independent polling schedule is created for each group, where the first device in each group is polled
``group_offset_interval`` * ``group`` seconds after the first device of group 0. Each device within the group will then
be polled ``driver_scrape_interval`` seconds apart.

Groups are most commonly useful in two cases:

* To ensure that certain devices are polled in close proximity to each other, you can put them in their own
  group. They are then guaranteed to be polled `driver_scrape_interval` seconds apart.
* You may poll devices on different networks in parallel for performance.  For instance BACnet devices behind a single
  MSTP router need to be polled slowly and serially, but devices behind different routers may be polled in parallel.
  Grouping devices by router will achieve this automatically.


.. _Registry-Configuration-File:

Registry Configuration File
---------------------------
Registry configuration files setup each individual point on a device. As the registry is a list of records, this file
will typically be provided as CSV, but may also be JSON (a list of objects). The exact set of fields in each record is
driver specific. See the section for a particular driver for the registry configuration format.

The following is a simple example of a Modbus registry configuration file:

.. csv-table:: Catalyst 371
    :header: Reference Point Name,Volttron Point Name,Units,Units Details,Modbus Register,Writable,Point Address,Default Value,Notes

    CO2Sensor,ReturnAirCO2,PPM,0.00-2000.00,>f,FALSE,1001,,CO2 Reading 0.00-2000.0 ppm
    CO2Stpt,ReturnAirCO2Stpt,PPM,1000.00 (default),>f,TRUE,1011,1000,Setpoint to enable demand control ventilation
    HeatCall2,HeatCall2,On / Off,on/off,BOOL,FALSE,1114,,Status indicator of heating stage 2 need


.. _Adding-Devices-To-Config-Store:

Adding Device Configurations to the Configuration Store
-------------------------------------------------------

Configurations are added to the Configuration Store using the command line:

.. code-block:: bash

    vctl config store platform.driver <name> <file name> <file type>

* **name** - The label given to the configuration in the store.
* **file name** - A file containing the contents of the configuration.
* **file type** - ``--raw``, ``--json``, or ``--csv``. Indicates the type of the file. Defaults to ``--json``.

The main configuration must have the name ``config``

Device configuration but **not** registry configurations **must** have a name prefixed with ``devices/``.  Scripts that
automate the process will prefix registry configurations with ``registry_configs/``, but that is not a requirement for
registry files.

The name of the device's configuration in the store is used to create the topic used to reference the device. For
instance, a configuration named `devices/PNNL/ISB1/vav1` will publish polling results to `devices/PNNL/ISB1/vav1`.

The name of a registry configuration **must** match the name used to refer to it in the driver configuration.
The reference is not case sensitive.

If the Platform Driver Agent is running, any changes to the configuration store will immediately affect the running devices
according to the changes.

Example
^^^^^^^

Consider the following three configuration files:  A platform driver configuration called `platform-driver.agent`, a
Modbus device configuration file called `modbus_driver.config` and corresponding Modbus registry configuration file called
`modbus_registry.csv`

To store the platform driver configuration run the command:

.. code-block:: bash

    vctl config store platform.driver config platform-driver.agent

To store the registry configuration run the command (note the ``--csv`` option):

.. code-block:: bash

    vctl config store platform.driver registry_configs/modbus_registry.csv modbus_registry.csv --csv

.. Note::

    The `registry_configs/modbus_registry.csv` argument in the above command must match the reference to the
    `registry_config` found in `modbus_driver.config`.

To store the driver configuration run the command:

.. code-block:: bash

    volttron-ctl config store platform.driver devices/my_campus/my_building/my_device modbus_config.config

Additional Configuration for production environments
----------------------------------------------------

Since platform driver is designed to read and write information from various devices, once you test your agents in development environment, we highly recommend you **add authorization rules to your production deployment** to prevent unauthorized access to your devices. Please refer to `VIP Authorization <https://eclipse-volttron.readthedocs.io/en/latest/platform-features/message-bus/vip/vip-authorization.html>`_  on how to configure authorization rules to protect driver rpc calls and devices topic that driver writes to.


Usage
=====

After installing the Platform Driver and loading driver configs into the config store, the installed drivers begin
polling and JSON-RPC endpoints become usable.


Polling
-------

Once running, the Platform Driver will spawn drivers using the `driver_type` parameter of the
:ref:`driver configuration <Device-Configuration-File>` and periodically poll devices for all point data specified in
the :ref:`registry configuration <Registry-Configuration-File>` at the interval specified by the interval parameter
of the driver configuration. This is done using the ``scrape_all`` method, which can also be called outside the polling
scheduler by a user using RPC.

Consider a device configured with the name ``devices/pnnl/isb1/vav1`` and a registry containing two points:
"Temperature" and "AirFlow". A "depth first all" publish to the topic `devices/pnnl/isb1/vav1/all` may contain the
following message:

    .. code-block:: python

        [
            {
                "Temperature": 75.2,
                "AirFlow": 302
            },
            {
                "Temperature": {
                    "units": "F"
                },
                "AirFlow": {
                    "units": "CFM"
                }
            }
        ]

The first dictionary in the publish contains the current values reported by the device when polled. The second
dictionary contains meta-data, which generally has been obtained by the driver interface from the registry file. The
exact contents of the meta-data dictionary will be interface specific. Note that if this data is being archived by
an historian, two topics will be stored in the database. These contain the topic (without "devices/" or "/all", and the
point name:

* "pnnl/isb1/vav1/Temperature"
* "pnnl/isb1/vav1/AirFlow"

.. note::

    For additional, non-default publish formats, see :ref:`Device Scalability Settings <Device-Scalability-Settings>`.


Getting Values
--------------

While a device driver for a device will periodically poll and publish
the state of a device you may want an up to the moment value for a
point on a device. This can be accomplished using the ``get_point`` &
``get_multiple_points`` RPC methods. The ``scrape_all`` method, employed
in polling, can also be used to retreive all points on a single device.

Get Point
^^^^^^^^^

 The ``get_point`` method returns the value of one device, and takes two parameters:

:topic: The topic of the device, without the point name.
:point_name: The point name.

Get Multiple Points
^^^^^^^^^^^^^^^^^^^

The ``get_multiple_points`` method return values corresponding to multiple points on a single device.
It takes two parameters:

:path: The device topic (without point name).
:point_names: An iterable of device point names.

Scrape All
^^^^^^^^^^

The ``scrape_all`` method returns values for all points on the specified device. This is the same method
called internally by the driver's polling mechanism. The ``scrape_all`` method
takes one parameter:

:path: The device topic (not including point names).


Setting Values
--------------

The value of points may be set using one of two methods. The ``set_point`` method sets an indvidual point, while
the ``set_points_multiple``, sets a batch of points with a single RPC call.

.. warning::

    When points are set by sending requests directly to the Platform Driver Agent, it bypasses the scheduling capability
    of the :ref:`Actuator <Actuator-Agent>` agent.

Set Point
^^^^^^^^^

The ``set_point`` method sets the value of a single device. If the global override is condition is set, it will raise
an OverrideError exception. Set point takes three parameters:

:path: The device topic (without point name).
:point_name: The name of the point to be set.
:value: The new value for the point.

Set Multiple Points
^^^^^^^^^^^^^^^^^^^

The ``set_multiple_points`` sets the values of multiple points on the same device.
If the global override is condition is set, it will raise an OverrideError exception.
``set_multiple_points`` takes two parameters:

:path: The device topic (not including point names).
:point_names_value: A list of tuples consisting of (point_name, value) pairs for each point.


Reverting Values and Devices to a Default State
-----------------------------------------------

The value of previously set devices may be reverted to default or prior values.
The exact mechanism used to accomplish this is driver specific. Points may be reverted
individually or across entire devices.

Revert Point
^^^^^^^^^^^^

The ``revert_point``reverts the value of a specific point on a device to a default state.
If the global override condition is set, it will raise an OverrideError exception.
This method requires two parameters:

:path: The device topic (not including the point name).
:point_name: The name of the point.


Revert Device
^^^^^^^^^^^^^

The ``revert_device`` reverts all points on a single device to default state/values.
If global override is condition is set, it will raise an OverrideError exception. This method
takes one parameter:

:path: The device topic (not including point names).


.. _Platform-Driver-Override:
Overrides
---------

By default, every user is allowed write access to the devices by the platform driver.
The override feature allows a user (for example, building administrator) to lock devices
from being written for a specified duration of time or indefinitely. Optionally,
the devices may also be reset when they are overridden.

Any changes made to override patterns are stored in the config store.  On startup, the list
of override patterns and their corresponding end times are retrieved from the config store.
If the end time is indefinite or greater than current time for any pattern, then override is set
on the matching devices for remaining duration of time.

Whenever a device is newly configured, a check is made to see if it is part of the overridden
patterns.  If yes, it is added to list of overridden devices. Conversely, when a device is being
removed, a check is made to see if it is part of the overridden devices.  If yes, it is removed
from the list of overridden devices.

.. admonition:: Override Patterns

    The topics to be overridden are determined by a "pattern" string. This pattern
    can be specify single devices, groups of devices, or even all configured devices.
    The pattern matching is based on bash style filename matching semantics.
    For example:

    * If the pattern is ``campus/building1/*`` the override condition is applied for all the devices under
      `campus/building1/`.
    * If the pattern is ``campus/building1/ahu1`` the override condition is applied for only the
      `campus/building1/ahu1` device. The pattern matching is based on bash style filename matching semantics.


Set Override On
^^^^^^^^^^^^^^^

The Platform Driver's ``set_override_on`` RPC method can be used to set the override condition for
all drivers with topic matching the provided pattern. It accepts four parameters:

:pattern: Override pattern to be applied.
:duration:  Time duration for the override in seconds. If duration <= 0.0, it implies an indefinite
    duration. (default 0.0)
:failsafe_revert: Flag to indicate if all the devices falling under the override condition has to be set to its
    default state/value immediately. If False, the value of overridden points is untouched.  (default True)
:staggered_revert: If this flag is set, reverting of devices will be staggered. (default False)

Example ``set_override_on`` RPC call:

.. code-block:: python

    self.vip.rpc.call(PLATFORM_DRIVER, "set_override_on", <override pattern>, <override duration>)

Set Override Off
^^^^^^^^^^^^^^^^

The override condition can also be toggled off based on a provided pattern using the Platform Driver's
``set_override_off`` RPC call. This method accepts one parameter:

:pattern:  Override pattern to be applied.


Get Override Devices
^^^^^^^^^^^^^^^^^^^^

A list of all overridden devices can be obtained with the Platform Driver's ``get_override_devices`` RPC call.

This method call does not take any parameters.


Get Override Patterns
^^^^^^^^^^^^^^^^^^^^^

A list of all patterns which have been requested for override can be obtained with the Platform Driver's
``get_override_patterns`` RPC call.

This method does not take any parameters

Clear Overrides
^^^^^^^^^^^^^^^

All overrides set by RPC calls described above can be toggled off at using a single ``clear_overrides`` RPC call.

This method call does not take any parameters
