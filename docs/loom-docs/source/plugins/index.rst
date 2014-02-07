:orphan:

.. _plugin-reference:
.. include:: /toplevel-links.rst

==========================
Provisioner Plugins
==========================

The Loom provisioner allows you to create custom plugins for allocating machines on your providers, or to custom implement your services.  This document is intended to provide everything needed to build a custom plugin for Loom.

Types of Plugins
================

Provider plugins
----------------
Provider plugins interact with various provider APIs to allocate machines.  They must be able to request machines, confirm/validate when they are ready, and delete machines.  They are also responsible for returning the ssh credentials to be used in subsequent tasks.

Automator plugins
-----------------
Automator plugins are responsible for implementing your various defined services on a cluster.  For example, a chef automator plugin could be used to invoke chef recipes that install/configure/start/stop your application.  Or you may choose to implement with a puppet plugin, or even shell commands.

Task Types
==========
In order to build plugins for Loom, it is first necessary to understand the tasks each plugin will be responsible for executing.  In order to bring up a cluster, Loom issues the following tasks:


.. list-table::

  * - Task name
    - Description
    - Possible return values
    - Handled by plugin
  * - CREATE
    - sends a request to the provider to initiate the provisioning of a machine. Typically should return as soon as the request is made
    - status, provider-id, root password
    - Provider
  * - CONFIRM
    - polls/waits for the machine to be ready, does any required provider-specific validation/preparation
    - status, routable ip address
    - Provider
  * - DELETE
    - sends a request to the provider to destroy a machine
    - status
    - Provider
  * - BOOTSTRAP
    - plugin can perform any operation it needs to carry out further tasks, for example copy chef cookbooks to the machine. this should be idempotent, and safe to run together with multiple plugins
    - status
    - Automator
  * - INSTALL
    - run the specified install service action script/data
    - status
    - Automator
  * - CONFIGURE
    - run the specified configure service action script/data
    - status
    - Automator
  * - INITIALIZE
    - run the specified initialize service action script/data
    - status
    -
  * - START
    - run the specified start service action script/data
    - status
    - Automator
  * - STOP
    - run the specified stop service action script/data
    - status
    - Automator
  * - REMOVE
    - run the specified remove service action script/data
    - status
    - Automator

Note that status is the only required return value (to indicate success/failure).  For other bits of information such as the ip address, etc, any task can write an arbitrary key/value pair which will then be included in subsequent requests.  This allows for the differences among various providers.

Writing a Plugin
================

Currently a plugin must be a Ruby class which extends from our base plugin classes.

Writing a Provider plugin
-------------------------

A provider plugin must extend from the base Provider class and implement three methods:  create, confirm, and delete.  Each of these methods will be called with a hash of key/value pairs which should be used in your implementation.  Note that your implementation can also refer to the @task instance variable if needed, which contains the entire input for this task.

Below is a provider plugin skeleton:
::
  #!/usr/bin/env ruby

  class MyProvider < Provider

    def create(inputmap)
      flavor = inputmap['flavor']
      image = inputmap['image']
      hostname = inputmap['hostname']
      #
      # implement requesting a machine from provider
      #
      @result['status'] = 0
      @result['result']['foo'] = "bar"
    end

    def confirm(inputmap)
      providerid = inputmap['providerid']
      #
      # implement confirmation/validation of this machine from provider
      #
      @result['status'] = 0
    end

    def delete(inputmap)
      providerid = inputmap['providerid']
      #
      # implement deletion of machine from provider
      #
      @result['status'] = 0
    end

When the task is complete, your implementation should simply write the results back to the @result instance variable.  The only required return value is 'status':  0 for success, anything else for failure.  A raised exception will also result in failure.
Additionally, your provider plugin will likely need to return the machine's id, ssh credentials, public ip, etc, so that they can be used in subsequent tasks.  For these cases, simply write the results as key/value pairs underneath @result['result']['key'] = 'value'.  Then, subsequent tasks will contain this information in 'config', for example: @task['config']['key'] = 'value'.  By convention, most plugins should reuse the following fields:
::
  @result['result']['providerid']
  @result['result']['ssh-auth']['user']
  @result['result']['ssh-auth']['password']
  @result['result']['ipaddress']

Writing an Automator plugin
---------------------------

An automator plugin must extend from the base Automator class and implement seven methods:  bootstrap, install, configure, init, start, stop, and remove.  Each of these methods will be called with a hash of key/value pairs which should be used in your implementation.  Note that your implementation can also refer to the @task instance variable if needed, which contains the entire input for this task.

Below is a automator plugin skeleton:
::
  #!/usr/bin/env ruby

  class MyAutomator < Automator

    def bootstrap(ssh_auth_hash)
      #
      # implement any preparation work required by this plugin (copy cookbooks, etc)
      # this should be idempotent and unintrusive to any other registered plugins
      @result['status'] = 0
    end

    def install(ssh_auth_hash, script_string, data_string)
      #
      # implement installing a service as specified by script_string, data_string
      #
      @result['status'] = 0
    end

    def configure(ssh_auth_hash, script_string, data_string)
      #
      # implement configuring a service as specified by script_string, data_string
      #
      @result['status'] = 0
    end

    def init(ssh_auth_hash, script_string, data_string)
      #
      # implement initializing a service as specified by script_string, data_string
      #
      @result['status'] = 0
    end

    def start(ssh_auth_hash, script_string, data_string)
      #
      # implement starting a service as specified by script_string, data_string
      #
      @result['status'] = 0
    end

    def stop(ssh_auth_hash, script_string, data_string)
      #
      # implement stopping a service as specified by script_string, data_string
      #
      @result['status'] = 0
    end

    def remove(ssh_auth_hash, script_string, data_string)
      #
      # implement removing a service as specified by script_string, data_string
      #
      @result['status'] = 0
    end

Note that the bootstrap step is unique in that a bootstrap task is not tied to a service.  The bootstrap task will actually run the bootstrap impelementation for all registered automator plugins, and may be run multiple times throughout the cluster lifecycle.  Therefore, bootstrap implementations should be idempotent and not interfere with one another.

Logging and Capturing Output
During execution, your plugin can write to the provisioner's instance of the ruby standard logger using the 'log' method:
::
  log.debug "my message"
  log.info "my message"
  log.warn "my warning message"
  log.error "my error message"
  Additionally, each task can return strings representing stdout and stderr to be displayed on the Loom UI.  Simply return
  @result['stdout'] = "my captured stdout message"
  @result['stderr'] = "my captured stderr message"

Registering your plugin
To register your plugin, you simply need to put it in the right directory, and provide a json file specifying your main class.  On startup, the Loom provisioner will scan the appropriate directories looking for json definition files.  Your plugin simply has to adhere to the following directory structure:

Below is an example directory structure:
::
  $LOOM_HOME/
      provisioner/
          daemon/
              plugins/
                  providers/
                      my_provider/
                          my_provider.json
                          my_provider.rb
                          [any additional data or lib directories]
                  automators/
                      my-automator/
                          my_provider.json
                          my_provider.rb
                          [any additional data or lib directories]

The content of the plugin definition *.json files simply need to specify the main class of your plugin as follows:
::
  {
    "my_provider" : {
      "classname": "MyProvider"
    }
  }

Given the above definition, Loom provisioner would expect the MyProvider class to be defined in $LOOM_HOME/provisioner/daemon/plugins/providers/my_provider/*.rb.  Check the provisioner logs on startup to confirm your plugin is registered.