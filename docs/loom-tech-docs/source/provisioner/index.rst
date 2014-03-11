:orphan:

.. _plugin-reference:


.. index::
   single: Provisioner
========================
Provisioner
========================

The Loom Provisioner is the worker daemon of Loom.  At a high level, it simply performs tasks given to it by the Loom Server.  These are the tasks necessary to orchestrate cluster operations and may include provisioning nodes from cloud providers, installing/configuring software, or running custom commands.  Each instance of the provisioner polls for the next task in the queue, and handles it to completion.  A plugin framework is utilized to handle any task for extensibility.  The provisioners are lightweight and stateless, therefore many can be run in parallel.  

Operational Model
=================

At a high-level, the Loom provisioner is responsible for the following:
  * polling the Loom Server for tasks
  * executing the received task by invoking the appropriate task handler plugin
  * reporting back the results of the operation, including success/failure and any appropriate metadata needed.

Each running Loom Provisioner instance will continually poll the Loom Server for tasks.  When a task is received, it consists of a JSON task definition.  This task definition contains all the information needed by the provisioner to carry out the task.  

Consider the typical scenario for provisioning a node on a cloud provider asynchronously
  1. the node is requested with given attributes (size, OS, region, etc)
  2. the provider accepts the request and returns an internal ID for the new node it is going to create
  3. during creation, the requestor must continually poll for the new node's status and public IP address using the internal ID
  4. the requestor does some additional validation using the IP address, and declares success

The internal provider ID obtained in step 2 is required input for step 3, and will be required again if we wish to delete this node.  Similarly, the IP address obtained in step 4 will be used in subsequent tasks.  

The following diagram illustrates how this is implemented by the Loom Provisioner:

.. figure:: /_images/provisioner_operational_model.png
    :align: center
    :alt: Provisioner Operational Model
    :figclass: align-center


In the diagram above, the Provisioner first receives a CREATE task that instructs it to request a node from a specific provider.  The task contains the all the necessary provider-specific options (truncated in the diagram for brevity).  The provisioner then executes the node create request through the provider API and receives the new node's provider ID as a result.  Since this provider ID will be critical for future operations against this node, it must report it back to the Loom Server.  It does so by populating a "result" key-value hash in the task result JSON.  The Loom server will preserve these key-values in a "config" hash on all subsequent tasks for this node.  In the diagram, the subsequent "CONFIRM" task is given the provider-id for the node, and similarly it reports back the IP address obtained from the provider in this step.  The third task shown now includes all metadata discovered thus far about the node in the request: the provider-id and the ipaddress.  In this way, Loom is building up a persistent payload of metadata about a node which can be used by any subsequent task. For example, many providers send back an identifier upon creation of a node. The “DELETE” task may require this provider specific identifier in order to tell the provider to delete the node. In this case, the “CREATE” task would send back the provider identifier in its payload so that the “DELETE” task can use it when it eventually runs. As another example, the ip address of a node may not be known until completion of the “CONFIRM” task. However, future services may need to be configured using the ip address, so Provisioners can return the ip address in the payload to provide the information to future tasks. 

In addition to this payload of key-value pairs, Loom also automatically provides additional metadata regarding cluster layout.  For example, once the nodes of a cluster are established, Loom Server will include a "nodes" hash in the task JSON which contains the hostnames and IP addresses of every node in the cluster.  This can be readily used by any task requiring cluster information, for example configuring software on a node which needs a list of all peer nodes in the cluster.



Plugin Framework
================

One of the design goals of Loom is to be agnostic to the type of cluster being managed.  To achieve this, Loom Provisioner makes extensive use of a plugin framework.  Plugins allow Loom to provision the same cluster in different providers.  They also allow an enterprise to customize implementation of their cluster services, for example integrating with their own SCM system of choice.

A plugin is a self-contained program designed to perform a specific set of tasks.  Currently, Loom supports plugins written in Ruby.  Each plugin must have a name and a type.  The name uniquely identifies each plugin, while the type groups related plugins together.  The type also corresponds to the list of tasks the plugin is capable of handling.  For example, consider the following diagram:

.. figure:: /_images/provisioner_plugin_framework.png
    :align: center
    :alt: Provisioner Plugin Framework
    :figclass: align-center

The diagram shows two tasks being consumed by provisioners and the logic used to invoke the appropriate plugin.  When a task is received, the provisioner first determines from the taskName which type of plugin is required to handle the task.  In the first example, a CREATE taskName indicates the task must be handled by a Provider plugin.  Loom then checks the task JSON for the providertype field to determine which plugin to invoke.  In the second example, an INSTALL taskName indicates the task must be handled by an Automator plugin.  Loom then checks the task JSON for the service action type field to determine which plugin to invoke.

A plugin must provide a descriptor file in which it declares its name, type, and execution class.  Upon startup, the provisioner scans its own directories looking for these descriptor files.  Upon successful verification, the plugin is considered registered.  

A plugin can contain any arbitrary data it needs to perform its tasks.  For example, a provider plugin may store api credentials locally, or a Chef plugin may keep a local repository of cookbooks.  This data can be packaged with and considered as part of the plugin.  Alternatively, a plugin may also specify certain configuration parameters that it expects to be filled in by the UI users.  For example, there are variances among cloud providers regarding the credentials needed to access their API.  Some require a password, some require a key on disk, etc.  Loom allows a plugin to specify the necessary configuration fields, so that an admin can simply fill in the values on the UI.  Then, when a task is received by that particular plugin, it will have the necessary key-value pairs it expects.

This plugin model is integral to supporting many providers and custom installation procedures.  It makes it easy to leverage existing provider plugins or community code as plugins within Loom.


