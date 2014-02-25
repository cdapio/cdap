:orphan:

.. _plugin-reference:


.. index::
   single: Chef Automator Plugin
========================
Chef Automator Plugin
========================

.. include:: /guide/admin/admin-links.rst

This section describes an automator that uses chef-solo. Basic knowledge of Chef and its primitives is assumed.

Overview
========

The Chef Solo Automator plugin, like all automator plugins, is responsible for performing the installation and
operation of services on remote hosts. The Chef Automator plugin achieves this by running chef-solo on the remote host
with a custom run-list and set of JSON attributes. The attributes provided to each chef-solo run will be a combination
of cluster-wide configuration attributes, as well as service-specific attributes definable for each action. Each
chef-solo run is self-contained and intended to perform a specific task such as starting a service. This differs from
the typical usage of Chef where one builds up a large run-list managing all resources on a box in one run.

To illustrate this, consider the following example which shows how we can manage the apache web server on Loom cluster
nodes using the "apache2" community cookbook. We define a Loom service "apache-httpd" as follows:
::
    {
        "dependson": [
            "hosts"
        ],
        "description": "Apache HTTP Server",
        "name": "apache-httpd",
        "provisioner": {
            "actions": {
                "install": {
                    "script": "recipe[apache2::default]",
                    "type": "chef"
                },
                "configure": {
                    "script": "recipe[apache2::default]",
                    "type": "chef"
                },
                "start": {
                    "data": "{\"loom\": { \"node\": { \"services\": { \"apache2\": \"start\" } } } }",
                    "script": "recipe[apache2::default],recipe[loom_service_runner::default]",
                    "type": "chef"
                },
                "stop": {
                    "data": "{\"loom\": { \"node\": { \"services\": { \"apache2\": \"stop\" } } } }",
                    "script": "recipe[apache2::default],recipe[loom_service_runner::default]",
                    "type": "chef"
                }
            }
        }
    }

For each action, we define the type, script, and data fields. (defaults to empty string if not specified). The type
field indicates to the provisioner to use the Chef Automator plugin to manage this action. The script field specifies
the run-list to use. The data field is any additional JSON data we wish to include in the Chef run (more on this
later). When the Chef Solo Automator plugin executes any of these actions for the apache-httpd service, it performs
the following actions:

        1. generate a task-specific JSON file containing any attributes defined in the data field, as well as base cluster attributes defined elsewhere in Loom.
        2. invoke chef-solo using the script field as the run-list using  ``chef-solo -o [script] -j [task-specific json] -r [cookbooks.tar.gz]``


In this example, to execute an "install" task for the apache-httpd service, the provisioner will simply run the default
recipe from the apache2 cookbook as a single chef-solo run. No additional JSON attributes are provided beyond the base
cluster configuration attributes.

For a "configure" task, the provisioner will also run the default recipe from the apache2 cookbook. For this community
cookbook, the installation and configuration are done in the same recipe, which is common but not always the case. So
one may wonder why we need both 'install' and 'configure' when they perform identical actions. It is best practice to
keep them both, since configure may be run many times throughout the lifecycle of the cluster, and install is needed
to satisfy dependencies.

The "start" and "stop" tasks introduce a couple of features. They make use of the data field to specify custom JSON
attributes. Note that the format is an escaped JSON string. The script field also contains an additional recipe,
``loom_service_runner::default``. More on this later, but essentially this is a helper cookbook that can operate on
any Chef service resource. It looks for any service names listed in node['loom']['node']['services'], finds the
corresponding Chef service resource, and invokes the specified action.


JSON Attributes
================

Loom maintains significant JSON data for a cluster, and makes it available for each task. This JSON data includes:
    * cluster-wide configuration defined in cluster templates (Catalog -> cluster template -> defaults -> config)
    * node data for each node of the cluster: hostname, ip, etc
    * service data, specified in the actions for each service

The ChefAutomator plugin automatically merges this data into a single JSON file, which is then passed to chef-solo via
the ``--json-attributes argument``. Any custom cookbooks that want to make use of this Loom data need to be familiar
with the JSON layout of the Loom data. In brief, cluster-wide configuration defined in cluster templates and
service-level action data are merged together, and preserved at the top-level. Loom data is then also merged in under
``loom/cluster``. For example:
::
    {
        // cluster config attributes defined in clustertemplates are preserved here at top-level
        // service-level action data string converted to json and merged here at top-level
        "loom": {
            "cluster": {
                //cluster config here as well
                "nodes": {
                    // node data
                }
            },
        },
    }


Consider the following two rules of thumb:
	* When using community cookbooks, attributes can be specified in Loom templates exactly as the cookbook expects (at the top-level).
	* When writing cookbooks specifically utilizing Loom metadata (cluster node data for example), recipes can access the metadata at ``node['loom']['cluster']...``

Bootstrap
=========

Each Loom Automator plugin is responsible for implementing a bootstrap method in which it performs any actions it needs to be able to carry out further tasks. The ChefAutomator plugin performs the following actions for a bootstrap task:
	1. Bundle its local copy of the cookbooks directory into a tarball, ``cookbooks.tar.gz``.
		* Unless the file exists already and was created in the last 10 minutes.
	2. Logs into the remote box and installs chef via the Opscode Omnibus installer (``curl -L https://www.opscode.com/chef/install.sh | bash``).
	3. Creates the remote loom cache directory ``/var/cache/loom``.
	4. SCP the local ``cookbooks.tar.gz`` to the remote Loom cache directory.

The most important things to note are that:
	* Upon adding any new cookbooks to the cookbooks directory, the tar ball will be regenerated within 10 minutes and used by all running provisioners.
	* This implementation of a Chef automator requires internet access to install Chef (and also required by the cookbooks used within).


Adding your own Cookbooks
=========================
**Cookbook requirements**

Since the ChefAutomator plugin is implemented using chef-solo, the following restrictions apply:

	* No chef search capability
	* No persistent attributes

Cookbooks should be fully attribute-driven. At this time the ChefAutomator does not support chef-solo data-bags,
environments, or roles. Attributes normally specified in these locations can instead be populated in Loom primitives
such as cluster templates or service action data.

In order to add a cookbook for use by the provisioners, simply add it to the cookbooks directory for the ChefAutomator
plugin. If using the default package install, this directory is currently:
::
    /opt/loom/provisioner/daemon/plugins/automators/chef_automator/chef_automator/cookbooks

Your cookbook should be readable by the 'loom-provisioner' user (default: 'loom'). The next provisioner which runs a
bootstrap task will regenerate the local cookbooks tarball
(``/opt/loom/provisioner/daemon/plugins/automators/chef_automator/chef_automator/cookbooks.tar.gz``) and it will be
available for use when chef-solo runs on the remote box.

In order to actually invoke your cookbook as part of a cluster provision, you will need to define a Loom service
definition with the following parameters:

	* Category: any action (install, configure, start, stop, etc)
	* Type: chef
	* Script: a run-list containing your cookbook's recipe(s). If your recipe depends on resources defined in other
	cookbooks which aren't declared dependencies in your cookbook's metadata, make sure to also add them to the run-list.
	* Data: any additional custom attributes you want to specify, unique to this action

Then simply add your service to a cluster template.


Loom Helper Cookbooks
=====================

Loom ships with several helper cookbooks.

**loom_hosts**
---------------

This simple cookbook's only purpose is to populate ``/etc/hosts`` with the hostnames and IP addresses of the cluster.
It achieves this by accessing the ``loom-populated`` attributes at ``node['loom']['cluster']['nodes']`` to get a list of
all the nodes in the cluster. It then simply utilizes the community "hostsfile" cookbook's LWRP to write entries for
each node.

The example loom service definition invoking this cookbook is called "hosts". It simply sets up a "configure" service
action of type "chef" and script ``recipe[loom_hosts::default]``. Note that the community "hostsfile" cookbook is not
needed in the runlist since it is declared in loom_hosts's metadata.

**loom_service_runner**
-----------------------

This cookbook comes in handy as a simple way to isolate the starting and stopping of various services within your
cluster. It allows you to simply specify the name of a Chef service resource and an action within a Loom service
definition. When run, it will simply lookup the Chef service resource of the given name, regardless of which cookbook
it is defined in, and run the given action. In the example apache-httpd service definition above, it is simply included
in the run-list to start or stop the apache2 service defined in the apache2 community cookbook. All that is needed is
to set the following attribute to "start" or "stop":
::
    node['loom']['node']['services']['apache2'] = "start"


**loom_firewall**
-----------------

This cookbook is a simple iptables firewall manager, with the added functionality of automatically whitelisting all
nodes of a cluster. To use, simply set any of the following attributes:
::
    node['loom_firewall']['INPUT_policy']  = (string)
    node['loom_firewall']['FORWARD_policy'] = (string)
    node['loom_firewall']['OUTPUT_policy'] = (string)
    node['loom_firewall']['notrack_ports'] = [ array ]
    node['loom_firewall']['open_tcp_ports'] = [ array ]
    node['loom_firewall']['open_udp_ports'] = [ array ]

If this recipe is included in the run-list and no attributes specified, the default behavior is to disable the firewall.


Best Practices
==============

* Loom is designed to use attribute-driven cookbooks. All user-defined attributes are specified in Loom primitives. Recipies that use Chef server capabilities like discovery and such do not operate well with Loom.
* Separate the install, configuration, initialization, starting/stopping, and deletion logic of your cookbooks into granular recipes. This way Loom services can often be defined with a 1:1 mapping to recipes. Remember that Loom will need to install, configure, initialize, start, stop, and remove your services, each independently through a combination of run-list and attributes.
* Use wrapper cookbooks in order to customize community cookbooks to suit your needs.
* Remember to declare cookbook dependencies in metadata.
