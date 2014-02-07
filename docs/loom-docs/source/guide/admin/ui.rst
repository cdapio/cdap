.. _guide_admin_ui:
.. include:: /toplevel-links.rst

=======================
Administrator Interface
=======================

.. include:: /guide/admin/admin-links.rst

This guide describes the different interfaces and functions of the administrator UI. Each screen in the administration
interface provides ways to create and edit settings for cluster provisioning.

The Overview Screen
===================

An administrator is redirected to the overview screen after log in. This page displays all the cluster configuration
elements that have already been defined. Clicking on the name of each element allows an administrator to enter its
management page where they can examine the element in detail and modify its configuration.

.. figure:: overview-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

.. _provision-templates:
Managing Provision Templates
============================

Loom templates provide a means to both enable specific cluster configurations, as well as providing restrictions to
the services made available. Templates tie in the configurations specified in the other four sections. Through this
interface, administrators can specify predefined combinations of parameters that are permitted for cluster creation.

The Catalog Home Screen
^^^^^^^^^^^^^^^^^^^^^^^

The Catalog screen lists the existing templates that the administrator has created. The page also provides the ability
to delete and view/edit each template.

Clicking on a template name will take you to the 'Edit template' page to view more details of the provider and the ability to edit the configurations.

.. figure:: catalog-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Creating a Template
^^^^^^^^^^^^^^^^^^^

Click on 'Create a template' on the top-left of the home screen to go to the Providers creation page.


.. figure:: catalog-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

The defaults tab screen defines the default specifications, settings and services provided when a cluster is initially created. The provider, hardware type and image type can be selected from the drop down menu among those defined in their corresponding sections. The 'Config' box allows JSON-formatted input to define additional custom configurations for defaults (for more information, see :doc:`Macros </guide/admin/macros>`).
Multiple service can be added as default software capabilities; select a service from a drop down menu and click 'Add service' to add. To remove a service, press the '-' next to the service you want removed.

.. figure:: catalog-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


The compatibility tab provides additional configurations that a user can optionally customize for their needs. Through this screen, the administrator can add options for hardware type, imagine type and services. These can all be added through selecting an element from the drop down menu and clicking the button next to the box to add it. To remove an option, press the '-' next to the option you want removed.

.. figure:: catalog-screenshot-4.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

The constraints tab allows the administrator to set complex rules for the sets of services that are installed on a cluster. 'Must coexist' is often used to identify service dependencies. For example, a Hadoop data node service depends on Yarn Node Manager and thus must coexist. On the other hand, 'Can't coexist' pertains to services that need to be mutually exclusive. An example of this is that HDFS namenode and datanode services cannot both be installed on the same node.

.. figure:: catalog-screenshot-5.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

To create a constrains group, click on either 'Add must co-exist group' or 'Add can't co-exist group', select a service you want to add to the group and select 'Add Service'. Services can be removed from the group by pressing the '-' next to the name of the service. Once all the required services are added, select 'Add Group'.

.. figure:: catalog-screenshot-6.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Administrators can additional limit the number of instances of each service. An example of this is to limit the number of instances of HDFS name node and Yarn resource manager to one in a Hadoop cluster. To do so, click 'Add service constraint', choose the item you want to limit from the drop down list, and set the maximum and minimum number of instances permitted. The constraint itself or the number of instances can be changed from the list of service constraints.

.. figure:: catalog-screenshot-7.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

To add the new setting to the list of templates, click 'Save'.

Managing Existing Templates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
A user can view/edit a template by clicking on the template's name on the Home screen, or selecting 'Templates' **->** <name of the template> on the top-left of the page.

The edit template page provides a similar interface to the 'Create a template' screen. Current settings for the template can be modified and deleted accordingly.

.. figure:: catalog-screenshot-8.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

.. _infrastructure-providers:
Managing Infrastructure Providers
=================================

Loom provides functionality to provision servers across a number of infrastructure providers, including but not
limited to Amazon Web Services, Rackspace and Joyent. Loom also supports OpenStack to enable integration with
custom infrastructures for both public and private cloud.

The Providers interface allows administrators to add available cloud providers and manage their credentials.

The Providers Home Screen
^^^^^^^^^^^^^^^^^^^^^^^^^

The Providers home screen lists the existing providers currently supported by the administrators. The page also
provides the ability to delete and view/edit each provider.

Clicking on an item's name will take you to the 'Edit provider' page to view more details of the provider and
allows you to edit the configurations.

.. figure:: providers-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Creating a Provider
^^^^^^^^^^^^^^^^^^^

Click on 'Create a provider' on the top-left of the home screen to go to the Providers creation
page. On this page, users can configure the Name, Description and Provider type of the service. When selecting a
Provider type, additional parameters will appear on screen specific to managing credentials on the chosen provider.


To add the new configuration to the list of providers, click 'Save'.

.. figure:: providers-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

.. _manage-existing-providers:
Managing Existing Providers
^^^^^^^^^^^^^^^^^^^^^^^^^^^

A user can view/edit a provider by clicking on the provider's name on the Home screen, or selecting 'Providers' **->**
<name of the provider> on the top-left of the page.

The provider edit page provides a similar interface to the 'Create a provider' screen. Current settings for the
provider can be modified and deleted accordingly.

.. figure:: providers-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


.. _hardware-configs:
Managing Hardware Configurations
================================

The hardware types section allows administrators to explicitly manage the hardware configurations available to users and
how they are specified in each provider.

The Hardwares Home Screen
^^^^^^^^^^^^^^^^^^^^^^^^^

The hardwares home screen lists the hardware types currently managed by the administrators. The page
also provides the ability to delete and view/edit each hardware type.

Clicking on an item's name will take you to the 'Edit hardware type' page to view more details of the hardware type
and allows you to edit the configurations.

.. figure:: hardware-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Creating a Hardware Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Click on 'Create a hardware type' on the top-left of the home screen to go to the Hardware
types creation page.

On this page, users can configure the Name, Description and how the hardware setting is specified on a provider.
The 'Providers' section define how the hardware setting pertains to the identifiers
used on each of the cloud infrastructure providers. Hardware settings on the provider side are specified
using virtual hardware templates called flavors.

.. figure:: hardware-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


Values specified in 'Providers' must map to a valid flavor on the corresponding provider. Below is a list of flavor
identifier codes commonly used by providers.

        `OpenStack <http://docs.openstack.org/trunk/openstack-ops/content/flavors.html>`_

        `Amazon Web Services <http://aws.amazon.com/ec2/instance-types/index.html>`_

        `Rackspace <http://docs.rackspace.com/servers/api/v2/cs-releasenotes/content/supported_flavors.html>`_

        `Joyent <http://serverbear.com/9798/joyent>`_


These codes are subject to change so be sure to verify the values with the provider's system.

To add the new configuration to the list of hardware types, click 'Save'.

Managing Existing Hardware Configurations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A user can view/edit a hardware type by clicking on the hardware type's name on the Home screen, or selecting
'Hardware types' **->** <name of the hardware type> on the top-left of the page.

The edit hardware type page provides a similar interface to the 'Create a hardware type' screen. Current
settings for the hardware type can be modified and deleted accordingly.

.. figure:: hardware-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

.. _image-types:
Managing Image Types
====================

The image types interface allows administrators to configure the options for basic disk images on the clusters
provisions by end users.

The Images Home Screen
^^^^^^^^^^^^^^^^^^^^^^
The images home screen lists the image types currently configured by the administrators. The page also provides
the ability to delete and view/edit each image type.

Clicking on an item's name will take you to the 'Edit image type' page to view more details of the image type and
allows you to edit the configurations.

.. figure:: images-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Creating a Disk Image Type
^^^^^^^^^^^^^^^^^^^^^^^^^^
Click on 'Create an image type' on the top-left of the home screen to go to the Image types creation page.

On this page, users can configure the Name, Description and how the image type is specified on a provider. The 'Providers' section can be used to define how the image type of the cluster provision template pertains to those used on each of the cloud infrastructure providers. Image settings are specified by a unique ID code on different providers. A list of IDs for images will need to be queried directly from the provider, as the list may change over time.

To add the new configuration to the list of image types, click 'Save'.

.. figure:: images-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Values specified in 'Providers' must map to a valid image on the corresponding provider.

Managing Existing Disk Images
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A user can view/edit an image type by clicking on the image type's name on the Home screen, or selecting 'Image types' **->** <name of the image type> on the top-left of the page.

The edit image type page provides a similar interface to the 'Create an image type' screen. Current settings for the image type can be modified and deleted accordingly.

.. figure:: images-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

.. _cluster-services:
Managing Cluster Services
=========================

After defining up the basic disk image, the administrator can then choose the software packages and services that can be installed on the images.

The Services Home Screen
^^^^^^^^^^^^^^^^^^^^^^^^

The services home screen lists the services currently configured by the administrators. The page also provides the ability to delete and view/edit each service.

Clicking on an item's name will take you to the 'Edit service' page to view more details of the service and allows you to edit the configurations.

.. figure:: services-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Creating a Service Option
^^^^^^^^^^^^^^^^^^^^^^^^^

Click on 'Create a service' on the top-left of the home screen to go to the Service creation page.
For service creation, the dependencies of the service being added need to be specified. In the 'Depends on' box, select the services which are dependencies on the current service.
The administrator then has to define the list of actions that need to be performed in order to make the service available on the cluster. Such actions may include install, remove, initialize, start and stop. Loom currently supports actions being performed through Chef recipes and shell scripts. The location or name of the script/recipe can be entered to the text field labeled 'Script' and any parameters to be passed to the script can be specified in the text field labeled 'Data'.
To add another action, click on 'Add' and an additional section will be added.

When creating a service, the administrator should

To add the new configuration to the list of services, click 'Save'.

.. figure:: services-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Managing Existing Provided Services
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A user can view/edit a provider by clicking on the service's name on the Home screen, or selecting 'Services' **->** <name of the service> on the top-left of the page.

The edit service page provides a similar interface to the 'Create a service' screen. Current settings for the service can be modified and deleted accordingly.

.. figure:: services-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

