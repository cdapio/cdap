.. _guide_admin_toplevel:
.. include:: /toplevel-links.rst

====================
Administration Guide
====================

Loom provides server administrators the ability to create flexible, customizable templates for machine provisioning. Each screen in the administration interface provides simple ways to create and edit a number of cluster settings. This guide describes the different interfaces and functions for server administrators.

The Overview Page
=================

Concepts
^^^^^^^^^

foo
----

foo1
----
=======
.. figure:: overview-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

.. _provision-templates:
Managing Provision Templates
============================

Templates are
Through this interface, administrators can specify the combinations of cluster parameters (providers, hardware types, images types and services) that they wish to allow to end-users to provision.

top level setting that ties in the configurations specified in the other four sections.


The Templates Home Screen
^^^^^^^^^^^^^^^^^^^^^^^^^

The templates home screen lists the existing templates that the administrator has created. The page also provides the ability to delete and view/edit each provider.

Clicking on a provider name will take you to the edit provider page to view more details of the provider and the ability to edit the configurations.

.. figure:: catalog-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Creating a Template
^^^^^^^^^^^^^^^^^^^

To create a template, click on 'Create a template' on the top left of the home screen to go to the Providers creation page. The


.. figure:: catalog-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

The defaults tab screen defines the default settings when a cluster is created
.. figure:: catalog-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Services that a user can install or choose to add to the service set.
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

To create a constrains group, click on either 'Add must co-exist group' or 'Add can't co-exist group', select a service you want to add to the group and select 'Add Service'. Once all the required services are added, select 'Add Group'.
.. figure:: catalog-screenshot-6.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center




To add the new setting to the list of templates, click 'Save'.

Managing Existing Templates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
A user can view/edit a template by clicking on the template name on the Home screen, or selecting 'Templates' -> <name of the template> on the top left of the screen.

The edit template page provides a similar interface to the create templates screen. Current settings for the template can be modified and deleted accordingly.

.. figure:: catalog-screenshot-7.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

.. _infrastructure-providers:
Managing Infrastructure Providers
=================================

Loom provides functionality to provision servers across a number of infrastructure providers, including but not limited to AWS, Rackspace and Joyent. Loom also supports OpenStack to enable integration with custom infrastructures.

The Providers interface allows administrators to add available cloud services and manage credentials.

The Providers Home Screen
^^^^^^^^^^^^^^^^^^^^^^^^^

The Providers home screen lists the existing providers currently allowed by the administrators. The page also provides the ability to delete and view/edit each provider.

Clicking on a provider name will take you to the edit provider page to view more details of the provider and the ability to edit the configurations.

.. figure:: providers-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Creating a Provider
^^^^^^^^^^^^^^^^^^^

To create a provider, click on 'Create a provider' on the top left of the home screen to go to the Providers creation page. On this page, users can configure the Name, Description and Provider type of the service. When selecting a Provider type, additional parameters will appear on screen specific to the individual infrastructure providers.


To add the new setting to the list of providers, click 'Save'.

.. figure:: providers-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

.. _manage-existing-providers:
Managing Existing Providers
^^^^^^^^^^^^^^^^^^^^^^^^^^^

A user can view/edit a provider by clicking on the provider name on the Home screen, or selecting 'Providers' -> <name of the provider> on the top left of the screen.

The provider edit page provides a similar interface to the create providers screen. Current settings for the provider can be modified and deleted accordingly.

.. figure:: providers-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


.. _hardware-configs:
Managing Hardware Configurations
================================

The hardware types section allows administrators to explicitly configure the hardware types available and how it is specified for each provider.

The Hardwares Home Screen
^^^^^^^^^^^^^^^^^^^^^^^^^

The hardwares home screen lists the hardware type current configured by and available to the administrator. The page also provides the ability to delete and view/edit each hardware type.

Clicking on a hardware type name will take you to the edit hardware type page to view more details of the hardware type and the ability to edit the configurations.

.. figure:: hardware-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Creating a Hardware Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create a hardware type, click on 'Create a hardware type' on the top left of the home screen to go to the Hardware types creation page.

The providers section can be used to define how the hardware type of the cluster provision template pertains to those used on each of the cloud infrastructure providers. Hardware settings on the provider side are specified using virtual hardware templates called flavors.

.. figure:: hardware-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


Must map to a valid image/flavor

Below is a list of flavor specification codes commonly used by providers. These codes are subject to change so be sure to verify the values with the provider's system.

        `OpenStack <http://docs.openstack.org/trunk/openstack-ops/content/flavors.html>`_

        `Amazon Web Services <http://aws.amazon.com/ec2/instance-types/index.html>`_

        `Rackspace <http://docs.rackspace.com/servers/api/v2/cs-releasenotes/content/supported_flavors.html>`_

        `Joyent <http://serverbear.com/9798/joyent>`_


To add the new setting to the list of hardware configuration types, click 'Save'.

For an example specification, see 'Managing Existing Hardware Configurations' below.

Managing Existing Hardware Configurations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A user can view/edit a hardware type by clicking on the hardware type name on the Home screen, or selecting 'Hardware types' -> <name of the hardware type> on the top left of the screen.

The edit hardware type page provides a similar interface to the create profile screen. Current settings for the hardware type can be modified and deleted accordingly.

.. figure:: hardware-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

.. _image-types:
Managing Image Types
====================

Using this interface, administrators can configure the basic disk images. Administrators can then choose among these images as the basic platform for clusters. Any services and software to be provided on top of these images are defined in the Services section

The Images Home Screen
^^^^^^^^^^^^^^^^^^^^^^
The images home screen lists the disk image types current configured by the administrator. The page also provides the ability to delete and view/edit each image.

Clicking on an image type name will take you to the edit image type page to view more details of the image type and the ability to edit the configurations.

.. figure:: images-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Creating a Disk Image Type
^^^^^^^^^^^^^^^^^^^^^^^^^^
To create an image type, click on 'Create an image type' on the top left of the home screen to go to the Image types creation page.

The providers section can be used to define how the image type of the cluster provision template pertains to those used on each of the cloud infrastructure providers. Image settings are specified by a unique ID code on different providers. A list of IDs for images can be queried from the user

To add the new setting to the list of image types, click 'Save'.

.. figure:: images-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Managing Existing Disk Images
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A user can view/edit an image type by clicking on the image type name on the Home screen, or selecting 'Image types' -> <name of the image type> on the top left of the screen.

The edit image type page provides a similar interface to the create profile screen. Current settings for the image type can be modified and deleted accordingly.

.. figure:: images-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

.. _cluster-services:
Managing Cluster Services
=========================

After defining up the basic disk image, the administrator can then choose the software packages and services that can be installed on the images.

Clicking on a service name will take you to the edit service page to view more details of the service and the ability to edit the configurations.

The Services Home Screen
^^^^^^^^^^^^^^^^^^^^^^^^

The services home screen lists the services current configured by the administrator. The page also provides the ability to delete and view/edit each service.

.. figure:: services-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Creating a Service Option
^^^^^^^^^^^^^^^^^^^^^^^^^

To create a service, click on 'Create a service' on the top left of the home screen to go to the Service creation page.

To add the new setting to the list of services, click 'Save'.

.. figure:: services-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Managing Existing Provided Services
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A user can view/edit a provider by clicking on the service name on the Home screen, or selecting 'Services' -> <name of the service> on the top left of the screen.

The edit service page provides a similar interface to the create profile screen. Current settings for the service can be modified and deleted accordingly.

.. figure:: services-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

