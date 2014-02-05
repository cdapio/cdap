.. _guide_admin_toplevel:
.. include:: /toplevel-links.rst

====================
Administration Guide
====================

Loom provides server administrators the ability to create flexible, customizable templates for machine provisioning. Each screen in the administration interface provides simple ways to create and edit a number of cluster settings. This guide describes the different interfaces and functions for server administrators.

The Overview Page
=================

Work in progress.


.. _provision-templates:
Managing Provision Templates
============================

Templates are
Through this inferface, administrators can specify the combinations of cluster parameters (providers, hardware types, images types and services) that they wish to allow to end-users to provision.

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



To add the new setting to the list of templates, click 'Save'.

Managing Existing Templates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
A user can view/edit a template by clicking on the template name on the Home screen, or selecting 'Templates' -> <name of the template> on the top left of the screen.

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

The provider edit page provides an identical interface to the create profile screen. Current settings for the provider can be modified and deleted accordingly.

.. figure:: providers-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


.. _hardware-configs:
Managing Hardware Configurations
================================


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

To add the new setting to the list of hardware configuration types, click 'Save'.

.. figure:: hardware-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Managing Existing Hardware Configurations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A user can view/edit a hardware type by clicking on the hardware type name on the Home screen, or selecting 'Hardware types' -> <name of the hardware type> on the top left of the screen.

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

To add the new setting to the list of image types, click 'Save'.

.. figure:: images-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Managing Existing Disk Images
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A user can view/edit an image type by clicking on the image type name on the Home screen, or selecting 'Image types' -> <name of the image type> on the top left of the screen.

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

.. figure:: services-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

