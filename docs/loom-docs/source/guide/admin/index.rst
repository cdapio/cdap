.. _guide_admin_toplevel:
.. include:: /toplevel-links.rst

====================
Administration Guide
====================


This page describes the different interfaces and functions for system administrators.
Loom provides flexible options for server administrators.

In short, each screen in the administration page provides a simple page to create,

Allows system administrators to customize settings and create templates for the


.. contents::
        :local:
        :class: faq
        :backlinks: none


.. _provision-templates:
Managing Provision Templates
============================

The Templates Home Screen
^^^^^^^^^^^^^^^^^^^^^^^^^

.. figure:: catalog-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Creating a Template
^^^^^^^^^^^^^^^^^^^

Managing Existing Templates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
A user can view/edit a template by clicking on the template name on the Home screen, or selecting 'Templates' -> <name of the template> on the top left corner.

.. _infrastructure-providers:
Managing Infrastructure Providers
=================================

Loom provides functionality to provision servers across a number of infrastructure providers, including but not limited to AWS, Rackspace and Joyent. Loom also supports OpenStack to enable integration with custom infrastructures.

The Providers interface allows administrators to add available cloud services and manage credentials.

The Providers Home Screen
^^^^^^^^^^^^^^^^^^^^^^^^^

The Providers home screen lists the existing providers currently allowed by the administrators. The page also provides the ability to delete and view/edit each provider.

Clicking on a provider will take you to the more details of the provider and the ability to delete and edit details of the provider.

.. figure:: providers-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Creating a Provider
^^^^^^^^^^^^^^^^^^^

To create a provider, click on "Create a provider" on the top left of the home screen. This will bring you to the Providers creation screen, where users can configure the Name, Description and Provider type of the service. When choosing a Provider type, additional parameters will appear on screen specific to the individual infrastructure providers.

To add the new setting to the list of providers, click 'Save'.

.. figure:: providers-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

.. _manage-existing-providers:
Managing Existing Providers
^^^^^^^^^^^^^^^^^^^^^^^^^^^

A user can view/edit a provider by clicking on the provider name on the Home screen, or selecting 'Providers' -> <name of the provider> on the top left corner.

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

The hardwares home screen lists the hardware types current configured by the administrator. The page also provides the ability to delete and view/edit each hardware type.

.. figure:: hardware-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Creating a Hardware Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To add the new setting to the list of hardware configuration types, click 'Save'.

.. figure:: hardware-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Managing Existing Hardware Configurations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A user can view/edit a hardware type by clicking on the hardware type name on the Home screen, or selecting 'Hardware types' -> <name of the hardware type> on the top left corner.

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

The images home screen lists the image types current configured by the administrator. The page also provides the ability to delete and view/edit each image.

.. figure:: images-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Creating a Disk Image Type
^^^^^^^^^^^^^^^^^^^^^^^^^^



To add the new setting to the list of image types, click 'Save'.

.. figure:: images-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Managing Existing Disk Images
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A user can view/edit an image type by clicking on the image type name on the Home screen, or selecting 'Image types' -> <name of the image type> on the top left corner.

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

The services home screen lists the services current configured by the administrator. The page also provides the ability to delete and view/edit each service.

.. figure:: services-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Creating a Service Option
^^^^^^^^^^^^^^^^^^^^^^^^^


To add the new setting to the list of services, click 'Save'.

.. figure:: services-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Managing Existing Provided Services
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A user can view/edit a provider by clicking on the service name on the Home screen, or selecting 'Services' -> <name of the service> on the top left corner.

.. figure:: services-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

