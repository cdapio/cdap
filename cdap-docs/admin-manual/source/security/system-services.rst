.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _admin-security-system-services:

================================
Enabling SSL for System Services
================================

Internal security governs the transmission of data between the :ref:`different components
of CDAP <admin-manual-cdap-components>`. In CDAP 4.0, SSL can be enabled between the CDAP
Router and CDAP Master (or App Fabric) components.

To enable perimeter security, see the section on :ref:`Perimeter Security <admin-perimeter-security>`.

.. _admin-security-system-services-master:

Enabling SSL for Master Service
===============================

To enable communication between the CDAP Router and App Fabric using SSL in CDAP, add this property to ``cdap-site.xml``:

================================================= ==================== ======================================================
Property                                          Value                Description
================================================= ==================== ======================================================
``ssl.internal.enabled``                          ``true``             ``true`` to enable SSL between Router and App Fabric
================================================= ==================== ======================================================

Default Ports
=============

**Without SSL**, these properties |---| unless set specifically |---| have these default values:

================================================= ==================== ======================================================
Property                                          Default Value        Description
================================================= ==================== ======================================================
``app.bind.port``                                 ``0``                App Fabric service bind port
================================================= ==================== ======================================================

**With SSL**, these properties |---| unless set specifically |---| have these default values:

================================================= ==================== ======================================================
Property                                          Default Value        Description
================================================= ==================== ======================================================
``app.ssl.bind.port``                             ``30443``            App Fabric service bind port
================================================= ==================== ======================================================
