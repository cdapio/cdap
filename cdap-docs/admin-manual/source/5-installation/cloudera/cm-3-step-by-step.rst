.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _step-by-step-cloudera-add-service:

=========================================
Cloudera Manager: Installing CDAP Service
=========================================

The following instructions show how to use the Cloudera Manager Admin Console *Add
Service* Wizard to install and start CDAP.

These steps can be followed once the CDAP CSD (Custom Service Descriptor) has been
:ref:`downloaded and installed <cloudera-installation-csd>` and the CDAP Parcel has been
:ref:`downloaded and distributed <cloudera-installation-download-distribute-parcel>`.

Once you have completed the installation and :ref:`started CDAP
<step-by-step-cloudera-add-service-startup>`, you can then 
:ref:`verify the installation <admin-manual-verification>`.

.. _cloudera-installation-setup-startup:

Step-by-Step
============
Run the Cloudera Manager Admin Console *Add Service* Wizard and select *CDAP*.
When completing the Wizard, these notes may help:

- *Add Service* Wizard, Page 2: **Optional Hive dependency** is for the optional CDAP
  "Explore" component which can be enabled later.
 
- *Add Service* Wizard, Page 3: **Choosing Role Assignments**. Ensure CDAP roles are assigned to hosts colocated
  with service or gateway roles for HBase, HDFS, Yarn, and optionally Hive.

- *Add Service* Wizard, Page 3: CDAP **Security Auth** service is an optional service
  for CDAP perimeter security; it can be configured and enabled post-wizard.
 
- *Add Service* Wizard, Pages 4 & 5: **Kerberos Auth Enabled** is needed if running against a
  secure Hadoop cluster.

- *Add Service* Wizard, Pages 4 & 5: **Router Server Port:** This should match the "Router Bind
  Port"; it’s used by the CDAP UI to connect to the Router service.

- *Add Service* Wizard, Page 4 & 5: **App Artifact Dir:** This should initially point to the
  bundled system artifacts included in the CDAP parcel directory. If you have modified
  ``${PARCELS_ROOT}``, please update this setting to match. Users will want to customize
  this directory to a location outside of the CDAP Parcel.

- **Additional CDAP configuration properties** can be added using the Cloudera Manager's 
  *Safety Valve* Advanced Configuration Snippets. Documentation of the available CDAP
  properties is in the :ref:`appendix-cdap-site.xml`.

Complete instructions, step-by-step, for using the Admin Console *Add Service* Wizard to
install CDAP follow.

Once you have completed the installation and :ref:`started CDAP
<step-by-step-cloudera-add-service-startup>`, you can then 
:ref:`verify the installation <admin-manual-verification>`.

.. _step-by-step-cloudera-add-a-service:

Add A Service
=============
Start from the Cloudera Manager Admin Console's *Home* page, selecting *Add a Service* from the menu for your cluster:

.. figure:: ../../_images/cloudera/cloudera-csd-01.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Cloudera Manager:** Starting the *Add Service* Wizard.

.. _step-by-step-cloudera-add-service-wizard:

Add Service Wizard
==================

Use the *Add Service* Wizard and select *Cask DAP*.

.. figure:: ../../_images/cloudera/cloudera-csd-02.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Add Service Wizard, Page 1:** Selecting CDAP (Cask DAP) as the service to be added.


The **Hive dependency** is for the optional CDAP "Explore" component, which can be enabled later.

.. figure:: ../../_images/cloudera/cloudera-csd-03.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Add Service Wizard, Page 2:** Setting the dependencies (in this case, including Hive).
   

**Choosing Role Assignments:** Ensure CDAP roles are assigned to hosts colocated
with service or gateway roles for HBase, HDFS, Yarn, and (optionally) Hive.

.. figure:: ../../_images/cloudera/cloudera-csd-04.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Add Service Wizard, Page 3:** When customizing Role Assignments, the *CDAP Security
   Auth Service* can be added later.
   
   
   
**Kerberos Auth Enabled** is needed if running against a secure Hadoop cluster.

**Router Server Port:** This should match the "Router Bind Port"; it’s used by the CDAP UI
to connect to the Router service.

**App Artifact Dir:** This should initially point to the bundled system artifacts included
in the CDAP parcel directory. If you have modified ``${PARCELS_ROOT}``, please update this
setting to match. Users will want to customize this directory to a location outside of the
CDAP Parcel.

.. figure:: ../../_images/cloudera/cloudera-csd-06.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Add Service Wizard, Pages 4 & 5:** Reviewing configurations; as Hive was included, CDAP Explore can be enabled.

**Additional CDAP configuration properties** can be added using the Cloudera Manager's 
*Safety Valve* Advanced Configuration Snippets. Documentation of the available CDAP
properties is in the :ref:`appendix-cdap-site.xml`.

At this point, the CDAP installation is configured and is ready to be installed. Review
your settings before continuing to the next step, which will install CDAP.


.. figure:: ../../_images/cloudera/cloudera-csd-07.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Add Service Wizard, Page 6:** Finishing first run of commands to install CDAP.
   

.. figure:: ../../_images/cloudera/cloudera-csd-08.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Add Service Wizard, Page 7:** Congratulations screen, though there is still work to be done.

.. _step-by-step-cloudera-add-service-startup:

Startup CDAP
============
After the Cloudera Manager Admin Console's *Add Service* Wizard completes, *Cask DAP* will
show in the list for the cluster where you installed it. You can select it, and go to the
*Cask DAP* page, with *Quick Links* and *Status Summary*. The lights of the *Status
Summary* should all turn green, showing completion of startup. 

The *Quick Links* includes a link to the **CDAP UI**, which by default is running on
port ``9999`` of the host where the UI role instance is running.

.. figure:: ../../_images/cloudera/cloudera-csd-09.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Cloudera Manager:** CDAP (Cask DAP) now added to the cluster.
   

.. figure:: ../../_images/cloudera/cloudera-csd-10.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Cloudera Manager:** CDAP completed startup: all lights green!
   
.. _step-by-step-cloudera-add-service-ui:

CDAP UI
=======
The CDAP UI may initially show errors while all of the CDAP YARN containers are
starting up. Allow for up to a few minutes for this. The *Services* link in the CDAP
UI in the upper right will show the status of the CDAP services. 

.. figure:: ../../../../admin-manual/source/_images/console/console_01_overview.png
   :figwidth: 100%
   :height: 714px
   :width: 800px
   :align: center
   :class: bordered-image

   **CDAP UI:** Showing started-up with applications deployed.

Further instructions for verifying your installation are contained in :ref:`admin-manual-verification`.
