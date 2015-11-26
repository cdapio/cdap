.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _step-by-step-cloudera-add-service:

====================================================
Cloudera Manager: *Add Service* Wizard, Step-by-Step
====================================================

As described in :ref:`cloudera-installation`, you can use `Cloudera Manager
<http://www.cloudera.com/content/cloudera/en/products-and-services/cloudera-enterprise/cloudera-manager.html>`__ 
to integrate CDAP into a Hadoop cluster by:

- :ref:`downloading and installing a CDAP CSD (Custom Service Descriptor) <cloudera-installation-csd>`; and
- :ref:`download and distributing the CDAP parcel <cloudera-installation-download-distribute-parcel>`.

Once you have done those two steps, these instructions show you how to use that CSD with
the Cloudera Manager Admin Console *Add Service* Wizard to install and start CDAP.

.. _step-by-step-cloudera-add-a-service:

Add A Service
=============

.. figure:: ../../_images/cloudera/cloudera-csd-01.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Cloudera Manager:** Starting the *Add Service* Wizard.

.. _step-by-step-cloudera-add-service-wizard:

The "Add Service" Wizard
========================

.. figure:: ../../_images/cloudera/cloudera-csd-02.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Add Service Wizard, Page 1:** Selecting CDAP (Cask DAP) as the service to be added.


.. figure:: ../../_images/cloudera/cloudera-csd-03.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Add Service Wizard, Page 2:** Setting the dependencies (in this case, including Hive).
   

.. figure:: ../../_images/cloudera/cloudera-csd-04.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Add Service Wizard, Page 3:** When customizing Role Assignments, CDAP **Security
   Auth** service can be added later.


.. figure:: ../../_images/cloudera/cloudera-csd-06.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Add Service Wizard, Pages 4 & 5:** Reviewing configurations; as Hive was included, CDAP Explore can be enabled.


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

Further instructions for verifying your installation are contained in :ref:`cloudera-verification`.
