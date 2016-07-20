.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-hydrator-plugin-management:

=================
Plugin Management
=================

.. NOTE: Because of the included files in this file, use these levels for headlines/titles:
  .. one   -----
  .. two   .....
  .. three `````
  .. etc.

To **package, present,** and **deploy** your plugin, see these instructions:

- `Plugin Packaging: <#plugin-packaging>`__ packaging in a JAR
- `Plugin Presentation: <#plugin-presentation>`__ controlling how your plugin appears in the Hydrator Studio
- `Plugin Deployment: <#deploying-a-system-artifact>`__ deploying as either a system or user *artifact*
- `Deployment Verification: <#deployment-verification>`__ verifying an artifact was added successfully

If you are installing a **third-party JAR** (such as a **JDBC driver**) to make it accessible to other
plugins or applications, see :ref:`these instructions <cask-hydrator-third-party-plugins>`.


Listing Plugins
===============
To Be Completed


.. _cask-hydrator-plugin-management-deployment:

Deploying Plugins
=================
.. include:: /../../developers-manual/source/building-blocks/plugins.rst
   :start-after: .. _plugins-deployment-artifact:
   :end-before:  .. _plugins-deployment-packaging:
   
.. include:: /../../developers-manual/source/building-blocks/plugins.rst
   :start-after: .. _plugins-deployment-system:
   :end-before:  .. _plugins-use-case:


.. _cask-hydrator-third-party-plugins:

Deploying Third-Party JARs
==========================

.. highlight:: json  

**Prebuilt JARs:** In a case where you'd like to use pre-built third-party JARs (such as a
JDBC driver) as a plugin, you will need to create a JSON file to describe the JAR.

For information on the format of the JSON, please refer to the sections on
:ref:`Third-Party Plugins <plugins-third-party>` and :ref:`Plugin Deployment <plugins-deployment>`.

A sample JDBC Driver Plugin configuration:

.. container:: highlight

  .. parsed-literal::
  
    {
      "parents": [ "cdap-etl-batch[|version|,\ |version|]" ],
      "plugins": [
        {
          "name" : "mysql",
          "type" : "jdbc",
          "className" : "com.mysql.jdbc.Driver",
          "description" : "Plugin for MySQL JDBC driver"
        },
        {
          "name" : "postgresql",
          "type" : "jdbc",
          "className" : "org.postgresql.Driver",
          "description" : "Plugin for PostgreSQL JDBC driver"
        }
      ]
    }


Managing Multiple Versions
--------------------------
To Be Completed


Deleting Plugins
----------------
To Be Completed

