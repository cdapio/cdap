.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-hydrator-plugin-management:

=================
Plugin Management
=================

This section covers how to mange the deployment of plugins on your system:

- :ref:`Plugin Deployment: <cask-hydrator-plugin-management-deployment>` deploying as
  either a system or user *artifact*

- :ref:`Deployment Verification: <plugins-deployment-verification>` verifying that an
  artifact was deployed successfully

- :ref:`Deploying Third-party JARs: <cask-hydrator-plugin-management-third-party-plugins>`
  making JDBC drivers and other artifacts available to applications 

- :ref:`Managing Multiple Version: <cask-hydrator-plugin-management-multiple-versions>` 
  different versions can co-exist and be available at the same time

- :ref:`Deleting Plugins: <cask-hydrator-plugin-management-deleting-plugins>` removing
  deployed artifacts from CDAP

If you are creating your own plugins, see the section on :ref:`developing plugins
<cask-hydrator-developing-plugins>` for information on writing plugins, including
:ref:`packaging plugins in a JAR <cask-hydrator-packaging-plugins-packaging>` and
their :ref:`presentation in a UI <cask-hydrator-packaging-plugins-presentation>`
such as Hydrator Studio.

If you are installing a **third-party JAR** (such as a **JDBC driver**) to make it
accessible to other plugins or applications, see :ref:`these instructions
<cask-hydrator-plugin-management-third-party-plugins>`.


Available Plugins
=================
Plugins available with CDAP are listed beginning on a :ref:`separate reference page <cask-hydrator-plugins>`.
User-installed plugins are not listed there, but if they are installed correctly, the reference
documentation for the plugin will be available through the :ref:`Hydrator Studio <cask-hydrator-studio>`.


.. _cask-hydrator-plugin-management-deployment:

Deploying Plugins
=================

.. include:: /../../developers-manual/source/building-blocks/plugins.rst
   :start-after: .. _plugins-deployment-artifact:
   :end-before:  .. _plugins-deployment-packaging:

.. include:: /../../developers-manual/source/building-blocks/plugins.rst
   :start-after: .. _plugins-deployment-system:
   :end-before:  .. _plugins-use-case:


.. _cask-hydrator-plugin-management-third-party-plugins:

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


.. _cask-hydrator-plugin-management-multiple-versions:

Managing Multiple Versions
==========================
Different versions of the same plugin (or artifact) can be loaded and available at the
same time. These will appear in the :ref:`Hydrator Studio <cask-hydrator-studio>` as
possible choices when selecting a plugin or creating a :ref:`plugin template
<cask-hydrator-studio-plugin-templates>`. If no version is specified for a plugin in the
:ref:`configuration file <hydrator-developing-pipelines-configuration-file-format>` used
to create an application, the highest version currently available in the system will be
used.


.. _cask-hydrator-plugin-management-deleting-plugins:

Deleting Plugins
================
Plugins can be deleted using either the :ref:`Artifact HTTP RESTful API <http-restful-api>` or
the :ref:`CDAP CLI <cdap-cli>`.

In the case of the CDAP CLI, only plugins that have been deployed in the ``user`` scope
can be deleted by the CDAP CLI. Both ``system`` and ``user`` scope plugins can be deleted
using the HTTP RESTful API by using the appropriate calls.

Note that in all cases, the actual files (JARs and JSON files) associated with the plugins
are not deleted. Instead, the references to them are deleted in the CDAP system. If the
files are not removed after these references are deleted, then |---| in the case of the
``system`` scope plugins |---|, the artifacts will be reloaded the next time CDAP is
restarted, as they are automatically loaded at startup from the `appropriate directory
<plugin-management#deploying-as-a-system-artifact>`_.
