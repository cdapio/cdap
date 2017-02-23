.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. _cdap-pipelines-packaging-plugins:

=================
Packaging Plugins
=================

.. NOTE: Because of the included files in this file, use these levels for headlines/titles:
  .. 1 -----
  .. 2 .....
  .. 3 `````
  .. etc.

To **package, present,** and **deploy** your plugin, see these instructions:

- :ref:`Plugin Packaging: <cdap-pipelines-packaging-plugins-packaging>` packaging in a JAR
- :ref:`Plugin Presentation: <cdap-pipelines-packaging-plugins-presentation>` controlling
  how your plugin appears in the CDAP Studio

If you are installing a **third-party JAR** (such as a **JDBC driver**) to make it
accessible to other plugins or applications, see :ref:`these instructions
<cdap-pipelines-plugin-management-third-party-plugins>`.

..   - UI
..   - REST
..   - CLI 

.. Plugin Packaging
.. ----------------
.. _cdap-pipelines-packaging-plugins-packaging:

.. .. include:: /../../developers-manual/source/building-blocks/plugins.rst

.. include:: /building-blocks/plugins.rst
   :start-after: .. _plugins-deployment-packaging:
   :end-before:  .. _plugins-deployment-system:
   
By using one of the available :ref:`Maven archetypes
<cdap-pipelines-developing-plugin-basics-maven-archetypes>`, your project will be set up to
generate the required JAR manifest. If you move the plugin class to a different Java
package after the project is created, you will need to modify the configuration of the
``maven-bundle-plugin`` in the ``pom.xml`` file to reflect the package name changes.

If you are developing plugins for the ``cdap-data-pipeline`` artifact, be aware that for
classes inside the plugin JAR that you have added to the Hadoop Job configuration directly
(for example, your custom ``InputFormat`` class), you will need to add the Java packages
of those classes to the "Export-Package" as well. This is to ensure those classes are
visible to the Hadoop MapReduce framework during the plugin execution. Otherwise, the
execution will typically fail with a ``ClassNotFoundException``.
