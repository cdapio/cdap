.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

:hide-toc: true

.. _cask-hydrator-developing-plugins:

==================
Developing Plugins
==================

.. toctree::
    :maxdepth: 4

    Plugin Basics <plugin-basics>
    Creating a Plugin <creating-a-plugin>
    Testing Plugins <testing-plugins>
    Packaging Plugins <packaging-plugins>

.. |plugin-basics| replace:: **Plugin Basics:**
.. _plugin-basics: plugin-basics.html

- |plugin-basics|_ Types, maven archetypes, annotations, and plugin configurations


.. |creating-a-plugin| replace:: **Creating a Plugin:**
.. _creating-a-plugin: creating-a-plugin.html

- |creating-a-plugin|_ Java methods that need to be implemented for each plugin type


.. |testing-plugins| replace:: **Testing Plugins:**
.. _testing-plugins: testing-plugins.html

- |testing-plugins|_ Using CDAP testing facilities and the Hydrator test module


.. |packaging-plugins| replace:: **Packaging Plugins:**
.. _packaging-plugins: packaging-plugins.html

- |packaging-plugins|_ :ref:`Packaging a plugin in a JAR <cask-hydrator-packaging-plugins>`, and
  :ref:`controlling how your plugin appears in the Hydrator Studio <cask-hydrator-packaging-plugins-presentation>`.
  
This section is intended for developers writing custom plugins. Users of these should
refer to the documentation on :ref:`using plugins <cask-hydrator-introduction-what-is-a-plugin>`.

Deploying plugins is covered under :ref:`Plugin Deployment:
<cask-hydrator-plugin-management-deployment>`, for deploying as either a system or user
artifact.

CDAP provides for the creation of custom plugins to extend the existing
``cdap-data-pipeline`` and ``cdap-etl-realtime`` system artifacts.

*Note:* The ``cdap-etl-batch`` artifact has been deprecated and replaced with the
``cdap-data-pipeline`` effective with CDAP 3.5.0.

