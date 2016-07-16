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

    Plugin Basics <basics>
    Creating a Plugin <creating-a-plugin>
    Testing Plugins <testing-plugins>
    Packaging Plugins <packaging-plugins>

This section is intended for developers writing custom plugins. Users of these should
refer to the documentation on :ref:`using plugins <cask-hydrator-introduction-what-is-a-plugin>`.

CDAP provides for the creation of custom plugins to extend the existing
``cdap-data-pipeline`` and ``cdap-etl-realtime`` system artifacts.

*Note:* The ``cdap-etl-batch`` artifact has been deprecated and replaced with the
``cdap-data-pipeline`` effective with CDAP 3.5.0.

- :ref:`Plugin Packaging: <cask-hydrator-packaging-plugins>` packaging in a JAR
- :ref:`Plugin Presentation: <cask-hydrator-packaging-plugins-presentation>` controlling
  how your plugin appears in the Hydrator Studio 
- :ref:`Plugin Deployment: <cask-hydrator-plugin-management-deployment>` deploying as
  either a system or user *artifact*
