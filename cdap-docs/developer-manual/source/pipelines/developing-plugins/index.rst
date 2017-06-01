.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

:hide-toc: true

.. _cdap-pipelines-developing-plugins:

==================
Developing Plugins
==================

.. toctree::
    :maxdepth: 4

    Plugin Basics <plugin-basics>
    Creating a Plugin <creating-a-plugin>
    Plugin Presentation <presentation-plugins>
    Testing Plugins <testing-plugins>
    Packaging Plugins <packaging-plugins>

This section is intended for developers writing custom plugins. Users of plugins and
pipelines should refer to the documentation on :ref:`using plugins
<cdap-pipelines-introduction-what-is-a-plugin>`.

CDAP provides for the creation of custom plugins to extend the existing
``cdap-data-pipeline`` and ``cdap-data-streams`` system artifacts.

**Note:** *As of CDAP 3.5.0, the* ``cdap-etl-batch`` *artifact has been deprecated and replaced with the*
``cdap-data-pipeline``, *artifact, and the* ``cdap-etl-realtime`` *artifact has been deprecated and replaced with the*
``cdap-data-streams``, *artifact.*

Deploying plugins is covered under :ref:`Plugin Management: Plugin Deployment
<cdap-pipelines-plugin-management-deployment>`, for deploying as either a system or user
artifact.


.. |plugin-basics| replace:: **Plugin Basics:**
.. _plugin-basics: plugin-basics.html

.. |creating-a-plugin| replace:: **Creating a Plugin:**
.. _creating-a-plugin: creating-a-plugin.html

.. |presentation-plugins| replace:: **Plugin Presentation:**
.. _presentation-plugins: presentation-plugins.html

.. |testing-plugins| replace:: **Testing Plugins:**
.. _testing-plugins: testing-plugins.html

.. |packaging-plugins| replace:: **Packaging Plugins:**
.. _packaging-plugins: packaging-plugins.html


- |plugin-basics|_ Plugin types, Maven archetypes, plugin class annotations, and plugin configuration

- |creating-a-plugin|_ The Java methods that need to be implemented for each plugin type

- |presentation-plugins|_ Controlling how your plugin :ref:`appears in the CDAP Studio 
  <plugins-presentation>`

- |testing-plugins|_ Using CDAP testing facilities and the CDAP Pipelines test module

- |packaging-plugins|_ Packaging a :ref:`plugin in a JAR <cdap-pipelines-packaging-plugins>`

