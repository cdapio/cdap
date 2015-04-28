.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _advanced-custom-etl:

==============================
Creating Application Templates
==============================

Overview
========
This section is intended for people writing custom Application Templates, Plugins and
custom ETL Plugins. Users of these should refer to the Users’ Manual.



Components
==========

Structured Records
------------------

Sinks
-----

Sources
-------

Transformations
---------------


Creating Custom App-Templates
=============================

ETL API
-------

Manifest
--------

JSON format
...........


Using Custom App-Templates
==========================

Java
----
CLI
---
RESTful API
-----------
Java Client API
---------------


Creating Custom Transformations
===============================
Written in Javascript?


Creating Plugins
=================

Overview
--------
- Written in Java
- Accessing existing Servers and Sinks

Plugins are classes that are not included in an Application Template, but can be made
available for their use during the creation and runtime of Adapters. Plugin classes are
packaged as OSGI JARs and they are made available to the CDAP platform [link that talks
about where to copy and how to create OSGI JARs]. 

You can also copy a third-party JAR and expose it as a Plugin by placing a JSON
configuration file for the Plugin along with the JAR. 

The CDAP Platform scans the Plugins directory during startup and gathers information about
the Application Templates and the Plugins available under each template. Each Plugin has a
name, type, classname and a set of properties associated with it. With the combination of
template-id, plugin-type, and plugin-name, the CDAP platform can uniquely identify a Plugin.

Directory Structure of Templates and Plugins
--------------------------------------------

The directory where Application Template JARs and Plugins are placed depends on the CDAP
runtime mode, standalone or distributed.

In Standalone CDAP:

- ``$CDAP_INSTALL_DIR/plugins``

In Distributed CDAP:

- Set through the ``cdap-site.xml`` by specifiying the property ``${app.template.dir}``.
  By default, it points to ``/opt/cdap/master/plugins``.


In the Plugins directory, there can be multiple Template JARs. Since Template JARs are
CDAP applications, each Application Template has associated with it the name that will be
set for it as a CDAP Application. This name is referred to as the *template-id*.

Plugins for a particular Template are to be copied to a subdirectory in the Plugins directory
which is named the same as the template-id.

For example, you will find the ``etlBatch`` and ``etlRealtime`` template JARs in the
``${app.template.dir}``. And you can also find the plugins for these two templates are in
the `etlBatch` and `etlRealtime` subdirectories, located in the same
``${app.template.dir}``.


Adding a Plugin through a File
------------------------------

If you want to add a plugin class (most likely present in a third-party JAR) through a
file, you create a file with the same name as the plugin JAR. For example, if the JAR
is named ``xyz-driver-v0.4.jar``, you create a JSON file with the same name,
``xyz-driver-v0.4.json``. The content of the file is a JSON array, and each entry represents a Plugin
that you want to expose to the CDAP platform for that Application Template.

For example, if you want to expose a class ``org.xyz.driver.MDriver``, which is of plugin
type *Driver* and name *SimpleDriver*, the JSON file *(xyz-driver-v0.4.json)* should look
as::

  [
    {
      "type":"Driver",
      "name":"SimpleDriver",
      "description":"Driver which is a Simple Driver",
      "className":"org.xyz.driver.MDriver"
    }
  ]


Adding a Plugin through an OSGi Bundle
--------------------------------------

If you want to building a plugin of your own, you have option to build an OSGi bundle and
export the Plugin classes for inspection by the platform. Let’s see an example of how a
plugin class will look like and then look at how to create an OSGi Bundle:

@Plugin -> Class to be exposed as a Plugin needs to be annotated with this and optionally
you can specify type of the plugin. By default, the plugin type will be ‘plugin’.

@Name -> Annotation used to name the Plugin as well as the properties in the Configuration
class of the Plugin (for example, SimplePluginForDemo.Config). By default the name of the
class (or the name of the field in case of annotation for the config property) is used.

@Description -> Annotation used to add description

@Nullable -> This annotation indicates that the specific configuration property is
optional. So this plugin class can be used without that property being specified.

Example::

  @Plugin(type = “Type1”)
  @Name(“SimplePlugin”)
  @Description(“Very simple demo plugin”)
  public class SimplePluginForDemo {

    private Config config;
  
    public static final Class Config extends PluginConfig {
    
      @Name(“property1”)
      @Description(“Description of the Property”)
      private Integer limit;

      @Name(“property2”)
      @Nullable
      private Long timeOut = new Long(5000); // Default value is 5000
    }
  }

TBD: How to create an OSGi bundle? 









Packaging
---------
- Packaging an App-Template
- Packaging an ETL Component

Installation
------------
- Plugins Directory
- Restart CDAP?
- Updating?

Testing
=======
- Test Framework (cdap-etl-test)
- For testing sources, sinks, transforms

