.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _advanced-custom-app-template:

==============================
Creating Application Templates (Beta)
==============================

Overview
========
This section is intended for people writing custom Application Templates, Plugins and
custom ETL Plugins. Users of these should refer to the Users’ Manual. [link]



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


Creating Custom Application Templates
=====================================

ETL API
-------

Manifest
--------

JSON format
...........


Using Custom Application Templates
==================================

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
file, you create a file with the same name as the plugin JAR. For example, if the JAR is
named ``xyz-driver-v0.4.jar``, you create a JSON file with the same name,
``xyz-driver-v0.4.json``. The content of the file is a JSON array, and each entry
represents a Plugin that you want to expose to the CDAP platform for that Application
Template.

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

How to create an OSGi bundle 
-----------------------------
TBD

Accessing existing Servers and Sinks
------------------------------------
TBD


Creating Custom ETL Plugins
===========================

Overview
--------

CDAP ships with two built-in templates: **ETL Batch** and **ETL Realtime**. It also ships
with a set of source, transform and sink **Plugins** that can be used to build ETL
Adapters by just specifying the properties for each of plugin used in the ETL Adapter. A
Source emits data of a certain type. A Transform receives it, transforms it, and emits it
to the next stage. The next stage can be an additional Transform. (There can be zero or
more Transforms in an ETL Adapter.) The last stage of the ETL Adapter is a Sink which
receives data of a certain type and persists it. 

But there might be circumstances where the provided set of plugins for ETL templates is
not enough. ETL Templates expose a simple, intuitive and domain-specific API that you can
use to build your own source, transform or sink. Once you build your own plugin, you can
include the plugin in your CDAP installation [link to the plugin directory structure
above].

Data Interchange Format: Structured Records
-------------------------------------------

All the ETL plugins (source, transform, sink) either emit or expect to receive StructuredRecords
[link to Javadoc]. A StructuredRecord has a schema and supports these field types: [TBD].

If you are building a single ETL plugin (either source, sink, or transform) and you want
to use it with the ETL plugins provided out-of-the-box, then your plugin must
be able to work with a StructuredRecord. If you are developing source, sink,
transform plugins on your own, you are free to adopt any data type for the plugins to
receive, emit, etc. But validation of the data types (whether a specific plugin can
receive data from the previous plugin in the ETL Adapter configuration that the user is
trying to create) will be performed during the creation phase of ETL Adapters.

Plugins
-------

- Transformations
- Batch Sources
- Batch Sinks
- Realtime Sources
- Realtime Sinks

Transformations
...............
In ETL Templates, transformations are purely functional. They operate on only one data
object at a time. You can implement a transform plugin by extending the TransformStage
class. The only method that you need to implement is:

- ``transform()``: The transform method is the logic that will be applied on each incoming
  data object. An emitter can be used to pass the results to the subsequent stages (which
  could be either another TransformStage or a Sink).

Optional methods to override:

- ``initialize()``: Used to perform any initialization step that might be required during the
  runtime of the TransformStage. It is guaranteed that this method will be invoked before
  the transform method.

- ``destroy()`` : Used to perform any cleanup that might be required at the end of the
  runtime of the TransformStage.

Example::

  @Plugin(type = "transform")
  @Name("Identity")
  @Description("Transformation Example that makes copies")
  public class DuplicateTransform extends TransformStage<StructuredRecord, StructuredRecord> {
    private final Config config;

    public static final class Config extends PluginConfig {
    
      @Name(“count”)
      @Description(“Field that indicates number of copies to make”)
      private String fieldName; 
    } 
  
    @Overide
    public void initialize(StageContext context) {
      super.initialize(context);
    }
  
    @Override
    public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
      Integer copies = input.get(config.fieldName);
      for (int i = 0; i < copies; i++) {
        emitter.emit(input);
      }
      getContext().getMetrics().count(“copies”, copies);
    }

    @Override
    public void destroy() {
    
    }
  }


The above is an example of a *DuplicateTransform* that emits copies of the incoming record
based on the value in the record. The fieldname that corresponds to the copies is received
as part of the Plugin configuration. The initialize and destroy methods are invoked at the
beginning and at the end of the runtime of the transform. The transform method is invoked
for each incoming input object.

Note that Plugins can emit their own metrics using the ``StageContext``'s ``getMetrics``
method. Logging through SLF4J is also supported. 

Transform Plugins can be used in both ETL Realtime and ETL Batch templates. A Transform
plugin needs to be copied into the ETL Realtime and ETL Batch dirs [link] to be used in
whichever of those templates you are adding it to.


Batch Sources
.............
In order to implement a Batch Source (to be used in the ETL Batch template), you can
extend the BatchSource class. You need to define the types of the KEY and VALUE that the
Batch Source will receive and also the type of object that the Batch Source will emit to
the subsequent stage (which could be either a TransformStage or a BatchSink). After
defining the types, only one method is required to be implemented:

- ``prepareJob()``: Used to configure the Hadoop Job configuration (for example, set the ``InputFormatClass``)

Optional methods to override:

- configurePipeline() : Used to create any Streams or Datasets that might act as the source for the Batch Source

- initialize() : Initialize the Batch Source runtime. Guaranteed to be executed before any call to the [plugin’s?] transform method.
- transform() : This method will be called for every input key-value pair generated by the Batch Job. By default, the value is emitted to the subsequent stage.
destroy() : ??

Example:

@Plugin(type = “source”)
@Name(“MyBatchSource”)
@Description(“Demo Source”)
public class MyBatchSource extends BatchSource<LongWritable, String, String> {

  @Override
  public void prepareJob(BatchSourceContext context) {
    Job job = context.getHadoopJob();
    job.setInputFormatClass(...);
    // Other Hadoop job configuration related to Input
  }
}



Batch Sinks
...........

In order to implement a Batch Sink (to be used in ETL Batch template), you can extend the BatchSink class. Similar to BatchSource, you need to define the types of the KEY and VALUE that the BatchSink will write in the Batch job and also the type of object that it will accept from the previous stage (which could be either a TransformStage or a BatchSource). After defining the types, only one method is required to be implemented:

prepareJob() : Used to configure the Hadoop Job configuration (for ex, set OutputFormatClass etc)

Methods that can be overridden:

configurePipeline(): Used to create any datasets that might act as the sink for the BatchSink
initialize() : Initialize the Batch Sink runtime. Guaranteed to be executed before any call to the plugin’s transform method.
transform() : This method will be called for every object that is emitted from the previous stage. The logic inside the method will transform the object to the key-value pair expected by the Batch Job. By default, the incoming object is set as the Key and the Value is set to null.
destroy() : ??

Example:

@Plugin(type = “sink”)
@Name(“MyBatchSink”)
@Description(“Demo Sink”)
public class MyBatchSource extends BatchSink<String, String, NullWritable> {

  @Override
  public void prepareJob(BatchSourceContext context) {
    Job job = context.getHadoopJob();
    job.setOutputFormatClass(...);
    // OtherHadoop job configuration related to Output
  }
}













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

