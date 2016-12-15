.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-hydrator-developing-plugin-basics:

=============
Plugin Basics
=============

Plugin Types
============
In Cask Hydrator, these plugin types are presently used:

- Action (*action*, restricted to batch pipelines)
- Batch Source (*batchsource*, restricted to batch pipelines)
- Batch Sink (*batchsink*)
- Streaming Source (*streamingsource*, restricted to real-time pipelines)
- Transformation (*transform*)
- Batch Aggregator (*batchaggregator*)
- Batch Joiner (*batchjoiner*)
- Spark Compute (*sparkcompute*)
- Spark Sink (*sparksink*, restricted to batch pipelines) 
- Windower (*windower*, restricted to real-time pipelines)
- Post-run Action (*postaction*, restricted to batch pipelines)
- Real-time Source (*realtimesource*, deprecated)
- Real-time Sink (*realtimesink*, deprecated)

In the Cask Hydrator UI, all Batch Aggregator, Batch Joiner, Spark Compute, and Spark Sink
plugins are grouped under the Analytics section. All Transformation and Windower plugins
are grouped under the Transforms section. 

.. _cask-hydrator-developing-plugin-basics-maven-archetypes:

Maven Archetypes
================
To get started on creating a custom plugin, you can use one of these Maven archetypes to create your project:

- ``cdap-data-pipeline-plugins-archetype`` (contains batch, Spark plugin, and other types)
- ``cdap-etl-transform-archetype`` (contains a transform)

This command will create a project from an archetype:

.. container:: highlight

  .. parsed-literal::

    |$| mvn archetype:generate \\
          -DarchetypeGroupId=co.cask.cdap \\
          -DarchetypeArtifactId=<archetype> \\
          -DarchetypeVersion=\ |release| \\
          -DgroupId=org.example.plugin
          
where ``<archetype>`` is one of the archetypes listed above.

**Note:** Replace the *groupId* parameter (``org.example.plugin``) with your own organization, but it must not be replaced with ``co.cask.cdap``.

Complete examples for each archetype:

.. tabbed-parsed-literal::
  :tabs: cdap-data-pipeline-plugins,cdap-etl-transform
  :dependent: hydrator-archetype

  .. cdap-data-pipeline-plugins

  $ mvn archetype:generate -DarchetypeGroupId=co.cask.cdap -DarchetypeArtifactId=cdap-data-pipeline-plugins-archetype -DarchetypeVersion=\ |release| -DgroupId=org.example.plugin
  
  .. cdap-etl-transform
  
  $ mvn archetype:generate -DarchetypeGroupId=co.cask.cdap -DarchetypeArtifactId=cdap-etl-transform-archetype -DarchetypeVersion=\ |release| -DgroupId=org.example.plugin

Maven supplies a guide to the naming convention used above at https://maven.apache.org/guides/mini/guide-naming-conventions.html.

Class Annotations
=================
These annotations are used for plugin classes:

- ``@Plugin``: The class to be exposed as a plugin needs to be annotated with the ``@Plugin``
  annotation and the type of the plugin must be specified.

- ``@Name``: Annotation used to name the plugin.

- ``@Description``: Annotation used to add a description of the plugin.

Plugin Configuration
====================
Each plugin can define a plugin config that specifies what properties the plugin requires.
When a user creates a pipeline, they will need to provide these properties in order to
use the plugin. This is done by extending the ``PluginConfig`` class, and populating that
class with the fields your plugin requires. Each field can be annotated to provide more
information to users:

- ``@Name``: The name of the field. Defaults to the Java field name. You may want to use this
  if you want the user-facing name to use syntax that is not legal Java syntax.

- ``@Description``: A description for the field.

- ``@Nullable``: Indicates that the specific configuration property is
  optional. Such a plugin class can be used without that property being specified.

At this time, fields in a ``PluginConfig`` must be primitive Java types (boxed or unboxed).

.. highlight:: java

Example::
 
  @Plugin(type = BatchSource.PLUGIN_TYPE)
  @Name("MyBatchSource")
  @Description("This is my Batch Source.")
  public class MyBatchSource extends BatchSource<LongWritable, Text, StructuredRecord> {
    private final Conf conf;

    public MyBatchSource(Conf conf) {
      this.conf = conf;
    )

    public static class Conf extends PluginConfig {
      @Name("input-path")
      @Description("Input path for the source.")
      private String inputPath;

      @Nullable
      @Description("Whether to clean up the previous run's output. Defaults to false.")
      private Boolean cleanOutput;

      public Conf() {
        cleanOutput = false;
      }
    }
    ...
  }

In this example, we have a plugin of type ``batchsource``, named ``MyBatchSource``.
This plugin takes two configuration properties. The first is named ``input-path`` and is required.
The second is named ``cleanOutput`` and is optional. Note that optional configuration fields should
have their default values set in the no-argument constructor.
