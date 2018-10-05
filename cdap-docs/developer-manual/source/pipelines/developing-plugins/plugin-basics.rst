.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. _cdap-pipelines-developing-plugin-basics:

=============
Plugin Basics
=============

Plugin Types
============
In CDAP pipelines, these plugin types are presently used:

- Action (*action*, restricted to batch pipelines)
- Batch Source (*batchsource*, restricted to batch pipelines)
- Batch Sink (*batchsink*)
- Streaming Source (*streamingsource*, restricted to real-time pipelines)
- Transformation (*transform*)
- Error Transformation (*errortransform*)
- Batch Aggregator (*batchaggregator*)
- Batch Joiner (*batchjoiner*)
- Spark Compute (*sparkcompute*)
- Spark Sink (*sparksink*, restricted to batch pipelines) 
- Windower (*windower*, restricted to real-time pipelines)
- Post-run Action (*postaction*, restricted to batch pipelines)

In the CDAP Pipelines UI, all Batch Aggregator, Batch Joiner, Spark Compute, and Spark Sink
plugins are grouped under the Analytics section. All Transformation and Windower plugins
are grouped under the Transforms section. 

.. _cdap-pipelines-developing-plugin-basics-maven-archetypes:

Maven Archetypes
================
To get started on creating a custom plugin, you can use the Maven archetype to create your project:

- ``cdap-data-pipeline-plugins-archetype`` (contains batch, Spark plugin, and other types)

This command will create a project from an archetype:

.. container:: highlight

  .. parsed-literal::

    |$| mvn archetype:generate \\
          -DarchetypeGroupId=co.cask.cdap \\
          -DarchetypeArtifactId=cdap-data-pipeline-plugins-archetype \\
          -DarchetypeVersion=\ |release| \\
          -DgroupId=org.example.plugin
          
**Note:** Replace the *groupId* parameter (``org.example.plugin``) with your own
organization, but it must not be replaced with ``co.cask.cdap``.

Complete examples for each archetype:

.. container:: highlight

  .. parsed-literal::

    $ mvn archetype:generate -DarchetypeGroupId=co.cask.cdap -DarchetypeArtifactId=cdap-data-pipeline-plugins-archetype -DarchetypeVersion=\ |release| -DgroupId=org.example.plugin
  
Maven supplies a guide to the naming convention used above at
https://maven.apache.org/guides/mini/guide-naming-conventions.html.

Class Annotations
=================
These annotations are used for plugin classes:

- ``@Plugin``: The class to be exposed as a plugin needs to be annotated with the ``@Plugin``
  annotation and the type of the plugin must be specified.

- ``@Name``: Annotation used to name the plugin.

- ``@Description``: Annotation used to add a description of the plugin.

- ``@Requirements``: Annotation used to specify the `Requirements <../../../reference-manual/javadocs/co/cask/cdap/api/annotation/Requirements.html>`__ needed by a plugin to run successfully.

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

Plugin Data Types
=================

Plugins support several physical data types: null, boolean, int, long, float, double, string, array, map, enum, union,
and record. They also support several logical data types: date, time-millis, time-micros, timestamp-millis,
and timestamp-micros. Logical types represent some abstract concept but are internally represented with a corresponding
physical type. The date logical type is an integer that holds the number of days since the Unix epoch. The time-millis
logical type is an integer that holds the number of milliseconds since midnight. The time-micros logical type is
a long that holds the number of microseconds since midnight. The timestamp-millis logical type is a long that holds
the number of milliseconds since the Unix epoch. The timestamp-micros logical type is a long that holds the number
of microseconds since the Unix epoch.

.. highlight:: java

To create a schema field of a logical type::

  Schema schema = Schema.recordOf("exampleSchema",
                                   Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                   Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                   Schema.Field.of("date", Schema.of(Schema.LogicalType.DATE)),
                                   Schema.Field.of("time-millis", Schema.of(Schema.LogicalType.TIME_MILLIS)),
                                   Schema.Field.of("time-micros", Schema.of(Schema.LogicalType.TIME_MICROS)),
                                   Schema.Field.of("ts-millis", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                   Schema.Field.of("ts-micros", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));

To set a record field for a logical type::

  StructuredRecord record = StructuredRecord.builder(schema)
        .set("id", 1)
        .set("name", "alice")
        // set date to August 30th, 2018
        .setDate("date", LocalDate.of(2018, 8, 30))
        // set time in millis to 11.0.0.111 hours
        .setTime("time-millis", LocalTime.of(11, 0, 0, 111 * 1000 * 1000))
        // set time in millis to 11.0.0.111111 hours
        .setTime("time-micros", LocalTime.of(11, 0, 0, 111111 * 1000))
        // set timestamp in millis to November 11, 2018 11:11:11.123 UTC
        .setTimestamp("ts-millis",
                       ZonedDateTime.of(2018, 11, 11, 11, 11, 11, 123 * 1000 * 1000, ZoneId.ofOffset("UTC", ZoneOffset.UTC)))
        // set timestamp in micros to November 11, 2018 11:11:11.123456 UTC
        .setTimestamp("ts-micros",
                       ZonedDateTime.of(2018, 11, 11, 11, 11, 11, 123456 * 1000, ZoneId.ofOffset("UTC", ZoneOffset.UTC)))
        .build();

To read a logical type from a record::

  LocalDate localDate = record.getDate("date");
  LocalTime localTimeMillis = record.getTime("time-millis");
  LocalTime localTimeMicros = record.getTime("time-micros");
  ZonedDateTime timestampMillis = record.getTimestamp("ts-millis");
  ZonedDateTime timestampMicros = record.getTimestamp("ts-micros");

Please note that logical types such as Date, Time and Timestamp are internally represented as primitive types.
Therefore these types can be set or retrieved as primitive types in structured record. For example::

  StructuredRecord record = StructuredRecord.builder(schema)
          .set("id", 1)
          .set("name", "alice")
          // set number of days since epoch representing November 30th, 2018
          .set("date", 17773)
          // set time in milli seconds representing 11.0.0.111 hours
          .set("time-millis", 39600111L)
          // set time in micro seconds representing 11.0.0.111111 hours
          .set("time-micros", 39600111111L)
          // set timestamp in milli seconds representing November 11, 2018 11:11:11.123 UTC
          .set("ts-millis", 1541934671123L)
          // set timestamp in micro seconds representing November 11, 2018 11:11:11.123456 UTC
          .set("ts-micros", 1541934671123456L)
          .build();

  // number of days since Unix epoch.
  int date = record.get("date");

