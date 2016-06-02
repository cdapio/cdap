.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cdap-apps-custom-etl-plugins:

===========================
Creating Custom ETL Plugins
===========================

.. highlight:: java

Overview
========
This section is intended for developers writing custom ETL plugins. Users of these should
refer to the :ref:`Included Applications <cdap-apps-index>`.

CDAP provides for the creation of custom ETL plugins to extend the existing ``cdap-etl-batch``,
``cdap-etl-realtime``, and ``cdap-data-pipeline`` system artifacts.


Plugin Types and Maven Archetypes
=================================
In Cask Hydrator, there are eight plugin types:

- Batch Source (*batchsource*)
- Batch Sink (*batchsink*)
- Real-time Source (*realtimesource*)
- Real-time Sink (*realtimesink*)
- Transformation (*transform*)
- Batch Aggregator (*batchaggregator*)
- Spark Compute (*sparkcompute*)
- Spark Sink (*sparksink*) 

To get started, you can use one of the Maven archetypes to create your project: 

- ``cdap-data-pipeline-plugins-archetype`` (contains all batch plugin types)
- ``cdap-etl-realtime-source-archetype`` (contains a realtime source)
- ``cdap-etl-realtime-sink-archetype`` (contains a realtime sink)
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

You can replace the groupId with your own organization, but it must not be ``co.cask.cdap``.

Plugin Basics
=============

Plugin Class Annotations
------------------------
These annotations are used for plugin classes:

- ``@Plugin``: The class to be exposed as a plugin needs to be annotated with the ``@Plugin``
  annotation and the type of the plugin must be specified.

- ``@Name``: Annotation used to name the plugin.

- ``@Description``: Annotation used to add a description of the plugin.

Plugin Config
-------------
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

.. highlight:: java

Creating a Batch Source
=======================
In order to implement a Batch Source (to be used in either the ETL Batch or Data Pipeline artifacts), you extend the
``BatchSource`` class. You need to define the types of the KEY and VALUE that the Batch
Source will receive and the type of object that the Batch Source will emit to the
subsequent stage (which could be either a Transformation or a Batch Sink). After defining
the types, only one method is required to be implemented::

  prepareRun()

.. rubric:: Methods

- ``prepareRun()``: Used to configure the input for each run of the pipeline. This is called by
  the client that will submit the job for the pipeline run.
- ``onRunFinish()``: Used to run any required logic at the end of a pipeline run. This is called
  by the client that submitted the job for the pipeline run.
- ``configurePipeline()``: Used to create any streams or datasets or perform any validation
  on the application configuration that are required by this plugin.
- ``initialize()``: Initialize the Batch Source. Guaranteed to be executed before any call
  to the plugin’s ``transform`` method. This is called by each executor of the job. For example,
  if the MapReduce engine is being used, each mapper will call this method.
- ``destroy()``: Destroy any resources created by ``initialize``. Guaranteed to be executed after all calls
  to the plugin’s ``transform`` method have been made. This is called by each executor of the job.
  For example, if the MapReduce engine is being used, each mapper will call this method.
- ``transform()``: This method will be called for every input key-value pair generated by
  the batch job. By default, the value is emitted to the subsequent stage.

Example::

  /**
   * Batch Source that reads from a FileSet that has its data formatted as text.
   *
   * LongWritable is the first parameter because that is the key used by Hadoop's {@link TextInputFormat}.
   * Similarly, Text is the second parameter because that is the value used by Hadoop's {@link TextInputFormat}.
   * {@link StructuredRecord} is the third parameter because that is what the source will output.
   * All the plugins included with Hydrator operate on StructuredRecords.
   */
  @Plugin(type = BatchSource.PLUGIN_TYPE)
  @Name(TextFileSetSource.NAME)
  @Description("Reads from a FileSet that has its data formatted as text.")
  public class TextFileSetSource extends BatchSource<LongWritable, Text, StructuredRecord> {
    public static final String NAME = "TextFileSet";
    public static final Schema OUTPUT_SCHEMA = Schema.recordOf(
      "textRecord",
      Schema.Field.of("position", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("text", Schema.of(Schema.Type.STRING))
    );
    private final Conf config;

    /**
     * Config properties for the plugin.
     */
    public static class Conf extends PluginConfig {
      public static final String FILESET_NAME = "fileSetName";
      public static final String CREATE_IF_NOT_EXISTS = "createIfNotExists";
      public static final String DELETE_INPUT_ON_SUCCESS = "deleteInputOnSuccess";

      // The name annotation tells CDAP what the property name is. It is optional, and defaults to the variable name.
      // Note: only primitives (including boxed types) and string are the types that are supported.
      @Name(FILESET_NAME)
      @Description("The name of the FileSet to read from.")
      private String fileSetName;

      // A nullable fields tells CDAP that this is an optional field.
      @Nullable
      @Name(CREATE_IF_NOT_EXISTS)
      @Description("Whether to create the FileSet if it doesn't already exist. Defaults to false.")
      private Boolean createIfNotExists;

      @Nullable
      @Name(DELETE_INPUT_ON_SUCCESS)
      @Description("Whether to delete the data read by the source after the run succeeds. Defaults to false.")
      private Boolean deleteInputOnSuccess;

      // Use a no-args constructor to set field defaults.
      public Conf() {
        fileSetName = "";
        createIfNotExists = false;
        deleteInputOnSuccess = false;
      }
    }

    // CDAP will pass in a config with its fields populated based on the configuration given when creating the pipeline.
    public TextFileSetSource(Conf config) {
      this.config = config;
    }

    // configurePipeline is called exactly once when the pipeline is being created.
    // Any static configuration should be performed here.
    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
      // if the user has set createIfNotExists to true, create the FileSet here.
      if (config.createIfNotExists) {
        pipelineConfigurer.createDataset(config.fileSetName,
                                         FileSet.class,
                                         FileSetProperties.builder()
                                           .setInputFormat(TextInputFormat.class)
                                           .setOutputFormat(TextOutputFormat.class)
                                           .setEnableExploreOnCreate(true)
                                           .setExploreFormat("text")
                                           .setExploreSchema("text string")
                                           .build()
        );
      }
      // Set the output schema of this stage so that stages further down the pipeline will know their input schema.
      pipelineConfigurer.getStageConfigurer().setOutputSchema(OUTPUT_SCHEMA);
    }

    // prepareRun is called before every pipeline run, and is used to configure what the input should be,
    // as well as any arguments the input should use. It is called by the client that is submitting the batch job.
    @Override
    public void prepareRun(BatchSourceContext context) throws IOException {
      context.setInput(Input.ofDataset(config.fileSetName));
    }

    // onRunFinish is called at the end of the pipeline run by the client that submitted the batch job.
    @Override
    public void onRunFinish(boolean succeeded, BatchSourceContext context) {
      // perform any actions that should happen at the end of the run.
      // in our case, we want to delete the data read during this run if the run succeeded.
      if (succeeded && config.deleteInputOnSuccess) {
        FileSet fileSet = context.getDataset(config.fileSetName);
        for (Location inputLocation : fileSet.getInputLocations()) {
          try {
            inputLocation.delete(true);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    // transform is used to transform the key-value pair output by the input into objects output by this source.
    // The output should be a StructuredRecord if you want the source to be compatible with the plugins included
    // with Hydrator.
    @Override
    public void transform(KeyValue<LongWritable, Text> input, Emitter<StructuredRecord> emitter) throws Exception {
      emitter.emit(StructuredRecord.builder(OUTPUT_SCHEMA)
                     .set("position", input.getKey().get())
                     .set("text", input.getValue().toString())
                     .build()
      );
    }
  }

Creating a Batch Sink
=====================
In order to implement a Batch Sink (to be used in either the ETL Batch or Data Pipeline artifacts), you extend the
``BatchSink`` class. Similar to a Batch Source, you need to define the types of the KEY and
VALUE that the Batch Sink will write in the Batch job and the type of object that it will
accept from the previous stage (which could be either a Transformation or a Batch Source).

.. highlight:: java

After defining the types, only one method is required to be implemented::

  prepareRun()

.. rubric:: Methods

- ``prepareRun()``: Used to configure the output for each run of the pipeline. This is called by
  the client that will submit the job for the pipeline run.
- ``onRunFinish()``: Used to run any required logic at the end of a pipeline run. This is called
  by the client that submitted the job for the pipeline run.
- ``configurePipeline()``: Used to create any streams or datasets or perform any validation
  on the application configuration that are required by this plugin.
- ``initialize()``: Initialize the Batch Sink. Guaranteed to be executed before any call
  to the plugin’s ``transform`` method. This is called by each executor of the job. For example,
  if the MapReduce engine is being used, each mapper will call this method.
- ``destroy()``: Destroy any resources created by ``initialize``. Guaranteed to be executed after all calls
  to the plugin’s ``transform`` method have been made. This is called by each executor of the job.
  For example, if the MapReduce engine is being used, each mapper will call this method.
- ``transform()``: This method will be called for every object that is received from the
  previous stage. The logic inside the method will transform the object to the key-value
  pair expected by the Batch Sink's output format. If you don't override this method, the
  incoming object is set as the key and the value is set to null.

Example::

  /**
   * Batch Sink that writes to a FileSet in text format.
   * Each record will be written as a single line, with record fields separated by a configurable separator.
   *
   * StructuredRecord is the first parameter because that is the input to the sink.
   * The second and third parameters are the key and value expected by Hadoop's {@link TextOutputFormat}.
   */
  @Plugin(type = BatchSink.PLUGIN_TYPE)
  @Name(TextFileSetSink.NAME)
  @Description("Writes to a FileSet in text format.")
  public class TextFileSetSink extends BatchSink<StructuredRecord, NullWritable, Text> {
    public static final String NAME = "TextFileSet";
    private final Conf config;

    /**
     * Config properties for the plugin.
     */
    public static class Conf extends PluginConfig {
      public static final String FILESET_NAME = "fileSetName";
      public static final String FIELD_SEPARATOR = "fieldSeparator";

      // The name annotation tells CDAP what the property name is. It is optional, and defaults to the variable name.
      // Note: only primitives (including boxed types) and string are the types that are supported.
      @Name(FILESET_NAME)
      @Description("The name of the FileSet to read from.")
      private String fileSetName;

      @Nullable
      @Name(FIELD_SEPARATOR)
      @Description("The separator to use to join input record fields together. Defaults to ','.")
      private String fieldSeparator;

      // Use a no-args constructor to set field defaults.
      public Conf() {
        fileSetName = "";
        fieldSeparator = ",";
      }
    }

    // CDAP will pass in a config with its fields populated based on the configuration given when creating the pipeline.
    public TextFileSetSink(Conf config) {
      this.config = config;
    }

    // configurePipeline is called exactly once when the pipeline is being created.
    // Any static configuration should be performed here.
    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
      // create the FileSet here.
      pipelineConfigurer.createDataset(config.fileSetName,
                                       FileSet.class,
                                       FileSetProperties.builder()
                                         .setInputFormat(TextInputFormat.class)
                                         .setOutputFormat(TextOutputFormat.class)
                                         .setEnableExploreOnCreate(true)
                                         .setExploreFormat("text")
                                         .setExploreSchema("text string")
                                         .build()
      );
    }

    // prepareRun is called before every pipeline run, and is used to configure what the input should be,
    // as well as any arguments the input should use. It is called by the client that is submitting the batch job.
    @Override
    public void prepareRun(BatchSinkContext context) throws Exception {
      context.addOutput(Output.ofDataset(config.fileSetName));
    }

    @Override
    public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Text>> emitter) throws Exception {
      StringBuilder joinedFields = new StringBuilder();
      Iterator<Schema.Field> fieldIter = input.getSchema().getFields().iterator();
      if (!fieldIter.hasNext()) {
        // shouldn't happen
        return;
      }

      Object val = input.get(fieldIter.next().getName());
      if (val != null) {
        joinedFields.append(val);
      }
      while (fieldIter.hasNext()) {
        String fieldName = fieldIter.next().getName();
        joinedFields.append(config.fieldSeparator);
        val = input.get(fieldName);
        if (val != null) {
          joinedFields.append(val);
        }
      }
      emitter.emit(new KeyValue<>(NullWritable.get(), new Text(joinedFields.toString())));
    }

  }

.. highlight:: java

Creating a Real-Time Source
===========================
The only method that needs to be implemented is::

  poll()

.. rubric:: Methods

- ``initialize()``: Initialize the real-time source runtime. Guaranteed to be executed
  before any call to the poll method. Usually used to setup the connection to external
  sources.
- ``configurePipeline()``: Used to create any streams or datasets or perform any validation
  on the application configuration that are required by this plugin.
- ``poll()``: Poll method will be invoked during the run of the plugin and in each call,
  the source is expected to emit zero or more objects for the next stage to process.
- ``destroy()``: Cleanup method executed during the shutdown of the Source. Could be used
  to tear down any external connections made during the initialize method.

Example::

  /**
   * Real-Time Source to poll data from external sources.
   */
  @Plugin(type = "realtimesource")
  @Name("Source")
  @Description("Real-Time Source")
  public class Source extends RealtimeSource<StructuredRecord> {

    private final SourceConfig config;

    public Source(SourceConfig config) {
      this.config = config;
    }

    /**
     * Config class for Source.
     */
    public static class SourceConfig extends PluginConfig {

      @Name("param")
      @Description("Source Param")
      private String param;
      // Note: only primitives (included boxed types) and string are the types that are supported.

    }

    @Nullable
    @Override
    public SourceState poll(Emitter<StructuredRecord> writer, SourceState currentState) {
      // Poll for new data
      // Write structured record to the writer
      // writer.emit(writeDefaultRecords(writer);
      return currentState;
    }

    @Override
    public void initialize(RealtimeContext context) throws Exception {
      super.initialize(context);
      // Get Config param and use to initialize
      // String param = config.param
      // Perform init operations, external operations etc.
    }

    @Override
    public void destroy() {
      super.destroy();
      // Handle destroy lifecycle
    }

    private void writeDefaultRecords(Emitter<StructuredRecord> writer){
      Schema.Field bodyField = Schema.Field.of("body", Schema.of(Schema.Type.STRING));
      StructuredRecord.Builder recordBuilder = StructuredRecord.builder(Schema.recordOf("defaultRecord", bodyField));
      recordBuilder.set("body", "Hello");
      writer.emit(recordBuilder.build());
    }
  }


.. highlight:: java

Creating a Real-Time Sink
=========================
The only method that needs to be implemented is::

  write()

.. rubric:: Methods

- ``initialize()``: Initialize the real-time sink runtime. Guaranteed to be executed before
  any call to the ``write`` method.
- ``configurePipeline()``: Used to create any datasets or perform any validation
  on the application configuration that are required by this plugin.
- ``write()``: The write method will be invoked for a set of objects that needs to be
  persisted. A ``DataWriter`` object can be used to write data to CDAP streams and/or datasets.
  The method is expected to return the number of objects written; this is used for collecting
  metrics.
- ``destroy()``: Cleanup method executed during the shutdown of the Sink.

Example::

  @Plugin(type = "realtimesink")
  @Name("Demo")
  @Description("Demo Real-Time Sink")
  public class DemoSink extends RealtimeSink<String> {

    @Override
    public int write(Iterable<String> objects, DataWriter dataWriter) {
      int written = 0;
      for (String object : objects) {
        written += 1;
        . . .
      }
      return written;
    }
  }

.. highlight:: java

Creating a Transformation
=========================
The only method that needs to be implemented is::

  transform()

.. rubric:: Methods

- ``initialize()``: Used to perform any initialization step that might be required during
  the runtime of the ``Transform``. It is guaranteed that this method will be invoked
  before the ``transform`` method.
- ``transform()``: This method contains the logic that will be applied on each
  incoming data object. An emitter can be used to pass the results to the subsequent stage
  (which could be either another Transformation or a Sink).
- ``destroy()``: Used to perform any cleanup before the plugin shuts down.

Below is an example of a ``DuplicateTransform`` that emits copies of the incoming record
based on the value in the record. In addition, a user metric indicating the number of
copies in each transform is emitted. The user metrics can be queried by using the CDAP
:ref:`Metrics HTTP RESTful API <http-restful-api-metrics>`::

  @Plugin(type = "transform")
  @Name("Duplicator")
  @Description("Transformation example that makes copies.")

  public class DuplicateTransform extends Transform<StructuredRecord, StructuredRecord> {

  private final Config config;

    public static final class Config extends PluginConfig {

      @Name("count")
      @Description("Field that indicates number of copies to make.")
      private String fieldName;
    }

    @Override
    public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
      int copies = input.get(config.fieldName);
      for (int i = 0; i < copies; i++) {
        emitter.emit(input);
      }
      getContext().getMetrics().count("copies", copies);
    }

    @Override
    public void destroy() {

    }
  }

.. highlight:: java

Script Transformations
----------------------
In the script transformations (*JavaScriptTransform*, *PythonEvaluator*, *ScriptFilterTransform*, and the *ValidatorTransform*), a
``ScriptContext`` object is passed to the ``transform()`` method::

  function transform(input, context);

The different Transforms that are passed this context object have similar signatures:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Transform
     - Signature
   * - ``JavaScriptTransform``
     - ``{{function transform(input, emitter, context)}}``
   * - ``PythonEvaluator``
     - ``{{function transform(input, emitter, context)}}``
   * - ``ScriptFilterTransform``
     - ``{{function shouldFilter(input, context)}}``
   * - ``ValidatorTransform``
     - ``{{function isValid(input, context)}}``

The ``ScriptContext`` has these methods::

  public Logger getLogger();
  public StageMetrics getMetrics();
  public ScriptLookup getLookup(String table);
  
The context passed by the *ValidatorTransform* has an additional method that returns a validator::

  public Object getValidator(String validatorName);

These methods allow access within the script to CDAP loggers, metrics, lookup tables, and the validator object.

**Logger**

``Logger`` is an `org.slf4j.Logger <http://www.slf4j.org/api/org/slf4j/Logger.html>`__.

**StageMetrics**

``StageMetrics`` has these methods:

- ``count(String metricName, int delta)``: Increases the value of the specific metric by delta. Metrics name will be prefixed by the
  stage ID, hence it will be aggregated for the current stage.
- ``gauge(String metricName, long value)``: Sets the specific metric to the provided value. Metrics name will be prefixed by the
  stage ID, hence it will be aggregated for the current stage.
- ``pipelineCount(String metricName, int delta)``: Increases the value of the specific metric by delta. Metrics emitted will be aggregated
  for the entire pipeline.
- ``pipelineGauge(String metricName, long value)``: Sets the specific metric to the provided value. Metrics emitted will be aggregated
  for the entire pipeline.

**ScriptLookup**

Currently, ``ScriptContext.getLookup(String table)`` only supports :ref:`key-value tables <datasets-index>`.

For example, if a lookup table *purchases* is configured, then you will be able to perform
operations with that lookup table in your script: ``context.getLookup('purchases').lookup('key')``

**Validator Object**

.. highlight:: javascript

For example, in a validator transform, you can retrieve the validator object and call its
functions as part of your JavaScript::

  var coreValidator = context.getValidator("coreValidator");
  if (!coreValidator.isDate(input.date)) {
  . . .

Creating a Batch Aggregator
===========================
In order to implement a Batch Aggregator (to be used in the Data Pipeline artifact), you extend the
``BatchAggregator`` class. Unlike a ``Transform``, which operates on a single record at a time, a
``BatchAggregator`` operates on a collection of records. An aggregation takes place in two steps:
*groupBy* and then *aggregate*. In the *groupBy* step, the aggregator creates zero or more group keys for each
input record. Before the *aggregate step occurs, Hydrator will take all records that have the same
group key, and collect them into a group. If a record does not have any of the group keys, it is filtered out.
If a record has multiple group keys, it will belong to multiple groups. The *aggregate* step is then
called. In this step, the plugin receives group keys and all records that had that group key.
It is then left to the plugin to decide what to do with each of the groups.

.. highlight:: java

.. rubric:: Methods

- ``configurePipeline()``: Used to create any streams or datasets or perform any validation
  on the application configuration that are required by this plugin.
- ``initialize()``: Initialize the Batch Aggregator. Guaranteed to be executed before any call
  to the plugin’s ``groupBy`` or ``aggregate`` methods. This is called by each executor of the job.
  For example, if the MapReduce engine is being used, each mapper will call this method.
- ``destroy()``: Destroy any resources created by ``initialize``. Guaranteed to be executed after all calls
  to the plugin’s ``groupBy`` or ``aggregate`` methods have been made. This is called by each executor of the job.
  For example, if the MapReduce engine is being used, each mapper will call this method.
- ``groupBy()``: This method will be called for every object that is received from the
  previous stage. This method returns zero or more group keys for each object it recieves.
  Objects with the same group key will be grouped together for the ``aggregate`` method.
- ``aggregate()``: The method is called after every object has been assigned their group keys.
  This method is called once for each group key emitted by the ``groupBy`` method.
  The method recieves a group key as well as an iterator over all objects that had that group key.
  Objects emitted in this method are the output for this stage. 

Example::

  /**
   * Aggregator that counts how many times each word appears in records input to the aggregator.
   */
  @Plugin(type = BatchAggregator.PLUGIN_TYPE)
  @Name(WordCountAggregator.NAME)
  @Description("Counts how many times each word appears in all records input to the aggregator.")
  public class WordCountAggregator extends BatchAggregator<String, StructuredRecord, StructuredRecord> {
    public static final String NAME = "WordCount";
    public static final Schema OUTPUT_SCHEMA = Schema.recordOf(
      "wordCount",
      Schema.Field.of("word", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("count", Schema.of(Schema.Type.LONG))
    );
    private static final Pattern WHITESPACE = Pattern.compile("\\s");
    private final Conf config;

    /**
     * Config properties for the plugin.
     */
    public static class Conf extends PluginConfig {
      @Description("The field from the input records containing the words to count.")
      private String field;
    }

    public WordCountAggregator(Conf config) {
      this.config = config;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
      // Any static configuration validation should happen here.
      // We will check that the field is in the input schema and is of type string.
      Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
      // A null input schema means it is unknown until runtime, or it is not constant.
      if (inputSchema != null) {
        // If the input schema is constant and known at configure time, check that the input field exists and is a string.
        Schema.Field inputField = inputSchema.getField(config.field);
        if (inputField == null) {
          throw new IllegalArgumentException(
            String.format("Field '%s' does not exist in input schema %s.", config.field, inputSchema));
        }
        Schema fieldSchema = inputField.getSchema();
        Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
        if (fieldType != Schema.Type.STRING) {
          throw new IllegalArgumentException(
            String.format("Field '%s' is of illegal type %s. Must be of type %s.",
                          config.field, fieldType, Schema.Type.STRING));
        }
      }
      // Set the output schema so downstream stages will know their input schema.
      pipelineConfigurer.getStageConfigurer().setOutputSchema(OUTPUT_SCHEMA);
    }

    @Override
    public void groupBy(StructuredRecord input, Emitter<String> groupKeyEmitter) throws Exception {
      String val = input.get(config.field);
      if (val == null) {
        return;
      }

      for (String word : WHITESPACE.split(val)) {
        groupKeyEmitter.emit(word);
      }
    }

    @Override
    public void aggregate(String groupKey, Iterator<StructuredRecord> groupValues,
                          Emitter<StructuredRecord> emitter) throws Exception {
      long count = 0;
      while (groupValues.hasNext()) {
        groupValues.next();
        count++;
      }
      emitter.emit(StructuredRecord.builder(OUTPUT_SCHEMA).set("word", groupKey).set("count", count).build());
    }
  }

Creating a SparkCompute Plugin
==============================
In order to implement a SparkCompute Plugin (to be used in the Data Pipeline artifact), you extend the
``SparkCompute`` class. A ``SparkCompute`` plugin is similar to a ``Transform``, except instead of
transforming its input record by record, it transforms an entire collection of records into another
collection of records. In a ``SparkCompute`` plugin, you are given access to anything you would be
able to do in a Spark program. 

.. highlight:: java

.. rubric:: Methods

- ``configurePipeline()``: Used to create any streams or datasets or perform any validation
  on the application configuration that are required by this plugin.
- ``transform()``: This method is given a Spark RDD (Resilient Distributed Dataset) containing 
  every object that is received from the previous stage. This method then performs Spark operations
  on the input to transform it into an output RDD that will be sent to the next stage.

Example::

  /**
   * SparkCompute plugin that counts how many times each word appears in records input to the compute stage.
   */
  @Plugin(type = SparkCompute.PLUGIN_TYPE)
  @Name(WordCountCompute.NAME)
  @Description("Counts how many times each word appears in all records input to the aggregator.")
  public class WordCountCompute extends SparkCompute<StructuredRecord, StructuredRecord> {
    public static final String NAME = "WordCount";
    public static final Schema OUTPUT_SCHEMA = Schema.recordOf(
      "wordCount",
      Schema.Field.of("word", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("count", Schema.of(Schema.Type.LONG))
    );
    private final Conf config;

    /**
     * Config properties for the plugin.
     */
    public static class Conf extends PluginConfig {
      @Description("The field from the input records containing the words to count.")
      private String field;
    }

    public WordCountCompute(Conf config) {
      this.config = config;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
      // Any static configuration validation should happen here.
      // We will check that the field is in the input schema and is of type string.
      Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
      if (inputSchema != null) {
        WordCount wordCount = new WordCount(config.field);
        wordCount.validateSchema(inputSchema);
      }
      // Set the output schema so downstream stages will know their input schema.
      pipelineConfigurer.getStageConfigurer().setOutputSchema(OUTPUT_SCHEMA);
    }

    @Override
    public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext sparkExecutionPluginContext,
                                               JavaRDD<StructuredRecord> javaRDD) throws Exception {
      WordCount wordCount = new WordCount(config.field);
      return wordCount.countWords(javaRDD)
        .flatMap(new FlatMapFunction<Tuple2<String, Long>, StructuredRecord>() {
          @Override
          public Iterable<StructuredRecord> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
            List<StructuredRecord> output = new ArrayList<>();
            output.add(StructuredRecord.builder(OUTPUT_SCHEMA)
                         .set("word", stringLongTuple2._1())
                         .set("count", stringLongTuple2._2())
                         .build());
            return output;
          }
        });
    }
  }

Creating a Spark Sink
=====================
In order to implement a SparkSink Plugin (to be used in the Data Pipeline artifact), you extend the
``SparkSink`` class. A ``SparkSink`` is like a ``SparkCompute`` plugin except that it has no
output. This means other plugins cannot be connected to it. In this way, it is similar to a
``BatchSink``. In a ``SparkSink``, you are given access to anything you would be able to do in a Spark program. 
For example, one common use case is to train a machine-learning model in this plugin.

.. highlight:: java

.. rubric:: Methods

- ``configurePipeline()``: Used to create any streams or datasets or perform any validation
  on the application configuration that are required by this plugin.
- ``run()``: This method is given a Spark RDD (Resilient Distributed Dataset) containing every 
  object that is received from the previous stage. This method then performs Spark operations
  on the input, and usually saves the result to a dataset.

Example::

  /**
   * SparkSink plugin that counts how many times each word appears in records input to it
   * and stores the result in a KeyValueTable.
   */
  @Plugin(type = SparkSink.PLUGIN_TYPE)
  @Name(WordCountSink.NAME)
  @Description("Counts how many times each word appears in all records input to the aggregator.")
  public class WordCountSink extends SparkSink<StructuredRecord> {
    public static final String NAME = "WordCount";
    private final Conf config;

    /**
     * Config properties for the plugin.
     */
    public static class Conf extends PluginConfig {
      @Description("The field from the input records containing the words to count.")
      private String field;

      @Description("The name of the KeyValueTable to write to.")
      private String tableName;
    }

    public WordCountSink(Conf config) {
      this.config = config;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
      // Any static configuration validation should happen here.
      // We will check that the field is in the input schema and is of type string.
      Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
      if (inputSchema != null) {
        WordCount wordCount = new WordCount(config.field);
        wordCount.validateSchema(inputSchema);
      }
      pipelineConfigurer.createDataset(config.tableName, KeyValueTable.class, DatasetProperties.EMPTY);
    }

    @Override
    public void run(SparkExecutionPluginContext sparkExecutionPluginContext,
                    JavaRDD<StructuredRecord> javaRDD) throws Exception {
      WordCount wordCount = new WordCount(config.field);
      JavaPairRDD outputRDD = wordCount.countWords(javaRDD)
        .mapToPair(new PairFunction<Tuple2<String, Long>, byte[], byte[]>() {
          @Override
          public Tuple2<byte[], byte[]> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
            return new Tuple2<>(Bytes.toBytes(stringLongTuple2._1()), Bytes.toBytes(stringLongTuple2._2()));
          }
        });
      sparkExecutionPluginContext.saveAsDataset(outputRDD, config.tableName);
    }
  }

.. highlight:: java

Test Framework for Plugins
==========================

.. include:: ../../../developers-manual/source/testing/testing.rst
   :start-after: .. _test-framework-strategies-artifacts:
   :end-before:  .. _test-framework-validating-sql:

Additional information on unit testing with CDAP is in the Developers’ Manual section
on :ref:`Testing a CDAP Application <test-framework>`.

.. highlight:: xml

In addition, CDAP provides a ``hydrator-test`` module that contains several mock plugins
for you to use in tests with your custom plugins. To use the module, add a dependency to
your ``pom.xml``::

    <dependency>
      <groupId>co.cask.cdap</groupId>
      <artifactId>hydrator-test</artifactId>
      <version>${cdap.version}</version>
      <scope>test</scope>
    </dependency>

.. highlight:: java

Then extend the ``HydratorTestBase`` class, and create a method that will setup up the
application artifact and mock plugins, as well as the artifact containing your custom plugins::

  /**
   * Unit tests for our plugins.
   */
  public class PipelineTest extends HydratorTestBase {
    private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");
    @ClassRule
    public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

    @BeforeClass
    public static void setupTestClass() throws Exception {
      ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

      // Add the data pipeline artifact and mock plugins.
      setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

      // Add our plugins artifact with the data pipeline artifact as its parent.
      // This will make our plugins available to the data pipeline.
      addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                        parentArtifact,
                        TextFileSetSource.class,
                        TextFileSetSink.class,
                        WordCountAggregator.class,
                        WordCountCompute.class,
                        WordCountSink.class);
    }

You can then add test cases as you see fit. The ``cdap-data-pipeline-plugins-archetype``
includes an example of this unit test.

.. highlight:: java

Source State in a Real-Time Source
==================================
Real-time plugins are executed in workers. During failure, there is the possibility that
the data that is emitted from the Source will not be processed by subsequent stages. In
order to avoid such data loss, SourceState can be used to persist the information about
the external source (for example, offset) if supported by the Source.

In case of failure, when the poll method is invoked, the offset last persisted is passed
to the poll method, which can be used to fetch the data from the last processed point. The
updated Source State information is returned by the poll method. After the data is
processed by any Transformations and then finally persisted by the Sink, the new Source
State information is also persisted. This ensures that there will be no data loss in case
of failures.

::

  @Plugin(type = "realtimesource")
  @Name("Demo")
  @Description("Demo Real-Time Source")
  public class DemoSource extends RealtimeSource<String> {
    private static final Logger LOG = LoggerFactory.getLogger(TestSource.class);
    private static final String COUNT = "count";

    @Nullable
    @Override
    public SourceState poll(Emitter<String> writer, SourceState currentState) {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        LOG.error("Some Error in Source");
      }

      int prevCount;
      if (currentState.getState(COUNT) != null) {
        prevCount = Bytes.toInt(currentState.getState(COUNT));
        prevCount++;
        currentState.setState(COUNT, Bytes.toBytes(prevCount));
      } else {
        prevCount = 1;
        currentState = new SourceState();
        currentState.setState(COUNT, Bytes.toBytes(prevCount));
      }

      LOG.info("Emitting data! {}", prevCount);
      writer.emit("Hello World!");
      return currentState;
    }
  }


.. _cdap-apps-custom-etl-plugins-plugin-packaging:

Plugin Packaging and Deployment
===============================
To package and deploy your plugin, follow these instructions on `plugin packaging <#plugin-packaging>`__,
`deployment <#deploying-a-system-artifact>`__ and `verification <#deployment-verification>`__.

To control how your plugin appears in the CDAP UI, include an appropriate :ref:`plugin
widget JSON file <cdap-apps-custom-widget-json>`, as described below.

By using one of the ``etl-plugin`` Maven archetypes, your project will be set up to generate
the required JAR manifest. If you move the plugin class to a different Java package after
the project is created, you will need to modify the configuration of the
``maven-bundle-plugin`` in the ``pom.xml`` file to reflect the package name changes.

If you are developing plugins for the ``cdap-etl-batch`` artifact, be aware that for
classes inside the plugin JAR that you have added to the Hadoop Job configuration directly
(for example, your custom ``InputFormat`` class), you will need to add the Java packages
of those classes to the "Export-Package" as well. This is to ensure those classes are
visible to the Hadoop MapReduce framework during the plugin execution. Otherwise, the
execution will typically fail with a ``ClassNotFoundException``.

Plugin Deployment
-----------------

.. include:: ../../../developers-manual/source/building-blocks/plugins.rst
   :start-after: .. _plugins-deployment-artifact:
   :end-before:  .. _plugins-use-case:

.. _cdap-apps-custom-widget-json:

Plugin Widget JSON
-------------------------
When a plugin is displayed in the CDAP UI, its properties are represented
by widgets in the :ref:`Cask Hydrator <cdap-apps-cask-hydrator>`. Each property of a
plugin is represented, by default, as a textbox in the user interface.

To customize the plugin display, a plugin can include a widget JSON file that
specifies the particular widgets and sets of widget attributes used to display the plugin
properties in the CDAP UI.

The widget JSON is composed of two lists:

- a list of property configuration groups and
- a list of output properties.

.. highlight:: json-ellipsis  

For example::

  {
    "configuration-groups": [
      {"group-1"},
      {"group-2"},
      ...
    ],
    "outputs": [
      {"ouput-property-1"},
      {"ouput-property-2"},
    ]
  }

.. highlight:: json

Configuration Groups
....................
Configuration groups are a simple grouping of properties of a plugin. A configuration
group is represented as a JSON object with a label and a list of plugin properties for that
group.

For example, in a *Batch Source* plugin, properties such as *Dataset Name*, *Dataset Base
Path*, *Duration*, and *Delay* can be grouped as the *Batch Source Configuration*.

In the case of the *Batch Source* plugin, it could look like this::

  {
    "configuration-groups": [
      {
        "label": "Batch Source Configuration",
        "properties": [
          {"field1"},
          {"field2"},
          {"field3"}
        ]
      }
    ],
    "outputs": [
      {"output-property1"},
      {"output-property2"}
    ]
  }

Once a group is established, we can configure how each of the properties inside the group is
represented in the CDAP UI.

The configuration of each property of the plugin is composed of:

- :ref:`widget-type: <custom-widgets>` The type of widget needed to represent this property.
- **label:** Label to be used in the CDAP UI for the property.
- **name:** Name of the field (as supplied by the CDAP UI backend).
- **widget-attributes:** A map of attributes that the widget type requires be defined in
  order to render the property in the CDAP UI. The attributes vary depending on the
  widget type.
- :ref:`plugin-function: <custom-plugin-function>` An optional map of plugin method and its widget attributes that can be
  applied to a particular plugin property.

Note that with the exception of the value of the *label*, all property values are case-sensitive.

To find the available field names, you can use the Artifact HTTP RESTful API to :ref:`retrieve plugin details <http-restful-api-artifact-plugin-detail>` for an artifact, which
will include all the available names. (If the artifact is your own, you will already know the
available field names.)

In the case of our *Batch Source* plugin example, the ``configuration-groups`` can be represented by::

  {
    "configuration-groups": [
      {
        "label": "Batch Source",
        "properties": [
          {
            "widget-type": "dataset-selector",
            "label": "Dataset Name",
            "name": "name"
          },
          {
            "widget-type": "textbox",
            "label": "Dataset Base Path",
            "name": "basePath"
          },
          {
            "widget-type": "textbox",
            "label": "Group By Fields",
            "name": "groupByFields",
            "plugin-function": {
              "method": "POST",
              "widget": "outputSchema",
              "output-property": "schema",
              "plugin-method": "outputSchema",
              "required-fields": ["groupByFields", "aggregates"],
              "missing-required-fields-message":
                "'Group By Fields' & 'Aggregates' properties are required to fetch schema."
            }
          },
          {
            "widget-type": "textbox",
            "label": "Delay",
            "name": "delay"
          }
        ]
      }
    ]
  }
  
.. _custom-widgets:

Widgets
.......

A widget in the CDAP UI represents a component that will be rendered and used to set a
value of a property of a plugin. These are the different widgets |---| their type, a
description, their attributes (if any), and their output data type |---| that we support in
Cask Hydrator as of version |version|:

.. list-table::
   :widths: 20 25 25 25
   :header-rows: 1

   * - Widget Type
     - Description
     - Attributes
     - Output Data Type
   * - ``textbox``
     - Default HTML textbox, used to enter any string
     - ``default``: default value for the widget
     - ``string``
   * - ``number``
     - Default HTML number textbox that only accepts valid numbers
     - | ``default``: default value for the widget
       | ``min``: minimum value for the number box
       | ``max``: maximum value for the number box
     - string
   * - ``passwordbox``
     - Default HTML password entry box
     - No attributes
     - string
   * - csv
     - Comma-separated values; each value is entered in a separate box
     - No attributes
     - comma-separated string
   * - dsv
     - Delimiter-separated values; each value is entered in a separate box
     - ``delimiter``: delimiter used to separate the values
     - delimiter-separated string
   * - ``json-editor``
     - JSON editor that pretty-prints and auto-formats JSON while it is being entered
     - ``default``: default serialized JSON value for the widget
     - string
   * - ``javascript-editor``, ``python-editor``
     - An editor to write JavaScript (``javascript-editor``) or Python (``python-editor``)
       code as a value for a property
     - ``default``: default string value for the widget
     - string
   * - ``keyvalue``
     - A key-value editor for constructing maps of key-value pairs
     - | ``delimiter``: delimiter for the key-value pairs
       | ``kv-delimiter``: delimiter between key and value
     - string
   * - ``keyvalue-dropdown``
     - Similar to *keyvalue* widget, but with a drop-down value list
     - | ``delimiter``: delimiter for the key-value pairs
       | ``kv-delimiter``: delimiter between key and value
       | ``dropdownOptions``: list of drop-down options to display
     - string
   * - ``select``
     - An HTML drop-down with a list of values; allows one choice from the list
     - | ``values``: list of values for the drop-down
       | ``default``: default value from the list
     - string
   * - ``dataset-selector``, ``stream-selector``
     - A type-ahead textbox with a list of datasets (``dataset-selector``) or streams
       (``stream-selector``) from the CDAP instance
     - No attributes
     - string
   * - ``schema``
     - A four-column, editable table to represent a schema of a plugin
     - | ``schema-types``: list of schema types for each field from which the user can chose when setting the schema
       | ``schema-default-type``: default type for each newly-added field in the schema
     - string
   * - ``non-editable-schema-editor``
     - A non-editable widget for displaying a schema
     - ``schema``: schema that will be used as the output schema for the plugin
     - string
   * - ``ds-multiplevalues``
     - A delimiter-separated values widget that allows specifying lists of values separated by delimiters
     - | ``numValues``: number of values (number of delimiter-separated values)
       | ``values-delimiter``: the delimiter between each value
       | ``delimiter``: the delimiter between each *set* of values
       | ``placeholders``: array of placeholders for each value's textbox
     - string

.. _custom-plugin-function:

Plugin Function
...............

Plugin functions are methods exposed by a particular plugin that can be used to fetch output schema for a plugin.
These are the fields that need  to be configured to use the plugin functions in the CDAP UI:

- **method:** Type of request to make when calling the plugin function from the CDAP UI (for instance GET or POST).
- **widget:** Type of widget to use to import output schema.
- **output-property:** Property to update once the CDAP UI receives the data from the plugin method.
- **plugin-method:** Name of the plugin method to call (as exposed by the plugin).
- **required-fields:** Fields required to call the plugin method.
- **missing-required-fields-message:** A message for the user as to why the action is disabled in the CDAP UI, when the required fields are missing values.

The last two properties are solely for the the CDAP UI and are not required all the time. However, the first four fields are required fields to use
a plugin method in the CDAP UI. In the case of a plugin function, if the widget is not supported in the CDAP UI or the plugin function map is not supplied, the user will not see it in the CDAP UI.

Outputs
.......
The *outputs* is a list of plugin properties that represent the output schema of a particular plugin.

The output schema for a plugin can be represented in two different ways, either:

- via an *implicit* schema; or
- via an *explicit* ``Schema`` property.

An **implicit** schema is a pre-determined output schema for a plugin that the plugin developer
enforces. The implicit schema is not associated with any properties of the plugin, but
just enforces the output schema of the plugin, for visual display purposes. An example of
this is the `Twitter real-time source plugin
<https://github.com/caskdata/hydrator-plugins/blob/release/1.2/core-plugins/widgets/Twitter-realtimesource.json>`__.

An **explicit** ``Schema`` property is one that can be defined as the output schema and can be
appropriately configured to make it editable through the CDAP UI.

Output properties are configured in a similar manner as individual properties in configuration
groups. They are composed of a name and a widget type, one of either ``schema`` or
``non-editable-schema-editor``. With the ``schema`` widget type, a list of widget attributes can be included;
with ``non-editable-schema-editor``, a schema is added instead.

For example:

The output properties of the Twitter real-time source, with an explicit, non-editable schema property,
composed of the fields *id*, *time*, *favCount*, *rtCount*, *geoLat*, *geoLong*, and *isRetweet*::

  {
    "outputs": [
      {
        "widget-type": "non-editable-schema-editor",
        "schema": {
          "id": "long",
          "message": "string",
          "lang": [
            "string",
            "null"
          ],
          "time": [
            "long",
            "null"
          ],
          "favCount": "int",
          "rtCount": "int",
          "source": [
            "string",
            "null"
          ],
          "geoLat": [
            "double",
            "null"
          ],
          "geoLong": [
            "double",
            "null"
          ],
          "isRetweet": "boolean"
        }
      }
    ]
  }

In contrast, our "Batch Source" plugin could have a configurable output schema::

  {
    "outputs": [
      {
        "name": "schema",
        "widget-type": "schema",
        "widget-attributes": {
          "schema-types": [
            "boolean",
            "int",
            "long",
            "float",
            "double",
            "string",
            "map<string, string>"
          ],
          "schema-default-type": "string"
        }
      }
    ]
  }

Widget types for output properties are limited to ensure that the schema that is propagated across
different plugins in the CDAP UI is consistent.

Example Widget JSON
..........................
Based on the above definitions, we could write the complete widget JSON for our *Batch Source* plugin
(with the properties of *name*, *basePath*, *duration*, *delay*, and an output *schema*) as::

  {
    "metadata": {
      "spec-version": "1.0"
    },
    "configuration-groups": [
      {
        "label": "Batch Source",
        "properties": [
          {
            "widget-type": "dataset-selector",
            "label": "Dataset Name",
            "name": "name"
          },
          {
            "widget-type": "textbox",
            "label": "Dataset Base Path",
            "name": "basePath"
          },
          {
            "widget-type": "textbox",
            "Label": "Group By Fields",
            "name": "groupByFields",
            "plugin-function": {
              "method": "POST",
              "widget": "outputSchema",
              "output-property": "schema",
              "plugin-method": "outputSchema",
              "required-fields": ["groupByFields", "aggregates"],
              "missing-required-fields-message":
                "'Group By Fields' & 'Aggregates' properties are required to fetch schema."
            }
          },
          {
            "widget-type": "textbox",
            "label": "Delay",
            "name": "delay"
          }
        ]
      }
    ],
    "outputs": [
      {
        "name": "schema",
        "widget-type": "schema",
        "widget-attributes": {
          "schema-types": [
            "boolean",
            "int",
            "long",
            "float",
            "double",
            "string",
            "map<string, string>"
          ],
          "schema-default-type": "string"
        }
      }
    ]
  }
