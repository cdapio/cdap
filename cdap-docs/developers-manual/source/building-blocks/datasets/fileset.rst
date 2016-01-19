.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _datasets-fileset:

===============
FileSet Dataset
===============

.. highlight:: java

While real-time programs such as flows normally require datasets with random access, batch-oriented
programming paradigms such as MapReduce are more suitable for data that can be read and written sequentially.
The most prominent form of such data is an HDFS file, and MapReduce is highly optimized for such files.
CDAP's abstraction for files is the *FileSet* dataset.

A *FileSet* represents a set of files on the file system that share certain properties:

- The location in the file system. All files in a FileSet are located relative to a
  base path, which is created when the FileSet is created. Deleting the
  FileSet will also delete this directory and all the files it contains.
- The Hadoop input and output format. They are given as dataset properties by their
  class names.  When a FileSet is used as the input or output of a MapReduce program,
  these classes are injected into the Hadoop configuration by the CDAP runtime
  system.
- Additional properties of the specified input and output format. Each format has its own 
  properties; consult the format's documentation for details. For example, the
  ``TextOutputFormat`` allows configuring the field separator character by setting the
  property ``mapreduce.output.textoutputformat.separator``. These properties are also set
  into the Hadoop configuration by the CDAP runtime system.

These properties are configured at the time the FileSet is created. They apply to all
files in the dataset. Every time you use a FileSet in your application code, you can
address either the entire dataset or, by specifying its relative path as a runtime argument,
an individual file in the dataset. Specifying an individual file is only supported for
MapReduce programs.

Creating a FileSet
==================

To create and use a FileSet in an application, you create it as part of the application configuration::

  public class FileSetExample extends AbstractApplication {

    @Override
    public void configure() {
      ...
      createDataset("lines", FileSet.class, FileSetProperties.builder()
        .setBasePath("example/data/lines")
        .setInputFormat(TextInputFormat.class)
        .setOutputFormat(TextOutputFormat.class).build());
        .setOutputProperty(TextOutputFormat.SEPERATOR, ":")
      ...
    }

This creates a new FileSet named *lines* that uses ``TextInputFormat`` and ``TextOutputFormat.``
For the output format, we specify an additional property to make it use a colon as the separator
between the key and the value in each line of output.

Input and output formats must be implementations of the standard Apache Hadoop
`InputFormat <https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/InputFormat.html>`_
and
`OutputFormat <https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/OutputFormat.html>`_
specifications.

If you do not specify a base path, the dataset framework will generate a path based on the dataset name.
This path |---| and any relative base path you specify |---| is relative to the data directory of the CDAP namespace
in which the FileSet is created. You can also specify an absolute base path (one that begins with the character ``/``).
This path is interpreted as an absolute path in the file system. Beware that if you create two FileSets with the
same base path |---| be it multiple FileSets in the same namespace with the same relative base path, or in different
namespaces with the same absolute base path |---| then these multiple FileSets will use the same directory and possibly
obstruct each other's operations.

You can configure a FileSet as "external". This means that the data (the actual files) in
the FileSet are managed by an external process. This allows you to use FileSets with
existing locations outside of CDAP. In that case, the FileSet will not allow the writing
or deleting of files: it treats the contents of the base path as read-only::

      createDataset("lines", FileSet.class, FileSetProperties.builder()
        .setBasePath("/existing/path")
        .setDataExternal(true)
        .setInputFormat(TextInputFormat.class)
        ...

If you do not specify an input format, you will not be able to use this as the input for a
MapReduce program; similarly for the output format.


Using a FileSet in MapReduce
============================

Using a FileSet as input or output of a MapReduce program is the same as for any other dataset::

  public class WordCount extends AbstractMapReduce {

    @Override
    public void configure() {
      setInputDataset("lines");
      setOutputDataset("counts");
    }
    ...

The MapReduce program only needs to specify the names of the input and output datasets.
Whether they are FileSets or another type of dataset is handled by the CDAP runtime system.

However, you do need to tell CDAP the relative paths of the input and output files. Currently,
this is only possible by specifying them as runtime arguments when the MapReduce program is started::

  curl -v <base-url>/apps/FileSetExample/mapreduce/WordCount/start -d '{ \
      "dataset.lines.input.paths":  "monday/my.txt", \
      "dataset.counts.output.path": "monday/counts.out" }'

Note that for the input you can specify multiple paths separated by commas::

      "dataset.lines.input.paths":  "monday/lines.txt,tuesday/lines.txt"

If you do not specify both the input and output paths, your MapReduce program will fail with an error.

Using a FileSet Programmatically
================================

You can interact with the files of a FileSet directly, through the ``Location`` abstraction
of the file system. For example, a Service can use a FileSet by declaring it with a ``@UseDataSet``
annotation, and then obtaining a ``Location`` for a relative path within the FileSet::

    @UseDataSet("lines")
    private FileSet lines;

    @GET
    @Path("{fileSet}")
    public void read(HttpServiceRequest request, HttpServiceResponder responder,
                     @QueryParam("path") String filePath) {

      Location location = lines.getLocation(filePath);
      try {
        InputStream inputStream = location.getInputStream();
        ...
      } catch (IOException e) {
        ...
      }
    }

See the Apache™ Twill®
`API documentation <http://twill.incubator.apache.org/apidocs/org/apache/twill/filesystem/Location.html>`__
for additional information about the ``Location`` abstraction.

Exploring FileSets
==================

A file set can be explored with ad-hoc queries if you enable it at creation time;
this is described under :ref:`fileset-exploration`.

==================
PartitionedFileSet
==================

While a FileSet is a convenient abstraction over actual file system interfaces, it still requires
the application to be aware of file system paths. For example, an application that maintains data
over time might have a new file for every month. One could come up with a naming convention that encodes
the month into each file name, and share that convention across all applications that use this file set.
Yet that can become tedious to manage, especially if the naming convention should ever change |---| then all
applications would have to be changed simultaneously for proper functioning.

The PartitionedFileSet dataset relieves applications from understanding file name conventions. Instead,
it associates a partition key with a path. Because different paths cannot have the same partition key,
this allows applications to address the file(s) at that path uniquely through their partition keys, or
more broadly through conditions over the partition keys. For example, the months of February through June
of a particular year, or the month of November in any year. By inheriting the attributes |---| such as
format and schema |---| of FileSets, PartitionedFileSets are a powerful abstraction over data that is
organized into files.

Creating a PartitionedFileSet
=============================

To create and use a PartitionedFileSet in an application, you create it as part of the application
configuration, similar to FileSets. However, the partitioning has to be given as an additional property::

  public void configure() {
    ...
    createDataset("results", PartitionedFileSet.class, PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addStringField("league").addIntField("season").build())
      // Properties for file set
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ",")
      .build());
    ...
  }

This creates a new PartitionedFileSet named *results*. Similar to FileSets, it specifies ``TextInputFormat`` and
``TextOutputFormat.``; for the output format, we specify that the separator between fields is a comma.
The difference to a FileSet is that this dataset is partitioned by league and season. This means that every file
added to this dataset must have a partitioning key with a unique combination of league and season.

Note that any of the properties that apply to FileSets can also be used for PartitionedFileSets (they apply to the
embedded FileSet). If you configure a PartitionedFileSet as external using ``setDataExternal(true)``, then the
embedded FileSet becomes read-only. You can still add partitions for locations that were written by an
external process. But dropping a partition will only delete the partition's metadata, whereas the actual file
remains intact. Similarly, if you drop or truncate an external PartitionedFileSet, its files will not be deleted.

Reading and Writing PartitionedFileSets
=======================================

You can interact with the files in a PartitionedFileSet directly through the ``Location`` abstraction
of the file system. This is similar to a FileSet, but instead of a relative path, you specify a
partition key to obtain a Partition; you can then get a Location from that Partition.

For example, to read the content of a partition::

      PartitionKey key = PartitionKey.builder().addStringField("league", ...)
                                               .addIntField("season", ...)
                                               .build());
      Partition partition = dataset.getPartition(key);
      if (partition != null) {
        try {
          Location location = partition.getLocation();
          InputStream inputStream = location.getInputStream();
          ...
        } catch (IOException e) {
          ...
        }
      }

Note that if the partition was written with MapReduce, the location is actually a directory
that contains part files. In that case, list the files in the directory to find the part files::

    for (Location file : location.list()) {
      if (file.getName().startsWith("part")) {
        InputStream inputStream = location.getInputStream();
        ...
      }
    }

Instead of reading a single partition, you can also specify a PartitionFilter to query the
partitioned file set for all partitions whose keys match that filter. The PartitionFilter
can specify either an exact value (en equality condition) or a range for the value of each
field in the dataset's partitioning. For example, the following code reads all partitions
for the NFL and the '80s seasons::

      PartitionFilter filter = PartitionFilter.builder().addValueCondition("league", "nfl")
                                                        .addRangeCondition("season", 1980, 1990)
                                                        .build());
      Set<Partition> partitions = dataset.getPartitions(filter);
      for (partition : partitions) {
        try {
          Location location = partition.getLocation();
          InputStream inputStream = location.getInputStream();
          ...
        } catch (IOException e) {
          ...
        }
      }

Note that the upper bound for the seasons (1990) is exclusive; that is, the 1990 season is not
included in the returned partitions. For a range condition, either the lower or the upper bound may
be null, meaning that the filter in unbounded in that direction.

Adding a partition is similar; however, instead of a Partition, you receive a ``PartitionOutput``
for the partition key. That object has methods to obtain a Location and to add the partition once
you have written to that Location.
For example, this code writes to a file named ``part`` under the location returned from the
``PartitionOutput``::

      PartitionKey key = ...
      PartitionOutput output = dataset.getPartitionOutput(key);
      try {
        Location location = output.getLocation().append("part");
        OutputStream outputStream = location.getOutputStream());
        ...
      } catch (IOException e) {
        ...
      }
      output.addPartition();

Using PartitionedFileSets in MapReduce
======================================

A partitioned file set can be accessed in MapReduce in a similar fashion to a FileSet. The difference
is that instead of input and output paths, you specify a partition filter for the input and a
partition key for the output. For example, the MapReduce program of the SportResults example
reads as input all partitions for the league given in its runtime arguments, and writes as output
a partition with that league as the only key::

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    ...
    String league = context.getRuntimeArguments().get("league");

    // Configure the input to read all seasons for the league
    Map<String, String> inputArgs = Maps.newHashMap();
    PartitionedFileSetArguments.setInputPartitionFilter(
      inputArgs, PartitionFilter.builder().addValueCondition("league", league).build());
    PartitionedFileSet input = context.getDataset("results", inputArgs);
    context.setInput("results", input);

    // Each run writes its output to a partition for the league
    Map<String, String> outputArgs = Maps.newHashMap();
    outputKey = PartitionKey.builder().addStringField("league", league).build();
    PartitionedFileSetArguments.setOutputPartitionKey(outputArgs, outputKey);
    outputFileSet = context.getDataset("totals", outputArgs);
    outputPath = FileSetArguments.getOutputPath(outputFileSet.getEmbeddedFileSet().getRuntimeArguments());
    context.setOutput("totals", outputFileSet);
  }

Here, the ``beforeSubmit()`` method of the MapReduce generates the runtime arguments for the
partitioned file sets that specify the input partition filter and output partition key. This
is convenient for starting the MapReduce, because only a single argument has to be given for
the MapReduce run. If that code was not in the ``beforeSubmit()``, you could still achieve the
same result by specifying the partition filter and key explicitly in the MapReduce runtime arguments.
For example, give these arguments when starting the MapReduce through a RESTful call::

  {
    "dataset.results.input.partition.filter.league.value": "nfl",
    "dataset.results.input.partition.filter.season.lower": "1980",
    "dataset.results.input.partition.filter.season.upper": "1990",
    "dataset.totals.output.partition.key.league" : "nfl"
  }

Dynamic Partitioning of MapReduce Output
========================================

A MapReduce job can write to multiple partitions of a PartitionedFileSet using the
``DynamicPartitioner`` class. To do so, define a class that implements ``DynamicPartitioner``.
The core method to override is the ``getPartitionKey`` method; it maps a record's key and value
to a ``PartitionKey``, which defines which ``Partition`` the record should be written to::

  public static final class TimeAndZipPartitioner extends DynamicPartitioner<NullWritable, Text> {

    private Long time;
    private JsonParser jsonParser;

    @Override
    public void initialize(MapReduceTaskContext<NullWritable, Text> mapReduceTaskContext) {
      this.time = mapReduceTaskContext.getLogicalStartTime();
      this.jsonParser = new JsonParser();
    }

    @Override
    public PartitionKey getPartitionKey(NullWritable key, Text value) {
      int zip = jsonParser.parse(value.toString()).getAsJsonObject().get("zip").getAsInt();
      return PartitionKey.builder().addLongField("time", time).addIntField("zip", zip).build();
    }
  }

Then set the class of the custom partitioner as runtime arguments of the output PartitionedFileSet::

  Map<String, String> cleanRecordsArgs = new HashMap<>();
  PartitionedFileSetArguments.setDynamicPartitioner(cleanRecordsArgs, TimeAndZipPartitioner.class);
  context.addOutput(DataCleansing.CLEAN_RECORDS, cleanRecordsArgs);

With this, each record processed by the MapReduce job will be written to a path corresponding
to the ``Partition`` that it was mapped to by the ``DynamicPartitioner``, and the set of new ``Partition``\ s
will be registered with the output ``PartitionedFileSet`` at the end of the job.
Note that any partitions written to must not previously exist. Otherwise, the MapReduce job will fail at the
end of the job and none of the partitions will be added to the ``PartitionedFileSet``.

Incrementally Processing PartitionedFileSets
============================================

One way to process a partitioned file set is with a repeatedly-running MapReduce program that,
in each run, reads all partitions that have been added since its previous run. This requires
that the MapReduce program persists between runs which partitions have already been consumed.
An easy way is to use the ``PartitionBatchInput``, an experimental feature introduced in CDAP 3.3.0.
Your MapReduce program is responsible for providing an implementation of ``DatasetStatePersistor`` to
persist and then read back its state. In this example, the state is persisted to a row in a
KeyValue Table, using the convenience class ``KVTableStatePersistor``; however, other types of
Datasets can also be used. In the ``beforeSubmit()`` method of the MapReduce, specify the
partitioned file set to be used as input as well as the ``DatasetStatePersistor`` to be used::

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      partitionCommitter =
        PartitionBatchInput.setInput(context, DataCleansing.RAW_RECORDS,
                                     new KVTableStatePersistor(DataCleansing.CONSUMING_STATE, "state.key"));
      ...
    }

This will read back the previously persisted state, determine the new partitions to read based upon this
state, and compute a new state to store in memory until a call to the ``onFinish`` method of the returned
``PartitionCommitter``. The dataset is instantiated with the set of new partitions to read as input and
set as input for the MapReduce job.

To save the state of partition processing, call the returned PartitionCommitter's ``onFinish`` method.
This ensures that the next time the MapReduce job runs, it processes only the newly committed partitions::

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    partitionCommitter.onFinish(succeeded);
  }

Exploring PartitionedFileSets
=============================

A partitioned file set can be explored with ad-hoc queries if you enable it at creation time::

    createDataset("results", PartitionedFileSet.class, PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addStringField("league").addIntField("season").build())
      // Properties for file set
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ",")
      // Properties for Explore (to create a partitioned Hive table)
      .setEnableExploreOnCreate(true)
      .setExploreFormat("csv")
      .setExploreSchema("date STRING, winner STRING, loser STRING, winnerpoints INT, loserpoints INT")
      .build());

This results in the creation of an external table in Hive with the schema given in the
``setExploreSchema()``. The supported format are ``text`` and ``csv``. Both mean that the
format is text. For ``csv``, the field delimiter is a comma, whereas for ``text``, you can
specify the field delimiter. For example, to use a colon as the field separator::

      .setExploreFormat("text")
      .setExploreFormatProperty("delimiter", ":");

If your file format is not text, you can still explore the dataset, but you need to give
detailed instructions when creating the dataset. For example, to use Avro as the file
format::

      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", SCHEMA_STRING)

You need to specify the SerDe, the input format, the output format, and any additional properties
any of these may need as table properties. This is an experimental feature and only tested for
Avro; see the :ref:`StreamConversion <examples-stream-conversion>` example and
the :ref:`fileset-exploration` for more details.

.. _datasets-timepartitioned-fileset:

======================
TimePartitionedFileSet
======================

TimePartitionedFileSets are a special case (and in fact, a subclass) of PartitionedFileSets, where
the partitioning is fixed to five integers representing the year, month, day of the month, hour of the day,
and minute of a partition's time. For convenience, it offers methods to address the partitions by
time instead of by partition key or filter. The time is interpreted as milliseconds since the Epoch.

These convenience methods provide access to partitions by time instead of by a partition key::

  @Nullable
  public TimePartition getPartitionByTime(long time);

  public Set<TimePartition> getPartitionsByTime(long startTime, long endTime);

  @Nullable
  public TimePartitionOutput getPartitionOutput(long time);

Essentially, these methods behave the same as if you had converted the time arguments into partition
keys and then called the corresponding methods of ``PartitionedFileSet`` with the resulting partition keys.
Additionally:

- The returned partitions have an extra method to retrieve the partition time as a long.
- The start and end times of ``getPartitionsByTime()`` do not correspond directly to a single partition filter,
  but to a series of partition filters. For example, to retrieve the partitions between November 2014 and
  March 2015, you need two partition filters: one for the months of November through December of 2014, and one
  for January through March of 2015. This method converts a given time range into the corresponding set
  of partition filters, retrieves the partitions for each filter, and returns the superset of all these
  partitions.

Using TimePartitionedFileSets in MapReduce
==========================================

Using time-partitioned file sets in MapReduce is similar to partitioned file sets; however, instead of
setting an input partition filter and an output partition key, you configure an input time range and an
output partition time in the ``beforeSubmit()`` of the MapReduce::

    TimePartitionedFileSetArguments.setInputStartTime(inputArgs, startTime);
    TimePartitionedFileSetArguments.setInputEndTime(inputArgs, endTime);

and::

    TimePartitionedFileSetArguments.setOutputPartitionTime(outputArgs, partitionTime);

You can achieve the same result by specifying the input time range and the output partition time
explicitly in the MapReduce runtime arguments. For example, you could give these arguments when starting
the MapReduce through a RESTful call::

  {
    "dataset.myInput.input.start.time": "1420099200000",
    "dataset.myInput.input.end.time": " 1422777600000",
    "dataset.results.output.partition.time": " 1422777600000",
  }

Note that the values for these times are milliseconds since the Epoch; the two times in this example represent
the midnight time of January 1st, 2015 and February 1st, 2015.

Exploring TimePartitionedFileSets
=================================

A time-partitioned file set can be explored with ad-hoc queries if you enable it at creation time,
similar to a FileSet, as described under :ref:`fileset-exploration`.

