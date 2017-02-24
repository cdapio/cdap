.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. _datasets-partitioned-fileset:

===================
Partitioned FileSet
===================

While a FileSet is a convenient abstraction over actual file system interfaces, it still requires
the application to be aware of file system paths. For example, an application that maintains data
over time might have a new file for every month. One could come up with a naming convention that encodes
the month into each file name, and share that convention across all applications that use this file set.
Yet that can become tedious to manage, especially if the naming convention should ever change |---| then all
applications would have to be changed simultaneously for proper functioning.

The ``PartitionedFileSet`` dataset relieves applications from understanding file name conventions. Instead,
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

In order to make the PartitionedFileSet explorable, additional properties are needed, as described
in :ref:`exploring-partitionedfilesets`.

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
  public void initialize() throws Exception {
    MapReduceContext context = getContext();
    ...
    String league = context.getRuntimeArguments().get("league");

    // Configure the input to read all seasons for the league
    Map<String, String> inputArgs = Maps.newHashMap();
    PartitionedFileSetArguments.setInputPartitionFilter(
      inputArgs, PartitionFilter.builder().addValueCondition("league", league).build());
    context.addInput(Input.ofDataset("results", inputArgs));

    // Each run writes its output to a partition for the league
    Map<String, String> outputArgs = Maps.newHashMap();
    outputKey = PartitionKey.builder().addStringField("league", league).build();
    PartitionedFileSetArguments.setOutputPartitionKey(outputArgs, outputKey);
    context.addOutput(Output.ofDataset("totals", outputArgs));
  }

Here, the ``initialize`` method of the MapReduce generates the runtime arguments for the
partitioned file sets that specify the input partition filter and output partition key. This
is convenient for starting the MapReduce, because only a single argument has to be given for
the MapReduce run. If that code was not in the ``initialize()``, you could still achieve the
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
  context.addOutput(Output.ofDataset(DataCleansing.CLEAN_RECORDS, cleanRecordsArgs));

With this, each record processed by the MapReduce job will be written to a path corresponding
to the ``Partition`` that it was mapped to by the ``DynamicPartitioner``, and the set of new ``Partition``\ s
will be registered with the output ``PartitionedFileSet`` at the end of the job.
Note that any partitions written to must not previously exist. Otherwise, the MapReduce job will fail at the
end of the job and none of the partitions will be added to the ``PartitionedFileSet``.

Incrementally Processing PartitionedFileSets
============================================

Processing using MapReduce
--------------------------
One way to process a partitioned file set is with a repeatedly-running MapReduce program that,
in each run, reads all partitions that have been added since its previous run. This requires
that the MapReduce program persists between runs which partitions have already been consumed.
An easy way is to use the ``PartitionBatchInput``, an experimental feature introduced in CDAP 3.3.0.
Your MapReduce program is responsible for providing an implementation of ``DatasetStatePersistor`` to
persist and then read back its state. In this example, the state is persisted to a row in a
KeyValue Table, using the convenience class ``KVTableStatePersistor``; however, other types of
Datasets can also be used. In the ``initialize`` method of the MapReduce, specify the
partitioned file set to be used as input as well as the ``DatasetStatePersistor`` to be used::

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      ...
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
  public void destroy() {
    boolean succeeded = getContext().getState().getStatus() == ProgramStatus.COMPLETED;
    partitionCommitter.onFinish(succeeded);
  }

Processing using Other Programs
-------------------------------
Partitions of a partitioned file set can also be incrementally processed from other program types
using the generic ``PartitionConsumer`` APIs. The implementation of these APIs that can be used from multiple instances
of a program is ``ConcurrentPartitionConsumer``. To use, you simply need to provide the instance of the
partitioned file set you want to consume from, along with a ``StatePersistor``, responsible for managing
persistence of the consumer's state::

  // This can be in any program where we have access to Datasets,
  // such as a Worker, Workflow Action, or even in a MapReduce
  PartitionConsumer consumer =
    new ConcurrentPartitionConsumer(partitionedFileSet, new CustomStatePersistor(persistenceTable));

  // Call consumePartitions to get a list of partitions to process
  final List<PartitionDetail> partitions = partitionConsumer.consumePartitions().getPartitions();

  // Process partitions
  ...

  // Once done processing, onFinish must be called with a boolean value indicating success or failure, so that
  // the partitions' can be marked accordingly for completion or retries in the future
  partitionConsumer.onFinish(partitions, true);

The ``consumePartitions`` method of the ``PartitionConsumer`` can optionally take in a limit (an int), which will
limit the number of returned partitions. It can also take in a ``PartitionAcceptor``, which allows you to
define a custom method to limit the number of partitions. For instance, it may be useful to limit the number of
partitions to process at a time, and have it be based on the size of the partitions::

  public class SizeLimitingAcceptor implements PartitionAcceptor {

    private final int sizeLimitMB;
    private int acceptedMBSoFar;

    public SizeLimitingAcceptor(int sizeLimitMB) {
      this.sizeLimitMB = sizeLimitMB;
      this.acceptedMBSoFar = 0;
    }

    @Override
    public Return accept(PartitionDetail partitionDetail) {
      // assuming that the metadata contains the size of that partition
      acceptedMBSoFar += Integer.valueOf(partitionDetail.getMetadata().get("sizeMB"));
      if (acceptedMBSoFar > sizeLimitMB) {
        return Return.STOP;
      }
      return Return.ACCEPT;
    }
  }


It can then be used as::

  // return only partitions, to process up to 500MB of data
  partitions = consumer.consumePartitions(new SizeLimitingAcceptor(500));

.. _exploring-partitionedfilesets:

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

The essential part (to enable exploration) of the above sample are these lines::

      . . .
      // Properties for Explore (to create a partitioned Hive table)
      .setEnableExploreOnCreate(true)
      .setExploreFormat("csv")
      .setExploreSchema("date STRING, winner STRING, loser STRING, winnerpoints INT, loserpoints INT")
      . . .

This results in the creation of an external table in Hive with the schema given in the
``setExploreSchema()``. The supported formats (set by ``setExploreFormat()``) are ``csv``
and ``text``. Both define that the format is text. For ``csv``, the field delimiter is a
comma, whereas for ``text``, you can specify the field delimiter using ``setExploreFormatProperty()``.

For example, to use a colon as the field separator::

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

.. _datasets-partitioned-fileset-transactions:

PartitionedFileSets and Transactions
====================================

A PartitionedFileSet is a hybrid of a non-transactional FileSet and a transactional Table
that stores the partition metadata. As a consequence, operations that need access to the
partition table (such as adding a partition or listing partitions) can only be performed
in the context of a transaction, while operations that only require access to the
FileSet (such as ``getPartitionOutput()`` or ``getEmbeddedFileSet()``) can be performed
without a transaction.

Because a FileSet is not a transactional dataset, it normally does not participate in a
transaction rollback: files written in a transaction are not rolled back if the transaction
fails; and files deleted in a transaction are not restored. However, in the context of a
PartitionedFileSet, consistency between the partition files and the partition metadata
is desired. As a consequence, the FileSet embedded in a PartitionedFileSet behaves
transactionally as follows:

- If ``PartitionOutput.addPartition()`` is used to add a new partition, and the
  transaction fails, then the location of that PartitionOutput is deleted.
- If a partition is added as the output of a MapReduce program, and the MapReduce fails,
  then the partition and its files are removed as part of the job cleanup.
- However, if a partition is added using ``PartitionedFileSet.addPartition()`` with
  an existing relative path in the FileSet, then the files at that location are not
  removed on transaction failure.
- If a partition is deleted using ``dropPartition()``, then the partition and its files
  are restored if the transaction fails.
