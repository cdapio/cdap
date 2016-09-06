.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015-2016 Cask Data, Inc.

.. _fileset-exploration:

===================
FileSet Exploration
===================

The ``FileSet``, ``PartitionedFileSet``, and ``TimePartitionedFileSet`` datasets can be
explored through ad-hoc SQL-like queries. To enable exploration, you must set several
properties when creating the dataset, and the files in your dataset must meet certain
requirements. These properties and requirements are described below. 

Explore Properties
------------------
A ``FileSet``, ``PartitionedFileSet``, or ``TimePartitionedFileSet`` is made explorable by setting several properties when
creating the dataset. The ``FileSetProperties`` class (``PartitionedFileSetProperties`` or ``TimePartitionedFileSetsProperties``
classes for the other two types) should be used to set the following required properties:

- ``EnableExploreOnCreate`` must be set to true to create a Hive table when the dataset is created
- ``SerDe`` class that Hive should use for serialization and deserialization
- ``InputFormat`` that Hive should use for reading files
- ``OutputFormat`` that Hive should use for writing files 

Any other table properties that the SerDe may need must also be set. 
For example, in the configure method of your application::

    Schema schema = Schema.recordOf(
      "purchase",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE))
    );
    createDataset("myfiles", "fileSet", FileSetProperties.builder()
      .setBasePath("mylocation")
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      // everything past here is a CDAP Explore property
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", schema.toString())
      .build());

For a ``FileSet`` using the ``AvroParquet`` format::

    FileSetProperties.builder()
    .setBasePath(basePath)
    .setInputFormat(AvroParquetInputFormat.class)
    .setOutputFormat(AvroParquetOutputFormat.class)
    .setEnableExploreOnCreate(true)
    .setExploreFormat("parquet")
    .setExploreSchema("id long, name string")

A ``PartitionedFileSet`` using the ``text`` format, with ``\n`` as the record delimiter::

    PartitionedFileSetProperties.builder()
    // Properties for partitioning
    .setPartitioning(Partitioning.builder().addLongField("time").build())
    // Properties for file set
    .setInputFormat(TextInputFormat.class)
    .setOutputFormat(TextOutputFormat.class)
    .setOutputProperty(TextOutputFormat.SEPERATOR, ",")
    // enable CDAP Explore
    .setEnableExploreOnCreate(true)
    .setExploreFormat("text")
    .setExploreFormatProperty("delimiter", "\n")
    .setExploreSchema("record STRING")
    .build()

If you are running a version of Hive that reserves keywords and any of your column names is a `Hive reserved keyword
<https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Keywords,
Non-reservedKeywordsandReservedKeywords>`__, you will need to enclose the column name in backticks.
For example::

    PartitionedFileSetProperties.builder()
    // Properties for partitioning
    .setPartitioning(Partitioning.builder().addLongField("time").build())
    // Properties for file set
    .setInputFormat(TextInputFormat.class)
    .setOutputFormat(TextOutputFormat.class)
    .setOutputProperty(TextOutputFormat.SEPERATOR, ",")
    // enable CDAP Explore
    .setEnableExploreOnCreate(true)
    .setExploreFormat("text")
    .setExploreFormatProperty("delimiter", "\n")
    .setExploreSchema("`date` STRING")
    .build() 

These dataset properties map directly to table properties in Hive. In the case of the
``setBasePath`` method, the partial-path given will be a sub-directory of
``<CDAP-home>/namespaces/<namespace-id>/data/``.

For example, if ``<CDAP-home>`` is */cdap*, and ``<namespace-id>`` is *default*, 
the first Dataset example above would result in this "create table" statement being generated::

  CREATE EXTERNAL TABLE dataset_myfiles(
    user string,
    item_id int,
    price double)
  ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.avro.AvroSerDe"
  STORED AS INPUTFORMAT "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"
  OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"
  LOCATION "/cdap/namespaces/default/data/mylocation"
  TBLPROPERTIES (
    "avro.schema.literal"="{\"type\": \"record\", \"name\": \"stringBody\", \"fields\": [{ \"name\":\"ts\", \"type\":\"long\" }, { \"name\":\"body\", \"type\":\"string\" } ] }"
  );

Please see the `Hive Language Manual
<https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Create/Drop/TruncateTable>`__
for more information about input formats, output format, SerDes, and table properties.

Limitations
-----------
There are several limitations for fileset exploration:

- All explorable files must be in a format supported by your version of Hive. 
- Your version of Hive must include `the appropriate SerDe 
  <https://cwiki.apache.org/confluence/display/Hive/SerDe#SerDe-Built-inSerDes>`__.
- Some versions of Hive may try to create a temporary staging directory at the table location when executing queries.
  If you are seeing permissions errors, try setting ``hive.exec.stagingdir`` in your Hive configuration to ``/tmp/hive-staging``.

A ``FileSet`` has some additional limitations that the ``PartitionedFileSet`` or ``TimePartitionedFileSet`` do not have:

- Hive tables created by a ``FileSet`` are not partitioned; this means all queries perform a full table scan.
- Only files at the base location of the ``FileSet`` are visible to queries. Directories are not read.
  Since MapReduce writes output files to a directory, you must move all output files to the base location for
  MapReduce output to be explorable.

If you wish to use Impala to explore a ``FileSet``, ``PartitionedFileSet``, or ``TimePartitionedFileSet``, there are several
additional restrictions you must keep in mind:

- Impala only supports scalar types. See `Data Type Considerations for Avro Tables 
  <http://www.cloudera.com/content/cloudera/en/documentation/cloudera-impala/latest/topics/impala_avro.html#avro_data_types_unique_1>`__ 
  for details.
- If your underlying data contains non-scalars, you cannot tell Impala to use a different read schema of just scalars.
  For example, if you have Avro files that contain a map field, you cannot simply leave out the field when specifying the table schema.
- Impala caches table metadata, which clients must invalidate when there are changes. 
  You have to issue an ``INVALIDATE METADATA [tablename]`` command whenever table metadata changes.
  You'll need to invalidate metadata if a new table or table partition is added. Otherwise, Impala will use the
  cached table metadata, preventing you from seeing the changes. If data is added to a table without changing the
  metadata (such as when adding a partition), then you need to issue a ``REFRESH [tablename]`` command to force
  Impala to see the changes. Though Impala also caches info on table files and blocks, any calls to the
  ``REFRESH`` command will cause it to re-read the information.
