.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _fileset-exploration:

============================================
Fileset Exploration
============================================

The ``FileSet`` and ``TimePartitionedFileSet`` datasets can be explored through ad-hoc SQL-like queries.
To enable exploration, you must set several properties when creating the Dataset, and the files in 
your dataset must fit certain constraints. These are described in more detail in the following sections. 

Explore Properties
------------------
A ``FileSet`` or ``TimePartitionedFileSet`` can be made explorable by setting several properties when
creating the dataset. For example, in the configure method of your application::

    Schema schema = Schema.recordOf(
      "purchase",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE))
    );
    createDataset("my-files", "fileSet", FileSetProperties.builder()
      .setBasePath("mylocation")
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      // everything past here is an explore property
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", schema.toString())
      .build());

These Dataset properties map directly to table properties in Hive. 
In fact, the create table statement that will be generated for the table above is::

  CREATE EXTERNAL TABLE cdap_user_myfiles(
    user string,
    item_id int,
    price double)
  ROW FORMAT SERDE org.apache.hadoop.hive.serde2.avro.AvroSerDe
  STORED AS INPUTFORMAT org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat
  OUTPUTFORMAT org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat
  LOCATION hdfs://<namenode>/cdap/mylocation
  TBLPROPERTIES (
    cdap.name=cdap.user.myfiles,
    avro.schema.literal="{\"type\": \"record\", \"name\": \"stringBody\", \"fields\": [{ \"name\":\"ts\", \"type\":\"long\" }, { \"name\":\"body\", \"type\":\"string\" } ] }"
  );

Please see the `Hive Language Manual
<https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Create/Drop/TruncateTable>`__
for more information.

Limitations
-----------
As exploration is tied directly to Hive, file exploration has several limitations that mirror limitations in Hive.

- All explorable files must be of the same format.
- All explorable files must be in a format known to the version of Hive you are using.
- Some versions of Hive may try to create a temporary staging directory at the table location when executing queries.
  If you are seeing permissions errors, try setting ``hive.exec.stagingdir`` in your Hive configuration to ``/tmp/hive-staging``.

A ``FileSet`` has some additional limitations that the ``TimePartitionedFileSet`` does not have.

- Hive tables created are not partitioned. This means any query will perform a full table scan.
- Only files at the base location of the ``FileSet`` are visible to queries. Directories are not read.
  Since Mapreduce writes output files to a directory, if you want Mapreduce output to be explorable you
  must move all output files to the base location. 

If you wish to use Impala to explore a ``FileSet`` or ``TimePartitionedFileSet``, there are several
additional restrictions you must keep in mind.

- Impala only supports scalar types. See `Data Types <http://www.cloudera.com/content/cloudera/en/documentation/cloudera-impala/latest/topics/impala_avro.html#avro_data_types_unique_1>`__ for more detail.
- If your underlying data contains non-scalars, you cannot tell Impala to use a different read schema of just scalars.
  For example, if you have Avro files that contain a map field, you cannot simply leave out the field when specifying the table schema.
- Impala caches table metadata, which clients must invalidate when there are changes. 
  You have to issue the INVALIDATE METADATA [tablename] command, whenever table metadata changes.
  This means you must invalidate metadata if a new table is added or a table partition is added.
  Impala caches the table metadata otherwise, preventing you from seeing changes.
  If data is added to the table without changing metadata (adding a partition), then you have to issue 
  a REFRESH [tablename] command to get Impala to see the changes. 
  It caches info on the table files and blocks as well, but the REFRESH command will cause it to re-read these.

