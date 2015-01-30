.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _fileset-exploration:

============================================
Fileset Exploration
============================================

The ``FileSet`` and ``TimePartitionedFileSet`` Datasets can be explored through ad-hoc SQL-like queries.
To enable exploration, you must set several properties when creating the Dataset, and the files in 
your Dataset must meet certain requirements. These properties and requirements are described below. 

Explore Properties
------------------
A ``FileSet`` or ``TimePartitionedFileSet`` is made explorable by setting several properties when
creating the dataset. The ``FileSetProperties`` class should be used to set the following required properties:

- EnableExploreOnCreate must be set to true to create a Hive table when the Dataset is created
- SerDe class that Hive should use for serialization and deserialization
- InputFormat that Hive should use for reading files
- OutputFormat that Hive should use for writing files 

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
      // everything past here is an explore property
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", schema.toString())
      .build());

These Dataset properties map directly to table properties in Hive. 
For example, Dataset above would result in the following "create table" statement being generated::

  CREATE EXTERNAL TABLE cdap_user_myfiles(
    user string,
    item_id int,
    price double)
  ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.avro.AvroSerDe"
  STORED AS INPUTFORMAT "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"
  OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"
  LOCATION "/cdap/mylocation"
  TBLPROPERTIES (
    "avro.schema.literal"="{\"type\": \"record\", \"name\": \"stringBody\", \"fields\": [{ \"name\":\"ts\", \"type\":\"long\" }, { \"name\":\"body\", \"type\":\"string\" } ] }"
  );

Please see the `Hive Language Manual
<https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Create/Drop/TruncateTable>`__
for more information about input formats, output format, SerDes, and table properties.

Limitations
-----------
As exploration is tied directly to Hive, file exploration has several limitations that mirror limitations in Hive:

- All explorable files must be of the same format.
- All explorable files must be in a format known to the version of Hive you are using.
- Some versions of Hive may try to create a temporary staging directory at the table location when executing queries.
  If you are seeing permissions errors, try setting ``hive.exec.stagingdir`` in your Hive configuration to ``/tmp/hive-staging``.

A ``FileSet`` has some additional limitations that the ``TimePartitionedFileSet`` does not have:

- Hive tables created by a ``FileSet`` are not partitioned; this means all queries perform a full table scan.
- Only files at the base location of the ``FileSet`` are visible to queries. Directories are not read.
  Since MapReduce writes output files to a directory, you must move all output files to the base location for
  MapReduce output to be explorable.

If you wish to use Impala to explore a ``FileSet`` or ``TimePartitionedFileSet``, there are several
additional restrictions you must keep in mind:

- Impala only supports scalar types. See `Data Type Considerations for Avro Tables <http://www.cloudera.com/content/cloudera/en/documentation/cloudera-impala/latest/topics/impala_avro.html#avro_data_types_unique_1>`__ for details.
- If your underlying data contains non-scalars, you cannot tell Impala to use a different read schema of just scalars.
  For example, if you have Avro files that contain a map field, you cannot simply leave out the field when specifying the table schema.
- Impala caches table metadata, which clients must invalidate when there are changes. 
  You have to issue an ``INVALIDATE METADATA [tablename]`` command whenever table metadata changes.
  You'll need to invalidate metadata if a new table or table partition is added. Otherwise, Impala will use the
  cached table metadata, preventing you from seeing the changes. If data is added to a table without changing the
  metadata (such as when adding a partition), then you need to issue a ``REFRESH [tablename]`` command to force
  Impala to see the changes. Though Impala also caches info on table files and blocks, any calls to the
  ``REFRESH`` command will cause it to re-read the information.

