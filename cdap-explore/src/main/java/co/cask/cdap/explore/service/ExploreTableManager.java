/*
 * Copyright © 2015-2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.explore.service;

import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.ExploreProperties;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.DatasetNotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.explore.table.AlterStatementBuilder;
import co.cask.cdap.explore.table.CreateStatementBuilder;
import co.cask.cdap.explore.utils.ExploreTableNaming;
import co.cask.cdap.hive.datasets.DatasetStorageHandler;
import co.cask.cdap.hive.objectinspector.ObjectInspectorFactory;
import co.cask.cdap.hive.stream.StreamStorageHandler;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Executes disabling and enabling of datasets and streams and adding and dropping of partitions.
 */
public class ExploreTableManager {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreTableManager.class);

  // A GSON object that knows how to serialize Schema type.
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  // additional session configuration to make Hive fail without sleep/retry if it can't acquire locks
  private static final Map<String, String> IMMEDIATE_TIMEOUT_CONF =
    ImmutableMap.of("hive.lock.numretries", "0",
                    "hive.lock.sleep.between.retries", "1");

  private final ExploreService exploreService;
  private final SystemDatasetInstantiatorFactory datasetInstantiatorFactory;
  private final ExploreTableNaming tableNaming;
  private final boolean shouldEscapeColumns;

  @Inject
  public ExploreTableManager(ExploreService exploreService,
                             SystemDatasetInstantiatorFactory datasetInstantiatorFactory,
                             ExploreTableNaming tableNaming,
                             Configuration hConf) {
    this.exploreService = exploreService;
    this.datasetInstantiatorFactory = datasetInstantiatorFactory;
    this.tableNaming = tableNaming;
    this.shouldEscapeColumns = ExploreServiceUtils.shouldEscapeColumns(hConf);
  }

  /**
   * Enable exploration on a stream by creating a corresponding Hive table. Enabling exploration on a
   * stream that has already been enabled is a no-op. Assumes the stream actually exists.
   *
   * @param tableName name of the Hive table to create
   * @param streamId the ID of the stream
   * @param formatSpec the format specification for the table
   * @return query handle for creating the Hive table for the stream
   * @throws UnsupportedTypeException if the stream schema is not compatible with Hive
   * @throws ExploreException if there was an exception submitting the create table statement
   * @throws SQLException if there was a problem with the create table statement
   */
  public QueryHandle enableStream(String tableName, StreamId streamId, FormatSpecification formatSpec)
    throws UnsupportedTypeException, ExploreException, SQLException {
    String streamName = streamId.getStream();
    LOG.debug("Enabling explore for stream {} with table {}", streamId, tableName);

    // schema of a stream is always timestamp, headers, and then the schema of the body.
    List<Schema.Field> fields = Lists.newArrayList(
      Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))));
    if (formatSpec.getSchema() != null) {
      fields.addAll(formatSpec.getSchema().getFields());
    }
    Schema schema = Schema.recordOf("streamEvent", fields);

    Map<String, String> serdeProperties = ImmutableMap.of(
      Constants.Explore.STREAM_NAME, streamName,
      Constants.Explore.STREAM_NAMESPACE, streamId.getNamespace(),
      Constants.Explore.FORMAT_SPEC, GSON.toJson(formatSpec));

    String createStatement = new CreateStatementBuilder(streamName, null, tableName, shouldEscapeColumns)
      .setSchema(schema)
      .setTableComment("CDAP Stream")
      .buildWithStorageHandler(StreamStorageHandler.class.getName(), serdeProperties);

    LOG.debug("Running create statement for stream {} with table {}: {}", streamName, tableName, createStatement);

    return exploreService.execute(streamId.getParent(), createStatement);
  }

  /**
   * Disable exploration on the given stream by dropping the Hive table for the stream.
   *
   * @param tableName name of the table to delete
   * @param streamId the ID of the stream to disable
   * @return the query handle for disabling the stream
   * @throws ExploreException if there was an exception dropping the table
   * @throws SQLException if there was a problem with the drop table statement
   */
  public QueryHandle disableStream(String tableName, StreamId streamId) throws ExploreException, SQLException {
    LOG.debug("Disabling explore for stream {} with table {}", streamId, tableName);
    String deleteStatement = generateDeleteTableStatement(null, tableName);
    return exploreService.execute(streamId.getParent(), deleteStatement);
  }

  /**
   * Enable ad-hoc exploration on the given dataset by creating a corresponding Hive table. If exploration has
   * already been enabled on the dataset, this will be a no-op. Assumes the dataset actually exists.
   *
   * @param datasetId the ID of the dataset to enable
   * @param spec the specification for the dataset to enable
   * @param truncating whether this call to create() is part of a truncate() operation, which is in some
   *                   case implemented using disableExplore() followed by enableExplore()
   *
   * @return query handle for creating the Hive table for the dataset
   * @throws IllegalArgumentException if some required dataset property like schema is not set
   * @throws UnsupportedTypeException if the schema of the dataset is not compatible with Hive
   * @throws ExploreException if there was an exception submitting the create table statement
   * @throws SQLException if there was a problem with the create table statement
   * @throws DatasetNotFoundException if the dataset had to be instantiated, but could not be found
   * @throws ClassNotFoundException if the was a missing class when instantiating the dataset
   */
  public QueryHandle enableDataset(DatasetId datasetId, DatasetSpecification spec, boolean truncating)
    throws IllegalArgumentException, ExploreException, SQLException,
    UnsupportedTypeException, DatasetNotFoundException, ClassNotFoundException {

    String createStatement = generateEnableStatement(datasetId, spec, truncating);
    if (createStatement != null) {
      return exploreService.execute(datasetId.getParent(), createStatement);
    } else {
      // if the dataset is not explorable, this is a no op.
      return QueryHandle.NO_OP;
    }
  }

  private String generateEnableStatement(DatasetId datasetId, DatasetSpecification spec, boolean truncating)
    throws UnsupportedTypeException, ExploreException {

    Dataset dataset = null;
    try (SystemDatasetInstantiator datasetInstantiator = datasetInstantiatorFactory.create()) {
      dataset = datasetInstantiator.getDataset(datasetId);
      return generateEnableStatement(dataset, spec, datasetId,
                                     tableNaming.getTableName(datasetId, spec.getProperties()), truncating);
    } catch (IOException e) {
      LOG.error("Exception instantiating dataset {}.", datasetId, e);
      throw new ExploreException("Exception while trying to instantiate dataset " + datasetId);
    } finally {
      Closeables.closeQuietly(dataset);
    }
  }

  /**
   * Update ad-hoc exploration on the given dataset by altering the corresponding Hive table. If exploration has
   * not been enabled on the dataset, this will fail. Assumes the dataset actually exists.
   *
   * @param datasetId the ID of the dataset to enable
   * @param spec the specification for the dataset to enable
   * @return query handle for creating the Hive table for the dataset
   * @throws IllegalArgumentException if some required dataset property like schema is not set
   * @throws UnsupportedTypeException if the schema of the dataset is not compatible with Hive
   * @throws ExploreException if there was an exception submitting the create table statement
   * @throws SQLException if there was a problem with the create table statement
   * @throws DatasetNotFoundException if the dataset had to be instantiated, but could not be found
   * @throws ClassNotFoundException if the was a missing class when instantiating the dataset
   */
  public QueryHandle updateDataset(DatasetId datasetId,
                                   DatasetSpecification spec, DatasetSpecification oldSpec)
    throws IllegalArgumentException, ExploreException, SQLException,
    UnsupportedTypeException, DatasetNotFoundException, ClassNotFoundException {

    String tableName = tableNaming.getTableName(datasetId, spec.getProperties());
    String databaseName = ExploreProperties.getExploreDatabaseName(spec.getProperties());

    String oldTableName = tableNaming.getTableName(datasetId, oldSpec.getProperties());
    String oldDatabaseName = ExploreProperties.getExploreDatabaseName(oldSpec.getProperties());

    try {
      exploreService.getTableInfo(datasetId.getNamespace(), oldDatabaseName, oldTableName);
    } catch (TableNotFoundException e) {
      // the dataset was not enabled for explore before;
      // but the new spec may be explorable, so attempt to enable it
      return enableDataset(datasetId, spec, false);
    }

    List<String> alterStatements;
    if (!(oldTableName.equals(tableName) && Objects.equals(oldDatabaseName, databaseName))) {
      alterStatements = new ArrayList<>();
      // database/table name changed. All we can do is disable the old table and enable the new one
      String disableStatement = generateDisableStatement(datasetId, oldSpec);
      if (disableStatement != null) {
        alterStatements.add(disableStatement);
      }
      String enableStatement = generateEnableStatement(datasetId, spec, false);
      if (enableStatement != null) {
        alterStatements.add(enableStatement);
      }
    } else {
      Dataset dataset = null;
      try (SystemDatasetInstantiator datasetInstantiator = datasetInstantiatorFactory.create()) {
        dataset = datasetInstantiator.getDataset(datasetId);
        alterStatements = generateAlterStatements(datasetId, tableName, dataset, spec, oldSpec);
      } catch (IOException e) {
        LOG.error("Exception instantiating dataset {}.", datasetId, e);
        throw new ExploreException("Exception while trying to instantiate dataset " + datasetId);
      } finally {
        Closeables.closeQuietly(dataset);
      }
    }
    LOG.trace("alter statements for update: {}", alterStatements);
    if (alterStatements == null || alterStatements.isEmpty()) {
      return QueryHandle.NO_OP;
    }
    if (alterStatements.size() == 1) {
      return exploreService.execute(datasetId.getParent(), alterStatements.get(0));
    }
    return exploreService.execute(datasetId.getParent(), alterStatements.toArray(new String[alterStatements.size()]));
  }

  /**
   * Disable exploration on the given dataset by dropping the Hive table for the dataset.
   *
   * @param datasetId the ID of the dataset to disable
   * @return the query handle for disabling the dataset
   * @throws ExploreException if there was an exception dropping the table
   * @throws SQLException if there was a problem with the drop table statement
   * @throws DatasetNotFoundException if the dataset had to be instantiated, but could not be found
   * @throws ClassNotFoundException if the was a missing class when instantiating the dataset
   */
  public QueryHandle disableDataset(DatasetId datasetId, DatasetSpecification spec)
    throws ExploreException, SQLException, DatasetNotFoundException, ClassNotFoundException {

    LOG.debug("Disabling explore for dataset instance {}", datasetId);
    String deleteStatement = generateDisableStatement(datasetId, spec);
    if (deleteStatement != null) {
      LOG.debug("Running delete statement for dataset {} - {}", datasetId, deleteStatement);
      return exploreService.execute(datasetId.getParent(), deleteStatement);
    } else {
      return QueryHandle.NO_OP;
    }
  }

  private String generateDisableStatement(DatasetId datasetId, DatasetSpecification spec) throws ExploreException {

    String tableName = tableNaming.getTableName(datasetId, spec.getProperties());
    String databaseName = ExploreProperties.getExploreDatabaseName(spec.getProperties());
    // If table does not exist, nothing to be done
    try {
      exploreService.getTableInfo(datasetId.getNamespace(), databaseName, tableName);
    } catch (TableNotFoundException e) {
      // Ignore exception, since this means table was not found.
      return null;
    }

    Dataset dataset = null;
    try (SystemDatasetInstantiator datasetInstantiator = datasetInstantiatorFactory.create()) {
      dataset = datasetInstantiator.getDataset(datasetId);
      if (dataset instanceof FileSet || dataset instanceof PartitionedFileSet) {
        // do not drop the explore table that dataset is reusing an existing table
        if (FileSetProperties.isUseExisting(spec.getProperties())) {
          return null;
        }
      }
      return generateDeleteStatement(dataset, databaseName, tableName);
    } catch (IOException e) {
      LOG.error("Exception creating dataset classLoaderProvider for dataset {}.", datasetId, e);
      throw new ExploreException("Exception instantiating dataset " + datasetId);
    } finally {
      Closeables.closeQuietly(dataset);
    }
  }

  /**
   * Adds a partition to the Hive table for the given dataset.
   *
   * @param datasetId the ID of the dataset to add a partition to
   * @param properties additional dataset properties relevant to this operation
   * @param partitionKey the partition key to add
   * @param fsPath the path of the partition
   * @return the query handle for adding the partition the dataset
   * @throws ExploreException if there was an exception adding the partition
   * @throws SQLException if there was a problem with the add partition statement
   */
  public QueryHandle addPartition(DatasetId datasetId, Map<String, String> properties,
                                  PartitionKey partitionKey, String fsPath)
    throws ExploreException, SQLException {

    StringBuilder str = new StringBuilder("ALTER TABLE ");
    String database = ExploreProperties.getExploreDatabaseName(properties);
    if (database != null) {
      str.append(database).append(".");
    }
    str.append(tableNaming.getTableName(datasetId, properties))
      .append(" ADD PARTITION ")
      .append(generateHivePartitionKey(partitionKey))
      .append(" LOCATION '")
      .append(fsPath)
      .append("'");
    String addPartitionStatement = str.toString();

    LOG.debug("Add partition for key {} dataset {} - {}", partitionKey, datasetId, addPartitionStatement);

    return exploreService.execute(datasetId.getParent(), addPartitionStatement);
  }

  /**
   * Drop a partition from the Hive table for the given dataset.
   *
   * @param datasetId the ID of the dataset to drop the partition from
   * @param properties additional dataset properties relevant to this operation
   * @param partitionKey the partition key to drop
   * @return the query handle for dropping the partition from the dataset
   * @throws ExploreException if there was an exception dropping the partition
   * @throws SQLException if there was a problem with the drop partition statement
   */
  public QueryHandle dropPartition(DatasetId datasetId, Map<String, String> properties, PartitionKey partitionKey)
    throws ExploreException, SQLException {

    StringBuilder str = new StringBuilder("ALTER TABLE ");
    String database = ExploreProperties.getExploreDatabaseName(properties);
    if (database != null) {
      str.append(database).append(".");
    }
    str.append(tableNaming.getTableName(datasetId, properties))
      .append(" DROP PARTITION ")
      .append(generateHivePartitionKey(partitionKey));
    String dropPartitionStatement = str.toString();

    LOG.debug("Drop partition for key {} dataset {} - {}", partitionKey, datasetId, dropPartitionStatement);

    return exploreService.execute(datasetId.getParent(), dropPartitionStatement, IMMEDIATE_TIMEOUT_CONF);
  }

  /**
   * Generate a Hive DDL statement to create a Hive table for the given dataset.
   *
   * @param dataset the instantiated dataset
   * @param spec the dataset specification
   * @param datasetId the dataset id
   * @param truncating whether this call to create() is part of a truncate() operation, which is in some
   *                   case implemented using disableExplore() followed by enableExplore()
   *
   * @return a CREATE TABLE statement, or null if the dataset is not explorable
   * @throws UnsupportedTypeException if the dataset is a RecordScannable of a type that is not supported by Hive
   */
  @Nullable
  private String generateEnableStatement(Dataset dataset, DatasetSpecification spec,
                                         DatasetId datasetId, String tableName, boolean truncating)
    throws UnsupportedTypeException, ExploreException {

    String datasetName = datasetId.getDataset();
    Map<String, String> serdeProperties = ImmutableMap.of(
      Constants.Explore.DATASET_NAME, datasetId.getDataset(),
      Constants.Explore.DATASET_NAMESPACE, datasetId.getNamespace());

    // TODO: refactor exploration (CDAP-1573)
    // explore should only have logic related to exploration and not dataset logic.
    // CDAP-1573: all these instanceofs are a sign that this logic really belongs in each dataset instead of here
    // To be enabled for explore, a dataset must either be RecordScannable/Writable,
    // or it must be a FileSet or a PartitionedFileSet with explore enabled in it properties.
    if (dataset instanceof Table) {
      // valid for a table not to have a schema property. this logic should really be in Table
      return generateCreateStatementFromSchemaProperty(spec, datasetId, tableName, serdeProperties, false);
    }
    if (dataset instanceof ObjectMappedTable) {
      return generateCreateStatementFromSchemaProperty(spec, datasetId, tableName, serdeProperties, true);
    }

    boolean isRecordScannable = dataset instanceof RecordScannable;
    boolean isRecordWritable = dataset instanceof RecordWritable;
    if (isRecordScannable || isRecordWritable) {
      Type recordType = isRecordScannable ?
        ((RecordScannable) dataset).getRecordType() : ((RecordWritable) dataset).getRecordType();

      // if the type is a structured record, use the schema property to create the table
      // Use == because that's what same class means.
      if (StructuredRecord.class == recordType) {
        return generateCreateStatementFromSchemaProperty(spec, datasetId, tableName, serdeProperties, true);
      }

      // otherwise, derive the schema from the record type
      LOG.debug("Enabling explore for dataset instance {}", datasetName);
      String databaseName = ExploreProperties.getExploreDatabaseName(spec.getProperties());
      return new CreateStatementBuilder(datasetName, databaseName, tableName, shouldEscapeColumns)
        .setSchema(hiveSchemaFor(recordType))
        .setTableComment("CDAP Dataset")
        .buildWithStorageHandler(DatasetStorageHandler.class.getName(), serdeProperties);

    } else if (dataset instanceof FileSet || dataset instanceof PartitionedFileSet) {
      Map<String, String> properties = spec.getProperties();
      if (FileSetProperties.isExploreEnabled(properties)) {
        LOG.debug("Enabling explore for dataset instance {}", datasetName);
        return generateFileSetCreateStatement(datasetId, dataset, properties, truncating);
      }
    }
    // dataset is not explorable
    return null;
  }

  /**
   * Generate a create statement from the "schema" property of the dataset (specification). This is used for
   * Table, ObjectMappedTable and RecordScannables with record type StructuredRecord, all of which use the
   * {@link DatasetStorageHandler}.
   *
   * @param spec the dataset specification
   * @param datasetId the dataset id
   * @param serdeProperties properties to be passed to the {@link co.cask.cdap.hive.datasets.DatasetSerDe}
   * @param shouldErrorOnMissingSchema whether the schema is required.
   * @return a CREATE TABLE statement, or null if the dataset is not explorable
   * @throws UnsupportedTypeException if the schema cannot be represented in Hive
   * @throws IllegalArgumentException if the schema cannot be parsed, or if shouldErrorOnMissingSchema is true and
   *                                  the dataset spec does not contain a schema.
   */
  @Nullable
  private String generateCreateStatementFromSchemaProperty(DatasetSpecification spec, DatasetId datasetId,
                                                           String tableName, Map<String, String> serdeProperties,
                                                           boolean shouldErrorOnMissingSchema)
    throws UnsupportedTypeException {

    Schema schema = getSchemaFromProperty(spec, datasetId, shouldErrorOnMissingSchema);
    if (schema == null) {
      return null;
    }
    String databaseName = ExploreProperties.getExploreDatabaseName(spec.getProperties());
    return new CreateStatementBuilder(datasetId.getDataset(), databaseName, tableName, shouldEscapeColumns)
      .setSchema(schema)
      .setTableComment("CDAP Dataset")
      .buildWithStorageHandler(DatasetStorageHandler.class.getName(), serdeProperties);
  }

  /**
   * Read the schema from a dataset specification.
   *
   * @param spec the dataset specification
   * @param datasetId the dataset id
   * @param shouldErrorOnMissingSchema whether the schema is required.
   * @return a valid Schema, or null if the dataset spec does not have a schema
   * @throws IllegalArgumentException if the schema cannot be parsed, or if shouldErrorOnMissingSchema is true and
   *                                  the dataset spec does not contain a schema.
   */
  @Nullable
  private Schema getSchemaFromProperty(DatasetSpecification spec, DatasetId datasetId,
                                       boolean shouldErrorOnMissingSchema) {

    String schemaStr = spec.getProperty(DatasetProperties.SCHEMA);
    // if there is no schema property, we cannot create the table and this is an error
    if (schemaStr == null) {
      if (shouldErrorOnMissingSchema) {
        throw new IllegalArgumentException(String.format(
          "Unable to enable exploration on dataset %s because the %s property is not set.",
          datasetId.getDataset(), DatasetProperties.SCHEMA));
      } else {
        return null;
      }
    }

    try {
      return Schema.parseJson(schemaStr);
    } catch (IOException e) {
      // shouldn't happen because datasets are supposed to verify this, but just in case
      throw new IllegalArgumentException("Unable to parse schema for dataset " + datasetId);
    }
  }

  /**
   * Generate a create statement for a ((time-)partitioned) file set.
   *
   * @param dataset the instantiated dataset
   * @param datasetId the dataset id
   * @param properties the properties from dataset specification
   * @param truncating whether this call to create() is part of a truncate() operation. The effect is:
   *                   If possessExisting is true, then the truncate() has just dropped this
   *                   dataset and that deleted the explore table: we must recreate it.

   * @return a CREATE TABLE statement, or null if the dataset is not explorable
   * @throws IllegalArgumentException if the schema cannot be parsed, or if shouldErrorOnMissingSchema is true and
   *                                  the dataset spec does not contain a schema.
   */
  @Nullable
  private String generateFileSetCreateStatement(DatasetId datasetId, Dataset dataset,
                                                Map<String, String> properties, boolean truncating)
    throws IllegalArgumentException, ExploreException {

    String tableName = tableNaming.getTableName(datasetId, properties);
    String databaseName = ExploreProperties.getExploreDatabaseName(properties);
    Map<String, String> tableProperties = FileSetProperties.getTableProperties(properties);

    // if this dataset reuses an existing table, do not attempt to create it
    if (FileSetProperties.isUseExisting(tableProperties)
      || (FileSetProperties.isPossessExisting(tableProperties) && !truncating)) {
      try {
        exploreService.getTableInfo(datasetId.getNamespace(), databaseName, tableName);
        // table exists: do not attempt to create
        return null;
      } catch (TableNotFoundException e) {
        throw new ExploreException(String.format(
          "Dataset '%s' is configured to use an existing explore table, but table '%s' does not " +
            "exist in database '%s'. ", datasetId.getDataset(), tableName, databaseName));
      }
    }

    Location baseLocation;
    Partitioning partitioning = null;
    if (dataset instanceof PartitionedFileSet) {
      partitioning = ((PartitionedFileSet) dataset).getPartitioning();
      baseLocation = ((PartitionedFileSet) dataset).getEmbeddedFileSet().getBaseLocation();
    } else {
      baseLocation = ((FileSet) dataset).getBaseLocation();
    }

    CreateStatementBuilder createStatementBuilder =
      new CreateStatementBuilder(datasetId.getDataset(), databaseName, tableName, shouldEscapeColumns)
        .setLocation(baseLocation)
        .setPartitioning(partitioning)
        .setTableProperties(tableProperties);

    String schema = FileSetProperties.getExploreSchema(properties);
    String format = FileSetProperties.getExploreFormat(properties);
    if (format != null) {
      if ("parquet".equals(format)) {
        return createStatementBuilder.setSchema(FileSetProperties.getExploreSchema(properties))
          .buildWithFileFormat("parquet");
      }
      // for text and csv, we know what to do
      Preconditions.checkArgument("text".equals(format) || "csv".equals(format),
                                  "Only text and csv are supported as native formats");
      Preconditions.checkNotNull(schema, "for native formats, explore schema must be given in dataset properties");
      String delimiter = null;
      if ("text".equals(format)) {
        delimiter = FileSetProperties.getExploreFormatProperties(properties).get("delimiter");
      } else if ("csv".equals(format)) {
        delimiter = ",";
      }
      return createStatementBuilder.setSchema(schema)
        .setRowFormatDelimited(delimiter, null)
        .buildWithFileFormat("TEXTFILE");
    } else {
      // for some odd reason, avro tables don't require schema.
      // They can be created by setting the avro.schema.literal table property
      if (schema != null) {
        createStatementBuilder.setSchema(schema);
      }
      // format not given, look for serde, input format, etc.
      String serde = FileSetProperties.getSerDe(properties);
      String inputFormat = FileSetProperties.getExploreInputFormat(properties);
      String outputFormat = FileSetProperties.getExploreOutputFormat(properties);

      Preconditions.checkArgument(serde != null && inputFormat != null && outputFormat != null,
                                  "All of SerDe, InputFormat and OutputFormat must be given in dataset properties");
      return createStatementBuilder.setRowFormatSerde(serde)
        .buildWithFormats(inputFormat, outputFormat);
    }
  }

  /**
   * Generate a Hive DDL statement to delete the Hive table for the given dataset.
   *
   * @param dataset the instantiated dataset
   * @param databaseName database name in which to delete the table. If null, the database is implied by the namespace.
   * @param tableName the table name corresponding to the dataset
   * @return a DROP TABLE statement, or null if the dataset is not explorable
   */
  @Nullable
  private String generateDeleteStatement(Dataset dataset, @Nullable String databaseName, String tableName) {
    if (dataset instanceof RecordScannable || dataset instanceof RecordWritable ||
      dataset instanceof FileSet || dataset instanceof PartitionedFileSet) {
      return generateDeleteTableStatement(databaseName, tableName);
    }
    return null;
  }

  /**
   * Generates a sequence of SQL statements that alter the table for a dataset after its spec was updated.
   * @param datasetId the dataset id
   * @param tableName the name of the Hive table to alter
   * @param dataset the instantiated dataset
   * @param spec the new dataset specification
   * @param oldSpec the old dataset specification (before the update)
   * @return a list of statements to execute, or null if the table needs no altering
   * @throws UnsupportedTypeException if the table schema is given by a type that cannot be represented in Hive
   */
  @Nullable
  private List<String> generateAlterStatements(DatasetId datasetId, String tableName, Dataset dataset,
                                               DatasetSpecification spec, DatasetSpecification oldSpec)
    throws UnsupportedTypeException, ExploreException {

    Map<String, String> properties = spec.getProperties();
    String databaseName = ExploreProperties.getExploreDatabaseName(properties);

    if (dataset instanceof FileSet || dataset instanceof PartitionedFileSet) {
      if (FileSetProperties.isExploreEnabled(properties)) {
        // if the dataset is reusing an existing explore table, do not attempt top alter it
        if (FileSetProperties.isUseExisting(spec.getProperties())) {
          return null;
        }
        return generateFileSetAlterStatements(datasetId, tableName, properties, oldSpec.getProperties());
      } else {
        // old spec was explorable but new spec is not -> disable explore
        return Collections.singletonList(generateDeleteStatement(dataset, databaseName, tableName));
      }
    }

    // all other dataset types use DatasetStorageHandler and DatasetSerDe. ALTER TABLE is not supported for these
    // datasets (because Hive does not allow altering a table in non-native format).
    // see: https://cwiki.apache.org/confluence/display/Hive/StorageHandlers#StorageHandlers-DDL
    // therefore we have no choice but to drop and recreate the dataset as if the dataset were truncated.
    String deleteStatement = generateDeleteStatement(dataset, databaseName, tableName);
    String createStatement = generateEnableStatement(dataset, spec, datasetId, tableName, true);
    List<String> statements = new ArrayList<>();
    if (deleteStatement != null) {
      statements.add(deleteStatement);
    }
    if (createStatement != null) {
      statements.add(createStatement);
    }
    return statements;
  }

  /**
   * Generates a sequence of SQL statements that alter the table for a file set dataset after its spec was updated.
   * @param datasetId the dataset id
   * @param tableName the name of the Hive table to alter
   * @param properties the properties from the new dataset specification
   * @param oldProperties the properties from the old dataset specification (before the update)
   * @return a list of statements to execute, or null if the table needs no altering
   */
  @Nullable
  private List<String> generateFileSetAlterStatements(DatasetId datasetId, String tableName,
                                                      Map<String, String> properties,
                                                      Map<String, String> oldProperties)
    throws IllegalArgumentException {

    String datasetName = datasetId.getDataset();
    String databaseName = ExploreProperties.getExploreDatabaseName(properties);
    List<String> alterStatements = new ArrayList<>();

    Map<String, String> tableProperties = FileSetProperties.getTableProperties(properties);
    Map<String, String> oldTableProperties = FileSetProperties.getTableProperties(oldProperties);
    if (!oldTableProperties.equals(tableProperties)) {
      alterStatements.add(new AlterStatementBuilder(datasetName, databaseName, tableName, shouldEscapeColumns)
                            .buildWithTableProperties(tableProperties));
    }

    String format = FileSetProperties.getExploreFormat(properties);
    Map<String, String> formatProps = FileSetProperties.getExploreFormatProperties(properties);
    String oldFormat = FileSetProperties.getExploreFormat(oldProperties);
    Map<String, String> oldFormatProps = FileSetProperties.getExploreFormatProperties(oldProperties);

    if (format != null) {
      if (!(format.equals(oldFormat) && formatProps.equals(oldFormatProps))) {
        if ("parquet".equals(format)) {
          alterStatements.add(new AlterStatementBuilder(datasetName, databaseName, tableName, shouldEscapeColumns)
                                .buildWithFileFormat("parquet"));
          return alterStatements;
        }
        // for text and csv, we know what to do
        Preconditions.checkArgument("text".equals(format) || "csv".equals(format),
                                    "Only text and csv are supported as native formats");
        String schema = FileSetProperties.getExploreSchema(properties);
        Preconditions.checkNotNull(schema, "for native formats, explore schema must be given in dataset properties");
        String delimiter = null;
        if ("text".equals(format)) {
          delimiter = FileSetProperties.getExploreFormatProperties(properties).get("delimiter");
        } else if ("csv".equals(format)) {
          delimiter = ",";
        }
        alterStatements.add(new AlterStatementBuilder(datasetName, databaseName, tableName, shouldEscapeColumns)
                              .buildWithDelimiter(delimiter));
      }
    } else {
      // format not given, look for serde, input format, etc.
      String serde = FileSetProperties.getSerDe(properties);
      String oldSerde = FileSetProperties.getSerDe(oldProperties);
      String inputFormat = FileSetProperties.getExploreInputFormat(properties);
      String oldInputFormat = FileSetProperties.getExploreInputFormat(oldProperties);
      String outputFormat = FileSetProperties.getExploreOutputFormat(properties);
      String oldOutputFormat = FileSetProperties.getExploreOutputFormat(oldProperties);

      Preconditions.checkArgument(serde != null && inputFormat != null && outputFormat != null,
                                  "All of SerDe, InputFormat and OutputFormat must be given in dataset properties");

      if (!inputFormat.equals(oldInputFormat) || !outputFormat.equals(oldOutputFormat) || !serde.equals(oldSerde)) {
        alterStatements.add(new AlterStatementBuilder(datasetName, databaseName, tableName, shouldEscapeColumns)
                              .buildWithFormats(inputFormat, outputFormat, serde));
      }
    }

    if (!Objects.equals(FileSetProperties.getBasePath(properties), FileSetProperties.getBasePath(oldProperties))) {
      // we cannot move an external table in Hive: all existing partitions will still have the old absolute path
      throw new IllegalArgumentException("Cannot change base path for dataset. Disable and re-enable explore instead.");
      // TODO (CDAP-6626): find a way to do this. It makes sense for an external file set to change its location
      // Location baseLocation = dataset instanceof PartitionedFileSet
      //   ? ((PartitionedFileSet) dataset).getEmbeddedFileSet().getBaseLocation()
      //   : ((FileSet) dataset).getBaseLocation();
      // alterStatements.add(new AlterStatementBuilder(datasetId.getDataset(), tableName, shouldEscapeColumns)
      //                      .buildWithLocation(baseLocation));
    }

    String schema = FileSetProperties.getExploreSchema(properties);
    String oldSchema = FileSetProperties.getExploreSchema(oldProperties);
    if (schema != null && !schema.equals(oldSchema)) {
      alterStatements.add(new AlterStatementBuilder(datasetName, databaseName, tableName, shouldEscapeColumns)
                            .buildWithSchema(schema));
    }

    return alterStatements;
  }

  private String generateDeleteTableStatement(@Nullable String databaseName, String name) {
    if (databaseName != null) {
      return String.format("DROP TABLE IF EXISTS %s.%s", databaseName, tableNaming.cleanTableName(name));
    } else {
      return String.format("DROP TABLE IF EXISTS %s", tableNaming.cleanTableName(name));
    }
  }

  private String generateHivePartitionKey(PartitionKey key) {
    StringBuilder builder = new StringBuilder("(");
    String sep = "";
    for (Map.Entry<String, ? extends Comparable> entry : key.getFields().entrySet()) {
      String fieldName = entry.getKey();
      Comparable fieldValue = entry.getValue();
      String quote = fieldValue instanceof String ? "'" : "";
      builder.append(sep);
      if (shouldEscapeColumns) {
        // a literal backtick(`) is just a double backtick(``)
        builder.append('`').append(fieldName.replace("`", "``")).append('`');
      } else {
        builder.append(fieldName);
      }
      builder.append("=").append(quote).append(fieldValue.toString()).append(quote);
      sep = ", ";
    }
    builder.append(")");
    return builder.toString();
  }

  // TODO: replace with SchemaConverter.toHiveSchema when we tackle queries on Tables.
  //       but unfortunately, SchemaConverter is not compatible with this, for example:
  //       - a byte becomes a tinyint here, but an int there
  //       - SchemaConverter sort fields alphabetically, whereas this preserves the order
  //       - ExploreExtensiveSchemaTableTestRun will fail because of this

  private String hiveSchemaFor(Type type) throws UnsupportedTypeException {
    // This call will make sure that the type is not recursive
    try {
      new ReflectionSchemaGenerator().generate(type, false);
    } catch (Exception e) {
      throw new UnsupportedTypeException("Unable to derive schema from " + type, e);
    }

    ObjectInspector objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(type);
    if (!(objectInspector instanceof StructObjectInspector)) {
      throw new UnsupportedTypeException(String.format("Type must be a RECORD, but is %s",
                                                       type.getClass().getName()));
    }
    StructObjectInspector structObjectInspector = (StructObjectInspector) objectInspector;

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (StructField structField : structObjectInspector.getAllStructFieldRefs()) {
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      ObjectInspector oi = structField.getFieldObjectInspector();
      String typeName;
      typeName = oi.getTypeName();
      if (shouldEscapeColumns) {
        // a literal backtick(`) is represented as a double backtick(``)
        sb.append('`').append(structField.getFieldName().replace("`", "``")).append('`');
      } else {
        sb.append(structField.getFieldName());
      }
      sb.append(" ").append(typeName);
    }

    return sb.toString();
  }
}
