/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.DatasetNotFoundException;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data2.dataset2.lib.table.ObjectMappedTableModule;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.explore.table.CreateStatementBuilder;
import co.cask.cdap.hive.objectinspector.ObjectInspectorFactory;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryHandle;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Executes disabling and enabling of datasets and streams and adding and dropping of partitions.
 */
public class ExploreTableManager {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreTableManager.class);

  private final ExploreService exploreService;
  private final SystemDatasetInstantiatorFactory datasetInstantiatorFactory;

  @Inject
  public ExploreTableManager(ExploreService exploreService,
                             SystemDatasetInstantiatorFactory datasetInstantiatorFactory) {
    this.exploreService = exploreService;
    this.datasetInstantiatorFactory = datasetInstantiatorFactory;
  }

  /**
   * Enable exploration on a stream by creating a corresponding Hive table. Enabling exploration on a
   * stream that has already been enabled is a no-op. Assumes the stream actually exists.
   *
   * @param streamID the ID of the stream
   * @param streamConfig the config for the stream
   * @return query handle for creating the Hive table for the stream
   * @throws UnsupportedTypeException if the stream schema is not compatible with Hive
   * @throws ExploreException if there was an exception submitting the create table statement
   * @throws SQLException if there was a problem with the create table statement
   */
  public QueryHandle enableStream(Id.Stream streamID, StreamConfig streamConfig)
    throws UnsupportedTypeException, ExploreException, SQLException {
    String streamName = streamID.getId();
    Location streamLocation = streamConfig.getLocation();
    LOG.debug("Enabling explore for stream {} at location {}", streamName, streamLocation.toURI());

    // schema of a stream is always timestamp, headers, and then the schema of the body.
    List<Schema.Field> fields = Lists.newArrayList(
      Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))));
    fields.addAll(streamConfig.getFormat().getSchema().getFields());
    Schema schema = Schema.recordOf("streamEvent", fields);

    Map<String, String> serdeProperties = ImmutableMap.of(
      Constants.Explore.STREAM_NAME, streamName,
      Constants.Explore.STREAM_NAMESPACE, streamID.getNamespaceId());

    String createStatement = new CreateStatementBuilder(streamName, getStreamTableName(streamID))
      .setSchema(schema)
      .setLocation(streamLocation)
      .setTableComment("CDAP Stream")
      .buildWithStorageHandler(Constants.Explore.STREAM_STORAGE_HANDLER_CLASS, serdeProperties);

    LOG.debug("Running create statement for stream {}", streamName);

    return exploreService.execute(streamID.getNamespace(), createStatement);
  }

  /**
   * Disable exploration on the given stream by dropping the Hive table for the stream.
   *
   * @param streamID the ID of the stream to disable
   * @return the query handle for disabling the stream
   * @throws ExploreException if there was an exception dropping the table
   * @throws SQLException if there was a problem with the drop table statement
   */
  public QueryHandle disableStream(Id.Stream streamID) throws ExploreException, SQLException {
    LOG.debug("Disabling explore for stream {}", streamID);
    String deleteStatement = generateDeleteStatement(getStreamTableName(streamID));
    return exploreService.execute(streamID.getNamespace(), deleteStatement);
  }

  /**
   * Enable ad-hoc exploration on the given dataset by creating a corresponding Hive table. If exploration has
   * already been enabled on the dataset, this will be a no-op. Assumes the dataset actually exists.
   *
   * @param datasetID the ID of the dataset to enable
   * @param spec the specification for the dataset to enable
   * @return query handle for creating the Hive table for the dataset
   * @throws IllegalArgumentException if some required dataset property like schema is not set
   * @throws UnsupportedTypeException if the schema of the dataset is not compatible with Hive
   * @throws ExploreException if there was an exception submitting the create table statement
   * @throws SQLException if there was a problem with the create table statement
   * @throws DatasetNotFoundException if the dataset had to be instantiated, but could not be found
   * @throws ClassNotFoundException if the was a missing class when instantiating the dataset
   */
  public QueryHandle enableDataset(Id.DatasetInstance datasetID, DatasetSpecification spec)
    throws IllegalArgumentException, ExploreException, SQLException,
    UnsupportedTypeException, DatasetNotFoundException, ClassNotFoundException {

    String datasetName = datasetID.getId();
    Map<String, String> serdeProperties = ImmutableMap.of(
      Constants.Explore.DATASET_NAME, datasetName,
      Constants.Explore.DATASET_NAMESPACE, datasetID.getNamespaceId());
    String createStatement = null;

    // some datasets cannot be instantiated here. For example, ObjectMappedTable is often parameterized with a type
    // that is only available in a program context and not available here in the system context.
    // explore should only have logic related to exploration and not dataset logic.
    // TODO: refactor exploration (CDAP-1573)
    String datasetType = spec.getType();
    // special casing here... but we really should clean this up
    // there are two ways to refer to each dataset type...
    if (ObjectMappedTableModule.FULL_NAME.equals(datasetType) ||
      ObjectMappedTableModule.SHORT_NAME.equals(datasetType)) {
      return createFromSchemaProperty(spec, datasetID, serdeProperties);
    }

    try (SystemDatasetInstantiator datasetInstantiator = datasetInstantiatorFactory.create()) {
      Dataset dataset = datasetInstantiator.getDataset(datasetID);
      if (dataset == null) {
        throw new DatasetNotFoundException(datasetID);
      }

      // To be enabled for explore, a dataset must either be RecordScannable/Writable,
      // or it must be a FileSet or a PartitionedFileSet with explore enabled in it properties.
      if (dataset instanceof Table) {
        return createFromSchemaProperty(spec, datasetID, serdeProperties);
      } else if (dataset instanceof RecordScannable || dataset instanceof RecordWritable) {
        LOG.debug("Enabling explore for dataset instance {}", datasetName);
        createStatement = new CreateStatementBuilder(datasetName, getDatasetTableName(datasetID))
          .setSchema(hiveSchemaFor(dataset))
          .setTableComment("CDAP Dataset")
          .buildWithStorageHandler(Constants.Explore.DATASET_STORAGE_HANDLER_CLASS, serdeProperties);
      } else if (dataset instanceof FileSet || dataset instanceof PartitionedFileSet) {
        Map<String, String> properties = spec.getProperties();
        if (FileSetProperties.isExploreEnabled(properties)) {
          LOG.debug("Enabling explore for dataset instance {}", datasetName);
          createStatement = generateFileSetCreateStatement(datasetID, dataset, properties);
        }
      }
    } catch (IOException e) {
      LOG.error("Exception instantiating dataset {}.", datasetID, e);
      throw new ExploreException("Exception while trying to instantiate dataset " + datasetID);
    }

    if (createStatement != null) {
      return exploreService.execute(datasetID.getNamespace(), createStatement);
    } else {
      // if the dataset is not explorable, this is a no op.
      return QueryHandle.NO_OP;
    }
  }

  private QueryHandle createFromSchemaProperty(DatasetSpecification spec, Id.DatasetInstance datasetID,
                                               Map<String, String> serdeProperties)
    throws ExploreException, SQLException, UnsupportedTypeException {

    String schemaStr = spec.getProperty(DatasetProperties.SCHEMA);
    // if there is no schema, this is a no-op.
    if (schemaStr == null) {
      return QueryHandle.NO_OP;
    }
    try {
      Schema schema = Schema.parseJson(schemaStr);
      String createStatement = new CreateStatementBuilder(datasetID.getId(), getDatasetTableName(datasetID))
        .setSchema(schema)
        .setTableComment("CDAP Dataset")
        .buildWithStorageHandler(Constants.Explore.DATASET_STORAGE_HANDLER_CLASS, serdeProperties);

      return exploreService.execute(datasetID.getNamespace(), createStatement);
    } catch (IOException e) {
      // shouldn't happen because datasets are supposed to verify this, but just in case
      throw new IllegalArgumentException("Unable to parse schema for dataset " + datasetID);
    }
  }

  /**
   * Disable exploration on the given dataset by dropping the Hive table for the dataset.
   *
   * @param datasetID the ID of the dataset to disable
   * @param spec the specification for the dataset to disable
   * @return the query handle for disabling the dataset
   * @throws ExploreException if there was an exception dropping the table
   * @throws SQLException if there was a problem with the drop table statement
   * @throws DatasetNotFoundException if the dataset had to be instantiated, but could not be found
   * @throws ClassNotFoundException if the was a missing class when instantiating the dataset
   */
  public QueryHandle disableDataset(Id.DatasetInstance datasetID, DatasetSpecification spec)
    throws ExploreException, SQLException, DatasetNotFoundException, ClassNotFoundException {
    LOG.debug("Disabling explore for dataset instance {}", datasetID);

    String tableName = getDatasetTableName(datasetID);
    // If table does not exist, nothing to be done
    try {
      exploreService.getTableInfo(datasetID.getNamespaceId(), tableName);
    } catch (TableNotFoundException e) {
      // Ignore exception, since this means table was not found.
      return QueryHandle.NO_OP;
    }

    String deleteStatement = null;
    String datasetType = spec.getType();
    if (ObjectMappedTableModule.FULL_NAME.equals(datasetType) ||
      ObjectMappedTableModule.SHORT_NAME.equals(datasetType)) {
      deleteStatement = generateDeleteStatement(tableName);
      LOG.debug("Running delete statement for dataset {} - {}", datasetID, deleteStatement);
      return exploreService.execute(datasetID.getNamespace(), deleteStatement);
    }

    try (SystemDatasetInstantiator datasetInstantiator = datasetInstantiatorFactory.create()) {
      Dataset dataset = datasetInstantiator.getDataset(datasetID);
      if (dataset == null) {
        throw new DatasetNotFoundException(datasetID);
      }

      if (dataset instanceof RecordScannable || dataset instanceof RecordWritable) {
        deleteStatement = generateDeleteStatement(tableName);
      } else if (dataset instanceof FileSet || dataset instanceof PartitionedFileSet) {
        Map<String, String> properties = spec.getProperties();
        if (FileSetProperties.isExploreEnabled(properties)) {
          deleteStatement = generateDeleteStatement(tableName);
        }
      }
    } catch (IOException e) {
      LOG.error("Exception creating dataset classLoaderProvider for dataset {}.", datasetID, e);
      throw new ExploreException("Exception instantiating dataset " + datasetID);
    }

    if (deleteStatement != null) {
      LOG.debug("Running delete statement for dataset {} - {}", datasetID, deleteStatement);
      return exploreService.execute(datasetID.getNamespace(), deleteStatement);
    } else {
      return QueryHandle.NO_OP;
    }
  }

  /**
   * Adds a partition to the Hive table for the given dataset.
   *
   * @param datasetID the ID of the dataset to add a partition to
   * @param partitionKey the partition key to add
   * @param fsPath the path of the partition
   * @return the query handle for adding the partition the dataset
   * @throws ExploreException if there was an exception adding the partition
   * @throws SQLException if there was a problem with the add partition statement
   */
  public QueryHandle addPartition(Id.DatasetInstance datasetID, PartitionKey partitionKey, String fsPath)
    throws ExploreException, SQLException {
    String addPartitionStatement = String.format(
      "ALTER TABLE %s ADD PARTITION %s LOCATION '%s'",
      getDatasetTableName(datasetID), generateHivePartitionKey(partitionKey), fsPath);

    LOG.debug("Add partition for key {} dataset {} - {}", partitionKey, datasetID, addPartitionStatement);

    return exploreService.execute(datasetID.getNamespace(), addPartitionStatement);
  }


  /**
   * Adds multiple partitions to the Hive table for the given dataset.
   *
   * @param datasetID the ID of the dataset to add partitions to
   * @param partitionDetails a map of partition key to partition path
   * @return the query handle for adding partitions to the dataset
   * @throws ExploreException if there was an exception adding the partition
   * @throws SQLException if there was a problem with the add partition statement
   */
  public QueryHandle addPartitions(Id.DatasetInstance datasetID,
                                   Set<PartitionDetail> partitionDetails) throws ExploreException, SQLException {
    if (partitionDetails.isEmpty()) {
      return QueryHandle.NO_OP;
    }
    StringBuilder statement = new StringBuilder()
      .append("ALTER TABLE ")
      .append(getDatasetTableName(datasetID))
      .append(" ADD");
    for (PartitionDetail partitionDetail : partitionDetails) {
      statement.append(" PARTITION")
        .append(generateHivePartitionKey(partitionDetail.getPartitionKey()))
        .append(" LOCATION '")
        .append(partitionDetail.getRelativePath())
        .append("'");
    }

    LOG.debug("Adding partitions for dataset {}", datasetID);

    return exploreService.execute(datasetID.getNamespace(), statement.toString());
  }

  /**
   * Drop a partition from the Hive table for the given dataset.
   *
   * @param datasetID the ID of the dataset to drop the partition from
   * @param partitionKey the partition key to drop
   * @return the query handle for dropping the partition from the dataset
   * @throws ExploreException if there was an exception dropping the partition
   * @throws SQLException if there was a problem with the drop partition statement
   */
  public QueryHandle dropPartition(Id.DatasetInstance datasetID, PartitionKey partitionKey)
    throws ExploreException, SQLException {

    String dropPartitionStatement = String.format(
      "ALTER TABLE %s DROP PARTITION %s",
      getDatasetTableName(datasetID), generateHivePartitionKey(partitionKey));

    LOG.debug("Drop partition for key {} dataset {} - {}", partitionKey, datasetID, dropPartitionStatement);

    return exploreService.execute(datasetID.getNamespace(), dropPartitionStatement);
  }

  private String getStreamTableName(Id.Stream streamId) {
    return cleanHiveTableName(String.format("stream_%s", streamId.getId()));
  }

  private String getDatasetTableName(Id.DatasetInstance datasetID) {
    return cleanHiveTableName(String.format("dataset_%s", datasetID.getId()));
  }

  private String cleanHiveTableName(String name) {
    // Instance name is like cdap.user.my_table.
    // For now replace . with _ and - with _ since Hive tables cannot have . or _ in them.
    return name.replaceAll("\\.", "_").replaceAll("-", "_").toLowerCase();
  }

  private String generateFileSetCreateStatement(Id.DatasetInstance datasetID, Dataset dataset,
                                                Map<String, String> properties) throws IllegalArgumentException {

    String tableName = getDatasetTableName(datasetID);
    Map<String, String> tableProperties = FileSetProperties.getTableProperties(properties);

    Location baseLocation;
    Partitioning partitioning = null;
    if (dataset instanceof PartitionedFileSet) {
      partitioning = ((PartitionedFileSet) dataset).getPartitioning();
      baseLocation = ((PartitionedFileSet) dataset).getEmbeddedFileSet().getBaseLocation();
    } else {
      baseLocation = ((FileSet) dataset).getBaseLocation();
    }

    CreateStatementBuilder createStatementBuilder = new CreateStatementBuilder(datasetID.getId(), tableName)
      .setLocation(baseLocation)
      .setPartitioning(partitioning)
      .setTableProperties(tableProperties);

    String format = FileSetProperties.getExploreFormat(properties);
    if (format != null) {
      if ("parquet".equals(format)) {
        return createStatementBuilder.setSchema(FileSetProperties.getExploreSchema(properties))
          .buildWithFileFormat("parquet");
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
      return createStatementBuilder.setSchema(schema)
        .setRowFormatDelimited(delimiter, null)
        .buildWithFileFormat("TEXTFILE");
    } else {
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

  private String generateDeleteStatement(String name) {
    return String.format("DROP TABLE IF EXISTS %s", cleanHiveTableName(name));
  }

  private String generateHivePartitionKey(PartitionKey key) {
    StringBuilder builder = new StringBuilder("(");
    String sep = "";
    for (Map.Entry<String, ? extends Comparable> entry : key.getFields().entrySet()) {
      String fieldName = entry.getKey();
      Comparable fieldValue = entry.getValue();
      String quote = fieldValue instanceof String ? "'" : "";
      builder.append(sep).append(fieldName).append("=").append(quote).append(fieldValue.toString()).append(quote);
      sep = ", ";
    }
    builder.append(")");
    return builder.toString();
  }

  /**
   * Given a record-enabled dataset, its record type and generate a schema string compatible with Hive.
   *
   * @param dataset The data set
   * @return the hive schema
   * @throws UnsupportedTypeException if the dataset is neither RecordScannable, nor RecordWritable,
   * or if the row type is not a record or contains null types.
   */
  private String hiveSchemaFor(Dataset dataset) throws UnsupportedTypeException {
    if (dataset instanceof RecordScannable) {
      return hiveSchemaFor(((RecordScannable) dataset).getRecordType());
    } else if (dataset instanceof RecordWritable) {
      return hiveSchemaFor(((RecordWritable) dataset).getRecordType());
    }
    throw new UnsupportedTypeException("Dataset neither implements RecordScannable not RecordWritable.");
  }

  // TODO: replace with SchemaConverter.toHiveSchema when we tackle queries on Tables.
  private String hiveSchemaFor(Type type) throws UnsupportedTypeException {
    // This call will make sure that the type is not recursive
    new ReflectionSchemaGenerator().generate(type, false);

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
      sb.append(structField.getFieldName()).append(" ").append(typeName);
    }

    return sb.toString();
  }
}
