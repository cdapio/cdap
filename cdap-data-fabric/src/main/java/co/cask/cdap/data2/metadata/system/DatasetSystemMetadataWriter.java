/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.system;

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.ObjectMappedTableProperties;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link AbstractSystemMetadataWriter} for a {@link Id.DatasetInstance dataset}.
 */
public class DatasetSystemMetadataWriter extends AbstractSystemMetadataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetSystemMetadataWriter.class);

  public static final String EXPLORE_TAG = "explore";
  public static final String BATCH_TAG = "batch";
  public static final String TYPE = "type";
  public static final String LOCAL_DATASET_TAG = "local-dataset";

  @VisibleForTesting
  static final String FILESET_AVRO_SCHEMA_PROPERTY = "avro.schema.literal";
  static final String FILESET_PARQUET_SCHEMA_OUTPUT_KEY = "parquet.avro.schema";
  static final String FILESET_AVRO_SCHEMA_OUTPUT_KEY = "avro.schema.output.key";

  private final Id.DatasetInstance dsInstance;
  private final String dsType;
  private final DatasetProperties dsProperties;
  private final Dataset dataset;
  private final long createTime;
  private final String description;

  public DatasetSystemMetadataWriter(MetadataStore metadataStore,
                                     Id.DatasetInstance dsInstance, DatasetProperties dsProperties,
                                     @Nullable Dataset dataset, @Nullable String dsType,
                                     @Nullable String description) {
    this(metadataStore, dsInstance, dsProperties, -1, dataset, dsType, description);
  }

  public DatasetSystemMetadataWriter(MetadataStore metadataStore,
                                     Id.DatasetInstance dsInstance, DatasetProperties dsProperties,
                                     long createTime,
                                     @Nullable Dataset dataset, @Nullable String dsType,
                                     @Nullable String description) {
    super(metadataStore, dsInstance);
    this.dsInstance = dsInstance;
    this.dsType = dsType;
    this.dsProperties = dsProperties;
    this.createTime = createTime;
    this.dataset = dataset;
    this.description = description;
    if (dataset == null) {
      LOG.warn("Dataset {} is null, some metadata will not be recorded for the dataset", dsInstance);
    }
  }

  @Override
  protected Map<String, String> getSystemPropertiesToAdd() {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    Map<String, String> datasetProperties = dsProperties.getProperties();
    if (dsType != null) {
      properties.put(TYPE, dsType);
    }
    if (datasetProperties.containsKey(Table.PROPERTY_TTL)) {
      properties.put(TTL_KEY, datasetProperties.get(Table.PROPERTY_TTL));
    }
    if (description != null) {
      properties.put(DESCRIPTION, description);
    }
    if (createTime > 0) {
      properties.put(CREATION_TIME, String.valueOf(createTime));
    }
    return properties.build();
  }

  @Override
  protected String[] getSystemTagsToAdd() {
    List<String> tags = new ArrayList<>();
    tags.add(dsInstance.getId());
    if (dataset instanceof RecordScannable) {
      tags.add(EXPLORE_TAG);
    }
    if (dataset instanceof FileSet || dataset instanceof PartitionedFileSet) {
      if (FileSetProperties.isExploreEnabled(dsProperties.getProperties())) {
        tags.add(EXPLORE_TAG);
      }
    }
    if (dataset instanceof BatchReadable || dataset instanceof BatchWritable ||
      dataset instanceof InputFormatProvider || dataset instanceof OutputFormatProvider) {
      tags.add(BATCH_TAG);
    }

    boolean isLocalDataset
      = Boolean.parseBoolean(dsProperties.getProperties().get(Constants.AppFabric.WORKFLOW_LOCAL_DATASET_PROPERTY));

    if (isLocalDataset) {
      tags.add(LOCAL_DATASET_TAG);
    }
    return tags.toArray(new String[tags.size()]);
  }

  @Nullable
  @Override
  protected String getSchemaToAdd() {
    // TODO: fix schema determination after CDAP-2790 is fixed (CDAP-5408)
    Map<String, String> datasetProperties = dsProperties.getProperties();
    String schemaStr = null;
    if (datasetProperties.containsKey(DatasetProperties.SCHEMA)) {
      schemaStr = datasetProperties.get(DatasetProperties.SCHEMA);
    } else if (datasetProperties.containsKey(ObjectMappedTableProperties.OBJECT_SCHEMA)) {
      // If it is an ObjectMappedTable, the schema is in a property called 'object.schema'
      schemaStr = datasetProperties.get(ObjectMappedTableProperties.OBJECT_SCHEMA);
    } else if (datasetProperties.containsKey(getExplorePropName(FILESET_AVRO_SCHEMA_PROPERTY))) {
      // Fileset with avro schema (CDAP-5322)
      schemaStr = datasetProperties.get(getExplorePropName(FILESET_AVRO_SCHEMA_PROPERTY));
    } else if (datasetProperties.containsKey(getOutputPropName(FILESET_AVRO_SCHEMA_OUTPUT_KEY))) {
      // Fileset with avro schema defined in output property (CDAP-5322)
      schemaStr = datasetProperties.get(getOutputPropName(FILESET_AVRO_SCHEMA_OUTPUT_KEY));
    } else if (datasetProperties.containsKey(getOutputPropName(FILESET_PARQUET_SCHEMA_OUTPUT_KEY))) {
      // Fileset with parquet schema defined in output property (CDAP-5322)
      schemaStr = datasetProperties.get(getOutputPropName(FILESET_PARQUET_SCHEMA_OUTPUT_KEY));
    }
    return schemaStr;
  }

  private static String getExplorePropName(String prop) {
    return FileSetProperties.PROPERTY_EXPLORE_TABLE_PROPERTY_PREFIX + prop;
  }

  private static String getOutputPropName(String prop) {
    return FileSetProperties.OUTPUT_PROPERTIES_PREFIX + prop;
  }
}
