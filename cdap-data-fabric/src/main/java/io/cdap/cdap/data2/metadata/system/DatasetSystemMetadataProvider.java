/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.system;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.batch.BatchReadable;
import io.cdap.cdap.api.data.batch.BatchWritable;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.dataset.lib.ObjectMappedTableProperties;
import io.cdap.cdap.api.dataset.table.TableProperties;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A {@link AbstractSystemMetadataWriter} for a {@link DatasetId dataset}.
 */
public class DatasetSystemMetadataProvider implements SystemMetadataProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetSystemMetadataProvider.class);

  public static final String BATCH_TAG = "batch";
  public static final String TYPE = "type";
  public static final String LOCAL_DATASET_TAG = "local-dataset";

  @VisibleForTesting
  static final String FILESET_AVRO_SCHEMA_PROPERTY = "avro.schema.literal";
  static final String FILESET_PARQUET_SCHEMA_OUTPUT_KEY = "parquet.avro.schema";
  static final String FILESET_AVRO_SCHEMA_OUTPUT_KEY = "avro.schema.output.key";

  private final DatasetId dsInstance;
  private final String dsType;
  private final DatasetProperties dsProperties;
  private final Dataset dataset;
  private final long createTime;
  private final String description;

  public DatasetSystemMetadataProvider(DatasetId dsInstance, DatasetProperties dsProperties,
                                       @Nullable Dataset dataset, @Nullable String dsType,
                                       @Nullable String description) {
    this(dsInstance, dsProperties, -1, dataset, dsType, description);
  }

  public DatasetSystemMetadataProvider(DatasetId dsInstance, DatasetProperties dsProperties,
                                       long createTime,
                                       @Nullable Dataset dataset, @Nullable String dsType,
                                       @Nullable String description) {
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
  public Map<String, String> getSystemPropertiesToAdd() {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    properties.put(MetadataConstants.ENTITY_NAME_KEY, dsInstance.getDataset());
    Map<String, String> datasetProperties = dsProperties.getProperties();
    if (dsType != null) {
      properties.put(TYPE, dsType);
    }
    // use TableProperties to extract the TTL, because it handles the case of a negative TTL (== no TTL)
    Long ttl = TableProperties.getTTL(datasetProperties);
    if (ttl != null) {
      properties.put(MetadataConstants.TTL_KEY, String.valueOf(ttl));
    }
    if (description != null) {
      properties.put(MetadataConstants.DESCRIPTION_KEY, description);
    }
    if (createTime > 0) {
      properties.put(MetadataConstants.CREATION_TIME_KEY, String.valueOf(createTime));
    }
    return properties.build();
  }

  @Override
  public Set<String> getSystemTagsToAdd() {
    Set<String> tags = new HashSet<>();
    if (dataset instanceof BatchReadable || dataset instanceof BatchWritable ||
      dataset instanceof InputFormatProvider || dataset instanceof OutputFormatProvider) {
      tags.add(BATCH_TAG);
    }

    boolean isLocalDataset
      = Boolean.parseBoolean(dsProperties.getProperties().get(Constants.AppFabric.WORKFLOW_LOCAL_DATASET_PROPERTY));

    if (isLocalDataset) {
      tags.add(LOCAL_DATASET_TAG);
    }
    return tags;
  }

  @Nullable
  @Override
  public String getSchemaToAdd() {
    // TODO: fix schema determination after CDAP-2790 is fixed (CDAP-5408)
    Map<String, String> datasetProperties = dsProperties.getProperties();
    String schemaStr = null;
    if (datasetProperties.containsKey(DatasetProperties.SCHEMA)) {
      schemaStr = datasetProperties.get(DatasetProperties.SCHEMA);
    } else if (datasetProperties.containsKey(ObjectMappedTableProperties.OBJECT_SCHEMA)) {
      // If it is an ObjectMappedTable, the schema is in a property called 'object.schema'
      schemaStr = datasetProperties.get(ObjectMappedTableProperties.OBJECT_SCHEMA);
    } else if (datasetProperties.containsKey(getOutputPropName(FILESET_AVRO_SCHEMA_OUTPUT_KEY))) {
      // Fileset with avro schema defined in output property (CDAP-5322)
      schemaStr = datasetProperties.get(getOutputPropName(FILESET_AVRO_SCHEMA_OUTPUT_KEY));
    } else if (datasetProperties.containsKey(getOutputPropName(FILESET_PARQUET_SCHEMA_OUTPUT_KEY))) {
      // Fileset with parquet schema defined in output property (CDAP-5322)
      schemaStr = datasetProperties.get(getOutputPropName(FILESET_PARQUET_SCHEMA_OUTPUT_KEY));
    }
    return schemaStr;
  }


  private static String getOutputPropName(String prop) {
    return FileSetProperties.OUTPUT_PROPERTIES_PREFIX + prop;
  }
}
