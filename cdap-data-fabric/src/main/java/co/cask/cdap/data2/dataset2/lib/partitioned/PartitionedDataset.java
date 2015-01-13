/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.partitioned;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.Partition;
import co.cask.cdap.api.dataset.lib.Partitioned;
import co.cask.cdap.api.dataset.lib.PartitionedProperties;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Implementation of partitioned datasets using a Table to store the meta data.
 */
public class PartitionedDataset extends AbstractDataset implements Partitioned {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionedDataset.class);
  private static final Type ARGUMENTS_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Gson GSON = new Gson();

  private static final String META_PREFIX = "m.";
  private static final byte[] ZERO = { 0x00 };
  private static final byte[] DATASET_NAME = { 'n' };
  private static final byte[] DATASET_ARGUMENTS = { 'a' };

  private final Table partitions;
  private final Map<String, FieldType> metaFields;

  public PartitionedDataset(String name, Table partitionTable, DatasetProperties properties) {
    super(name, partitionTable);
    this.metaFields = ImmutableMap.copyOf(PartitionedProperties.getMetadataFields(properties));
    this.partitions = partitionTable;
  }

  @Override
  public Map<String, FieldType> getMetaFields() {
    return metaFields;
  }

  @Override
  public Collection<Partition> getPartitions(Map<String, Object> metadata) {
    validateMetaQuery(metadata);
    List<Partition> result = Lists.newArrayList();
    Scanner scanner = partitions.scan(null, null);
    while (true) {
      Row row = scanner.next();
      if (row == null) {
        break;
      }
      Partition partition = matchRow(metadata, row);
      if (partition != null) {
        result.add(partition);
      }
    }
    return result;
  }

  @Override
  public void addPartition(Partition partition) {
    Put put = generatePut(partition.getMetadata());
    Row row = partitions.get(put.getRow());
    if (row != null && !row.isEmpty()) {
      throw new DataSetException(String.format("Dataset '%s' already has a partition with metadata: %s.",
                                 getName(), partition.getMetadata().toString()));
    }
    put.add(DATASET_NAME, partition.getDatasetName());
    put.add(DATASET_ARGUMENTS, GSON.toJson(partition.getArguments()));
    partitions.put(put);
  }

  @Override
  public void close() throws IOException {
    partitions.close();
  }

  @Override
  public <T> Class<? extends T> getInputFormatClass() {
    return null; // TODO auto generated body
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return null; // TODO auto generated body
  }

  private Put generatePut(Map<String, Object> meta) {
    if (!meta.keySet().equals(metaFields.keySet())) {
      throw new DataSetException(String.format(
        "Dataset '%s' declared with mta data fields %s but actual meta data has fields %s",
        getName(), Joiner.on(",").join(metaFields.keySet()), Joiner.on(",").join(meta.keySet())));
    }
    byte[] rowKey = { };
    Map<String, byte[]> byteFields = Maps.newHashMap();
    for (Map.Entry<String, FieldType> entry : metaFields.entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();
      Object fieldValue = meta.get(fieldName);
      byte[] byteValue = metaToBytes(fieldName, fieldType, fieldValue);
      byteFields.put(META_PREFIX + fieldName, byteValue);
      rowKey = Bytes.add(rowKey, ZERO, byteValue);
    }
    Put put = new Put(rowKey);
    for (Map.Entry<String, byte[]> entry : byteFields.entrySet()) {
      put.add(entry.getKey(), entry.getValue());
    }
    return put;
  }

  private Partition matchRow(Map<String, Object> metadata, Row row) {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (Map.Entry<String, Object> entry : metadata.entrySet()) {
      String fieldName = entry.getKey();
      byte[] value = row.get(META_PREFIX + fieldName);
      if (value == null) {
        // should not happen because we require all fields to be present, and the query is validated
        LOG.warn("This should never happen: Partition '{}' of dataset '{}' does not have field '{}'.",
                 Bytes.toStringBinary(row.getRow()), getName(), fieldName);
        return null;
      }
      FieldType fieldType = metaFields.get(fieldName);
      if (fieldType == null) {
        // cannot happen because the query is validated. if it does, we are in trouble.
        LOG.warn("This should never happen: Validated query for dataset '{}' contains undeclared field '{}'.",
                 getName(), fieldName);
        return null;
      }
      Object expectedValue = entry.getValue();
      Object actualValue = bytesToMeta(fieldName, fieldType, value);
      if (!expectedValue.equals(actualValue)) {
        return null;
      }
      builder.put(fieldName, actualValue);
    }
    String datasetName = row.getString(DATASET_NAME);
    Map<String, String> arguments = GSON.fromJson(row.getString(DATASET_ARGUMENTS), ARGUMENTS_TYPE);
    return new Partition(datasetName, arguments, builder.build());
  }


  private void validateMetaQuery(Map<String, Object> metadata) {
    for (Map.Entry<String, Object> entry : metadata.entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = metaFields.get(fieldName);
      if (fieldType == null) {
        throw new DataSetException(
          String.format("Field '%s' given in query but not declared as metadata for dataset '%s'",
                        fieldName, getName()));
      }
      Object value = entry.getValue();
      if (value == null) {
        throw new DataSetException(
          String.format("Field '%s' given as null in query for dataset '%s'", fieldName, getName()));
      }
      switch (fieldType) {
        case STRING:
          if (value instanceof String) {
            continue;
          }
          break;
        case LONG:
          if (value instanceof Long) {
            continue;
          }
          break;
        case DATE:
          if (value instanceof Date) {
            continue;
          }
          break;
        default:
          throw new RuntimeException(String.format(
            "Unexepected enum value '%s' for '%s'", fieldType.name(), FieldType.class.getName()));
      }
      throw new DataSetException(String.format(
        "Metadata field '%s' declared with type '%s' for dataset '%s', but actual value '%s' is of type '%s.",
        fieldName, fieldType.name(), getName(), value.toString(), value.getClass().getName()));
    }
  }

  private byte[] metaToBytes(String fieldName, FieldType fieldType, Object value) {
    if (value == null) {
      throw new DataSetException(String.format(
        "Null given as value for meta data field '%s' for dataset '%s'.", fieldName, getName()));
    }
    switch (fieldType) {
      case STRING:
        if (value instanceof String) {
          return Bytes.toBytes((String) value);
        }
        break;
      case LONG:
        if (value instanceof Long) {
          return Bytes.toBytes((Long) value);
        }
        break;
      case DATE:
        if (value instanceof Date) {
          return Bytes.toBytes(((Date) value).getTime());
        }
        break;
      default:
        throw new RuntimeException(String.format(
          "Unexepected enum value '%s' for '%s'", fieldType.name(), FieldType.class.getName()));
    }
    throw new DataSetException(String.format(
      "Metadata field '%s' declared with type '%s' for dataset '%s', but actual value '%s' is of type '%s.",
      fieldName, fieldType.name(), getName(), value.toString(), value.getClass().getName()));
  }

  private Object bytesToMeta(String fieldName, FieldType fieldType, byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    switch (fieldType) {
      case STRING:
        return Bytes.toString(bytes);
      case LONG:
        return Bytes.toLong(bytes);
      case DATE:
        return new Date(Bytes.toLong(bytes));
      default:
        throw new RuntimeException(String.format(
          "Unexepected enum value '%s' for '%s'", fieldType.name(), FieldType.class.getName()));
    }
  }

}
