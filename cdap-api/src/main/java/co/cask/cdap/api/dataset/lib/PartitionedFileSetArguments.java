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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.lib.Partitioning.FieldType;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Helpers for manipulating runtime arguments of time-partitioned file sets.
 */
@Beta
public class PartitionedFileSetArguments {

  public static final String OUTPUT_PARTITION_KEY_PREFIX = "output.partition.key.";
  public static final String OUTPUT_PARTITION_METADATA_PREFIX = "output.partition.metadata.";
  public static final String DYNAMIC_PARTITIONER_CLASS_NAME = "output.dynamic.partitioner.class.name";
  public static final String INPUT_PARTITION_LOWER_PREFIX = "input.filter.lower.";
  public static final String INPUT_PARTITION_UPPER_PREFIX = "input.filter.upper.";
  public static final String INPUT_PARTITION_VALUE_PREFIX = "input.filter.value.";

  /**
   * Set the partition key of the output partition when using PartitionedFileSet as an OutputFormatProvider.
   * This key is used as the partition key for the new file, and also to generate an output file path - if that path
   * is not explicitly given as an argument itself.
   *
   * @param key the partition key
   * @param arguments the runtime arguments for a partitioned dataset
   */
  public static void setOutputPartitionKey(Map<String, String> arguments, PartitionKey key) {
    for (Map.Entry<String, ? extends Comparable> entry : key.getFields().entrySet()) {
      arguments.put(OUTPUT_PARTITION_KEY_PREFIX + entry.getKey(), entry.getValue().toString());
    }
  }

  /**
   * @return the partition key of the output partition to be written; or null if no partition key was found
   *
   * @param arguments the runtime arguments for a partitioned dataset
   * @param partitioning the declared partitioning for the dataset, needed for proper interpretation of values
   */
  @Nullable
  public static PartitionKey getOutputPartitionKey(Map<String, String> arguments, Partitioning partitioning) {
    // extract the arguments that describe the output partition key
    Map<String, String> keyArguments = FileSetProperties.propertiesWithPrefix(arguments, OUTPUT_PARTITION_KEY_PREFIX);
    if (keyArguments.isEmpty()) {
      return null; // there is no output partition key
    }
    // there is a partition key; now it is required to match the partitioning
    PartitionKey.Builder builder = PartitionKey.builder();
    for (Map.Entry<String, FieldType> entry : partitioning.getFields().entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();
      String stringValue = keyArguments.get(fieldName);
      Comparable fieldValue = convertFieldValue("key", "value", fieldName, fieldType, stringValue, false);
      builder.addField(fieldName, fieldValue);
    }
    return builder.build();
  }


  /**
   * Sets the metadata of the output partition with using PartitionedFileSet as an OutputFormatProvider.
   *
   * @param arguments the arguments to set the metadata in to
   * @param metadata the metadata to be written to the output partition
   */
  public static void setOutputPartitionMetadata(Map<String, String> arguments, Map<String, String> metadata) {
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      arguments.put(OUTPUT_PARTITION_METADATA_PREFIX + entry.getKey(), entry.getValue());
    }
  }

  /**
   * @return the metadata of the output partition to be written
   */
  public static Map<String, String> getOutputPartitionMetadata(Map<String, String> arguments) {
    return FileSetProperties.propertiesWithPrefix(arguments, OUTPUT_PARTITION_METADATA_PREFIX);
  }

  /**
   * Set the partition filter for the input to be read.
   *
   * @param arguments the runtime arguments for a partitioned dataset
   * @param filter The partition filter.
   */
  public static void setInputPartitionFilter(Map<String, String> arguments, PartitionFilter filter) {
    for (Map.Entry<String, PartitionFilter.Condition<? extends Comparable>> entry : filter.getConditions().entrySet()) {
      String fieldName = entry.getKey();
      PartitionFilter.Condition<? extends Comparable> condition = entry.getValue();
      if (condition.isSingleValue()) {
        arguments.put(INPUT_PARTITION_VALUE_PREFIX + fieldName, condition.getValue().toString());
      } else {
        if (condition.getLower() != null) {
          arguments.put(INPUT_PARTITION_LOWER_PREFIX + fieldName, condition.getLower().toString());
        }
        if (condition.getUpper() != null) {
          arguments.put(INPUT_PARTITION_UPPER_PREFIX + fieldName, condition.getUpper().toString());
        }
      }
    }
  }

  /**
   * Get the partition filter for the input to be read.
   *
   * @param arguments the runtime arguments for a partitioned dataset
   * @param partitioning the declared partitioning for the dataset, needed for proper interpretation of values
   * @return the PartitionFilter specified in the arguments or null if no filter is specified.
   */
  @Nullable
  public static PartitionFilter getInputPartitionFilter(Map<String, String> arguments, Partitioning partitioning) {
    PartitionFilter.Builder builder = PartitionFilter.builder();
    for (Map.Entry<String, FieldType> entry : partitioning.getFields().entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();

      // is it a single-value condition?
      String stringValue = arguments.get(INPUT_PARTITION_VALUE_PREFIX + fieldName);
      Comparable fieldValue = convertFieldValue("filter", "value", fieldName, fieldType, stringValue, true);
      if (null != fieldValue) {
        @SuppressWarnings({ "unchecked", "unused" }) // we know it's type safe, but Java does not
        PartitionFilter.Builder unused = builder.addValueCondition(fieldName, fieldValue);
        continue;
      }
      // must be a range condition
      String stringLower = arguments.get(INPUT_PARTITION_LOWER_PREFIX + fieldName);
      String stringUpper = arguments.get(INPUT_PARTITION_UPPER_PREFIX + fieldName);
      Comparable lowerValue = convertFieldValue("filter", "lower bound", fieldName, fieldType, stringLower, true);
      Comparable upperValue = convertFieldValue("filter", "upper bound", fieldName, fieldType, stringUpper, true);
      if (null == lowerValue && null == upperValue) {
        continue; // this field was not present in the filter
      }
      @SuppressWarnings({ "unchecked", "unused" }) // we know it's type safe, but Java does not
      PartitionFilter.Builder unused = builder.addRangeCondition(fieldName, lowerValue, upperValue);
    }
    return builder.isEmpty() ? null : builder.build();
  }

  // helper to convert a string value into a field value in a partition key or filter
  public static Comparable convertFieldValue(String where, String kind, String fieldName,
                                             FieldType fieldType, String stringValue, boolean acceptNull) {
    if (null == stringValue) {
      if (acceptNull) {
        return null;
      } else {
        throw new IllegalArgumentException(
          String.format("Incomplete partition %s: %s for field '%s' is missing", where, kind, fieldName));
      }
    }
    try {
      return fieldType.parse(stringValue);
    } catch (Exception e) {
      throw new IllegalArgumentException(
        String.format("Invalid partition %s: %s '%s' for field '%s' cannot be converted to %s.",
                      where, kind, stringValue, fieldName, fieldType.name()), e);
    }
  }

  /**
   * Sets partitions as input for a PartitionedFileSet. If both a PartitionFilter and Partition(s) are specified, the
   * PartitionFilter takes precedence and the specified Partition(s) will be ignored.
   *
   * @param arguments the runtime arguments for a partitioned dataset
   * @param partitionIterator the iterator of partitions to add as input
   */
  public static void addInputPartitions(Map<String, String> arguments,
                                        Iterator<? extends Partition> partitionIterator) {
    while (partitionIterator.hasNext()) {
      addInputPartition(arguments, partitionIterator.next());
    }
  }

  /**
   * Sets partitions as input for a PartitionedFileSet. If both a PartitionFilter and Partition(s) are specified, the
   * PartitionFilter takes precedence and the specified Partition(s) will be ignored.
   *
   * @param arguments the runtime arguments for a partitioned dataset
   * @param partitions an iterable of partitions to add as input
   */
  public static void addInputPartitions(Map<String, String> arguments,
                                        Iterable<? extends Partition> partitions) {
    addInputPartitions(arguments, partitions.iterator());
  }

  /**
   * Sets a partition as input for a PartitionedFileSet. If both a PartitionFilter and Partition(s) are specified, the
   * PartitionFilter takes precedence and the specified Partition(s) will be ignored.
   *
   * @param arguments the runtime arguments for a partitioned dataset
   * @param partition the partition to add as input
   */
  public static void addInputPartition(Map<String, String> arguments, Partition partition) {
    FileSetArguments.addInputPath(arguments, partition.getRelativePath());
  }

  /**
   * Sets a DynamicPartitioner class to be used during the output of a PartitionedFileSet.
   *
   * @param arguments the runtime arguments for a partitioned dataset
   * @param dynamicPartitionerClass the class to set
   * @param <K> type of key
   * @param <V> type of value
   */
  public static <K, V> void setDynamicPartitioner(Map<String, String> arguments,
                                                  Class<? extends DynamicPartitioner<K, V>> dynamicPartitionerClass) {
    setDynamicPartitioner(arguments, dynamicPartitionerClass.getName());
  }

  /**
   * Sets a DynamicPartitioner class to be used during the output of a PartitionedFileSet.
   *
   * @param arguments the runtime arguments for a partitioned dataset
   * @param dynamicPartitionerClassName the name of the class to set
   */
  public static void setDynamicPartitioner(Map<String, String> arguments, String dynamicPartitionerClassName) {
    arguments.put(DYNAMIC_PARTITIONER_CLASS_NAME, dynamicPartitionerClassName);
  }

  /**
   * Return the DynamicPartitioner class that was previously assigned onto runtime arguments.
   *
   * @param arguments the runtime arguments to get the class from
   * @return name of the DynamicPartitioner class
   */
  public static String getDynamicPartitioner(Map<String, String> arguments) {
    return arguments.get(DYNAMIC_PARTITIONER_CLASS_NAME);
  }
}
