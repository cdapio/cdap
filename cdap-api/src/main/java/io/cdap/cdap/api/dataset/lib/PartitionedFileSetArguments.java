/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package io.cdap.cdap.api.dataset.lib;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.dataset.lib.Partitioning.FieldType;
import io.cdap.cdap.api.dataset.lib.partitioned.PartitionKeyCodec;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
  public static final String DYNAMIC_PARTITIONER_WRITE_OPTION = "output.dynamic.partitioner.write.option";
  public static final String DYNAMIC_PARTITIONER_ALLOW_CONCURRENCY = "output.dynamic.partitioner.allow.concurrency";
  public static final String INPUT_PARTITION_FILTER = "input.partition.filter";
  public static final String INPUT_PARTITION_LIST = "input.partition.list";

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(PartitionFilter.Condition.class, new ConditionCodec())
      .registerTypeAdapter(PartitionKey.class, new PartitionKeyCodec())
      .create();
  private static final Type PARTITION_FILTER_LIST_TYPE = new TypeToken<List<PartitionFilter>>() {
  }.getType();
  private static final Type PARTITION_KEY_LIST_TYPE = new TypeToken<List<PartitionKey>>() {
  }.getType();

  /**
   * Set the partition key of the output partition when using PartitionedFileSet as an
   * OutputFormatProvider. This key is used as the partition key for the new file, and also to
   * generate an output file path - if that path is not explicitly given as an argument itself.
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
   * @param arguments the runtime arguments for a partitioned dataset
   * @param partitioning the declared partitioning for the dataset, needed for proper
   *     interpretation of values
   * @return the partition key of the output partition to be written; or null if no partition key
   *     was found
   */
  @Nullable
  public static PartitionKey getOutputPartitionKey(Map<String, String> arguments,
      Partitioning partitioning) {
    // extract the arguments that describe the output partition key
    Map<String, String> keyArguments = FileSetProperties.propertiesWithPrefix(arguments,
        OUTPUT_PARTITION_KEY_PREFIX);
    if (keyArguments.isEmpty()) {
      return null; // there is no output partition key
    }
    // there is a partition key; now it is required to match the partitioning
    PartitionKey.Builder builder = PartitionKey.builder();
    for (Map.Entry<String, FieldType> entry : partitioning.getFields().entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();
      String stringValue = keyArguments.get(fieldName);
      Comparable fieldValue = convertFieldValue("key", "value", fieldName, fieldType, stringValue,
          false);
      builder.addField(fieldName, fieldValue);
    }
    return builder.build();
  }


  /**
   * Sets the metadata of the output partition with using PartitionedFileSet as an
   * OutputFormatProvider.
   *
   * @param arguments the arguments to set the metadata in to
   * @param metadata the metadata to be written to the output partition
   */
  public static void setOutputPartitionMetadata(Map<String, String> arguments,
      Map<String, String> metadata) {
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
  public static void setInputPartitionFilter(Map<String, String> arguments,
      PartitionFilter filter) {
    // Serialize a singleton list for now. Support for multiple PartitionFilters can be added in the future.
    // See: https://cdap.atlassian.net/browse/CDAP-5618
    arguments.put(INPUT_PARTITION_FILTER, GSON.toJson(Collections.singletonList(filter)));
  }

  /**
   * Get the partition filter for the input to be read.
   *
   * @param arguments the runtime arguments for a partitioned dataset
   * @return the PartitionFilter specified in the arguments or null if no filter is specified.
   */
  @Nullable
  public static PartitionFilter getInputPartitionFilter(Map<String, String> arguments) {
    if (!arguments.containsKey(INPUT_PARTITION_FILTER)) {
      return null;
    }
    List<PartitionFilter> singletonList
        = GSON.fromJson(arguments.get(INPUT_PARTITION_FILTER), PARTITION_FILTER_LIST_TYPE);
    // this shouldn't happen based upon how we are serializing in #setInputPartitionFilter.
    // however, user might not use that method and attempt to specify/construct the runtime arguments themselves.
    if (singletonList.size() != 1) {
      throw new IllegalArgumentException(
          "Expected serialized list to have length 1. Actual: " + singletonList.size());
    }
    return singletonList.get(0);
  }

  // helper to convert a string value into a field value in a partition key or filter
  private static Comparable convertFieldValue(String where, String kind, String fieldName,
      FieldType fieldType, String stringValue, boolean acceptNull) {
    if (null == stringValue) {
      if (acceptNull) {
        return null;
      } else {
        throw new IllegalArgumentException(
            String.format("Incomplete partition %s: %s for field '%s' is missing", where, kind,
                fieldName));
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

  // TODO: should be able to have a method that just takes in PartitionKey, instead of Partition

  /**
   * Sets partitions as input for a PartitionedFileSet. If both a PartitionFilter and Partition(s)
   * are specified, the PartitionFilter takes precedence and the specified Partition(s) will be
   * ignored.
   *
   * @param arguments the runtime arguments for a partitioned dataset
   * @param partitionIterator the iterator of partitions to add as input
   */
  public static void addInputPartitions(Map<String, String> arguments,
      Iterator<? extends Partition> partitionIterator) {
    Collection<PartitionKey> inputPartitionKeys = new ArrayList<>(getInputPartitionKeys(arguments));
    while (partitionIterator.hasNext()) {
      inputPartitionKeys.add(partitionIterator.next().getPartitionKey());
    }
    arguments.put(INPUT_PARTITION_LIST, GSON.toJson(inputPartitionKeys));
  }

  /**
   * Sets partitions as input for a PartitionedFileSet. If both a PartitionFilter and Partition(s)
   * are specified, the PartitionFilter takes precedence and the specified Partition(s) will be
   * ignored.
   *
   * @param arguments the runtime arguments for a partitioned dataset
   * @param partitions an iterable of partitions to add as input
   */
  public static void addInputPartitions(Map<String, String> arguments,
      Iterable<? extends Partition> partitions) {
    addInputPartitions(arguments, partitions.iterator());
  }

  /**
   * Sets a partition as input for a PartitionedFileSet. If both a PartitionFilter and Partition(s)
   * are specified, the PartitionFilter takes precedence and the specified Partition(s) will be
   * ignored.
   *
   * @param arguments the runtime arguments for a partitioned dataset
   * @param partition the partition to add as input
   */
  public static void addInputPartition(Map<String, String> arguments, Partition partition) {
    addInputPartitions(arguments, Collections.singletonList(partition));
  }

  /**
   * Get the partition keys to be read for the input.
   *
   * @param arguments the runtime arguments for a partitioned dataset
   * @return a Collection containing the partition keys specified in the arguments. Empty collection
   *     if no keys were specified.
   */
  public static Collection<PartitionKey> getInputPartitionKeys(Map<String, String> arguments) {
    if (!arguments.containsKey(INPUT_PARTITION_LIST)) {
      return Collections.emptyList();
    }
    return GSON.fromJson(arguments.get(INPUT_PARTITION_LIST), PARTITION_KEY_LIST_TYPE);
  }

  /**
   * Sets a DynamicPartitioner class to be used during the output of a PartitionedFileSet. By
   * default, {@link DynamicPartitioner.PartitionWriteOption.NEW_ONLY} will be used.
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
   * @param dynamicPartitionerClass the class to set
   * @param partitionWriteOption options for the output partitions
   * @param <K> type of key
   * @param <V> type of value
   */
  public static <K, V> void setDynamicPartitioner(Map<String, String> arguments,
      Class<? extends DynamicPartitioner<K, V>> dynamicPartitionerClass,
      DynamicPartitioner.PartitionWriteOption partitionWriteOption) {
    setDynamicPartitioner(arguments, dynamicPartitionerClass.getName(), partitionWriteOption);
  }

  /**
   * Sets a DynamicPartitioner class to be used during the output of a PartitionedFileSet.
   *
   * @param arguments the runtime arguments for a partitioned dataset
   * @param dynamicPartitionerClassName the name of the class to set
   */
  public static void setDynamicPartitioner(Map<String, String> arguments,
      String dynamicPartitionerClassName) {
    setDynamicPartitioner(arguments, dynamicPartitionerClassName,
        DynamicPartitioner.PartitionWriteOption.CREATE);
  }

  /**
   * Sets a DynamicPartitioner class to be used during the output of a PartitionedFileSet.
   *
   * @param arguments the runtime arguments for a partitioned dataset
   * @param dynamicPartitionerClassName the name of the class to set
   * @param partitionWriteOption options for the output partitions
   */
  public static void setDynamicPartitioner(Map<String, String> arguments,
      String dynamicPartitionerClassName,
      DynamicPartitioner.PartitionWriteOption partitionWriteOption) {
    arguments.put(DYNAMIC_PARTITIONER_CLASS_NAME, dynamicPartitionerClassName);
    arguments.put(DYNAMIC_PARTITIONER_WRITE_OPTION, partitionWriteOption.name());
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

  /**
   * Return the DynamicPartitioner.PartitionWriteOption class that was previously assigned onto
   * runtime arguments.
   *
   * @param args the runtime arguments to get the class from
   * @return name of the DynamicPartitioner.PartitionWriteOption class
   */
  public static DynamicPartitioner.PartitionWriteOption getDynamicPartitionerWriteOption(
      Map<String, String> args) {
    ensureDynamicPartitionerConfigured(args);
    // if DynamicPartitioner is configured, we always also configure the following option, so we can safely call
    // Enum#valueOf(String)
    return DynamicPartitioner.PartitionWriteOption.valueOf(
        args.get(DYNAMIC_PARTITIONER_WRITE_OPTION));
  }

  /**
   * Sets whether concurrent writers are allowed when using DynamicPartitioner. When concurrency is
   * not allowed, the writer for a particular {@link PartitionKey} is closed when a new partition
   * key is encountered. If any partition key is encountered for which a corresponding writer has
   * already been closed, the job's Outputformat will throw an IllegalArgumentException. Therefore,
   * this should only be used if the data encountered in a task is grouped by the partition key
   * returned by the DynamicPartitioner.
   *
   * @param arguments arguments the runtime arguments to get the class from
   * @param allowConcurrency whether to allow multiple partition writers concurrently
   */
  public static void setDynamicPartitionerConcurrency(Map<String, String> arguments,
      boolean allowConcurrency) {
    ensureDynamicPartitionerConfigured(arguments);
    arguments.put(DYNAMIC_PARTITIONER_ALLOW_CONCURRENCY, Boolean.toString(allowConcurrency));
  }

  /**
   * Determines whether concurrent writers are allowed in the case of DynamicPartitioner.
   *
   * @param arguments arguments the runtime arguments to get the class from
   * @return whether concurrent writers are allowed in the case of DynamicPartitioner.
   */
  public static boolean isDynamicPartitionerConcurrencyAllowed(Map<String, String> arguments) {
    ensureDynamicPartitionerConfigured(arguments);
    String concurrencyAllowed = arguments.get(DYNAMIC_PARTITIONER_ALLOW_CONCURRENCY);
    if (concurrencyAllowed == null) {
      // default to true
      return true;
    }
    return Boolean.valueOf(concurrencyAllowed);
  }

  private static void ensureDynamicPartitionerConfigured(Map<String, String> args) {
    if (getDynamicPartitioner(args) == null) {
      throw new IllegalArgumentException(
          "Cannot get or set a setting of DynamicPartitioner, without first setting "
              + "a DynamicPartitioner.");
    }
  }
}
