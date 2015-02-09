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

package co.cask.cdap.data2.dataset2.lib.partitioned;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.lib.Partitioning.FieldType;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.explore.client.ExploreFacade;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Provider;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of partitioned datasets using a Table to store the meta data.
 */
public class PartitionedFileSetDataset extends AbstractDataset implements PartitionedFileSet {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionedFileSetDataset.class);

  private static final byte[] RELATIVE_PATH = { 'p' };
  private static final byte[] FIELD_PREFIX = { 'f', '.' };

  private final FileSet files;
  private final Table partitions;
  private final Map<String, String> runtimeArguments;
  private final DatasetSpecification spec;
  private final Provider<ExploreFacade> exploreFacadeProvider;
  private final Partitioning partitioning;

  public PartitionedFileSetDataset(String name, Partitioning partitioning,
                                   FileSet fileSet, Table partitionTable,
                                   DatasetSpecification spec, Map<String, String> arguments,
                                   Provider<ExploreFacade> exploreFacadeProvider) {
    super(name, partitionTable);
    this.files = fileSet;
    this.partitions = partitionTable;
    this.spec = spec;
    this.exploreFacadeProvider = exploreFacadeProvider;
    this.runtimeArguments = arguments;
    this.partitioning = partitioning;
  }

  @Override
  public Partitioning getPartitioning() {
    return partitioning;
  }

  @Override
  public void addPartition(PartitionKey key, String path) {
    final byte[] rowKey = generateRowKey(key, partitioning);
    Row row = partitions.get(rowKey);
    if (row != null && !row.isEmpty()) {
      throw new DataSetException(String.format("Dataset '%s' already has a partition with the same key: %s",
                                               getName(), key.toString()));
    }
    Put put = new Put(rowKey);
    put.add(RELATIVE_PATH, Bytes.toBytes(path));
    for (Map.Entry<String, ? extends Comparable> entry : key.getFields().entrySet()) {
      put.add(Bytes.add(FIELD_PREFIX, Bytes.toBytes(entry.getKey())), // "f.<field name>"
              Bytes.toBytes(entry.getValue().toString()));            // "<string rep. of value>"
    }
    partitions.put(put);

    if (FileSetProperties.isExploreEnabled(spec.getProperties())) {
      ExploreFacade exploreFacade = exploreFacadeProvider.get();
      if (exploreFacade != null) {
        try {
          // TODO add partition in Hive (next PR)
          // exploreFacade.addPartition(getName(), key, files.getLocation(path).toURI().getPath());
        } catch (Exception e) {
          throw new DataSetException(String.format(
            "Unable to add partition for time %s with path %s to explore table.", key.toString(), path), e);
        }
      }
    }
  }

  @Override
  public void dropPartition(PartitionKey key) {
    final byte[] rowKey = generateRowKey(key, partitioning);
    partitions.delete(rowKey);

    if (FileSetProperties.isExploreEnabled(spec.getProperties())) {
      ExploreFacade exploreFacade = exploreFacadeProvider.get();
      if (exploreFacade != null) {
        try {
          // TODO drop partition in Hive (next PR)
          // exploreFacade.dropPartition(getName(), key);
        } catch (Exception e) {
          throw new DataSetException(String.format(
            "Unable to drop partition for time %s from explore table.", key.toString()), e);
        }
      }
    }
  }

  @Override
  public String getPartition(PartitionKey key) {
    final byte[] rowKey = generateRowKey(key, partitioning);
    Row row = partitions.get(rowKey);
    if (row == null) {
      return null;
    }
    byte[] pathBytes = row.get(RELATIVE_PATH);
    if (pathBytes == null) {
      return null;
    }
    return Bytes.toString(pathBytes);
  }

  @Override
  public Set<String> getPartitionPaths(PartitionFilter filter) {
    // this is the same as getPartitions(startTime, endTime).values(), but we want to avoid construction of the map
    final byte[] startKey = generateStartKey(filter);
    final byte[] endKey = generateStopKey(filter);
    Set<String> paths = Sets.newHashSet();
    Scanner scanner = partitions.scan(startKey, endKey);
    try {
      while (true) {
        Row row = scanner.next();
        if (row == null) {
          break;
        }
        if (!validateFilter(filter, row)) {
          continue;
        }
        byte[] pathBytes = row.get(RELATIVE_PATH);
        if (pathBytes != null) {
          paths.add(Bytes.toString(pathBytes));
        }
      }
      return paths;
    } finally {
      scanner.close();
    }
  }

  @Override
  public Map<PartitionKey, String> getPartitions(PartitionFilter filter) {
    final byte[] startKey = generateStartKey(filter);
    final byte[] endKey = generateStopKey(filter);
    Map<PartitionKey, String> paths = Maps.newHashMap();
    Scanner scanner = partitions.scan(startKey, endKey);
    try {
      while (true) {
        Row row = scanner.next();
        if (row == null) {
          break;
        }
        if (!validateFilter(filter, row)) {
          continue;
        }
        PartitionKey key = parseRowKey(row.getRow(), partitioning);
        byte[] pathBytes = row.get(RELATIVE_PATH);
        if (pathBytes != null) {
          paths.put(key, Bytes.toString(pathBytes));
        }
      }
      return paths;
    } finally {
      scanner.close();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      files.close();
    } finally {
      partitions.close();
    }
  }

  @Override
  public <T> Class<? extends T> getInputFormatClass() {
    return files.getInputFormatClass();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    PartitionFilter filter;
    try {
      filter = PartitionedFileSetArguments.getInputPartitionFilter(runtimeArguments, partitioning);
    } catch (Exception e) {
      throw new DataSetException("Partition filter must be correctly specified in arguments.");
    }
    Collection<String> inputPaths = getPartitionPaths(filter);
    List<Location> inputLocations = Lists.newArrayListWithExpectedSize(inputPaths.size());
    for (String path : inputPaths) {
      inputLocations.add(files.getLocation(path));
    }
    return files.getInputFormatConfiguration(inputLocations);
  }

  @Override
  public <T> Class<? extends T> getOutputFormatClass() {
    // we verify that the output partition time is configured in getOutputFormatConfiguration()
    // todo use a wrapper that adds the new partition when the job is committed - that means we must serialize the
    //      cconf into the hadoop conf to be able to instantiate this dataset in the output committer
    return files.getOutputFormatClass();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    // all runtime arguments are passed on to the file set, so we can expect the partition time in the file set's
    // output format configuration. If it is not there, the output format will fail to register this partition.
    Map<String, String> config = files.getOutputFormatConfiguration();
    Long time = TimePartitionedFileSetArguments.getOutputPartitionTime(runtimeArguments);
    if (time == null) {
      throw new DataSetException("Time must be given for the new output partition as a runtime argument.");
    }
    // add the output partition time to the output arguments of the embedded file set
    Map<String, String> outputArgs = Maps.newHashMap();
    outputArgs.putAll(config);
    TimePartitionedFileSetArguments.setOutputPartitionTime(outputArgs, time);
    return ImmutableMap.copyOf(outputArgs);
  }

  @Override
  public FileSet getEmbeddedFileSet() {
    return files;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  //------ private helpers below here --------------------------------------------------------------

  @VisibleForTesting
  static byte[] generateRowKey(PartitionKey key, Partitioning partitioning) {
    // validate partition key, convert values, and compute size of output
    Map<String, FieldType> partitionFields = partitioning.getFields();
    int totalSize = partitionFields.size() - 1; // one \0 between each of the fields
    ArrayList<byte[]> values = Lists.newArrayListWithCapacity(partitionFields.size());
    for (Map.Entry<String, FieldType> entry : partitionFields.entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();
      Comparable fieldValue = key.getField(fieldName);
      if (fieldValue == null) {
        throw new IllegalArgumentException(
          String.format("Incomplete partition key: value for field '%s' is missing", fieldName));
      }
      if (!FieldTypes.validateType(fieldValue, fieldType)) {
        throw new IllegalArgumentException(
          String.format("Invalid partition key: value for %s field '%s' has incompatible type %s",
                        fieldType.name(), fieldName, fieldValue.getClass().getName()));
      }
      byte[] bytes = FieldTypes.toBytes(fieldValue, fieldType);
      totalSize += bytes.length;
      values.add(bytes);
    }
    byte[] rowKey = new byte[totalSize];
    int offset = 0;
    for (byte[] bytes : values) {
      System.arraycopy(bytes, 0, rowKey, offset, bytes.length);
      offset += bytes.length;
      if (offset < rowKey.length) {
        rowKey[offset] = 0;
        offset++;
      }
    }
    return rowKey;
  }

  private byte[] generateStartKey(PartitionFilter filter) {
    // validate partition filter, convert values, and compute size of output
    Map<String, FieldType> partitionFields = partitioning.getFields();
    int totalSize = 0;
    ArrayList<byte[]> values = Lists.newArrayListWithCapacity(partitionFields.size());
    for (Map.Entry<String, FieldType> entry : partitionFields.entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();
      PartitionFilter.Condition<? extends Comparable> condition = filter.getCondition(fieldName);
      if (condition == null) {
        break; // this field is not present; we can't include any more fields in the start key
      }
      Comparable lowerValue = condition.getLower();
      if (lowerValue == null) {
        break; // this field has no lower bound; we can't include any more fields in the start key
      }
      if (!FieldTypes.validateType(lowerValue, fieldType)) {
        throw new IllegalArgumentException(
          String.format("Invalid partition filter: lower bound for %s field '%s' has incompatible type %s",
                        fieldType.name(), fieldName, lowerValue.getClass().getName()));
      }
      byte[] bytes = FieldTypes.toBytes(lowerValue, fieldType);
      totalSize += bytes.length;
      values.add(bytes);
    }
    if (values.isEmpty()) {
      return null;
    }
    totalSize += values.size() - 1; // one \0 between each of the fields
    byte[] startKey = new byte[totalSize];
    int offset = 0;
    for (byte[] bytes : values) {
      System.arraycopy(bytes, 0, startKey, offset, bytes.length);
      offset += bytes.length;
      if (offset < startKey.length) {
        startKey[offset] = 0;
        offset++;
      }
    }
    return startKey;
  }

  private byte[] generateStopKey(PartitionFilter filter) {
    // validate partition filter, convert values, and compute size of output
    Map<String, FieldType> partitionFields = partitioning.getFields();
    int totalSize = 0;
    boolean allSingleValue = true;
    ArrayList<byte[]> values = Lists.newArrayListWithCapacity(partitionFields.size());
    for (Map.Entry<String, FieldType> entry : partitionFields.entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();
      PartitionFilter.Condition<? extends Comparable> condition = filter.getCondition(fieldName);
      if (condition == null) {
        break; // this field is not present; we can't include any more fields in the stop key
      }
      Comparable upperValue = condition.getUpper();
      if (upperValue == null) {
        break; // this field is not present; we can't include any more fields in the stop key
      }
      if (!FieldTypes.validateType(upperValue, fieldType)) {
        throw new IllegalArgumentException(
          String.format("Invalid partition filter: upper bound for %s field '%s' has incompatible type %s",
                        fieldType.name(), fieldName, upperValue.getClass().getName()));
      }
      byte[] bytes = FieldTypes.toBytes(upperValue, fieldType);
      totalSize += bytes.length;
      values.add(bytes);
      if (!condition.isSingleValue()) {
        allSingleValue = false;
        break; // upper bound for this field, following fields don't matter
      }
    }
    if (values.isEmpty()) {
      return null;
    }
    totalSize += values.size() - 1; // one \0 between each of the fields
    if (allSingleValue) {
      totalSize++; // in this case the start and stop key are equal, we append one \1 to ensure the scan is not empty
    }
    byte[] stopKey = new byte[totalSize];
    int offset = 0;
    for (byte[] bytes : values) {
      System.arraycopy(bytes, 0, stopKey, offset, bytes.length);
      offset += bytes.length;
      if (offset < stopKey.length) {
        if (allSingleValue && offset == stopKey.length - 1) {
          stopKey[offset] = 1; // see above - append \1 to make sure scan is not empty
        } else {
          stopKey[offset] = 0;
          offset++;
        }
      }
    }
    return stopKey;
  }

  @VisibleForTesting
  static PartitionKey parseRowKey(byte[] rowKey, Partitioning partitioning) {
    PartitionKey.Builder builder = PartitionKey.builder();
    int offset = 0;
    boolean first = true;
    for (Map.Entry<String, FieldType> entry : partitioning.getFields().entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();
      if (!first) {
        if (offset >= rowKey.length) {
          throw new IllegalArgumentException(
            String.format("Invalid row key: Expecting field '%s' at offset %d " +
                            "but the end of the row key is reached.", fieldName, offset));
        }
        if (rowKey[offset] != 0) {
          throw new IllegalArgumentException(
            String.format("Invalid row key: Expecting field separator \\0 before field '%s' at offset %d " +
                            "but found byte value %x.", fieldName, offset, rowKey[offset]));
        }
        offset++;
      }
      first = false;
      int size = FieldTypes.determineLengthInBytes(rowKey, offset, fieldType);
      if (size + offset > rowKey.length) {
        throw new IllegalArgumentException(
          String.format("Invalid row key: Expecting field '%s' of type %s, " +
                          "requiring %d bytes at offset %d, but only %d bytes remain.",
                        fieldName, fieldType.name(), size, offset, rowKey.length - offset));
      }
      Comparable fieldValue = FieldTypes.fromBytes(rowKey, offset, size, fieldType);
      offset += size;
      builder.addField(fieldName, fieldValue);
    }
    if (offset != rowKey.length) {
      throw new IllegalArgumentException(
        String.format("Invalid row key: Read all fields at offset %d but %d extra bytes remain.",
                      offset, rowKey.length - offset));
    }
    return builder.build();
  }

  private boolean validateFilter(PartitionFilter filter, Row row) {
    PartitionKey key;
    try {
      key = parseRowKey(row.getRow(), partitioning);
    } catch (Exception e) {
      LOG.debug(String.format("Failed to parse row key for partitioned file set '%s': %s",
                              getName(), Bytes.toStringBinary(row.getRow())));
      return false;
    }
    return filter.match(key);
  }



}
