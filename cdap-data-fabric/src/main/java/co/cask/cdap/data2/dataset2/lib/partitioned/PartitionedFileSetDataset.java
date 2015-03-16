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
import co.cask.cdap.api.dataset.DatasetContext;
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
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.proto.Id;
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
import javax.annotation.Nullable;

/**
 * Implementation of partitioned datasets using a Table to store the meta data.
 */
public class PartitionedFileSetDataset extends AbstractDataset implements PartitionedFileSet {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionedFileSetDataset.class);

  protected static final byte[] RELATIVE_PATH = { 'p' };
  protected static final byte[] FIELD_PREFIX = { 'f', '.' };

  protected final FileSet files;
  protected final Table partitionsTable;
  protected final Map<String, String> runtimeArguments;
  protected final DatasetSpecification spec;
  protected final Provider<ExploreFacade> exploreFacadeProvider;
  protected final Partitioning partitioning;
  protected boolean ignoreInvalidRowsSilently = false;

  private final Id.DatasetInstance datasetInstanceId;

  public PartitionedFileSetDataset(DatasetContext datasetContext, String name,
                                   Partitioning partitioning, FileSet fileSet, Table partitionTable,
                                   DatasetSpecification spec, Map<String, String> arguments,
                                   Provider<ExploreFacade> exploreFacadeProvider) {
    super(name, partitionTable);
    this.files = fileSet;
    this.partitionsTable = partitionTable;
    this.spec = spec;
    this.exploreFacadeProvider = exploreFacadeProvider;
    this.runtimeArguments = arguments;
    this.partitioning = partitioning;
    this.datasetInstanceId = Id.DatasetInstance.from(datasetContext.getNamespaceId(), name);
  }

  @Override
  public Partitioning getPartitioning() {
    return partitioning;
  }

  @Override
  public void addPartition(PartitionKey key, String path) {
    addPartition(key, path, true);
  }

  protected void addPartition(PartitionKey key, String path, boolean addToExplore) {
    final byte[] rowKey = generateRowKey(key, partitioning);
    Row row = partitionsTable.get(rowKey);
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
    partitionsTable.put(put);
    if (addToExplore) {
      addPartitionToExplore(key, path);
      // TODO: make DDL operations transactional [CDAP-1393]
    }
  }

  protected void addPartitionToExplore(PartitionKey key, String path) {
    if (FileSetProperties.isExploreEnabled(spec.getProperties())) {
      ExploreFacade exploreFacade = exploreFacadeProvider.get();
      if (exploreFacade != null) {
        try {
          exploreFacade.addPartition(datasetInstanceId, key, files.getLocation(path).toURI().getPath());
        } catch (Exception e) {
          throw new DataSetException(String.format(
            "Unable to add partition for key %s with path %s to explore table.", key.toString(), path), e);
        }
      }
    }
  }

  @Override
  public void dropPartition(PartitionKey key) {
    final byte[] rowKey = generateRowKey(key, partitioning);
    partitionsTable.delete(rowKey);
    dropPartitionFromExplore(key);
    // TODO: make DDL operations transactional [CDAP-1393]
  }

  private void dropPartitionFromExplore(PartitionKey key) {
    if (FileSetProperties.isExploreEnabled(spec.getProperties())) {
      ExploreFacade exploreFacade = exploreFacadeProvider.get();
      if (exploreFacade != null) {
        try {
          exploreFacade.dropPartition(datasetInstanceId, key);
        } catch (Exception e) {
          throw new DataSetException(String.format(
            "Unable to drop partition for key %s from explore table.", key.toString()), e);
        }
      }
    }
  }

  @Override
  public String getPartition(PartitionKey key) {
    final byte[] rowKey = generateRowKey(key, partitioning);
    Row row = partitionsTable.get(rowKey);
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
  public Set<String> getPartitionPaths(@Nullable PartitionFilter filter) {
    final Set<String> paths = Sets.newHashSet();
    getPartitions(filter, new PartitionConsumer() {
      @Override
      public void consume(PartitionKey key, String path) {
        paths.add(path);
      }
    });
    return paths;
  }

  @Override
  public Map<PartitionKey, String> getPartitions(@Nullable PartitionFilter filter) {
    final Map<PartitionKey, String> paths = Maps.newHashMap();
    getPartitions(filter, new PartitionConsumer() {
      @Override
      public void consume(PartitionKey key, String path) {
        paths.put(key, path);
      }
    });
    return paths;
  }

  protected void getPartitions(@Nullable PartitionFilter filter, PartitionConsumer consumer) {
    final byte[] startKey = generateStartKey(filter);
    final byte[] endKey = generateStopKey(filter);
    Scanner scanner = partitionsTable.scan(startKey, endKey);
    try {
      while (true) {
        Row row = scanner.next();
        if (row == null) {
          break;
        }
        PartitionKey key;
        try {
          key = parseRowKey(row.getRow(), partitioning);
        } catch (IllegalArgumentException e) {
          if (!ignoreInvalidRowsSilently) {
            LOG.debug(String.format("Failed to parse row key for partitioned file set '%s': %s",
                                    getName(), Bytes.toStringBinary(row.getRow())));
          }
          continue;
        }
        if (filter != null && !filter.match(key)) {
          continue;
        }
        byte[] pathBytes = row.get(RELATIVE_PATH);
        if (pathBytes != null) {
          consumer.consume(key, Bytes.toString(pathBytes));
        }
      }
    } finally {
      scanner.close();
    }
  }

  private interface PartitionConsumer {
    void consume(PartitionKey key, String path);
  }

  @Override
  public void close() throws IOException {
    try {
      files.close();
    } finally {
      partitionsTable.close();
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
    // we verify that the output partition key is configured in getOutputFormatConfiguration()
    // todo use a wrapper that adds the new partition when the job is committed - that means we must serialize the
    //      cconf into the hadoop conf to be able to instantiate this dataset in the output committer
    return files.getOutputFormatClass();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    // we set the fileset's output path in the definition's getDataset(), so there is no need to configure it again.
    // here we just want to validate that an output partition key was specified in the arguments.
    PartitionKey outputKey = PartitionedFileSetArguments.getOutputPartitionKey(runtimeArguments, getPartitioning());
    if (outputKey == null) {
      throw new DataSetException("Partition key must be given for the new output partition as a runtime argument.");
    }
    // copy the output partition key to the output arguments of the embedded file set
    // this will be needed by the output format to register the new partition.
    Map<String, String> config = files.getOutputFormatConfiguration();
    Map<String, String> outputArgs = Maps.newHashMap();
    outputArgs.putAll(config);
    PartitionedFileSetArguments.setOutputPartitionKey(outputArgs, outputKey);
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
      offset += bytes.length + 1; // this leaves a \0 byte after the value
    }
    return rowKey;
  }

  private byte[] generateStartKey(PartitionFilter filter) {
    if (null == filter) {
      return null;
    }
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
      offset += bytes.length + 1; // this leaves a \0 byte after the value
    }
    return startKey;
  }

  private byte[] generateStopKey(PartitionFilter filter) {
    if (null == filter) {
      return null;
    }
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
      offset += bytes.length + 1; // this leaves a \0 byte after the value
      if (allSingleValue && offset == stopKey.length) {
        stopKey[offset - 1] = 1; // see above - we \1 instead of \0 at the end, to make sure scan is not empty
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
}
