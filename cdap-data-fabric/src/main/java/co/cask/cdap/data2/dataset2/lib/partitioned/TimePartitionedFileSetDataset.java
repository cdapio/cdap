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
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.explore.client.ExploreFacade;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Implementation of partitioned datasets using a Table to store the meta data.
 */
public class TimePartitionedFileSetDataset extends AbstractDataset implements TimePartitionedFileSet {

  private static final Logger LOG = LoggerFactory.getLogger(TimePartitionedFileSetDataset.class);

  private static final byte[] RELATIVE_PATH = { 'p' };

  private final FileSet files;
  private final Table partitions;
  private final Map<String, String> runtimeArguments;
  private final DatasetSpecification spec;
  private final Provider<ExploreFacade> exploreFacadeProvider;

  public TimePartitionedFileSetDataset(String name,
                                       FileSet fileSet, Table partitionTable,
                                       DatasetSpecification spec, Map<String, String> arguments,
                                       Provider<ExploreFacade> exploreFacadeProvider) {
    super(name, partitionTable);
    this.files = fileSet;
    this.partitions = partitionTable;
    this.spec = spec;
    this.exploreFacadeProvider = exploreFacadeProvider;
    this.runtimeArguments = arguments;
  }

  @Override
  public void addPartition(long time, String path) {
    final byte[] rowkey = Bytes.toBytes(time);
    Row row = partitions.get(rowkey);
    if (row != null && !row.isEmpty()) {
      throw new DataSetException(String.format("Dataset '%s' already has a partition with time: %d.",
                                               getName(), time));
    }
    Put put = new Put(rowkey);
    put.add(RELATIVE_PATH, Bytes.toBytes(path));
    partitions.put(put);

    if (FileSetProperties.isExploreEnabled(spec.getProperties())) {
      ExploreFacade exploreFacade = exploreFacadeProvider.get();
      if (exploreFacade != null) {
        try {
          exploreFacade.addPartition(getName(), time, files.getLocation(path).toURI().getPath());
        } catch (Exception e) {
          throw new DataSetException(String.format(
            "Unable to add partition for time %d with path %s to explore table.", time, path), e);
        }
      }
    }
  }

  @Override
  public void dropPartition(long time) {
    final byte[] rowkey = Bytes.toBytes(time);
    partitions.delete(rowkey);

    if (FileSetProperties.isExploreEnabled(spec.getProperties())) {
      ExploreFacade exploreFacade = exploreFacadeProvider.get();
      if (exploreFacade != null) {
        try {
          exploreFacade.dropPartition(getName(), time);
        } catch (Exception e) {
          throw new DataSetException(String.format(
            "Unable to drop partition for time %d from explore table.", time), e);
        }
      }
    }
  }

  @Override
  public String getPartition(long time) {
    final byte[] rowkey = Bytes.toBytes(time);
    Row row = partitions.get(rowkey);
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
  public Collection<String> getPartitions(long startTime, long endTime) {
    final byte[] startKey = Bytes.toBytes(startTime);
    final byte[] endKey = Bytes.toBytes(endTime);
    List<String> paths = Lists.newArrayList();
    Scanner scanner = partitions.scan(startKey, endKey);
    try {
      while (true) {
        Row row = scanner.next();
        if (row == null) {
          break;
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
    Long startTime = TimePartitionedFileSetArguments.getInputStartTime(runtimeArguments);
    if (startTime == null) {
      throw new DataSetException("Start time for input time range must be given as argument.");
    }
    Long endTime = TimePartitionedFileSetArguments.getInputEndTime(runtimeArguments);
    if (endTime == null) {
      throw new DataSetException("End time for input time range must be given as argument.");
    }
    Collection<String> inputPaths = getPartitions(startTime, endTime);
    Map<String, String> config = files.getInputFormatConfiguration();
    if (!inputPaths.isEmpty()) {
      config = Maps.newHashMap(config);
      for (String path : inputPaths) {
        FileSetArguments.addInputPath(config, path);
      }
      config = ImmutableMap.copyOf(config);
    }
    return config;
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
  public FileSet getUnderlyingFileSet() {
    return files;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }
}
