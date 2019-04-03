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

package co.cask.cdap.internal.app.runtime.batch.dataset.input;

import co.cask.cdap.api.data.batch.PartitionedFileSetInputContext;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.partitioned.PartitionKeyCodec;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetDataset;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.lang.reflect.Type;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A basic implementation of {@link PartitionedFileSetInputContext}.
 */
class BasicPartitionedFileSetInputContext extends BasicInputContext implements PartitionedFileSetInputContext {

  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(PartitionKey.class, new PartitionKeyCodec()).create();

  private static final Type STRING_PARTITION_KEY_MAP_TYPE = new TypeToken<Map<String, PartitionKey>>() { }.getType();

  private final Map<String, PartitionKey> pathToPartitionMapping;

  private final boolean isCombineInputFormat;
  private final Configuration conf;

  private final Path[] inputPaths;
  private Set<PartitionKey> partitionKeys;

  // for caching in case of CombineFileInputFormat
  private String currentInputfileName;
  private PartitionKey currentPartitionKey;

  BasicPartitionedFileSetInputContext(MultiInputTaggedSplit multiInputTaggedSplit) {
    super(multiInputTaggedSplit.getName());

    InputSplit inputSplit = multiInputTaggedSplit.getInputSplit();
    if (inputSplit instanceof FileSplit) {
      isCombineInputFormat = false;
      Path path = ((FileSplit) inputSplit).getPath();
      inputPaths = new Path[] { path };
    } else if (inputSplit instanceof CombineFileSplit) {
      isCombineInputFormat = true;
      inputPaths = ((CombineFileSplit) inputSplit).getPaths();
    } else {
      throw new IllegalArgumentException(String.format("Expected either a '%s' or a '%s', but got '%s'.",
                                                       FileSplit.class.getName(), CombineFileSplit.class.getName(),
                                                       inputSplit.getClass().getName()));
    }

    this.conf = multiInputTaggedSplit.getConf();
    String mappingString = conf.get(PartitionedFileSetDataset.PATH_TO_PARTITIONING_MAPPING);
    this.pathToPartitionMapping =
      GSON.fromJson(Objects.requireNonNull(mappingString), STRING_PARTITION_KEY_MAP_TYPE);
  }

  @Override
  public PartitionKey getInputPartitionKey() {
    if (isCombineInputFormat) {
      // org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader sets this in its initNextRecordReader method
      String inputFileName = conf.get(MRJobConfig.MAP_INPUT_FILE);
      if (inputFileName == null) {
        throw new IllegalStateException(
          String.format("The value of '%s' in the configuration must be set by the RecordReader in case of using an " +
                          "InputFormat that returns CombineFileSplit.",
                        MRJobConfig.MAP_INPUT_FILE));
      }
      if (!inputFileName.equals(currentInputfileName)) {
        currentPartitionKey = getPartitionKey(URI.create(inputFileName));
        currentInputfileName = inputFileName;
      }
      return currentPartitionKey;
    }

    // single split per mapper task
    Set<PartitionKey> inputPartitionKeys = getInputPartitionKeys();
    if (inputPartitionKeys.size() != 1) {
      throw new IllegalStateException(String.format("Expected a single PartitionKey, but found: %s",
                                                    inputPartitionKeys));
    }
    return inputPartitionKeys.iterator().next();
  }

  @Override
  public Set<PartitionKey> getInputPartitionKeys() {
    if (partitionKeys == null) {
      partitionKeys = new HashSet<>();
      for (Path inputPath : inputPaths) {
        partitionKeys.add(getPartitionKey(inputPath.toUri()));
      }
    }
    return partitionKeys;
  }

  private PartitionKey getPartitionKey(URI inputPathURI) {
    if (pathToPartitionMapping.containsKey(inputPathURI.toString())) {
      return pathToPartitionMapping.get(inputPathURI.toString());
    }
    for (Map.Entry<String, PartitionKey> pathEntry : pathToPartitionMapping.entrySet()) {
      if (isParentOrEquals(URI.create(pathEntry.getKey()), inputPathURI)) {
        return pathEntry.getValue();
      }
    }
    StringBuilder errorMessage = new StringBuilder(String.format("Failed to derive PartitionKey from input path '%s'.",
                                                                 inputPathURI));
    if (pathToPartitionMapping.size() <= 1000) {
      errorMessage.append(String.format("Keys of path to key mapping: '%s'", pathToPartitionMapping.keySet()));
    } else {
      // uncommon case, but if there are too many partitions being processed by a single task, and if the partition
      // key can not be derived from the path, omit the mapping from the logs
      errorMessage.append(String.format("Path to key mapping had too many entries (%s) to log.",
                                        pathToPartitionMapping.size()));
    }
    throw new IllegalArgumentException(errorMessage.toString());
  }

  // compares only the paths of the URI, ignoring the scheme, host, port, etc. of the URIs.
  private boolean isParentOrEquals(URI potentialParent, URI potentialChild) {
    return potentialChild.normalize().getPath().startsWith(potentialParent.normalize().getPath());
  }
}
