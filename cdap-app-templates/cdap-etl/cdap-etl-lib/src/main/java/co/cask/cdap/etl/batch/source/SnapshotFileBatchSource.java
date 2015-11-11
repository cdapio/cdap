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

package co.cask.cdap.etl.batch.source;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.common.SnapshotFileSetConfig;
import co.cask.cdap.etl.dataset.SnapshotFileSet;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads the latest snapshot written by a {@link co.cask.cdap.etl.batch.sink.SnapshotFileBatchSink}.
 *
 * @param <KEY> type of key to output
 * @param <VALUE> type of value to output
 */
public abstract class SnapshotFileBatchSource<KEY, VALUE> extends BatchSource<KEY, VALUE, StructuredRecord> {
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final SnapshotFileSetConfig config;

  public SnapshotFileBatchSource(SnapshotFileSetConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    PartitionedFileSetProperties.Builder fileProperties = SnapshotFileSet.getBaseProperties(config);
    addFileProperties(fileProperties);

    pipelineConfigurer.createDataset(config.getName(), PartitionedFileSet.class, fileProperties.build());
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    PartitionedFileSet partitionedFileSet = context.getDataset(config.getName());
    SnapshotFileSet snapshotFileSet = new SnapshotFileSet(partitionedFileSet);

    Map<String, String> arguments = new HashMap<>();

    if (config.getFileProperties() != null) {
      arguments = GSON.fromJson(config.getFileProperties(), MAP_TYPE);
    }

    context.setInput(config.getName(), snapshotFileSet.getInputArguments(arguments));
  }

  /**
   * add all fileset properties specific to the type of sink, such as schema and output format.
   */
  protected abstract void addFileProperties(FileSetProperties.Builder propertiesBuilder);
}
