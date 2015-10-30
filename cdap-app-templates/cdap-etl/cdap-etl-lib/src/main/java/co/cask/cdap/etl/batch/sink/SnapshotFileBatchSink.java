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

package co.cask.cdap.etl.batch.sink;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.common.SnapshotFileSetConfig;
import co.cask.cdap.etl.dataset.SnapshotFileSet;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Sink that stores snapshots on HDFS, and keeps track of which snapshot is the latest snapshot.
 *
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class SnapshotFileBatchSink<KEY_OUT, VAL_OUT> extends BatchSink<StructuredRecord, KEY_OUT, VAL_OUT> {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotFileBatchSink.class);
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final SnapshotFileSetConfig config;
  private SnapshotFileSet snapshotFileSet;

  public SnapshotFileBatchSink(SnapshotFileSetConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FileSetProperties.Builder fileProperties = SnapshotFileSet.getBaseProperties(config);
    addFileProperties(fileProperties);
    pipelineConfigurer.createDataset(config.getName(), PartitionedFileSet.class, fileProperties.build());
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    PartitionedFileSet files = context.getDataset(config.getName());
    snapshotFileSet = new SnapshotFileSet(files);

    Map<String, String> arguments = new HashMap<>();

    if (config.getFileProperties() != null) {
      arguments = GSON.fromJson(config.getFileProperties(), MAP_TYPE);
    }
    context.addOutput(config.getName(), snapshotFileSet.getOutputArguments(context.getLogicalStartTime(), arguments));
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    super.onRunFinish(succeeded, context);
    if (succeeded) {
      try {
        snapshotFileSet.onSuccess(context.getLogicalStartTime());
      } catch (Exception e) {
        LOG.error("Exception updating state file with value of latest snapshot, ", e);
      }
    }
  }

  /**
   * add all fileset properties specific to the type of sink, such as schema and output format.
   */
  protected abstract void addFileProperties(FileSetProperties.Builder propertiesBuilder);
}
