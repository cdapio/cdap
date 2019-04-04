/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch.connector;

import com.google.gson.Gson;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetArguments;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.workflow.WorkflowConfigurer;
import io.cdap.cdap.etl.api.Alert;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.common.Constants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * Internal batch sink used as a connector between a mapreduce job and the workflow client that actually publishes
 * the alerts. Though this extends BatchSink, this will not be instantiated through the plugin framework, but will
 * be created explicitly through the application.
 *
 * The batch connector is just a PartitionedFileSet, where a partition is the name of a phase that wrote to it.
 * This way, multiple phases can have the same local PartitionedFileSet as a sink, and the workflow client will read
 * data from all partitions.
 *
 * This is because we don't want this to show up as a plugin that users can select and use, and also because
 * it uses features not exposed in the etl api (local workflow datasets).
 *
 * TODO: improve storage format. It is currently a json of the record but that is not ideal
 */
public class AlertPublisherSink extends BatchSink<Alert, NullWritable, Text> {
  private static final Gson GSON = new Gson();
  private final String datasetName;
  private final String phaseName;

  public AlertPublisherSink(String datasetName, String phaseName) {
    this.datasetName = datasetName;
    this.phaseName = phaseName;
  }

  // not the standard configurePipeline method. Need a workflowConfigurer to create a local dataset
  // we may want to expose local datasets in cdap-etl-api, but that is a separate track.
  public void configure(WorkflowConfigurer workflowConfigurer) {
    workflowConfigurer.createLocalDataset(datasetName, FileSet.class,
                                          FileSetProperties.builder()
                                            .setInputFormat(CombineTextInputFormat.class)
                                            .setInputProperty(FileInputFormat.INPUT_DIR_RECURSIVE, "true")
                                            .setOutputFormat(TextOutputFormat.class)
                                            .build());
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    Map<String, String> arguments = new HashMap<>();
    FileSetArguments.setOutputPath(arguments, Constants.Connector.DATA_DIR + "/" + phaseName);
    context.addOutput(Output.ofDataset(datasetName, arguments));
  }

  @Override
  public void transform(Alert input,
                        Emitter<KeyValue<NullWritable, Text>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(NullWritable.get(), new Text(GSON.toJson(input))));
  }
}
