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

package co.cask.cdap.etl.batch.connector;

import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.workflow.WorkflowConfigurer;
import co.cask.cdap.etl.api.Alert;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import com.google.gson.Gson;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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
    Partitioning partitioning = Partitioning.builder()
      .addField("phase", Partitioning.FieldType.STRING)
      .build();
    workflowConfigurer.createLocalDataset(datasetName, PartitionedFileSet.class,
                                          PartitionedFileSetProperties.builder()
                                            .setPartitioning(partitioning)
                                            .setOutputFormat(TextOutputFormat.class)
                                            .build());
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Map<String, String> arguments = new HashMap<>();
    PartitionKey outputPartition = PartitionKey.builder().addStringField("phase", phaseName).build();
    PartitionedFileSetArguments.setOutputPartitionKey(arguments, outputPartition);
    context.addOutput(Output.ofDataset(datasetName, arguments));
  }

  @Override
  public void transform(Alert input,
                        Emitter<KeyValue<NullWritable, Text>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(NullWritable.get(), new Text(GSON.toJson(input))));
  }
}
