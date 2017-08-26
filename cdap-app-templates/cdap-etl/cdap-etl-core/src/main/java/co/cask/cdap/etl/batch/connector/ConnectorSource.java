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

package co.cask.cdap.etl.batch.connector;

import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.workflow.WorkflowConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * Internal batch source used as a connector between pipeline phases.
 * Though this extends BatchSource, this will not be instantiated through the plugin framework, but will
 * be created explicitly through the application.
 *
 * The batch connector is just a PartitionedFileSet, where a partition is the name of a phase that wrote to it.
 * This way, multiple phases can have the same local PartitionedFileSet as a sink, and the source will read data
 * from all partitions.
 *
 * This is because we don't want this to show up as a plugin that users can select and use, and also because
 * it uses features not exposed in the etl api (local workflow datasets).
 *
 * @param <T> type of output object
 */
public class ConnectorSource<T> extends BatchSource<LongWritable, Text, T> {
  private final String datasetName;

  protected ConnectorSource(String datasetName) {
    this.datasetName = datasetName;
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
                                            .setInputFormat(TextInputFormat.class)
                                            .setOutputFormat(TextOutputFormat.class)
                                            .build());
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Map<String, String> arguments = new HashMap<>();
    PartitionedFileSet inputFileset = context.getDataset(datasetName);
    for (PartitionDetail partitionDetail : inputFileset.getPartitions(PartitionFilter.ALWAYS_MATCH)) {
      PartitionedFileSetArguments.addInputPartition(arguments, partitionDetail);
    }
    context.setInput(Input.ofDataset(datasetName, arguments));
  }

}
