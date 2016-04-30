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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.workflow.WorkflowConfigurer;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.format.StructuredRecordStringConverter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

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
 * TODO: improve storage format. It is currently a json of the record but that is obviously not ideal
 */
public class ConnectorSource extends BatchSource<LongWritable, Text, StructuredRecord> {
  static final Schema RECORD_WITH_SCHEMA = Schema.recordOf(
    "record",
    Schema.Field.of("schema", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("record", Schema.of(Schema.Type.STRING)));
  private final String datasetName;
  @Nullable
  private final Schema schema;

  public ConnectorSource(String datasetName, @Nullable Schema schema) {
    this.datasetName = datasetName;
    this.schema = schema;
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
    context.setInput(datasetName, arguments);
  }

  @Override
  public void transform(KeyValue<LongWritable, Text> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord output;
    String inputStr = input.getValue().toString();
    if (schema == null) {
      StructuredRecord recordWithSchema =
        StructuredRecordStringConverter.fromJsonString(inputStr, RECORD_WITH_SCHEMA);
      Schema outputSchema = Schema.parseJson((String) recordWithSchema.get("schema"));
      output = StructuredRecordStringConverter.fromJsonString((String) recordWithSchema.get("record"), outputSchema);
    } else {
      output = StructuredRecordStringConverter.fromJsonString(inputStr, schema);
    }
    emitter.emit(output);
  }

}
