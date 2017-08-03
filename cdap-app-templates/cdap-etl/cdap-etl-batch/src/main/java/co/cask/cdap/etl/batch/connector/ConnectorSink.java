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

import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.common.RecordInfo;
import co.cask.cdap.format.StructuredRecordStringConverter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Internal batch sink used as a connector between pipeline phases.
 * Though this extends BatchSink, this will not be instantiated through the plugin framework, but will
 * be created explicitly through the application.
 *
 * The batch connector is just a PartitionedFileSet, where a partition is the name of a phase that wrote to it.
 * This way, multiple phases can have the same local PartitionedFileSet as a sink, and the source will read data
 * from all partitions.
 *
 * This is because we don't want this to show up as a plugin that users can select and use, and also because
 * it uses features not exposed in the etl api (local workflow datasets).
 *
 * Connectors store the stage name each record came from in case they are placed in front of a joiner.
 *
 * TODO: improve storage format. It is currently a json of the record but that is obviously not ideal
 */
public class ConnectorSink extends BatchSink<RecordInfo<StructuredRecord>, NullWritable, Text> {
  private final String datasetName;
  private final String phaseName;

  public ConnectorSink(String datasetName, String phaseName) {
    this.datasetName = datasetName;
    this.phaseName = phaseName;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Map<String, String> arguments = new HashMap<>();
    PartitionKey outputPartition = PartitionKey.builder().addStringField("phase", phaseName).build();
    PartitionedFileSetArguments.setOutputPartitionKey(arguments, outputPartition);
    context.addOutput(Output.ofDataset(datasetName, arguments));
  }

  @Override
  public void transform(RecordInfo<StructuredRecord> input, Emitter<KeyValue<NullWritable, Text>> emitter)
    throws Exception {
    StructuredRecord modifiedRecord = modifyRecord(input);
    emitter.emit(new KeyValue<>(NullWritable.get(), new Text(StructuredRecordStringConverter.
      toJsonString(modifiedRecord))));
  }

  private StructuredRecord modifyRecord(RecordInfo<StructuredRecord> input) throws IOException {
    String stageName = input.getFromStage();
    Schema inputSchema = input.getValue().getSchema();
    return StructuredRecord.builder(ConnectorSource.RECORD_WITH_SCHEMA)
      .set("stageName", stageName)
      .set("type", input.getType().name())
      .set("schema", inputSchema.toString())
      .set("record", StructuredRecordStringConverter.toJsonString(input.getValue()))
      .build();
  }
}
