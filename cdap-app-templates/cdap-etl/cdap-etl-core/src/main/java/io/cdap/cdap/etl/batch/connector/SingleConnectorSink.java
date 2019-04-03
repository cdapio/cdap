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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.format.StructuredRecordStringConverter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Connector sink that only writes a single type of record.
 * This is used in the Spark engine, where connectors are only used for conditions.
 */
public class SingleConnectorSink extends ConnectorSink<StructuredRecord> {

  public SingleConnectorSink(String datasetName, String phaseName) {
    super(datasetName, phaseName);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Text>> emitter)
    throws Exception {
    StructuredRecord modifiedRecord = modifyRecord(input);
    emitter.emit(new KeyValue<>(NullWritable.get(),
                                new Text(StructuredRecordStringConverter.toJsonString(modifiedRecord))));
  }

  private StructuredRecord modifyRecord(StructuredRecord input) throws IOException {
    Schema inputSchema = input.getSchema();
    return StructuredRecord.builder(SingleConnectorSource.RECORD_WITH_SCHEMA)
      .set("schema", inputSchema.toString())
      .set("record", StructuredRecordStringConverter.toJsonString(input))
      .build();
  }
}
