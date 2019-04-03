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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import javax.annotation.Nullable;

/**
 * Used to read data written by {@link SingleConnectorSink}.
 */
public class SingleConnectorSource extends ConnectorSource<StructuredRecord> {
  static final Schema RECORD_WITH_SCHEMA = Schema.recordOf(
    "record",
    Schema.Field.of("schema", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("record", Schema.of(Schema.Type.STRING)));
  @Nullable
  private final Schema schema;

  public SingleConnectorSource(String datasetName, @Nullable Schema schema) {
    super(datasetName);
    this.schema = schema;
  }

  @Override
  public void transform(KeyValue<LongWritable, Text> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord output;
    String inputStr = input.getValue().toString();
    StructuredRecord recordWithSchema =
      StructuredRecordStringConverter.fromJsonString(inputStr, RECORD_WITH_SCHEMA);
    if (schema == null) {
      Schema outputSchema = Schema.parseJson((String) recordWithSchema.get("schema"));
      output = StructuredRecordStringConverter.fromJsonString((String) recordWithSchema.get("record"), outputSchema);
    } else {
      output = StructuredRecordStringConverter.fromJsonString(inputStr, schema);
    }
    emitter.emit(output);
  }

}
