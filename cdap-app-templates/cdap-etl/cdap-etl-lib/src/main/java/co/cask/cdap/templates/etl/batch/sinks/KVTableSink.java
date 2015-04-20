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

package co.cask.cdap.templates.etl.batch.sinks;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.common.Properties;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.nio.ByteBuffer;

/**
 * CDAP Table Dataset Batch Sink.
 */
public class KVTableSink extends BatchWritableSink<StructuredRecord, byte[], byte[]> {
  private String keyField;
  private String valueField;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(getClass().getSimpleName());
    configurer.setDescription("CDAP Key Value Table Dataset Batch Sink");
    configurer.addProperty(new Property(NAME, "Dataset Name", true));
    configurer.addProperty(new Property(
      Properties.KeyValueTable.KEY_FIELD,
      "The name of the field to use as the key. Its type must be bytes. Defaults to 'key'.", false));
    configurer.addProperty(new Property(
      Properties.KeyValueTable.VALUE_FIELD,
      "The name of the field to use as the value. Its type must be bytes. Defaults to 'value'.", false));
  }

  @Override
  protected String getDatasetType(ETLStage config) {
    return KeyValueTable.class.getName();
  }

  @Override
  public void initialize(ETLStage stageConfig) throws Exception {
    keyField = stageConfig.getProperties().get(Properties.KeyValueTable.KEY_FIELD);
    if (Strings.isNullOrEmpty(keyField)) {
      keyField = "key";
    }
    valueField = stageConfig.getProperties().get(Properties.KeyValueTable.VALUE_FIELD);
    if (Strings.isNullOrEmpty(valueField)) {
      valueField = "value";
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], byte[]>> emitter) throws Exception {
    Object key = input.get(keyField);
    Preconditions.checkArgument(key != null, "Key cannot be null.");
    byte[] keyBytes = key instanceof ByteBuffer ? Bytes.toBytes((ByteBuffer) key) : (byte[]) key;
    Object val = input.get(valueField);
    byte[] valBytes = val instanceof ByteBuffer ? Bytes.toBytes((ByteBuffer) val) : (byte[]) val;
    emitter.emit(new KeyValue<byte[], byte[]>(keyBytes, valBytes));
  }
}
