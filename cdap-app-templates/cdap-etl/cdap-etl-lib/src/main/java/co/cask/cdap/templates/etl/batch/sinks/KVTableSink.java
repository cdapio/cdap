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

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSinkWriter;
import co.cask.cdap.templates.etl.api.config.ETLStage;

/**
 * CDAP Table Dataset Batch Sink.
 */
public class KVTableSink extends BatchWritableSink<KeyValue<byte[], byte[]>, byte[], byte[]> {

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("KVTableSink");
    configurer.setDescription("CDAP Key Value Table Dataset Batch Sink");
    configurer.addProperty(new Property(NAME, "Dataset Name", true));
  }

  @Override
  protected String getDatasetType(ETLStage config) {
    return KeyValueTable.class.getName();
  }

  @Override
  public void write(KeyValue<byte[], byte[]> input, BatchSinkWriter<byte[], byte[]> writer) throws Exception {
    writer.write(input.getKey(), input.getValue());
  }
}
