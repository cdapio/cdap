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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.common.RecordPutTransformer;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * CDAP Table Dataset Batch Sink.
 */
public class TableSink extends BatchWritableSink<StructuredRecord, byte[], Put> {
  private RecordPutTransformer recordPutTransformer;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("TableSink");
    configurer.setDescription("CDAP Table Dataset Batch Sink");
    configurer.addProperty(new Property(
      NAME, "Name of the table. If the table does not already exist, one will be created.", true));
    configurer.addProperty(new Property(
      Table.PROPERTY_SCHEMA,
      "Optional schema of the table as a JSON Object. If the table does not already exist," +
        " one will be created with this schema, which will allow the table to be explored through Hive.",
      false));
    configurer.addProperty(new Property(
      Table.PROPERTY_SCHEMA_ROW_FIELD,
      "The name of the record field that should be used as the row key when writing to the table.",
      true));
  }

  @Override
  public void initialize(ETLStage stageConfig) throws Exception {
    super.initialize(stageConfig);
    String rowField = stageConfig.getProperties().get(Table.PROPERTY_SCHEMA_ROW_FIELD);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(rowField), "Row field must be given as a property.");
    recordPutTransformer = new RecordPutTransformer(rowField);
  }

  @Override
  protected String getDatasetType(ETLStage config) {
    return Table.class.getName();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], Put>> emitter) throws Exception {
    Put put = recordPutTransformer.toPut(input);
    emitter.emit(new KeyValue<byte[], Put>(put.getRow(), put));
  }
}
