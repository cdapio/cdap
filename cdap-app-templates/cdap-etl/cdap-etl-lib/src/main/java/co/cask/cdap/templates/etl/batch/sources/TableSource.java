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

package co.cask.cdap.templates.etl.batch.sources;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.common.RowRecordTransformer;
import com.google.common.base.Preconditions;

/**
 * CDAP Table Dataset Batch Source.
 */
public class TableSource extends BatchReadableSource<byte[], Row, StructuredRecord> {
  private RowRecordTransformer rowRecordTransformer;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("TableSource");
    configurer.setDescription("CDAP Table Dataset Batch Source");
    configurer.addProperty(new Property(
      NAME,
      "Table name. If the table does not already exist, it will be created when the pipeline is created",
      true));
    configurer.addProperty(new Property(
      Table.PROPERTY_SCHEMA,
      "Schema of records read from the Table. Row columns map to record fields. " +
        "For example, if the schema contains a field named 'user' of type string, " +
        "the value of that field will be taken from the value stored in the 'user' column. " +
        "Only simple types are allowed (boolean, int, long, float, double, bytes, string).", true));
    configurer.addProperty(new Property(
      Table.PROPERTY_SCHEMA_ROW_FIELD,
      "Optional field name indicating that the field value should come from the row key instead of a row column. " +
        "The field name specified must be present in the schema, and must not be nullable.",
      false));
  }

  @Override
  protected String getType(ETLStage stageConfig) {
    return Table.class.getName();
  }

  @Override
  public void initialize(ETLStage stageConfig) {
    super.initialize(stageConfig);
    String schemaStr = stageConfig.getProperties().get(Table.PROPERTY_SCHEMA);
    Preconditions.checkArgument(schemaStr != null && !schemaStr.isEmpty(), "Schema must be specified.");
    try {
      Schema schema = Schema.parseJson(schemaStr);
      String rowFieldName = stageConfig.getProperties().get(Table.PROPERTY_SCHEMA_ROW_FIELD);
      rowRecordTransformer = new RowRecordTransformer(schema, rowFieldName);
    } catch (Exception e) {
      throw new IllegalArgumentException("Schema is invalid", e);
    }
  }

  @Override
  public void emit(byte[] key, Row val, Emitter<StructuredRecord> emitter) {
    emitter.emit(rowRecordTransformer.toRecord(val));
  }
}
