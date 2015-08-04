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

package co.cask.cdap.template.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSourceContext;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.common.RowRecordTransformer;
import co.cask.cdap.template.etl.common.TableConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * CDAP Table Dataset Batch Source.
 */
@Plugin(type = "source")
@Name("Table")
@Description("Reads the entire contents of a CDAP Table. Outputs one record for each row in the Table.")
public class TableSource extends BatchReadableSource<byte[], Row, StructuredRecord> {
  private RowRecordTransformer rowRecordTransformer;

  private final TableConfig tableConfig;

  public TableSource(TableConfig tableConfig) {
    this.tableConfig = tableConfig;
  }

  @Override
  protected Map<String, String> getProperties() {
    Map<String, String> properties = Maps.newHashMap(tableConfig.getProperties().getProperties());
    properties.put(Properties.BatchReadableWritable.NAME, tableConfig.getName());
    properties.put(Properties.BatchReadableWritable.TYPE, Table.class.getName());
    return properties;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableConfig.getSchemaStr()), "Schema must be specified.");
  }

  @Override
  public void initialize(BatchSourceContext context) throws Exception {
    super.initialize(context);
    Schema schema = Schema.parseJson(tableConfig.getSchemaStr());
    rowRecordTransformer = new RowRecordTransformer(schema, tableConfig.getRowField());
  }

  @Override
  public void transform(KeyValue<byte[], Row> input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(rowRecordTransformer.toRecord(input.getValue()));
  }
}
