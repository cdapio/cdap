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

package co.cask.cdap.template.etl.realtime.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.realtime.DataWriter;
import co.cask.cdap.template.etl.api.realtime.RealtimeContext;
import co.cask.cdap.template.etl.api.realtime.RealtimeSink;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.common.RecordPutTransformer;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Real-time sink for Table
 */
@Plugin(type = "sink")
@Name("Table")
@Description("Real Time Sink for CDAP Table dataset")
public class RealtimeTableSink extends RealtimeSink<StructuredRecord> {

  private static final String NAME_DESC = "Name of the table. If the table does not already exist, one will be " +
    "created.";
  private static final String PROPERTY_SCHEMA_DESC = "Optional schema of the table as a JSON Object. If the table " +
    "does not already exist, one will be created with this schema, which will allow the table to be explored " +
    "through Hive.\"";
  private static final String PROPERTY_SCHEMA_ROW_FIELD_DESC = "The name of the record field that should be used as " +
    "the row key when writing to the table.";

  private RecordPutTransformer recordPutTransformer;

  /**
   * Config class for RealtimeTableSink
   */
  public static class TableConfig extends PluginConfig {
    @Name(Properties.Table.NAME)
    @Description(NAME_DESC)
    private String name;

    @Name(Properties.Table.PROPERTY_SCHEMA)
    @Description(PROPERTY_SCHEMA_DESC)
    @Nullable
    String schemaStr;

    @Name(Properties.Table.PROPERTY_SCHEMA_ROW_FIELD)
    @Description(PROPERTY_SCHEMA_ROW_FIELD_DESC)
    String rowField;

    public TableConfig(String name, String schemaStr, String rowField) {
      this.name = name;
      this.schemaStr = schemaStr;
      this.rowField = rowField;
    }
  }

  private final TableConfig tableConfig;

  public RealtimeTableSink(TableConfig tableConfig) {
    this.tableConfig = tableConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Map<String, String> properties = tableConfig.getProperties().getProperties();
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableConfig.name), "Dataset name must be given.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableConfig.rowField),
                                "Field to be used as rowkey must be given.");
    pipelineConfigurer.createDataset(tableConfig.name, Table.class.getName(), DatasetProperties.builder()
      .addAll(properties)
      .build());
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    recordPutTransformer = new RecordPutTransformer(tableConfig.rowField);
  }

  @Override
  public int write(Iterable<StructuredRecord> records, DataWriter writer) throws Exception {
    Table table = writer.getDataset(tableConfig.name);
    int numRecords = 0;
    for (StructuredRecord record : records) {
      Put put = recordPutTransformer.toPut(record);
      table.put(put);
      numRecords++;
    }
    return numRecords;
  }
}
