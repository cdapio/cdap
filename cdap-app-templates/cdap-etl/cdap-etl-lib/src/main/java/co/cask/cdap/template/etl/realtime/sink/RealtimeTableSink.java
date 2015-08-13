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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.realtime.DataWriter;
import co.cask.cdap.template.etl.api.realtime.RealtimeContext;
import co.cask.cdap.template.etl.api.realtime.RealtimeSink;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.common.RecordPutTransformer;
import co.cask.cdap.template.etl.common.TableSinkConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Map;

/**
 * Real-time sink for Table
 */
@Plugin(type = "sink")
@Name("Table")
@Description("Real Time Sink for CDAP Table dataset")
public class RealtimeTableSink extends RealtimeSink<StructuredRecord> {

  private RecordPutTransformer recordPutTransformer;

  private final TableSinkConfig tableSinkConfig;

  public RealtimeTableSink(TableSinkConfig tableSinkConfig) {
    this.tableSinkConfig = tableSinkConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Map<String, String> properties = tableSinkConfig.getProperties().getProperties();
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableSinkConfig.getName()), "Dataset name must be given.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableSinkConfig.getRowField()),
                                "Field to be used as rowkey must be given.");
    pipelineConfigurer.createDataset(tableSinkConfig.getName(), Table.class.getName(), DatasetProperties.builder()
      .addAll(properties)
      .build());
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    Schema outputSchema = null;
    // If a schema string is present in the properties, use that to construct the outputSchema and pass it to the
    // recordPutTransformer
    String schemaString = context.getPluginProperties().getProperties().get(Properties.Table.PROPERTY_SCHEMA);
    if (schemaString != null) {
      outputSchema = Schema.parseJson(schemaString);
    }
    recordPutTransformer = new RecordPutTransformer(tableSinkConfig.getRowField(), outputSchema,
                                                    tableSinkConfig.isRowFieldCaseInsensitive());
  }

  @Override
  public int write(Iterable<StructuredRecord> records, DataWriter writer) throws Exception {
    Table table = writer.getDataset(tableSinkConfig.getName());
    int numRecords = 0;
    for (StructuredRecord record : records) {
      Put put = recordPutTransformer.toPut(record);
      table.put(put);
      numRecords++;
    }
    return numRecords;
  }
}
