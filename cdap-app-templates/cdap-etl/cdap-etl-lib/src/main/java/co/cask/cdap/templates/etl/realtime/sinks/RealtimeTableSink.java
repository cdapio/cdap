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

package co.cask.cdap.templates.etl.realtime.sinks;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.api.realtime.DataWriter;
import co.cask.cdap.templates.etl.api.realtime.RealtimeContext;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSink;
import co.cask.cdap.templates.etl.common.RecordPutTransformer;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Map;

/**
 * Real-time sink for Table
 */
public class RealtimeTableSink extends RealtimeSink<StructuredRecord> {
  private static final String NAME = "name";
  private RecordPutTransformer recordPutTransformer;
  private String datasetName;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setDescription("Real Time Sink for CDAP Table dataset");
    configurer.addProperty(new Property(NAME, "Name of the table. If the table does not already exist," +
      " one will be created.", true));
    configurer.addProperty(
      new Property(Table.PROPERTY_SCHEMA,
                   "Optional schema of the table as a JSON Object. If the table does not already exist," +
                     " one will be created with this schema, which will allow the table to be explored through Hive.",
                   false));
    configurer.addProperty(
      new Property(Table.PROPERTY_SCHEMA_ROW_FIELD,
                   "The name of the record field that should be used as the row key when writing to the table.",
                   true));
  }

  @Override
  public void configurePipeline(ETLStage stageConfig, PipelineConfigurer pipelineConfigurer) {
    String tableName = stageConfig.getProperties().get(NAME);
    String rowKeyField = stageConfig.getProperties().get(Table.PROPERTY_SCHEMA_ROW_FIELD);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "Dataset name must be given.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(rowKeyField), "Field to be used as rowkey must be given.");
    pipelineConfigurer.createDataset(tableName, Table.class.getName(), DatasetProperties.builder()
      .addAll(stageConfig.getProperties())
      .build());
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    Map<String, String> runtimeArguments = context.getRuntimeArguments();
    String rowField = runtimeArguments.get(Table.PROPERTY_SCHEMA_ROW_FIELD);
    recordPutTransformer = new RecordPutTransformer(rowField);
    datasetName = runtimeArguments.get(NAME);
  }

  @Override
  public int write(Iterable<StructuredRecord> records, DataWriter writer) throws Exception {
    Table table = writer.getDataset(datasetName);
    int numRecords = 0;
    for (StructuredRecord record : records) {
      Put put = recordPutTransformer.toPut(record);
      table.put(put);
      numRecords++;
    }
    return numRecords;
  }
}
