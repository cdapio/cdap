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

package co.cask.cdap.templates.etl.batch;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.templates.etl.api.EndPointStage;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.templates.etl.batch.sinks.BatchCubeSink;
import co.cask.cdap.templates.etl.batch.sinks.DBSink;
import co.cask.cdap.templates.etl.batch.sinks.KVTableSink;
import co.cask.cdap.templates.etl.batch.sinks.TableSink;
import co.cask.cdap.templates.etl.batch.sinks.TimePartitionedFileSetDatasetAvroSink;
import co.cask.cdap.templates.etl.batch.sources.BatchReadableSource;
import co.cask.cdap.templates.etl.batch.sources.DBSource;
import co.cask.cdap.templates.etl.batch.sources.KVTableSource;
import co.cask.cdap.templates.etl.batch.sources.KafkaSource;
import co.cask.cdap.templates.etl.batch.sources.StreamBatchSource;
import co.cask.cdap.templates.etl.batch.sources.TableSource;
import co.cask.cdap.templates.etl.common.Properties;
import co.cask.cdap.templates.etl.common.kafka.CamusWrapper;
import co.cask.cdap.test.AdapterManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ETLKafkaBatchSourceTest  extends TestBase {
  @Test
  public void testKafkaBatch() throws Exception {
    Id.ApplicationTemplate templateId = Id.ApplicationTemplate.from("etlBatch");
    addTemplatePlugins(templateId, "kafka-batch.jar", KafkaSource.class, KVTableSource.class,
                       DBSource.class, BatchReadableSource.class, StreamBatchSource.class,
                       TableSource.class);
    addTemplatePlugins(templateId, "batch-sinks-1.0.0.jar",
                       BatchCubeSink.class, DBSink.class, KVTableSink.class,
                       TableSink.class, TimePartitionedFileSetDatasetAvroSink.class);

    deployTemplate(Constants.DEFAULT_NAMESPACE_ID, templateId, ETLBatchTemplate.class,
                   EndPointStage.class.getPackage().getName(),
                   ETLStage.class.getPackage().getName(),
                   BatchSource.class.getPackage().getName(),
                   CamusWrapper.class.getPackage().getName());

    ETLStage source = new ETLStage("Kafka",
                                   ImmutableMap.of("brokers", "10.150.26.255:9092"));

    Schema.Field topicField = Schema.Field.of("topic", Schema.of(Schema.Type.STRING));
    Schema.Field partitionField = Schema.Field.of("partition", Schema.of(Schema.Type.INT));
    Schema.Field offsetField = Schema.Field.of("offset", Schema.of(Schema.Type.LONG));
    Schema schema = Schema.recordOf("kmsg", topicField, partitionField, offsetField);

    ETLStage sink = new ETLStage("Table", ImmutableMap.of(Properties.BatchWritable.NAME, "table2",
                                                          Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "msg",
                                                          Properties.Table.PROPERTY_SCHEMA, schema.toString()));

    ETLBatchConfig etlBatchConfig = new ETLBatchConfig("*/1 * * * *", source, sink, Lists.<ETLStage>newArrayList());

    //appTemplate.configureAdapter("myAdapter", adapterConfig, adapterConfigurer);

    AdapterConfig adapterConfig = new AdapterConfig("description", "etlBatch",
                                                       new Gson().toJsonTree(etlBatchConfig));
    Id.Adapter adapterId = Id.Adapter.from(Id.Namespace.from("default"), "kafkabatch");
    AdapterManager manager = createAdapter(adapterId, adapterConfig);

    manager.start();
    manager.waitForOneRunToFinish(4, TimeUnit.MINUTES);
    manager.stop();

    DataSetManager<Table> tableManager = getDataset("table1");
    Table inputTable = tableManager.get();
    Scanner scanner = inputTable.scan(null, null);
    Row row;
    int numRows = 0;
    while ((row = scanner.next()) != null) {
      numRows += 1;
    }
    Assert.assertEquals(1, numRows);
  }
}
