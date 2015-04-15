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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.templates.etl.batch.sinks.TableSink;
import co.cask.cdap.templates.etl.batch.sources.BatchReadableSource;
import co.cask.cdap.templates.etl.batch.sources.TableSource;
import co.cask.cdap.templates.etl.transforms.RowToStructuredRecordTransform;
import co.cask.cdap.templates.etl.transforms.StructuredRecordToPutTransform;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ETLBatchTemplate}.
 */
public class ETLMapReduceTest extends TestBase {
  private static ApplicationManager templateManager;

  @BeforeClass
  public static void setupTests() {
    // simulate template deployment
    templateManager = deployApplication(ETLBatchTemplate.class);
  }

  @Test
  public void testConfig() throws Exception {
    // simulate pipeline creation
    ApplicationTemplate<ETLBatchConfig> appTemplate = new ETLBatchTemplate();

    // kv table to kv table pipeline
    ETLStage source = new ETLStage(BatchReadableSource.class.getSimpleName(),
                                   ImmutableMap.of("name", "table1", "type", KeyValueTable.class.getName()));
    ETLStage sink = new ETLStage("KVTableSink", ImmutableMap.of("name", "table2"));
    ETLStage transform = new ETLStage("IdentityTransform", ImmutableMap.<String, String>of());
    List<ETLStage> transformList = Lists.newArrayList(transform);
    ETLBatchConfig adapterConfig = new ETLBatchConfig("", source, sink, transformList);

    MockAdapterConfigurer adapterConfigurer = new MockAdapterConfigurer();
    appTemplate.configureAdapter("myAdapter", adapterConfig, adapterConfigurer);

    // add dataset instances that the source and sink added
    addDatasetInstances(adapterConfigurer);
    // add some data to the input table
    DataSetManager<KeyValueTable> table1 = getDataset("table1");
    KeyValueTable inputTable = table1.get();
    for (int i = 0; i < 10000; i++) {
      inputTable.write("hello" + i, "world" + i);
    }
    table1.flush();

    // add the runtime args that CDAP would normally add to the mapreduce job
    Map<String, String> mapReduceArgs = Maps.newHashMap();
    for (Map.Entry<String, String> entry : adapterConfigurer.getArguments().entrySet()) {
      mapReduceArgs.put(entry.getKey(), entry.getValue());
    }
    MapReduceManager mrManager = templateManager.startMapReduce("ETLMapReduce", mapReduceArgs);
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    templateManager.stopAll();
    DataSetManager<KeyValueTable> table2 = getDataset("table2");
    KeyValueTable outputTable = table2.get();
    for (int i = 0; i < 10000; i++) {
      Assert.assertEquals("world" + i, Bytes.toString(outputTable.read("hello" + i)));
    }
  }

  //TODO: Run after validation logic is fixed
  @Ignore
  @Test(expected = IllegalArgumentException.class)
  public void testSourceTransformTypeMismatchConfig() throws Exception {
    ApplicationTemplate<ETLBatchConfig> appTemplate = new ETLBatchTemplate();
    ETLBatchConfig adapterConfig = constructTypeMismatchConfig();
    MockAdapterConfigurer mockAdapterConfigurer = new MockAdapterConfigurer();
    appTemplate.configureAdapter("myAdapter", adapterConfig, mockAdapterConfigurer);
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testTableToTable() throws Exception {

    // simulate pipeline creation
    ApplicationTemplate<ETLBatchConfig> appTemplate = new ETLBatchTemplate();

    ETLStage source = new ETLStage(TableSource.class.getSimpleName(), ImmutableMap.of("name", "inputTable"));
    ETLStage sink = new ETLStage(TableSink.class.getSimpleName(), ImmutableMap.of("name", "outputTable"));
    Schema schema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("count", Schema.of(Schema.Type.INT)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("item", Schema.of(Schema.Type.STRING))
    );
    ETLStage transform1 = new ETLStage(RowToStructuredRecordTransform.class.getSimpleName(),
                                       ImmutableMap.of(
                                         "schema", schema.toString(),
                                         "row.field", "rowkey"
                                       ));
    ETLStage transform2 = new ETLStage(StructuredRecordToPutTransform.class.getSimpleName(),
                                       ImmutableMap.of(
                                         "schema", schema.toString(),
                                         "row.field", "rowkey"
                                       ));
    ETLBatchConfig adapterConfig = new ETLBatchConfig("", source, sink, Lists.newArrayList(transform1, transform2));

    MockAdapterConfigurer adapterConfigurer = new MockAdapterConfigurer();
    appTemplate.configureAdapter("myAdapter", adapterConfig, adapterConfigurer);

    // add dataset instances that the source and sink added
    addDatasetInstances(adapterConfigurer);
    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset("inputTable");
    Table inputTable = inputManager.get();
    Put put = new Put(Bytes.toBytes("row1"));
    put.add("user", "samuel");
    put.add("count", 5);
    put.add("price", 123.45);
    put.add("item", "scotch");
    inputTable.put(put);
    inputManager.flush();
    put = new Put(Bytes.toBytes("row2"));
    put.add("user", "jackson");
    put.add("count", 10);
    put.add("price", 123456789d);
    put.add("item", "island");
    inputTable.put(put);
    inputManager.flush();

    // add the runtime args that CDAP would normally add to the mapreduce job
    Map<String, String> mapReduceArgs = Maps.newHashMap();
    for (Map.Entry<String, String> entry : adapterConfigurer.getArguments().entrySet()) {
      mapReduceArgs.put(entry.getKey(), entry.getValue());
    }
    MapReduceManager mrManager = templateManager.startMapReduce("ETLMapReduce", mapReduceArgs);
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    templateManager.stopAll();

    DataSetManager<Table> outputManager = getDataset("outputTable");
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("row1"));
    Assert.assertEquals("samuel", row.getString("user"));
    Assert.assertEquals(5, (int) row.getInt("count"));
    Assert.assertTrue(Math.abs(123.45 - row.getDouble("price")) < 0.000001);
    Assert.assertEquals("scotch", row.getString("item"));

    row = outputTable.get(Bytes.toBytes("row2"));
    Assert.assertEquals("jackson", row.getString("user"));
    Assert.assertEquals(10, (int) row.getInt("count"));
    Assert.assertTrue(Math.abs(123456789 - row.getDouble("price")) < 0.000001);
    Assert.assertEquals("island", row.getString("item"));
  }

  private void addDatasetInstances(MockAdapterConfigurer configurer) throws Exception {
    for (Map.Entry<String, ImmutablePair<String, DatasetProperties>> entry :
      configurer.getDatasetInstances().entrySet()) {
      String typeName = entry.getValue().getFirst();
      DatasetProperties properties = entry.getValue().getSecond();
      String instanceName = entry.getKey();
      addDatasetInstance(typeName, instanceName, properties);
    }
  }

  private ETLBatchConfig constructTypeMismatchConfig() {
    ETLStage source = new ETLStage("KVTableSource", ImmutableMap.of("name", "table1"));
    ETLStage sink = new ETLStage("KVTableSink", ImmutableMap.of("name", "table2"));
    ETLStage transform = new ETLStage("StructuredRecordToGenericRecordTransform", ImmutableMap.<String, String>of());
    List<ETLStage> transformList = Lists.newArrayList(transform);
    return new ETLBatchConfig("", source, sink, transformList);
  }
}
