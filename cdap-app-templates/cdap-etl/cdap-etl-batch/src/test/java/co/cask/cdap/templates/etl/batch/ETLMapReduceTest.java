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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ETLBatchTemplate}.
 */
public class ETLMapReduceTest extends TestBase {

  @Test
  public void testConfig() throws Exception {

    // simulate template deployment
    ApplicationManager batchManager = deployApplication(ETLBatchTemplate.class);

    // simulate pipeline creation
    ApplicationTemplate<ETLBatchConfig> appTemplate = new ETLBatchTemplate();
    ETLBatchConfig adapterConfig = constructETLBatchConfig();
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
    MapReduceManager mrManager = batchManager.startMapReduce("ETLMapReduce", mapReduceArgs);
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    batchManager.stopAll();
    DataSetManager<KeyValueTable> table2 = getDataset("table2");
    KeyValueTable outputTable = table2.get();
    for (int i = 0; i < 10000; i++) {
      Assert.assertEquals("world" + i, Bytes.toString(outputTable.read("hello" + i)));
    }
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

  @Test(expected = IllegalArgumentException.class)
  public void testSourceTransformTypeMismatchConfig() throws Exception {
    ApplicationTemplate<ETLBatchConfig> appTemplate = new ETLBatchTemplate();
    ETLBatchConfig adapterConfig = constructTypeMismatchConfig();
    MockAdapterConfigurer mockAdapterConfigurer = new MockAdapterConfigurer();
    appTemplate.configureAdapter("myAdapter", adapterConfig, mockAdapterConfigurer);
  }

  private ETLBatchConfig constructETLBatchConfig() {
    ETLStage source = new ETLStage("KVTableSource", ImmutableMap.of("name", "table1"));
    ETLStage sink = new ETLStage("KVTableSink", ImmutableMap.of("name", "table2"));
    ETLStage transform = new ETLStage("IdentityTransform", ImmutableMap.<String, String>of());
    List<ETLStage> transformList = Lists.newArrayList(transform);
    return new ETLBatchConfig("", source, sink, transformList);
  }

  private ETLBatchConfig constructTypeMismatchConfig() {
    ETLStage source = new ETLStage("KVTableSource", ImmutableMap.of("name", "table1"));
    ETLStage sink = new ETLStage("KVTableSink", ImmutableMap.of("name", "table2"));
    ETLStage transform = new ETLStage("StructuredRecordToGenericRecordTransform", ImmutableMap.<String, String>of());
    List<ETLStage> transformList = Lists.newArrayList(transform);
    return new ETLBatchConfig("", source, sink, transformList);
  }
}
