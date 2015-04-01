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
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ETLMapReduceTest extends TestBase {

  private static final Gson GSON = new Gson();

  @Test
  public void testConfig() throws Exception {
//    addDatasetInstance("keyValueTable", "table1").create();
//    addDatasetInstance("keyValueTable", "table2").create();

    ApplicationManager batchManager = deployApplication(ETLBatchTemplate.class);
    DataSetManager<KeyValueTable> table1 = getDataset("table1");
    KeyValueTable inputTable = table1.get();
    inputTable.write("hello", "world");
    table1.flush();

    ApplicationTemplate<JsonObject> appTemplate = new ETLBatchTemplate();
    DefaultManifestConfigurer manifestConfigurer = new DefaultManifestConfigurer();
    JsonObject adapterConfig = constructJson();
    appTemplate.configureManifest(adapterConfig, manifestConfigurer);
    Map<String, String> workflowArgs = Maps.newHashMap();
    for (Map.Entry<String, String> entry : manifestConfigurer.getArguments().entrySet()) {
      workflowArgs.put(entry.getKey(), entry.getValue());
    }
    workflowArgs.put("config", GSON.toJson(adapterConfig.get("config")));
    MapReduceManager mrManager = batchManager.startMapReduce("ETLMapReduce", workflowArgs);
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    batchManager.stopAll();
    DataSetManager<KeyValueTable> table2 = getDataset("table2");
    KeyValueTable outputTable = table2.get();
    Assert.assertEquals("world", Bytes.toString(outputTable.read("hello")));
  }

  private JsonObject constructJson() {
    JsonObject adapter = new JsonObject();
    JsonObject config = new JsonObject();
    adapter.addProperty("name", "Table2TableAdapter");
    adapter.addProperty("template", "etlbatch");
    adapter.addProperty("description", "Table To Table Dataset Adapter");
    adapter.add("config", config);
    config.addProperty("schedule", "cron entry");
    JsonObject source = new JsonObject();
    JsonArray transform = new JsonArray();
    JsonObject sink = new JsonObject();

    JsonObject sourceProperties = new JsonObject();
    JsonObject sinkProperties = new JsonObject();

    config.add("source", source);
    config.add("sink", sink);
    config.add("transforms", transform);
    source.addProperty("name", "KVTableSource");
    sink.addProperty("name", "KVTableSink");
    source.add("properties", sourceProperties);
    sink.add("properties", sinkProperties);

    sourceProperties.addProperty("name", "table1");
    sinkProperties.addProperty("name", "table2");
    return adapter;
  }
}
