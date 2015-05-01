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

package co.cask.cdap.test.app;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.AdapterManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import co.cask.cdap.test.template.WorkerTemplate;
import co.cask.cdap.test.template.WorkflowTemplate;
import co.cask.cdap.test.template.plugin.FlipPlugin;
import co.cask.cdap.test.template.plugin.SquarePlugin;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

/**
 *
 */
@Category(SlowTests.class)
public class TestAdapterFrameworkTestRun extends TestFrameworkTestBase {
  private static final Gson GSON = new Gson();

  @Category(XSlowTests.class)
  @Test
  public void testWorkflowAdapter() throws Exception {
    Id.ApplicationTemplate templateId = Id.ApplicationTemplate.from(WorkflowTemplate.NAME);
    addTemplatePlugins(templateId, "flip-1.0.jar", FlipPlugin.class);
    deployTemplate(Constants.DEFAULT_NAMESPACE_ID, templateId, WorkflowTemplate.class);

    WorkflowTemplate.Config config = new WorkflowTemplate.Config("flip");
    Id.Adapter adapterId = Id.Adapter.from(Constants.DEFAULT_NAMESPACE_ID, "workflowX");
    AdapterConfig adapterConfig = new AdapterConfig("description", WorkflowTemplate.NAME, GSON.toJsonTree(config));
    AdapterManager manager = createAdapter(adapterId, adapterConfig);

    DataSetManager<KeyValueTable> inputManager = getDataset(Constants.DEFAULT_NAMESPACE_ID, WorkflowTemplate.INPUT);
    inputManager.get().write(Bytes.toBytes(1L), Bytes.toBytes(10L));
    inputManager.flush();
    
    manager.start();
    // TODO: CDAP-2281 test schedules in a better way
    // perhaps we get a special schedule that lets you trigger jobs on command
    manager.waitForOneRunToFinish(4, TimeUnit.MINUTES);
    manager.stop();

    DataSetManager<KeyValueTable> outputManager = getDataset(Constants.DEFAULT_NAMESPACE_ID, WorkflowTemplate.OUTPUT);
    long outputVal = Bytes.toLong(outputManager.get().read(Bytes.toBytes(1L)));
    Assert.assertEquals(-10L, outputVal);
  }

  @Category(XSlowTests.class)
  @Test
  public void testWorkerAdapter() throws Exception {
    Id.ApplicationTemplate templateId = Id.ApplicationTemplate.from(WorkerTemplate.NAME);
    addTemplatePlugins(templateId, "square-1.0.jar", SquarePlugin.class);
    deployTemplate(Constants.DEFAULT_NAMESPACE_ID, templateId, WorkerTemplate.class);

    String tableName = "kvoutput";
    WorkerTemplate.Config config = new WorkerTemplate.Config(tableName, "square", 5L);
    Id.Adapter adapterId = Id.Adapter.from(Constants.DEFAULT_NAMESPACE_ID, "workerX");
    AdapterConfig adapterConfig = new AdapterConfig("description", WorkerTemplate.NAME, GSON.toJsonTree(config));
    AdapterManager manager = createAdapter(adapterId, adapterConfig);

    manager.start();

    byte[] key = Bytes.toBytes(5L);
    DataSetManager<KeyValueTable> kvTableManager = getDataset(Constants.DEFAULT_NAMESPACE_ID, tableName);
    byte[] valBytes = kvTableManager.get().read(key);
    while (valBytes == null) {
      TimeUnit.MILLISECONDS.sleep(200);
      valBytes = kvTableManager.get().read(key);
      kvTableManager.flush();
    }
    manager.stop();

    Assert.assertEquals(25L, Bytes.toLong(valBytes));
  }
}

