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

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.DataTemplate;
import co.cask.cdap.DummyBatchTemplate;
import co.cask.cdap.DummyWorkerTemplate;
import co.cask.cdap.ExtendedBatchTemplate;
import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.common.AdapterNotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.AdapterStatus;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.templates.AdapterDefinition;
import com.google.common.collect.Iterables;
import com.google.gson.JsonObject;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * AdapterService life cycle tests.
 */
public class AdapterServiceTest extends AppFabricTestBase {
  private static final Id.Namespace NAMESPACE = Id.Namespace.from(TEST_NAMESPACE1);
  private static AdapterService adapterService;

  @BeforeClass
  public static void setup() throws Exception {
    setupAdapters();
    adapterService = getInjector().getInstance(AdapterService.class);
    adapterService.registerTemplates();
  }

  @Test(expected = RuntimeException.class)
  public void testInvalidTemplate() throws Exception {
    AdapterConfig adapterConfig = new AdapterConfig("description", BadTemplate.NAME, null);
    adapterService.createAdapter(Id.Namespace.from(TEST_NAMESPACE1), "badAdapter", adapterConfig);
  }

  @Test
  public void testMultilevelTemplate() throws Exception {
    DummyBatchTemplate.Config config = new DummyBatchTemplate.Config("abcd", "0 0 1 1 *");
    AdapterConfig adapterConfig = new AdapterConfig("desc", ExtendedBatchTemplate.class.getSimpleName(),
                                                    GSON.toJsonTree(config));
    adapterService.createAdapter(Id.Namespace.from(TEST_NAMESPACE1), "myAdap", adapterConfig);
    adapterService.removeAdapter(Id.Namespace.from(TEST_NAMESPACE1), "myAdap");
  }

  @Test(expected = RuntimeException.class)
  public void testInvalidAdapter() throws Exception {
    Id.Namespace namespace = Id.Namespace.from(TEST_NAMESPACE1);
    String adapterName = "myInvalidAdapter";
    // the template should check that the first field is not null.
    DummyBatchTemplate.Config config = new DummyBatchTemplate.Config(null, "0 0 1 1 *");
    AdapterConfig adapterConfig = new AdapterConfig("description", DummyBatchTemplate.NAME, GSON.toJsonTree(config));

    // Create Adapter
    adapterService.createAdapter(namespace, adapterName, adapterConfig);
  }

  @Test
  public void checkForbiddenOperations() throws Exception {
    String adapterName = "myAdp";
    DummyBatchTemplate.Config config = new DummyBatchTemplate.Config("some", "0 0 1 1 *");
    AdapterConfig adapterConfig = new AdapterConfig("desc", DummyBatchTemplate.NAME, GSON.toJsonTree(config));

    // Create an adapter to deploy template application.
    adapterService.createAdapter(NAMESPACE, adapterName, adapterConfig);
    AdapterDefinition adapterSpec = adapterService.getAdapter(NAMESPACE, adapterName);
    Assert.assertNotNull(adapterSpec);

    // We should not be able to delete the application since we have created an adapter.
    Assert.assertFalse(adapterService.canDeleteApp(Id.Application.from(NAMESPACE, DummyBatchTemplate.NAME)));

    // Remove adapter but this does not delete the template app automatically.
    adapterService.removeAdapter(NAMESPACE, adapterName);

    // We should be able to delete the application since no adapters exist.
    Assert.assertTrue(adapterService.canDeleteApp(Id.Application.from(NAMESPACE, DummyBatchTemplate.NAME)));

    // This request should fail since the application is a template application.
    HttpResponse response = doPost(String.format("%s/namespaces/%s/apps/%s/workflows/%s/start",
                                                 Constants.Gateway.API_VERSION_3, TEST_NAMESPACE1,
                                                 adapterConfig.getTemplate(), DummyBatchTemplate.AdapterWorkflow.NAME));
    Assert.assertEquals(HttpResponseStatus.FORBIDDEN.code(), response.getStatusLine().getStatusCode());

    // the deletion of the only adapter using the application should have deleted the app and an attempt to delete the
    // application should reutrn not found
    response = doDelete(String.format("%s/namespaces/%s/apps/%s", Constants.Gateway.API_VERSION_3, TEST_NAMESPACE1,
                                      adapterConfig.getTemplate()));
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getStatusLine().getStatusCode());

    String workerAdapter = "workAdapter";
    DummyWorkerTemplate.Config config1 = new DummyWorkerTemplate.Config(2);
    AdapterConfig adapterConfig1 = new AdapterConfig("desc1", DummyWorkerTemplate.NAME, GSON.toJsonTree(config1));
    adapterService.createAdapter(NAMESPACE, workerAdapter, adapterConfig1);
    adapterSpec = adapterService.getAdapter(NAMESPACE, workerAdapter);
    Assert.assertNotNull(adapterSpec);

    // This request should fail since the application is a template application.
    response = doPost(String.format("%s/namespaces/%s/apps/%s/workers/%s/stop",
                                    Constants.Gateway.API_VERSION_3, TEST_NAMESPACE1,
                                    adapterConfig1.getTemplate(), DummyWorkerTemplate.TWorker.NAME));
    Assert.assertEquals(HttpResponseStatus.FORBIDDEN.code(), response.getStatusLine().getStatusCode());
    adapterService.removeAdapter(NAMESPACE, workerAdapter);
  }

  @Test
  public void testWorkerAdapter() throws Exception {
    final String adapter1 = "myWorkerAdapter";
    final String adapter2 = "newAdapter";
    DummyWorkerTemplate.Config config1 = new DummyWorkerTemplate.Config(2);
    DummyWorkerTemplate.Config config2 = new DummyWorkerTemplate.Config(3);
    AdapterConfig adapterConfig1 = new AdapterConfig("desc1", DummyWorkerTemplate.NAME, GSON.toJsonTree(config1));
    AdapterConfig adapterConfig2 = new AdapterConfig("desc2", DummyWorkerTemplate.NAME, GSON.toJsonTree(config2));

    // Create Adapter
    adapterService.createAdapter(NAMESPACE, adapter1, adapterConfig1);
    adapterService.createAdapter(NAMESPACE, adapter2, adapterConfig2);

    // Start Adapter
    adapterService.startAdapter(NAMESPACE, adapter1);
    adapterService.startAdapter(NAMESPACE, adapter2);
    Assert.assertEquals(AdapterStatus.STARTED, adapterService.getAdapterStatus(NAMESPACE, adapter1));
    Assert.assertEquals(AdapterStatus.STARTED, adapterService.getAdapterStatus(NAMESPACE, adapter2));

    List<RunRecord> runRecords = adapterService.getRuns(NAMESPACE, adapter1, ProgramRunStatus.ALL,
                                                        0, Long.MAX_VALUE, Integer.MAX_VALUE);
    Assert.assertTrue(runRecords.size() == 1);
    RunRecord runRecord = Iterables.getFirst(runRecords, null);
    Assert.assertEquals(ProgramRunStatus.RUNNING, runRecord.getStatus());

    // Stop Adapter
    adapterService.stopAdapter(NAMESPACE, adapter1);
    adapterService.stopAdapter(NAMESPACE, adapter2);

    runRecords = adapterService.getRuns(NAMESPACE, adapter1, ProgramRunStatus.ALL,
                                        0, Long.MAX_VALUE, Integer.MAX_VALUE);
    Assert.assertTrue(runRecords.size() == 1);
    RunRecord stopRecord = Iterables.getFirst(runRecords, null);
    Assert.assertEquals(ProgramRunStatus.KILLED, stopRecord.getStatus());
    Assert.assertEquals(runRecord.getPid(), stopRecord.getPid());
    Assert.assertTrue(stopRecord.getStopTs() >= stopRecord.getStartTs());

    Assert.assertEquals(AdapterStatus.STOPPED, adapterService.getAdapterStatus(NAMESPACE, adapter1));
    Assert.assertEquals(AdapterStatus.STOPPED, adapterService.getAdapterStatus(NAMESPACE, adapter2));

    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        List<RunRecord> runs = adapterService.getRuns(NAMESPACE, adapter1, ProgramRunStatus.KILLED,
                                                      0, Long.MAX_VALUE, Integer.MAX_VALUE);
        return runs.size();
      }
    }, 10, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);

    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        List<RunRecord> runs = adapterService.getRuns(NAMESPACE, adapter2, ProgramRunStatus.KILLED,
                                                      0, Long.MAX_VALUE, Integer.MAX_VALUE);
        return runs.size();
      }
    }, 10, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);

    // Delete Adapter
    adapterService.removeAdapter(NAMESPACE, adapter1);

    // check the template still exists
    List<JsonObject> deployedApps = getAppList(NAMESPACE.getId());
    Assert.assertEquals(1, deployedApps.size());

    adapterService.removeAdapter(NAMESPACE, adapter2);

    // check if the template got deleted when we deleted the second and last adapter of that type
    deployedApps = getAppList(NAMESPACE.getId());
    Assert.assertEquals(0, deployedApps.size());

    try {
      adapterService.getAdapter(NAMESPACE, adapter1);
      Assert.fail(String.format("Found adapterSpec with name %s; it should be deleted.", adapter1));
    } catch (AdapterNotFoundException ex) {
      // expected
    }

    try {
      adapterService.getAdapter(NAMESPACE, adapter2);
      Assert.fail(String.format("Found adapterSpec with name %s; it should be deleted.", adapter2));
    } catch (AdapterNotFoundException ex) {
      // expected
    }
  }

  @Test
  public void testBatchAdapters() throws Exception {
    String adapterName = "myAdapter";
    DummyBatchTemplate.Config config = new DummyBatchTemplate.Config("somestream", "0 0 1 1 *");
    AdapterConfig adapterConfig = new AdapterConfig("description", DummyBatchTemplate.NAME, GSON.toJsonTree(config));

    // Create Adapter
    adapterService.createAdapter(NAMESPACE, adapterName, adapterConfig);
    PreferencesStore preferencesStore = getInjector().getInstance(PreferencesStore.class);
    Map<String, String> prop = preferencesStore.getResolvedProperties(
      TEST_NAMESPACE1, adapterConfig.getTemplate());
    try {
      // Expect another call to create Adapter with the same adapterName to throw an AdapterAlreadyExistsException.
      adapterService.createAdapter(NAMESPACE, adapterName, adapterConfig);
      Assert.fail("Second call to create adapter with same adapterName did not throw AdapterAlreadyExistsException.");
    } catch (AdapterAlreadyExistsException expected) {
      // expected
    }

    AdapterDefinition actualAdapterSpec = adapterService.getAdapter(NAMESPACE, adapterName);
    Assert.assertNotNull(actualAdapterSpec);
    assertDummyConfigEquals(adapterConfig, actualAdapterSpec);

    // list all adapters
    Collection<AdapterDefinition> adapters = adapterService.getAdapters(NAMESPACE, DummyBatchTemplate.NAME);
    Assert.assertEquals(1, adapters.size());
    AdapterDefinition actual = adapters.iterator().next();
    assertDummyConfigEquals(adapterConfig, actual);

    // adapter should be stopped
    Assert.assertEquals(AdapterStatus.STOPPED, adapterService.getAdapterStatus(NAMESPACE, adapterName));

    // start adapter
    adapterService.startAdapter(NAMESPACE, adapterName);
    Assert.assertEquals(AdapterStatus.STARTED, adapterService.getAdapterStatus(NAMESPACE, adapterName));

    // stop adapter
    adapterService.stopAdapter(NAMESPACE, adapterName);
    Assert.assertEquals(AdapterStatus.STOPPED, adapterService.getAdapterStatus(NAMESPACE, adapterName));

    // Delete all Adapters
    adapterService.removeAdapters(NAMESPACE);
    // verify that the adapter is deleted
    try {
      adapterService.getAdapter(NAMESPACE, adapterName);
      Assert.fail(String.format("Found adapterSpec with name %s; it should be deleted.", adapterName));
    } catch (AdapterNotFoundException expected) {
      // expected
    }

    adapters = adapterService.getAdapters(NAMESPACE, DummyBatchTemplate.NAME);
    Assert.assertTrue(adapters.isEmpty());
  }

  @Test
  public void testRedeploy() throws Exception {
    ApplicationTemplateInfo info1 = adapterService.getApplicationTemplateInfo(DummyTemplate1.NAME);
    // Update the jar and force redeploy
    setupAdapter(DummyTemplate1.class);
    adapterService.deployTemplate(NAMESPACE, DummyTemplate1.NAME);
    ApplicationTemplateInfo info2 = adapterService.getApplicationTemplateInfo(DummyTemplate1.NAME);
    Assert.assertNotEquals(info1.getDescription(), info2.getDescription());
  }

  @Test
  public void testDataCreation() throws Exception {
    String streamName = "somestream";
    String tableName = "sometable";
    String adapterName = "streamAdapter";
    DataTemplate.Config config = new DataTemplate.Config(streamName, tableName);
    AdapterConfig adapterConfig = new AdapterConfig("description", DataTemplate.NAME, GSON.toJsonTree(config));
    adapterService.createAdapter(NAMESPACE, adapterName, adapterConfig);

    Assert.assertTrue(streamExists(Id.Stream.from(NAMESPACE, streamName)));
    Assert.assertTrue(datasetExists(Id.DatasetInstance.from(NAMESPACE, tableName)));
  }

  private void assertDummyConfigEquals(AdapterConfig expected, AdapterDefinition actual) {
    Assert.assertEquals(expected.getDescription(), actual.getDescription());
    Assert.assertEquals(expected.getTemplate(), actual.getTemplate());
    Assert.assertEquals(expected.getConfig(), actual.getConfig());
  }

  private static void setupAdapters() throws IOException {
    setupAdapter(DummyBatchTemplate.class);
    setupAdapter(DummyTemplate1.class);
    setupAdapter(DummyTemplate2.class);
    setupAdapter(BadTemplate.class);
    setupAdapter(DataTemplate.class);
    setupAdapter(DummyWorkerTemplate.class);
    setupAdapter(ExtendedBatchTemplate.class);
  }

  public static class DummyTemplate1 extends DummyBatchTemplate {
    public static final String NAME = "template1";

    @Override
    public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
      super.configure(configurer, context);
      configurer.setName(NAME);
    }
  }

  public static class DummyTemplate2 extends DummyBatchTemplate {
    public static final String NAME = "template2";

    @Override
    public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
      super.configure(configurer, context);
      configurer.setName(NAME);
    }
  }

  /**
   * Bad template that contains 2 workflows.
   */
  public static class BadTemplate extends ApplicationTemplate {
    public static final String NAME = "badtemplate";

    @Override
    public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
      configurer.setName(NAME);
      configurer.addWorkflow(new SomeWorkflow1());
      configurer.addWorkflow(new SomeWorkflow2());
    }

    public static class SomeWorkflow1 extends AbstractWorkflow {
      @Override
      protected void configure() {
        setName("wf1");
        addMapReduce("DummyMapReduceJob");
      }
    }

    public static class SomeWorkflow2 extends AbstractWorkflow {
      @Override
      protected void configure() {
        setName("wf2");
        addMapReduce("DummyMapReduceJob");
      }
    }

    public static class DummyMapReduceJob extends AbstractMapReduce {
      @Override
      protected void configure() {
        setName("DummyMapReduceJob");
      }
    }
  }
}
