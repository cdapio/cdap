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

package co.cask.cdap.client;

import co.cask.cdap.client.app.DummyWorkerTemplate;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.AdapterNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.AdapterDetail;
import co.cask.cdap.proto.AdapterStatus;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.template.ApplicationTemplateMeta;
import co.cask.cdap.test.XSlowTests;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link AdapterClient}.
 * This can not be a apart of ClientTestsSuite, because it needs to do some setup before cdap is started. All test cases
 * in ClientTestsSuite share the same CDAP instance, and so may not have an opportunity to perform a setup step before
 * CDAP startup.
 */
@Category(XSlowTests.class)
public class AdapterClientTestRun extends ClientTestBase {

  private static final Gson GSON = new Gson();

  private AdapterClient adapterClient;
  private ApplicationTemplateClient appTemplateClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    adapterClient = new AdapterClient(clientConfig);
    appTemplateClient = new ApplicationTemplateClient(clientConfig);
  }

  @Test
  public void testAdapters() throws Exception {
    Id.Adapter adapter = Id.Adapter.from(Id.Namespace.DEFAULT, "realtimeAdapter");

    List<AdapterDetail> initialList = adapterClient.list(Id.Namespace.DEFAULT);
    Assert.assertEquals(0, initialList.size());

    DummyWorkerTemplate.Config config = new DummyWorkerTemplate.Config(2);
    AdapterConfig adapterConfig = new AdapterConfig("description", DummyWorkerTemplate.NAME, GSON.toJsonTree(config));

    // Create Adapter
    adapterClient.create(adapter, adapterConfig);

    // Check that the created adapter is present
    adapterClient.waitForExists(adapter, 30, TimeUnit.SECONDS);
    Assert.assertTrue(adapterClient.exists(adapter));
    AdapterDetail someAdapter = adapterClient.get(adapter);
    Assert.assertNotNull(someAdapter);

    // list all adapters
    List<AdapterDetail> list = adapterClient.list(Id.Namespace.DEFAULT);
    Assert.assertArrayEquals(new AdapterDetail[] {someAdapter}, list.toArray());

    adapterClient.waitForStatus(adapter, AdapterStatus.STOPPED, 30, TimeUnit.SECONDS);
    adapterClient.start(adapter);
    adapterClient.waitForStatus(adapter, AdapterStatus.STARTED, 30, TimeUnit.SECONDS);

    List<RunRecord> adapterRuns = adapterClient.getRuns(adapter, ProgramRunStatus.COMPLETED,
                                                        0, Long.MAX_VALUE, null);
    Assert.assertEquals(0, adapterRuns.size());
    adapterClient.stop(adapter);
    adapterClient.waitForStatus(adapter, AdapterStatus.STOPPED, 30, TimeUnit.SECONDS);

    List<RunRecord> runs = adapterClient.getRuns(adapter, ProgramRunStatus.ALL, 0, Long.MAX_VALUE, 10);
    Assert.assertEquals(1, runs.size());

    String logs = adapterClient.getLogs(adapter);
    Assert.assertNotNull(logs);

    // Delete Adapter
    adapterClient.delete(adapter);

    // verify that the adapter is deleted
    Assert.assertFalse(adapterClient.exists(adapter));
    try {
      adapterClient.get(adapter);
      Assert.fail();
    } catch (AdapterNotFoundException e) {
      // Expected
    }

    List<AdapterDetail> finalList = adapterClient.list(Id.Namespace.DEFAULT);
    Assert.assertEquals(0, finalList.size());
  }

  @Test
  public void testApplicationTemplates() throws IOException, UnauthorizedException, NotFoundException {
    List<ApplicationTemplateMeta> templates = appTemplateClient.list();
    String templateId = templates.get(0).getName();

    appTemplateClient.get(templateId);
    appTemplateClient.getPlugins(templateId, "foo");
  }
}
