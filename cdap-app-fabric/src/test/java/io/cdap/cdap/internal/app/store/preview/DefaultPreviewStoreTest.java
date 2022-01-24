/*
 * Copyright © 2016 Cask Data, Inc.
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
package io.cdap.cdap.internal.app.store.preview;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.app.preview.PreviewConfigModule;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.preview.PreviewConfig;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.twill.api.RunId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the {@link DefaultPreviewStore}.
 */
public class DefaultPreviewStoreTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Gson GSON = new Gson();
  private static DefaultPreviewStore store;

  @BeforeClass
  public static void beforeClass() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    Injector injector = Guice.createInjector(
      new PreviewConfigModule(cConf, new Configuration(), SConfiguration.create())
    );
    store = injector.getInstance(DefaultPreviewStore.class);
  }

  @AfterClass
  public static void tearDown() {
    AppFabricTestHelper.shutdown();
  }

  @Before
  public void before() throws Exception {
    store.clear();
  }

  @Test
  public void testPreviewStore() {
    String firstApplication = RunIds.generate().getId();
    ApplicationId firstApplicationId = new ApplicationId(NamespaceMeta.DEFAULT.getName(), firstApplication);

    String secondApplication = RunIds.generate().getId();
    ApplicationId secondApplicationId = new ApplicationId(NamespaceMeta.DEFAULT.getName(), secondApplication);

    // put data for the first application
    store.put(firstApplicationId, "mytracer", "key1", "value1");
    store.put(firstApplicationId, "mytracer", "key1", 2);
    store.put(firstApplicationId, "mytracer", "key2", 3);
    Map<Object, Object> propertyMap = new HashMap<>();
    propertyMap.put("key1", "value1");
    propertyMap.put("1", "value2");
    store.put(firstApplicationId, "mytracer", "key2", propertyMap);
    store.put(firstApplicationId, "myanothertracer", "key2", 3);

    // put data for the second application
    store.put(secondApplicationId, "mytracer", "key1", "value1");

    // get the data for first application and logger name "mytracer"
    Map<String, List<JsonElement>> firstApplicationData = store.get(firstApplicationId, "mytracer");
    // key1 and key2 are two keys inserted for the first application.
    Assert.assertEquals(2, firstApplicationData.size());
    Assert.assertEquals("value1", firstApplicationData.get("key1").get(0).getAsString());
    Assert.assertEquals(2, firstApplicationData.get("key1").get(1).getAsInt());
    Assert.assertEquals(3, firstApplicationData.get("key2").get(0).getAsInt());
    Assert.assertEquals(propertyMap, GSON.fromJson(firstApplicationData.get("key2").get(1),
                                                   new TypeToken<HashMap<Object, Object>>() { }.getType()));

    // get the data for second application and logger name "mytracer"
    Map<String, List<JsonElement>> secondApplicationData = store.get(secondApplicationId, "mytracer");
    Assert.assertEquals(1, secondApplicationData.size());
    Assert.assertEquals("value1", secondApplicationData.get("key1").get(0).getAsString());

    // remove the data from first application
    store.remove(firstApplicationId);
    firstApplicationData = store.get(firstApplicationId, "mytracer");
    Assert.assertEquals(0, firstApplicationData.size());
  }

  @Test
  public void testPreviewInfo() throws IOException {
    // test non existing preview
    ApplicationId nonexist = new ApplicationId("ns1", "nonexist");
    Assert.assertNull(store.getProgramRunId(nonexist));
    Assert.assertNull(store.getPreviewStatus(nonexist));

    // test put and get
    ApplicationId applicationId = new ApplicationId("ns1", "app1");
    ProgramRunId runId = new ProgramRunId("ns1", "app1", ProgramType.WORKFLOW, "test",
                                          RunIds.generate().getId());
    PreviewStatus status = new PreviewStatus(PreviewStatus.Status.COMPLETED, System.currentTimeMillis(), null, 0L,
                                             System.currentTimeMillis());
    store.setProgramId(runId);
    store.setPreviewStatus(applicationId, status);

    Assert.assertEquals(runId, store.getProgramRunId(applicationId));
    Assert.assertEquals(status, store.getPreviewStatus(applicationId));
  }

  @Test
  public void testPreviewWaitingRequests() throws Exception {
    byte[] pollerInfo = Bytes.toBytes("runner-1");

    PreviewConfig previewConfig = new PreviewConfig("WordCount", ProgramType.WORKFLOW, null, null);
    AppRequest<?> testRequest = new AppRequest<>(new ArtifactSummary("test", "1.0"), null, previewConfig);
    Assert.assertEquals(0, store.getAllInWaitingState().size());

    RunId id1 = RunIds.generate();
    ApplicationId applicationId = new ApplicationId("ns1", id1.getId());
    store.add(applicationId, testRequest, null);
    List<PreviewRequest> allWaiting = store.getAllInWaitingState();
    Assert.assertEquals(1, allWaiting.size());

    AppRequest appRequest = allWaiting.get(0).getAppRequest();
    Assert.assertNotNull(appRequest);
    Assert.assertNotNull(appRequest.getPreview());
    Assert.assertEquals("WordCount", appRequest.getPreview().getProgramName());
    store.setPreviewRequestPollerInfo(applicationId, pollerInfo);

    Assert.assertEquals(0, store.getAllInWaitingState().size());

    // add 2 requests to the queue
    ApplicationId applicationId2 = new ApplicationId("ns1", RunIds.generate().getId());
    store.add(applicationId2, testRequest, null);
    ApplicationId applicationId3 = new ApplicationId("ns1", RunIds.generate().getId());
    store.add(applicationId3, testRequest, null);

    allWaiting = store.getAllInWaitingState();
    Assert.assertEquals(2, allWaiting.size());
    Assert.assertEquals(applicationId2, allWaiting.get(0).getProgram().getParent());
    Assert.assertEquals(applicationId3, allWaiting.get(1).getProgram().getParent());

    store.setPreviewRequestPollerInfo(applicationId2, pollerInfo);
    allWaiting = store.getAllInWaitingState();
    Assert.assertEquals(1, allWaiting.size());
    Assert.assertEquals(applicationId3, allWaiting.get(0).getProgram().getParent());

    store.setPreviewRequestPollerInfo(applicationId3, pollerInfo);
    allWaiting = store.getAllInWaitingState();
    Assert.assertEquals(0, allWaiting.size());

    // Add a preview request that has a principle associated with it
    ApplicationId applicationId4 = new ApplicationId("ns1", RunIds.generate().getId());
    Principal principal = new Principal("userForApplicationId4",
                                        Principal.PrincipalType.USER,
                                        new Credential("userForApplicationId4Credential",
                                                       Credential.CredentialType.EXTERNAL));
    store.add(applicationId4, testRequest, principal);
    allWaiting = store.getAllInWaitingState();
    Assert.assertEquals(1, allWaiting.size());
    Assert.assertEquals(applicationId4, allWaiting.get(0).getProgram().getParent());
    Assert.assertTrue(allWaiting.get(0).getPrincipal().equals(principal));
    store.setPreviewRequestPollerInfo(applicationId4, pollerInfo);
    allWaiting = store.getAllInWaitingState();
    Assert.assertEquals(0, allWaiting.size());
  }

  @Test
  public void testPreviewTTL() throws Exception {
    PreviewConfig previewConfig = new PreviewConfig("WordCount", ProgramType.WORKFLOW, null, null);
    AppRequest<?> testRequest = new AppRequest<>(new ArtifactSummary("test", "1.0"), null, previewConfig);

    String firstApplication = RunIds.generate(System.currentTimeMillis() - 5000).getId();
    ApplicationId firstApplicationId = new ApplicationId(NamespaceMeta.DEFAULT.getName(), firstApplication);

    String secondApplication = RunIds.generate(System.currentTimeMillis() - 4000).getId();
    ApplicationId secondApplicationId = new ApplicationId(NamespaceMeta.DEFAULT.getName(), secondApplication);

    String thirdApplication = RunIds.generate(System.currentTimeMillis()).getId();
    ApplicationId thirdApplicationId = new ApplicationId(NamespaceMeta.DEFAULT.getName(), thirdApplication);

    store.add(firstApplicationId, testRequest, null);
    store.add(secondApplicationId, testRequest, null);
    store.add(thirdApplicationId, testRequest, null);

    // set poller info so that it gets removed from WAITING state
    store.setPreviewRequestPollerInfo(firstApplicationId, null);

    // put data for the first application
    store.put(firstApplicationId, "mytracer", "key1", "value1");
    store.put(firstApplicationId, "mytracer", "key1", 2);

    // get the data for first application and logger name "mytracer"
    Map<String, List<JsonElement>> firstApplicationData = store.get(firstApplicationId, "mytracer");
    // key1 and key2 are two keys inserted for the first application.
    Assert.assertEquals(1, firstApplicationData.size());
    Assert.assertEquals("value1", firstApplicationData.get("key1").get(0).getAsString());
    Assert.assertEquals(2, firstApplicationData.get("key1").get(1).getAsInt());

    // secondApplication and thirdApplication are in waiting state
    Assert.assertEquals(2, store.getAllInWaitingState().size());
    // Delete data for firstApplication as well as secondApplication
    store.deleteExpiredData(2);

    firstApplicationData = store.get(firstApplicationId, "mytracer");
    Assert.assertEquals(0, firstApplicationData.size());

    // only thirdApplication should be in waiting now
    Assert.assertEquals(1, store.getAllInWaitingState().size());
    Assert.assertEquals(thirdApplicationId, store.getAllInWaitingState().iterator().next().getProgram().getParent());
  }
}
