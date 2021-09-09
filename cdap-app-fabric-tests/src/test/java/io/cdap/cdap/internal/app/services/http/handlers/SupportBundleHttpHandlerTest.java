/*
 * Copyright © 2015-2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.WorkflowApp;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.api.RunId;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Monitor handler tests. */
public class SupportBundleHttpHandlerTest extends AppFabricTestBase {

  private static final ApplicationId WORKFLOW_APP = NamespaceId.DEFAULT.app("WorkflowApp");
  private static final String STOPPED = "STOPPED";
  private static final String RUNNING = "RUNNING";
  private static Store store;
  private int sourceId;

  @BeforeClass
  public static void setup() {
    store = getInjector().getInstance(DefaultStore.class);
  }

  @Test
  public void testCreateSupportBundleWithValidNamespace() throws Exception {
    createNamespace("default");
    String path =
        String.format("%s/support/bundle?namespaceId=default", Constants.Gateway.API_VERSION_3);
    HttpResponse response = doPost(path);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    String bodyResponse = response.getResponseBodyAsString();
    if (bodyResponse.startsWith("Support Bundle")) {
      String uuid =
        bodyResponse
          .substring(bodyResponse.indexOf("Bundle") + 6, bodyResponse.indexOf("generated"))
          .trim();
      // Delete this File
      String supportBundleDeleteFile = String.format("support/bundle/%s", uuid);
      HttpResponse supportBundleDeleteFileResponse =
        doDelete(
          String.format(
            "/%s/%s", Constants.Gateway.API_VERSION_3_TOKEN, supportBundleDeleteFile));
      Assert.assertEquals(200, supportBundleDeleteFileResponse.getResponseCode());
    }
  }

  @Test
  public void testCreateSupportBundleWithSystemLog() throws Exception {
    createNamespace("default");
    String path =
        String.format("%s/support/bundle?need-system-log=true", Constants.Gateway.API_VERSION_3);
    HttpResponse response = doPost(path);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    Assert.assertTrue(response.getResponseBodyAsString().startsWith("Support Bundle"));

    String bodyResponse = response.getResponseBodyAsString();
    if (bodyResponse.startsWith("Support Bundle")) {
      String uuid =
        bodyResponse
          .substring(bodyResponse.indexOf("Bundle") + 6, bodyResponse.indexOf("generated"))
          .trim();
      // Delete this File
      String supportBundleDeleteFile = String.format("support/bundle/%s", uuid);
      HttpResponse supportBundleDeleteFileResponse =
        doDelete(
          String.format(
            "/%s/%s", Constants.Gateway.API_VERSION_3_TOKEN, supportBundleDeleteFile));
      Assert.assertEquals(200, supportBundleDeleteFileResponse.getResponseCode());
    }
  }

  @Test
  public void testWorkerLogs() throws Exception {
    testLogsRunId();
  }

  private void testLogsRunId() throws Exception {
    deploy(
        WorkflowApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, WORKFLOW_APP.getNamespace());
    long startTime = System.currentTimeMillis();

    ProgramId workflowProgram =
        new ProgramId(
            WORKFLOW_APP.getNamespace(),
            WORKFLOW_APP.getApplication(),
            ProgramType.WORKFLOW,
            WorkflowApp.FunWorkflow.NAME);
    RunId workflowRunId = RunIds.generate(startTime);
    ArtifactId artifactId =
        WORKFLOW_APP.getNamespaceId().artifact("testArtifact", "1.0").toApiArtifactId();
    setStartAndRunning(workflowProgram, workflowRunId.getId(), artifactId);

    // start a program
    startProgram(workflowProgram);
    waitState(workflowProgram, RUNNING);

    List<RunRecord> runs = getProgramRuns(workflowProgram, ProgramRunStatus.RUNNING);
    Assert.assertEquals(1, runs.size());

    // stop the program
    stopProgram(workflowProgram);

    HttpResponse appsResponse =
        doGet(
            getVersionedAPIPath(
                "apps/",
                Constants.Gateway.API_VERSION_3_TOKEN,
                WORKFLOW_APP.getNamespaceId().getNamespace()));
    Assert.assertEquals(200, appsResponse.getResponseCode());

    String supportBundleCreateUrl =
        String.format(
            "support/bundle?namespace-id=%s&app-id=%s&workflow-name=%s&run-id=%s&need-system-log=true",
            WORKFLOW_APP.getNamespaceId().getNamespace(),
            WORKFLOW_APP.getApplication(),
            WorkflowApp.FunWorkflow.NAME,
            runs.get(0).getPid());

    HttpResponse supportBundleCreateResponse =
        doPost(
            String.format("/%s/%s", Constants.Gateway.API_VERSION_3_TOKEN, supportBundleCreateUrl));

    String bodyResponse = supportBundleCreateResponse.getResponseBodyAsString();
    if (bodyResponse.startsWith("Support Bundle")) {
      String uuid =
        bodyResponse
          .substring(bodyResponse.indexOf("Bundle") + 6, bodyResponse.indexOf("generated"))
          .trim();
      String supportBundleGetUrl =
        String.format(
          "support/bundle/%s/files?folder-name=%s&data-file-name=%s",
          uuid, WORKFLOW_APP.getApplication(), runs.get(0).getPid() + ".json");
      HttpResponse supportBundleGetFilesResponse =
        doGet(String.format("/%s/%s", Constants.Gateway.API_VERSION_3_TOKEN, supportBundleGetUrl));

      verifyLogs(supportBundleGetFilesResponse);
      // workflow ran for 1 minute
      long workflowStopTime = TimeUnit.MILLISECONDS.toSeconds(startTime) + 60;
      store.setStop(
        workflowProgram.run(workflowRunId.getId()),
        workflowStopTime,
        ProgramRunStatus.COMPLETED,
        AppFabricTestHelper.createSourceId(++sourceId));

      // Delete this File
      String supportBundleDeleteFile = String.format("support/bundle/%s", uuid);
      HttpResponse supportBundleDeleteFileResponse =
        doDelete(
          String.format(
            "/%s/%s", Constants.Gateway.API_VERSION_3_TOKEN, supportBundleDeleteFile));
      Assert.assertEquals(200, supportBundleDeleteFileResponse.getResponseCode());

      supportBundleGetFilesResponse =
        doGet(String.format("/%s/%s", Constants.Gateway.API_VERSION_3_TOKEN, supportBundleGetUrl));
      Assert.assertEquals(String.format("No such uuid %s in Support Bundle.", uuid),
                          supportBundleGetFilesResponse.getResponseBodyAsString());
    }
  }

  /**
   * Verify the logs returned in the {@link HttpResponse}.
   *
   * @param response {@link HttpResponse}
   */
  private void verifyLogs(HttpResponse response) throws JSONException {
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    String runInfoResponse = response.getResponseBodyAsString();
    JSONObject out =
        runInfoResponse != null && runInfoResponse.length() > 0
            ? new JSONObject(runInfoResponse)
            : null;
    if (out != null) {
      Assert.assertEquals(RUNNING, out.getString("status"));
      Assert.assertEquals("native", out.getString("profileName"));
    }
  }

  private void setStartAndRunning(ProgramId id, String pid, ArtifactId artifactId) {
    setStartAndRunning(id, pid, ImmutableMap.of(), ImmutableMap.of(), artifactId);
  }

  private void setStartAndRunning(
      ProgramId id,
      String pid,
      Map<String, String> runtimeArgs,
      Map<String, String> systemArgs,
      ArtifactId artifactId) {
    if (!systemArgs.containsKey(SystemArguments.PROFILE_NAME)) {
      systemArgs =
          ImmutableMap.<String, String>builder()
              .putAll(systemArgs)
              .put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName())
              .build();
    }
    long startTime = RunIds.getTime(pid, TimeUnit.SECONDS);
    store.setProvisioning(
        id.run(pid),
        runtimeArgs,
        systemArgs,
        AppFabricTestHelper.createSourceId(++sourceId),
        artifactId);
    store.setProvisioned(id.run(pid), 0, AppFabricTestHelper.createSourceId(++sourceId));
    store.setStart(id.run(pid), null, systemArgs, AppFabricTestHelper.createSourceId(++sourceId));
    store.setRunning(
        id.run(pid), startTime + 1, null, AppFabricTestHelper.createSourceId(++sourceId));
  }
}
