/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.support.handlers;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.AppWithWorkflow;
import io.cdap.cdap.SupportBundleTestBase;
import io.cdap.cdap.SupportBundleTestHelper;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Monitor handler tests.
 */
public class SupportBundleHttpHandlerTest extends SupportBundleTestBase {

  private static final NamespaceId NAMESPACE = new NamespaceId("test");
  private static final NamespaceId NAMESPACE_DEFAULT_ID = NamespaceId.DEFAULT;
  private static final ApplicationId APP_WORKFLOW = NAMESPACE.app("AppWithWorkflow");
  private static final String RUNNING = "RUNNING";
  private static CConfiguration cConf;
  private static Store store;
  private int sourceId;

  @Before
  public void setup() throws Exception {
    Assert.assertEquals(HttpURLConnection.HTTP_OK, createNamespace(NAMESPACE).getResponseCode());
    store = getInjector().getInstance(DefaultStore.class);
    cConf = getInjector().getInstance(CConfiguration.class);
  }

  @After
  public void cleanup() throws IOException {
    Assert.assertEquals(HttpURLConnection.HTTP_OK, deleteNamespace(NAMESPACE).getResponseCode());
  }

  @Test
  public void testCreateSupportBundleWithValidNamespace() throws Exception {

    String bundleId = requestBundle(Collections.singletonMap("namespace", NAMESPACE.getNamespace()));

    Assert.assertNotNull(bundleId);
    Assert.assertFalse(bundleId.isEmpty());
  }

  @Test
  public void testCreateSupportBundleWithMaxRunsSpecified() throws Exception {
    String bundleId = requestBundle(Collections.singletonMap("maxRunsPerProgram", "2"));

    Assert.assertNotNull(bundleId);
    Assert.assertFalse(bundleId.isEmpty());
  }

  @Test
  public void testWorkerLogs() throws Exception {
    testLogsRunId();
  }

  private void testLogsRunId() throws Exception {
    String runId = generateWorkflowLog();

    Map<String, String> createParams = Stream.of(
      new String[][]{
        {"namespace", NAMESPACE_DEFAULT_ID.getNamespace()},
        {"application", APP_WORKFLOW.getApplication()},
        {"programId", AppWithWorkflow.SampleWorkflow.NAME},
        {"run", runId}}).collect(Collectors.toMap(data -> data[0], data -> data[1]));

    String bundleId = requestBundle(createParams);
    if (bundleId != null) {
      File tempFolder = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR));
      File uuidFile = new File(tempFolder, bundleId);

      File folderDirectory = new File(uuidFile, APP_WORKFLOW.getApplication());
      File runFileDirectory = new File(folderDirectory, runId + ".json");
      Tasks.waitFor(true, () -> {
        return runFileDirectory.exists();
      }, 60, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);

      Map<String, String> getBundleFilesParams = Stream.of(
        new String[][]{
          {"folder-name", APP_WORKFLOW.getApplication()},
          {"data-file-name", runId + ".json"}})
        .collect(Collectors.toMap(data -> data[0], data -> data[1]));

      String runInfoResponse = requestGetBundle(getBundleFilesParams, bundleId);

      verifyLogs(runInfoResponse);

      // Delete this File
      requestDeleteBundle(bundleId);
      Assert.assertEquals(String.format("No such uuid '%s' in Support Bundle.", bundleId),
                          requestGetBundle(getBundleFilesParams, bundleId));
    }
  }

  /**
   * Verify the logs returned in the {@link HttpResponse}.
   *
   * @param runInfoResponse {@link String}
   */
  private void verifyLogs(String runInfoResponse) throws JSONException {
    JSONObject out = runInfoResponse != null && runInfoResponse.length() > 0 ? new JSONObject(runInfoResponse) : null;
    if (out != null) {
      Assert.assertEquals(RUNNING, out.getString("status"));
      Assert.assertEquals("native", out.getString("profileName"));
    }
  }

  /**
   * Requests generation of support bundle.
   *
   * @param params a map of query parameters
   * @return the bundle UUID
   * @throws IOException if failed to request bundle generation
   */
  private String requestBundle(Map<String, String> params) throws IOException {
    DiscoveryServiceClient discoveryServiceClient = getInjector().getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable =
      new RandomEndpointStrategy(() -> discoveryServiceClient.discover(Constants.Service.SUPPORT_BUNDLE_SERVICE)).pick(
        5, TimeUnit.SECONDS);

    Assert.assertNotNull("No service for support bundle", discoverable);

    StringBuilder queryBuilder = new StringBuilder();
    String sep = "?";
    for (Map.Entry<String, String> entry : params.entrySet()) {
      queryBuilder.append(sep)
        .append(URLEncoder.encode(entry.getKey(), "UTF-8"))
        .append("=")
        .append(URLEncoder.encode(entry.getValue(), "UTF-8"));
      sep = "&";
    }

    String path = String.format("%s/support/bundle%s", Constants.Gateway.API_VERSION_3, queryBuilder);

    HttpRequest request = HttpRequest.post(URIScheme.createURI(discoverable, path).toURL()).build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    return response.getResponseBodyAsString();
  }

  /**
   * Get support bundle file.
   *
   * @param params a map of query parameters
   * @param bundleId support bundle unique Id
   * @return the bundle file messages
   * @throws IOException if failed to request bundle generation
   */
  private String requestGetBundle(Map<String, String> params, String bundleId) throws IOException {
    DiscoveryServiceClient discoveryServiceClient = getInjector().getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable =
      new RandomEndpointStrategy(() -> discoveryServiceClient.discover(Constants.Service.SUPPORT_BUNDLE_SERVICE)).pick(
        5, TimeUnit.SECONDS);

    Assert.assertNotNull("No service for support bundle", discoverable);

    StringBuilder queryBuilder = new StringBuilder();
    String sep = "?";
    for (Map.Entry<String, String> entry : params.entrySet()) {
      queryBuilder.append(sep)
        .append(URLEncoder.encode(entry.getKey(), "UTF-8"))
        .append("=")
        .append(URLEncoder.encode(entry.getValue(), "UTF-8"));
      sep = "&";
    }

    String path =
      String.format("%s/support/bundle/%s/files%s", Constants.Gateway.API_VERSION_3, bundleId, queryBuilder);

    HttpRequest request = HttpRequest.get(URIScheme.createURI(discoverable, path).toURL()).build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    return response.getResponseBodyAsString();
  }

  /**
   * Delete certain support bundle.
   *
   * @param bundleId support bundle id
   * @throws IOException if failed to delete bundle
   */
  private void requestDeleteBundle(String bundleId) throws IOException {
    DiscoveryServiceClient discoveryServiceClient = getInjector().getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable =
      new RandomEndpointStrategy(() -> discoveryServiceClient.discover(Constants.Service.SUPPORT_BUNDLE_SERVICE)).pick(
        5, TimeUnit.SECONDS);

    Assert.assertNotNull("No service for support bundle", discoverable);

    String path = String.format("%s/support/bundle/%s", Constants.Gateway.API_VERSION_3, bundleId);

    HttpRequest request = HttpRequest.delete(URIScheme.createURI(discoverable, path).toURL()).build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
  }

  private String generateWorkflowLog() throws Exception {
    deploy(AppWithWorkflow.class, 200, Constants.Gateway.API_VERSION_3_TOKEN,
           NAMESPACE_DEFAULT_ID.getNamespace());
    long startTime = System.currentTimeMillis();

    ProgramId workflowProgram = new ProgramId(NAMESPACE_DEFAULT_ID.getNamespace(), AppWithWorkflow.NAME,
                                              ProgramType.WORKFLOW, AppWithWorkflow.SampleWorkflow.NAME);
    RunId workflowRunId = RunIds.generate(startTime);
    ArtifactId artifactId = NAMESPACE_DEFAULT_ID.artifact("testArtifact", "1.0").toApiArtifactId();
    setStartAndRunning(workflowProgram, workflowRunId.getId(), artifactId);

    List<RunRecord> runs = getProgramRuns(workflowProgram, ProgramRunStatus.RUNNING);
    Assert.assertEquals(1, runs.size());

    HttpResponse appsResponse =
      doGet(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN,
                                NAMESPACE_DEFAULT_ID.getNamespace()));
    Assert.assertEquals(200, appsResponse.getResponseCode());

    // workflow ran for 1 minute
    long workflowStopTime = TimeUnit.MILLISECONDS.toSeconds(startTime) + 60;
    store.setStop(workflowProgram.run(workflowRunId.getId()), workflowStopTime, ProgramRunStatus.COMPLETED,
                  SupportBundleTestHelper.createSourceId(++sourceId));

    return runs.get(0).getPid();
  }

  private void setStartAndRunning(ProgramId id, String pid, ArtifactId artifactId) {
    setStartAndRunning(id, pid, ImmutableMap.of(), ImmutableMap.of(), artifactId);
  }

  private void setStartAndRunning(ProgramId id, String pid, Map<String, String> runtimeArgs,
                                  Map<String, String> systemArgs, ArtifactId artifactId) {
    if (!systemArgs.containsKey(SystemArguments.PROFILE_NAME)) {
      systemArgs = ImmutableMap.<String, String>builder()
        .putAll(systemArgs)
        .put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName())
        .build();
    }
    long startTime = RunIds.getTime(pid, TimeUnit.SECONDS);
    store.setProvisioning(id.run(pid), runtimeArgs, systemArgs, SupportBundleTestHelper.createSourceId(++sourceId),
                          artifactId);
    store.setProvisioned(id.run(pid), 0, SupportBundleTestHelper.createSourceId(++sourceId));
    store.setStart(id.run(pid), null, systemArgs, SupportBundleTestHelper.createSourceId(++sourceId));
    store.setRunning(id.run(pid), startTime + 1, null, SupportBundleTestHelper.createSourceId(++sourceId));
  }
}
