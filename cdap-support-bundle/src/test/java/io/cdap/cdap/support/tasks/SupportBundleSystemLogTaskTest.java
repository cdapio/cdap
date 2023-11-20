/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.support.tasks;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.cdap.cdap.AppWithWorkflow;
import io.cdap.cdap.SupportBundleTestBase;
import io.cdap.cdap.SupportBundleTestHelper;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.logging.gateway.handlers.RemoteLogsFetcher;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.support.metadata.RemoteMonitorServicesFetcher;
import io.cdap.cdap.support.task.SupportBundleSystemLogTask;
import io.cdap.common.http.HttpResponse;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.twill.api.RunId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SupportBundleSystemLogTaskTest extends SupportBundleTestBase {
  private static final NamespaceId NAMESPACE = TEST_NAMESPACE_META1.getNamespaceId();
  private static CConfiguration configuration;
  private static Store store;
  private static RemoteLogsFetcher remoteLogsFetcher;
  private static RemoteMonitorServicesFetcher remoteMonitorServicesFetcher;
  private int sourceId;

  @BeforeClass
  public static void setup() throws Exception {
    Injector injector = getInjector();
    configuration = injector.getInstance(CConfiguration.class);
    store = injector.getInstance(DefaultStore.class);
    remoteLogsFetcher = injector.getInstance(RemoteLogsFetcher.class);
    remoteMonitorServicesFetcher = injector.getInstance(RemoteMonitorServicesFetcher.class);
  }
  

  @Test
  public void testSupportBundleSystemLogTask() throws Exception {
    generateWorkflowLog();
    String uuid = UUID.randomUUID().toString();
    File tempFolder = new File(configuration.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    File uuidFile = new File(tempFolder, uuid);
    SupportBundleSystemLogTask systemLogTask =
      new SupportBundleSystemLogTask(uuidFile, remoteLogsFetcher, configuration, remoteMonitorServicesFetcher);
    systemLogTask.collect();

    File systemLogFolder = new File(uuidFile, "system-log");
    List<File> systemLogFiles = DirUtils.listFiles(systemLogFolder,
                                                   file -> !file.isHidden() && !file.getParentFile().isHidden());

    Assert.assertEquals(9, systemLogFiles.size());
  }

  @Before
  public void setupNamespace() throws Exception {
    Assert.assertEquals(HttpURLConnection.HTTP_OK, createNamespace(NAMESPACE).getResponseCode());
  }

  @After
  public void cleanup() throws IOException {
    Assert.assertEquals(HttpURLConnection.HTTP_OK, deleteNamespace(NAMESPACE).getResponseCode());
  }

  private String generateWorkflowLog() throws Exception {
    deploy(AppWithWorkflow.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, NAMESPACE.getNamespace());
    long startTime = System.currentTimeMillis();

    HttpResponse appsResponse =
      doGet(getVersionedApiPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN,
                                NAMESPACE.getNamespace()));
    Assert.assertEquals(200, appsResponse.getResponseCode());
    String version = getResponseApplicationRecordVersion(appsResponse.getResponseBodyAsString());

    // We need the exact version here because setStartAndRunning() writes to store directly
    ProgramId workflowProgram = new ProgramId(NAMESPACE.getNamespace(), AppWithWorkflow.NAME, version,
                                              ProgramType.WORKFLOW, AppWithWorkflow.SampleWorkflow.NAME);
    RunId workflowRunId = RunIds.generate(startTime);
    ArtifactId artifactId = NAMESPACE.getNamespaceId().artifact("testArtifact", "1.0").toApiArtifactId();
    setStartAndRunning(workflowProgram, workflowRunId.getId(), artifactId);

    List<RunRecord> runs = getProgramRuns(workflowProgram, ProgramRunStatus.RUNNING);
    Assert.assertEquals(1, runs.size());

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
