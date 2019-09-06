/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.client;

import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.client.app.AllProgramsApp;
import io.cdap.cdap.client.common.ClientTestBase;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.test.XSlowTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Tests for {@link io.cdap.cdap.gateway.handlers.UsageHandler}
 */
@Category(XSlowTests.class)
public class UsageHandlerTestRun extends ClientTestBase {

  private static final Gson GSON = new Gson();

  private void deployApp(Class<? extends Application> appCls) throws Exception {
    new ApplicationClient(getClientConfig()).deploy(NamespaceId.DEFAULT, createAppJarFile(appCls));
  }

  private void deleteApp(ApplicationId appId) throws Exception {
    new ApplicationClient(getClientConfig()).delete(appId);
  }

  private void startProgram(ProgramId programId) throws Exception {
    getProgramClient().start(programId);
  }

  private ProgramClient getProgramClient() {
    return new ProgramClient(getClientConfig());
  }

  @Test
  public void testWorkerUsage() throws Exception {
    final ApplicationId app = NamespaceId.DEFAULT.app(AllProgramsApp.NAME);
    final ProgramId program = app.worker(AllProgramsApp.NoOpWorker.NAME);
    final DatasetId dataset = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME);

    Assert.assertEquals(0, getAppDatasetUsage(app).size());
    Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());

    deployApp(AllProgramsApp.class);

    try {
      startProgram(program);
      // Wait for the worker to complete
      assertProgramRuns(getProgramClient(), program, ProgramRunStatus.COMPLETED, 1, 20);

      Assert.assertTrue(getProgramDatasetUsage(program).contains(dataset));
      Assert.assertTrue(getAppDatasetUsage(app).contains(dataset));
      Assert.assertTrue(getDatasetProgramUsage(dataset).contains(program));
    } finally {
      deleteApp(app);

      Assert.assertEquals(0, getAppDatasetUsage(app).size());
      Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());
    }
  }

  @Test
  public void testMapReduceUsage() throws Exception {
    final ApplicationId app = NamespaceId.DEFAULT.app(AllProgramsApp.NAME);
    final ProgramId program = app.mr(AllProgramsApp.NoOpMR.NAME);
    final DatasetId dataset = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME);
    final DatasetId dataset3 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME3);

    Assert.assertEquals(0, getAppDatasetUsage(app).size());
    Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());
    Assert.assertEquals(0, getDatasetProgramUsage(dataset3).size());

    deployApp(AllProgramsApp.class);
    // now that we only support dynamic dataset instantiation in initialize (and not in configure as before),
    // we must run the mapreduce program to register its usage
    startProgram(program);
    assertProgramRunning(getProgramClient(), program);
    assertProgramStopped(getProgramClient(), program);
    try {
      Assert.assertTrue(getProgramDatasetUsage(program).contains(dataset));
      Assert.assertTrue(getAppDatasetUsage(app).contains(dataset));
      Assert.assertTrue(getDatasetProgramUsage(dataset).contains(program));

      Assert.assertTrue(getProgramDatasetUsage(program).contains(dataset3));
      Assert.assertTrue(getAppDatasetUsage(app).contains(dataset3));
      Assert.assertTrue(getDatasetProgramUsage(dataset3).contains(program));
    } finally {
      deleteApp(app);
      Assert.assertEquals(0, getAppDatasetUsage(app).size());
      Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());
      Assert.assertEquals(0, getDatasetProgramUsage(dataset3).size());
    }
  }

  @Test
  public void testSparkUsage() throws Exception {
    final ApplicationId app = NamespaceId.DEFAULT.app(AllProgramsApp.NAME);
    final ProgramId program = app.spark(AllProgramsApp.NoOpSpark.NAME);
    List<DatasetId> datasets = Arrays.asList(
      NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME),
      NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME2),
      NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME3)
    );

    Assert.assertEquals(0, getAppDatasetUsage(app).size());
    for (DatasetId dataset : datasets) {
      Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());
    }

    deployApp(AllProgramsApp.class);

    try {
      // the program will run and stop by itself.
      startProgram(program);
      assertProgramRuns(getProgramClient(), program, ProgramRunStatus.COMPLETED, 1, 60);

      for (DatasetId dataset : datasets) {
        Assert.assertTrue(getProgramDatasetUsage(program).contains(dataset));
        Assert.assertTrue(getAppDatasetUsage(app).contains(dataset));
        Assert.assertTrue(getDatasetProgramUsage(dataset).contains(program));
      }
    } finally {
      deleteApp(app);

      Assert.assertEquals(0, getAppDatasetUsage(app).size());
      for (DatasetId dataset : datasets) {
        Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());
      }
    }
  }

  @Test
  public void testServiceUsage() throws Exception {
    final ApplicationId app = NamespaceId.DEFAULT.app(AllProgramsApp.NAME);
    final ProgramId program = app.service(AllProgramsApp.NoOpService.NAME);
    final DatasetId dataset = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME);

    Assert.assertEquals(0, getAppDatasetUsage(app).size());
    Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());

    deployApp(AllProgramsApp.class);

    try {
      Assert.assertTrue(getProgramDatasetUsage(program).contains(dataset));
      Assert.assertTrue(getAppDatasetUsage(app).contains(dataset));
      Assert.assertTrue(getDatasetProgramUsage(dataset).contains(program));
    } finally {
      deleteApp(app);

      Assert.assertEquals(0, getAppDatasetUsage(app).size());
      Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());
    }
  }

  private <T> T doGet(String path, Type responseType) throws IOException {
    ConnectionConfig connectionConfig = getClientConfig().getConnectionConfig();
    URL url = new URL(String.format("%s://%s:%d%s",
                                    connectionConfig.isSSLEnabled() ? "https" : "http",
                                    connectionConfig.getHostname(),
                                    connectionConfig.getPort(), path));

    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    try {
      Assert.assertEquals(200, urlConn.getResponseCode());
      try (Reader reader = new BufferedReader(new InputStreamReader(urlConn.getInputStream(), Charsets.UTF_8))) {
        return GSON.fromJson(reader, responseType);
      }
    } finally {
      urlConn.disconnect();
    }
  }

  private Set<DatasetId> getAppDatasetUsage(ApplicationId app) throws Exception {
    Set<DatasetId> datasetIds =
      doGet(String.format("/v3/namespaces/%s/apps/%s/datasets", app.getNamespace(), app.getEntityName()),
            new TypeToken<Set<DatasetId>>() { }.getType());
    return datasetIds;
  }

  private Set<DatasetId> getProgramDatasetUsage(ProgramId program) throws Exception {
    Set<DatasetId> datasetIds =
      doGet(String.format("/v3/namespaces/%s/apps/%s/%s/%s/datasets",
                          program.getNamespace(), program.getApplication(), program.getType().getCategoryName(),
                          program.getEntityName()),
            new TypeToken<Set<DatasetId>>() { }.getType());
    return datasetIds;
  }

  // dataset -> program

  private Set<ProgramId> getDatasetProgramUsage(DatasetId dataset) throws Exception {
    Set<ProgramId> programIds =
      doGet(String.format("/v3/namespaces/%s/data/datasets/%s/programs",
                          dataset.getNamespace(), dataset.getEntityName()),
            new TypeToken<Set<ProgramId>>() { }.getType());
    return programIds;
  }
}
