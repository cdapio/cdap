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

package co.cask.cdap.client;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.client.app.AllProgramsApp;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.XSlowTests;
import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link co.cask.cdap.gateway.handlers.UsageHandler}
 */
@Category(XSlowTests.class)
public class UsageHandlerTestRun extends ClientTestBase {

  private static final Gson GSON = new Gson();

  private void deployApp(Class<? extends Application> appCls) throws Exception {
    new ApplicationClient(getClientConfig()).deploy(Id.Namespace.DEFAULT, createAppJarFile(appCls));
  }

  private void deleteApp(Id.Application appId) throws Exception {
    new ApplicationClient(getClientConfig()).delete(appId);
  }

  private void startProgram(Id.Program programId) throws Exception {
    new ProgramClient(getClientConfig()).start(programId);
  }

  private void stopProgram(Id.Program programId) throws Exception {
    new ProgramClient(getClientConfig()).stop(programId);
  }

  private void waitState(final Id.Program programId, ProgramStatus status) throws Exception {
    final ProgramClient programclient = new ProgramClient(getClientConfig());
    Tasks.waitFor(status, new Callable<ProgramStatus>() {
      @Override
      public ProgramStatus call() throws Exception {
        return ProgramStatus.valueOf(programclient.getStatus(programId));
      }
    }, 60, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testFlowUsage() throws Exception {
    final Id.Application app = Id.Application.from("default", AllProgramsApp.NAME);
    final Id.Program program = Id.Program.from(app, ProgramType.FLOW, AllProgramsApp.NoOpFlow.NAME);
    final Id.Stream stream = Id.Stream.from("default", AllProgramsApp.STREAM_NAME);
    final Id.DatasetInstance dataset = Id.DatasetInstance.from("default", AllProgramsApp.DATASET_NAME);

    Assert.assertEquals(0, getAppStreamUsage(app).size());
    Assert.assertEquals(0, getProgramStreamUsage(program).size());
    Assert.assertEquals(0, getStreamProgramUsage(stream).size());

    Assert.assertEquals(0, getProgramDatasetUsage(program).size());
    Assert.assertEquals(0, getAppDatasetUsage(app).size());
    Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());

    deployApp(AllProgramsApp.class);

    try {
      Assert.assertTrue(getAppStreamUsage(app).contains(stream));
      Assert.assertTrue(getProgramStreamUsage(program).contains(stream));
      Assert.assertTrue(getStreamProgramUsage(stream).contains(program));

      Assert.assertTrue(getProgramDatasetUsage(program).contains(dataset));
      Assert.assertTrue(getAppDatasetUsage(app).contains(dataset));
      Assert.assertTrue(getDatasetProgramUsage(dataset).contains(program));
    } finally {
      deleteApp(app);

      Assert.assertEquals(0, getAppStreamUsage(app).size());
      Assert.assertEquals(0, getProgramStreamUsage(program).size());
      Assert.assertEquals(0, getStreamProgramUsage(stream).size());
    }
  }

  @Test
  public void testWorkerUsage() throws Exception {
    final Id.Application app = Id.Application.from("default", AllProgramsApp.NAME);
    final Id.Program program = Id.Program.from(app, ProgramType.WORKER, AllProgramsApp.NoOpWorker.NAME);
    final Id.Stream stream = Id.Stream.from("default", AllProgramsApp.STREAM_NAME);
    final Id.DatasetInstance dataset = Id.DatasetInstance.from("default", AllProgramsApp.DATASET_NAME);

    Assert.assertEquals(0, getAppStreamUsage(app).size());
    Assert.assertEquals(0, getProgramStreamUsage(program).size());
    Assert.assertEquals(0, getStreamProgramUsage(stream).size());

    Assert.assertEquals(0, getAppDatasetUsage(app).size());
    Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());

    deployApp(AllProgramsApp.class);

    try {
      startProgram(program);
      // Wait for the worker to run and then stop.
      waitState(program, ProgramStatus.RUNNING);
      waitState(program, ProgramStatus.STOPPED);

      Assert.assertTrue(getAppStreamUsage(app).contains(stream));
      Assert.assertTrue(getProgramStreamUsage(program).contains(stream));
      Assert.assertTrue(getStreamProgramUsage(stream).contains(program));

      Assert.assertTrue(getProgramDatasetUsage(program).contains(dataset));
      Assert.assertTrue(getAppDatasetUsage(app).contains(dataset));
      Assert.assertTrue(getDatasetProgramUsage(dataset).contains(program));
    } finally {
      deleteApp(app);

      Assert.assertEquals(0, getAppStreamUsage(app).size());
      Assert.assertEquals(0, getProgramStreamUsage(program).size());
      Assert.assertEquals(0, getStreamProgramUsage(stream).size());

      Assert.assertEquals(0, getAppDatasetUsage(app).size());
      Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());
    }
  }

  @Test
  public void testMapReduceUsage() throws Exception {
    final Id.Application app = Id.Application.from("default", AllProgramsApp.NAME);
    final Id.Program program = Id.Program.from(app, ProgramType.MAPREDUCE, AllProgramsApp.NoOpMR.NAME);
    final Id.Stream stream = Id.Stream.from("default", AllProgramsApp.STREAM_NAME);
    final Id.DatasetInstance dataset = Id.DatasetInstance.from("default", AllProgramsApp.DATASET_NAME);

    Assert.assertEquals(0, getAppStreamUsage(app).size());
    Assert.assertEquals(0, getProgramStreamUsage(program).size());
    Assert.assertEquals(0, getStreamProgramUsage(stream).size());

    Assert.assertEquals(0, getAppDatasetUsage(app).size());
    Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());

    deployApp(AllProgramsApp.class);
    // now that we only support dynamic dataset instantiation in initialize (and not in configure as before),
    // we must run the mapreduce program to register its usage
    startProgram(program);
    waitState(program, ProgramStatus.STOPPED);

    try {
      Assert.assertTrue(getAppStreamUsage(app).contains(stream));
      Assert.assertTrue(getProgramStreamUsage(program).contains(stream));
      Assert.assertTrue(getStreamProgramUsage(stream).contains(program));

      Assert.assertTrue(getProgramDatasetUsage(program).contains(dataset));
      Assert.assertTrue(getAppDatasetUsage(app).contains(dataset));
      Assert.assertTrue(getDatasetProgramUsage(dataset).contains(program));
    } finally {
      deleteApp(app);

      Assert.assertEquals(0, getAppStreamUsage(app).size());
      Assert.assertEquals(0, getProgramStreamUsage(program).size());
      Assert.assertEquals(0, getStreamProgramUsage(stream).size());

      Assert.assertEquals(0, getAppDatasetUsage(app).size());
      Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());
    }
  }

  @Test
  public void testSparkUsage() throws Exception {
    final Id.Application app = Id.Application.from("default", AllProgramsApp.NAME);
    final Id.Program program = Id.Program.from(app, ProgramType.SPARK, AllProgramsApp.NoOpSpark.NAME);
    final Id.Stream stream = Id.Stream.from("default", AllProgramsApp.STREAM_NAME);
    final Id.DatasetInstance dataset = Id.DatasetInstance.from("default", AllProgramsApp.DATASET_NAME);

    Assert.assertEquals(0, getAppStreamUsage(app).size());
    Assert.assertEquals(0, getProgramStreamUsage(program).size());
    Assert.assertEquals(0, getStreamProgramUsage(stream).size());

    Assert.assertEquals(0, getAppDatasetUsage(app).size());
    Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());

    deployApp(AllProgramsApp.class);

    try {
      // the program will run and stop by itself.
      startProgram(program);
      waitState(program, ProgramStatus.STOPPED);

      Assert.assertTrue(getAppStreamUsage(app).contains(stream));
      Assert.assertTrue(getProgramStreamUsage(program).contains(stream));
      Assert.assertTrue(getStreamProgramUsage(stream).contains(program));

      Assert.assertTrue(getProgramDatasetUsage(program).contains(dataset));
      Assert.assertTrue(getAppDatasetUsage(app).contains(dataset));
      Assert.assertTrue(getDatasetProgramUsage(dataset).contains(program));
    } finally {
      deleteApp(app);

      Assert.assertEquals(0, getAppStreamUsage(app).size());
      Assert.assertEquals(0, getProgramStreamUsage(program).size());
      Assert.assertEquals(0, getStreamProgramUsage(stream).size());

      Assert.assertEquals(0, getAppDatasetUsage(app).size());
      Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());
    }

    deployApp(AllProgramsApp.class);

    try {
      // the program will run and stop by itself.
      startProgram(program);
      waitState(program, ProgramStatus.STOPPED);

      Assert.assertTrue(getAppStreamUsage(app).contains(stream));
      Assert.assertTrue(getProgramStreamUsage(program).contains(stream));
      Assert.assertTrue(getStreamProgramUsage(stream).contains(program));

      Assert.assertTrue(getProgramDatasetUsage(program).contains(dataset));
      Assert.assertTrue(getAppDatasetUsage(app).contains(dataset));
      Assert.assertTrue(getDatasetProgramUsage(dataset).contains(program));
    } finally {
      deleteApp(app);

      Assert.assertEquals(0, getAppStreamUsage(app).size());
      Assert.assertEquals(0, getProgramStreamUsage(program).size());
      Assert.assertEquals(0, getStreamProgramUsage(stream).size());

      Assert.assertEquals(0, getAppDatasetUsage(app).size());
      Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());
    }
  }

  @Test
  public void testServiceUsage() throws Exception {
    final Id.Application app = Id.Application.from("default", AllProgramsApp.NAME);
    final Id.Program program = Id.Program.from(app, ProgramType.SERVICE, AllProgramsApp.NoOpService.NAME);
    final Id.DatasetInstance dataset = Id.DatasetInstance.from("default", AllProgramsApp.DATASET_NAME);

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
    URL url = new URL(String.format("http://%s:%d%s",
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

  private Set<Id.DatasetInstance> getAppDatasetUsage(Id.Application app) throws Exception {
    return doGet(String.format("/v3/namespaces/%s/apps/%s/datasets", app.getNamespaceId(), app.getId()),
                 new TypeToken<Set<Id.DatasetInstance>>() { }.getType());
  }

  private Set<Id.Stream> getAppStreamUsage(Id.Application app) throws Exception {
    return doGet(String.format("/v3/namespaces/%s/apps/%s/streams", app.getNamespaceId(), app.getId()),
                 new TypeToken<Set<Id.Stream>>() { }.getType());
  }

  private Set<Id.DatasetInstance> getProgramDatasetUsage(Id.Program program) throws Exception {
    return doGet(String.format("/v3/namespaces/%s/apps/%s/%s/%s/datasets",
                               program.getNamespaceId(), program.getApplicationId(),
                               program.getType().getCategoryName(), program.getId()),
                 new TypeToken<Set<Id.DatasetInstance>>() { }.getType());
  }

  private Set<Id.Stream> getProgramStreamUsage(Id.Program program) throws Exception {
    return doGet(String.format("/v3/namespaces/%s/apps/%s/%s/%s/streams",
                               program.getNamespaceId(), program.getApplicationId(),
                               program.getType().getCategoryName(),
                               program.getId()),
                 new TypeToken<Set<Id.Stream>>() { }.getType());
  }

  // dataset/stream -> program

  private Set<Id.Program> getStreamProgramUsage(Id.Stream stream) throws Exception {
    return doGet(String.format("/v3/namespaces/%s/streams/%s/programs",
                               stream.getNamespaceId(), stream.getId()),
                 new TypeToken<Set<Id.Program>>() { }.getType());
  }

  private Set<Id.Program> getDatasetProgramUsage(Id.DatasetInstance dataset) throws Exception {
    return doGet(String.format("/v3/namespaces/%s/data/datasets/%s/programs",
                               dataset.getNamespaceId(), dataset.getId()),
                 new TypeToken<Set<Id.Program>>() { }.getType());
  }
}
