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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.XSlowTests;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;

/**
 * Tests for {@link co.cask.cdap.gateway.handlers.UsageHandler}
 */
@Category(XSlowTests.class)
public class UsageHandlerTest extends AppFabricTestBase {

  private static final Gson GSON = new Gson();

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

    deploy(AllProgramsApp.class);

    try {
      Assert.assertTrue(getAppStreamUsage(app).contains(stream));
      Assert.assertTrue(getProgramStreamUsage(program).contains(stream));
      Assert.assertTrue(getStreamProgramUsage(stream).contains(program));

      Assert.assertTrue(getProgramDatasetUsage(program).contains(dataset));
      Assert.assertTrue(getAppDatasetUsage(app).contains(dataset));
      Assert.assertTrue(getDatasetProgramUsage(dataset).contains(program));
    } finally {
      deleteApp(app, 200);

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

    deploy(AllProgramsApp.class);

    try {
      startProgram(program);
      waitState(program, "RUNNING");
      stopProgram(program);
      waitState(program, "STOPPED");

      Assert.assertTrue(getAppStreamUsage(app).contains(stream));
      Assert.assertTrue(getProgramStreamUsage(program).contains(stream));
      Assert.assertTrue(getStreamProgramUsage(stream).contains(program));

      Assert.assertTrue(getProgramDatasetUsage(program).contains(dataset));
      Assert.assertTrue(getAppDatasetUsage(app).contains(dataset));
      Assert.assertTrue(getDatasetProgramUsage(dataset).contains(program));
    } finally {
      deleteApp(app, 200);

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

    deploy(AllProgramsApp.class);

    try {
      Assert.assertTrue(getAppStreamUsage(app).contains(stream));
      Assert.assertTrue(getProgramStreamUsage(program).contains(stream));
      Assert.assertTrue(getStreamProgramUsage(stream).contains(program));

      Assert.assertTrue(getProgramDatasetUsage(program).contains(dataset));
      Assert.assertTrue(getAppDatasetUsage(app).contains(dataset));
      Assert.assertTrue(getDatasetProgramUsage(dataset).contains(program));
    } finally {
      deleteApp(app, 200);

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

    deploy(AllProgramsApp.class);

    try {
      // the program will run and stop by itself.
      startProgram(program);
      waitState(program, "STOPPED");

      Assert.assertTrue(getAppStreamUsage(app).contains(stream));
      Assert.assertTrue(getProgramStreamUsage(program).contains(stream));
      Assert.assertTrue(getStreamProgramUsage(stream).contains(program));

      Assert.assertTrue(getProgramDatasetUsage(program).contains(dataset));
      Assert.assertTrue(getAppDatasetUsage(app).contains(dataset));
      Assert.assertTrue(getDatasetProgramUsage(dataset).contains(program));
    } finally {
      deleteApp(app, 200);

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

    deploy(AllProgramsApp.class);

    try {
      Assert.assertTrue(getProgramDatasetUsage(program).contains(dataset));
      Assert.assertTrue(getAppDatasetUsage(app).contains(dataset));
      Assert.assertTrue(getDatasetProgramUsage(dataset).contains(program));
    } finally {
      deleteApp(app, 200);

      Assert.assertEquals(0, getAppDatasetUsage(app).size());
      Assert.assertEquals(0, getDatasetProgramUsage(dataset).size());
    }
  }

  @Test
  public void testAdapterStreamUsage() throws Exception {
    final Id.Adapter adapter = Id.Adapter.from("somespace", "noadapter");
    final Id.Stream stream = Id.Stream.from("somespace", "nostream");

    Assert.assertEquals(0, getAdapterStreamUsage(adapter).size());
    Assert.assertEquals(0, getStreamAdapterUsage(stream).size());

    // TODO: test actual adapter - adapter isn't easy to test from unit test right now
    getUsageRegistry().register(adapter, stream);

    Assert.assertEquals(1, getAdapterStreamUsage(adapter).size());
    Assert.assertEquals(stream, getAdapterStreamUsage(adapter).iterator().next());
    Assert.assertEquals(1, getStreamAdapterUsage(stream).size());
    Assert.assertEquals(adapter, getStreamAdapterUsage(stream).iterator().next());

    getUsageRegistry().unregister(adapter);
  }

  @Test
  public void testAdapterDatasetUsage() throws Exception {
    final Id.Adapter adapter = Id.Adapter.from("somespace", "noadapter");
    final Id.DatasetInstance dataset = Id.DatasetInstance.from("somespace", "nods");

    Assert.assertEquals(0, getAdapterDatasetUsage(adapter).size());
    Assert.assertEquals(0, getDatasetAdapterUsage(dataset).size());

    // TODO: test actual adapter - adapter isn't easy to test from unit test right now
    getUsageRegistry().register(adapter, dataset);

    Assert.assertEquals(1, getAdapterDatasetUsage(adapter).size());
    Assert.assertEquals(dataset, getAdapterDatasetUsage(adapter).iterator().next());
    Assert.assertEquals(1, getDatasetAdapterUsage(dataset).size());
    Assert.assertEquals(adapter, getDatasetAdapterUsage(dataset).iterator().next());

    getUsageRegistry().unregister(adapter);
  }

  private UsageRegistry getUsageRegistry() {
    return getInjector().getInstance(UsageRegistry.class);
  }

  // app/program/adapter -> dataset/stream

  private Set<Id.DatasetInstance> getAppDatasetUsage(Id.Application app) throws Exception {
    HttpResponse response = doGet(
      String.format("/v3/namespaces/%s/apps/%s/datasets", app.getNamespaceId(), app.getId()));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return GSON.fromJson(EntityUtils.toString(response.getEntity()),
                         new TypeToken<Set<Id.DatasetInstance>>() { }.getType());
  }

  private Set<Id.Stream> getAppStreamUsage(Id.Application app) throws Exception {
    HttpResponse response = doGet(
      String.format("/v3/namespaces/%s/apps/%s/streams", app.getNamespaceId(), app.getId()));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return GSON.fromJson(EntityUtils.toString(response.getEntity()),
                         new TypeToken<Set<Id.Stream>>() { }.getType());
  }

  private Set<Id.DatasetInstance> getProgramDatasetUsage(Id.Program program) throws Exception {
    HttpResponse response = doGet(
      String.format("/v3/namespaces/%s/apps/%s/%s/%s/datasets",
                    program.getNamespaceId(), program.getApplicationId(),
                    program.getType().getCategoryName(),
                    program.getId()));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return GSON.fromJson(EntityUtils.toString(response.getEntity()),
                         new TypeToken<Set<Id.DatasetInstance>>() { }.getType());
  }

  private Set<Id.Stream> getProgramStreamUsage(Id.Program program) throws Exception {
    HttpResponse response = doGet(
      String.format("/v3/namespaces/%s/apps/%s/%s/%s/streams",
                    program.getNamespaceId(), program.getApplicationId(),
                    program.getType().getCategoryName(),
                    program.getId()));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return GSON.fromJson(EntityUtils.toString(response.getEntity()),
                         new TypeToken<Set<Id.Stream>>() { }.getType());
  }

  private Set<Id.DatasetInstance> getAdapterDatasetUsage(Id.Adapter adapter) throws Exception {
    HttpResponse response = doGet(
      String.format("/v3/namespaces/%s/adapters/%s/datasets", adapter.getNamespaceId(), adapter.getId()));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return GSON.fromJson(EntityUtils.toString(response.getEntity()),
                         new TypeToken<Set<Id.DatasetInstance>>() { }.getType());
  }

  private Set<Id.Stream> getAdapterStreamUsage(Id.Adapter adapter) throws Exception {
    HttpResponse response = doGet(
      String.format("/v3/namespaces/%s/adapters/%s/streams", adapter.getNamespaceId(), adapter.getId()));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return GSON.fromJson(EntityUtils.toString(response.getEntity()),
                         new TypeToken<Set<Id.Stream>>() { }.getType());
  }

  // dataset/stream -> program/adapter

  private Set<Id.Program> getStreamProgramUsage(Id.Stream stream) throws Exception {
    HttpResponse response = doGet(
      String.format("/v3/namespaces/%s/streams/%s/programs", stream.getNamespaceId(), stream.getId()));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return GSON.fromJson(EntityUtils.toString(response.getEntity()),
                         new TypeToken<Set<Id.Program>>() { }.getType());
  }

  private Set<Id.Adapter> getStreamAdapterUsage(Id.Stream stream) throws Exception {
    HttpResponse response = doGet(
      String.format("/v3/namespaces/%s/streams/%s/adapters", stream.getNamespaceId(), stream.getId()));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return GSON.fromJson(EntityUtils.toString(response.getEntity()),
                         new TypeToken<Set<Id.Adapter>>() { }.getType());
  }

  private Set<Id.Program> getDatasetProgramUsage(Id.DatasetInstance dataset) throws Exception {
    HttpResponse response = doGet(
      String.format("/v3/namespaces/%s/data/datasets/%s/programs", dataset.getNamespaceId(), dataset.getId()));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return GSON.fromJson(EntityUtils.toString(response.getEntity()),
                         new TypeToken<Set<Id.Program>>() { }.getType());
  }

  private Set<Id.Adapter> getDatasetAdapterUsage(Id.DatasetInstance dataset) throws Exception {
    HttpResponse response = doGet(
      String.format("/v3/namespaces/%s/data/datasets/%s/adapters", dataset.getNamespaceId(), dataset.getId()));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return GSON.fromJson(EntityUtils.toString(response.getEntity()),
                         new TypeToken<Set<Id.Adapter>>() { }.getType());
  }

}
