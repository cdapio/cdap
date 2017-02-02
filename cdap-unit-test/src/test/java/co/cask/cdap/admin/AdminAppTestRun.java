/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.admin;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ProgramManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests whether admin operations work in program contexts.
 */
public class AdminAppTestRun extends TestFrameworkTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  private static final Gson GSON = new Gson();
  private static File artifactJar;

  private ApplicationManager appManager;

  @BeforeClass
  public static void init() throws IOException {
    artifactJar = createArtifactJar(AdminApp.class);
  }

  @Before
  public void deploy() throws Exception {
    appManager = deployWithArtifact(AdminApp.class, artifactJar);
  }

  @Test
  public void testAdminFlow() throws Exception {

    // start the worker and wait for it to finish
    FlowManager flowManager = appManager.getFlowManager(AdminApp.FLOW_NAME).start();

    try {
      flowManager.waitForStatus(true, 5, 5);

      // send some events to the stream
      StreamManager streamManager = getStreamManager("events");
      streamManager.send("aa ab bc aa bc");
      streamManager.send("xx xy aa ab aa");

      // wait for flow to process them
      flowManager.getFlowletMetrics("counter").waitForProcessed(10, 30, TimeUnit.SECONDS);

      // validate that the flow created tables for a, b, and x, and that the counts are correct
      DataSetManager<KeyValueTable> aManager = getDataset("counters_a");
      Assert.assertNotNull(aManager.get());
      Assert.assertEquals(4L, Bytes.toLong(aManager.get().read("aa")));
      Assert.assertEquals(2L, Bytes.toLong(aManager.get().read("ab")));

      DataSetManager<KeyValueTable> bManager = getDataset("counters_b");
      Assert.assertNotNull(bManager.get());
      Assert.assertEquals(2L, Bytes.toLong(bManager.get().read("bc")));

      DataSetManager<KeyValueTable> xManager = getDataset("counters_x");
      Assert.assertNotNull(xManager.get());
      Assert.assertEquals(1L, Bytes.toLong(xManager.get().read("xx")));
      Assert.assertEquals(1L, Bytes.toLong(xManager.get().read("xy")));

    } finally {
      flowManager.stop();
    }
    flowManager.waitForFinish(30, TimeUnit.SECONDS);

    // flowlet destroy() deletes all the tables - validate
    Assert.assertNull(getDataset("counters_a").get());
    Assert.assertNull(getDataset("counters_b").get());
    Assert.assertNull(getDataset("counters_x").get());
  }

  @Test
  public void testAdminWorker() throws Exception {
    testAdminProgram(appManager.getWorkerManager(AdminApp.WORKER_NAME));
  }

  @Test
  public void testAdminWorkflow() throws Exception {
    testAdminProgram(appManager.getWorkflowManager(AdminApp.WORKFLOW_NAME));
  }

  private <T extends ProgramManager<T>>
  void testAdminProgram(ProgramManager<T> manager) throws Exception {

    // create fileset b; it will be updated by the worker
    addDatasetInstance(FileSet.class.getName(), "b", FileSetProperties
      .builder().setBasePath("some/path").setInputFormat(TextInputFormat.class).build());
    DataSetManager<FileSet> bManager = getDataset("b");
    String bFormat = bManager.get().getInputFormatClassName();
    String bPath = bManager.get().getBaseLocation().toURI().getPath();
    Assert.assertTrue(bPath.endsWith("some/path/"));
    bManager.flush();

    // create table c and write some data to it; it will be truncated by the worker
    addDatasetInstance("table", "c");
    DataSetManager<Table> cManager = getDataset("c");
    cManager.get().put(new Put("x", "y", "z"));
    cManager.flush();

    // create table d; it will be dropped by the worker
    addDatasetInstance("table", "d");

    // start the worker and wait for it to finish
    File newBasePath = new File(TMP_FOLDER.newFolder(), "extra");
    Assert.assertFalse(newBasePath.exists());
    manager.start(ImmutableMap.of("new.base.path", newBasePath.getPath()));
    manager.waitForRun(ProgramRunStatus.COMPLETED, 30, TimeUnit.SECONDS);

    // validate that worker created dataset a
    DataSetManager<Table> aManager = getDataset("a");
    Assert.assertNull(aManager.get().scan(null, null).next());
    aManager.flush();

    // validate that worker update fileset b, Get a new instance of b
    bManager = getDataset("b");
    Assert.assertEquals(bFormat, bManager.get().getInputFormatClassName());
    String newBPath = bManager.get().getBaseLocation().toURI().getPath();
    Assert.assertTrue(newBPath.endsWith("/extra/"));
    // make sure the directory was created by fileset update (by moving the existing base path)
    Assert.assertTrue(newBasePath.exists());
    bManager.flush();

    // validate that dataset c is empty
    Assert.assertNull(cManager.get().scan(null, null).next());
    cManager.flush();

    // validate that dataset d is gone
    Assert.assertNull(getDataset("d").get());

    // run the worker again to drop all datasets
    manager.start(ImmutableMap.of("dropAll", "true"));
    manager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 30, TimeUnit.SECONDS);

    Assert.assertNull(getDataset("a").get());
    Assert.assertNull(getDataset("b").get());
    Assert.assertNull(getDataset("c").get());
    Assert.assertNull(getDataset("d").get());
  }

  @Test
  public void testAdminService() throws Exception {

    // Start the service
    ServiceManager serviceManager = appManager.getServiceManager(AdminApp.SERVICE_NAME).start();

    try {
      URI serviceURI = serviceManager.getServiceURL(10, TimeUnit.SECONDS).toURI();

      // dataset nn should not exist
      HttpResponse response = HttpRequests.execute(HttpRequest.get(serviceURI.resolve("exists/nn").toURL()).build());
      Assert.assertEquals(200, response.getResponseCode());
      Assert.assertEquals("false", response.getResponseBodyAsString());

      // create nn as a table
      response = HttpRequests.execute(HttpRequest.put(serviceURI.resolve("create/nn/table").toURL()).build());
      Assert.assertEquals(200, response.getResponseCode());

      // now nn should exist
      response = HttpRequests.execute(HttpRequest.get(serviceURI.resolve("exists/nn").toURL()).build());
      Assert.assertEquals(200, response.getResponseCode());
      Assert.assertEquals("true", response.getResponseBodyAsString());

      // create it again as a fileset -> should fail with conflict
      response = HttpRequests.execute(HttpRequest.put(serviceURI.resolve("create/nn/fileSet").toURL()).build());
      Assert.assertEquals(409, response.getResponseCode());

      // get the type for xx -> not found
      response = HttpRequests.execute(HttpRequest.get(serviceURI.resolve("type/xx").toURL()).build());
      Assert.assertEquals(404, response.getResponseCode());

      // get the type for nn -> table
      response = HttpRequests.execute(HttpRequest.get(serviceURI.resolve("type/nn").toURL()).build());
      Assert.assertEquals(200, response.getResponseCode());
      Assert.assertEquals("table", response.getResponseBodyAsString());

      // update xx's properties -> should get not-found
      Map<String, String> nnProps = TableProperties.builder().setTTL(1000L).build().getProperties();
      response = HttpRequests.execute(HttpRequest.put(serviceURI.resolve("update/xx").toURL())
                                        .withBody(GSON.toJson(nnProps)).build());
      Assert.assertEquals(404, response.getResponseCode());

      // update nn's properties
      response = HttpRequests.execute(HttpRequest.put(serviceURI.resolve("update/nn").toURL())
                                        .withBody(GSON.toJson(nnProps)).build());
      Assert.assertEquals(200, response.getResponseCode());

      // get properties for xx -> not found
      response = HttpRequests.execute(HttpRequest.get(serviceURI.resolve("props/xx").toURL()).build());
      Assert.assertEquals(404, response.getResponseCode());

      // get properties for nn and validate
      response = HttpRequests.execute(HttpRequest.get(serviceURI.resolve("props/nn").toURL()).build());
      Assert.assertEquals(200, response.getResponseCode());
      Map<String, String> returnedProps = GSON.fromJson(response.getResponseBodyAsString(),
                                                        new TypeToken<Map<String, String>>() {
                                                        }.getType());
      Assert.assertEquals(nnProps, returnedProps);

      // write some data to the table
      DataSetManager<Table> nnManager = getDataset("nn");
      nnManager.get().put(new Put("x", "y", "z"));
      nnManager.flush();

      // in a new tx, validate that data is in table
      Assert.assertFalse(nnManager.get().get(new Get("x")).isEmpty());
      Assert.assertEquals("z", nnManager.get().get(new Get("x", "y")).getString("y"));
      nnManager.flush();

      // truncate xx -> not found
      response = HttpRequests.execute(HttpRequest.post(serviceURI.resolve("truncate/xx").toURL()).build());
      Assert.assertEquals(404, response.getResponseCode());

      // truncate nn
      response = HttpRequests.execute(HttpRequest.post(serviceURI.resolve("truncate/nn").toURL()).build());
      Assert.assertEquals(200, response.getResponseCode());

      // validate table is empty
      Assert.assertTrue(nnManager.get().get(new Get("x")).isEmpty());
      nnManager.flush();

      // delete nn
      response = HttpRequests.execute(HttpRequest.delete(serviceURI.resolve("delete/nn").toURL()).build());
      Assert.assertEquals(200, response.getResponseCode());

      // delete again -> not found
      response = HttpRequests.execute(HttpRequest.delete(serviceURI.resolve("delete/nn").toURL()).build());
      Assert.assertEquals(404, response.getResponseCode());

      // delete xx which never existed -> not found
      response = HttpRequests.execute(HttpRequest.delete(serviceURI.resolve("delete/xx").toURL()).build());
      Assert.assertEquals(404, response.getResponseCode());

      // exists should now return false for nn
      response = HttpRequests.execute(HttpRequest.get(serviceURI.resolve("exists/nn").toURL()).build());
      Assert.assertEquals(200, response.getResponseCode());
      Assert.assertEquals("false", response.getResponseBodyAsString());

      Assert.assertNull(getDataset("nn").get());

    } finally {
      serviceManager.stop();
    }
  }

  @Test
  public void testAdminSpark() throws Exception {
    testAdminBatchProgram(appManager.getSparkManager(AdminApp.SPARK_NAME));
  }

  @Test
  public void testAdminScalaSpark() throws Exception {
    testAdminBatchProgram(appManager.getSparkManager(AdminApp.SPARK_SCALA_NAME));
  }

  @Test
  public void testAdminMapReduce() throws Exception {
    testAdminBatchProgram(appManager.getMapReduceManager(AdminApp.MAPREDUCE_NAME));
  }

  private <T extends ProgramManager<T>>
  void testAdminBatchProgram(ProgramManager<T> manager) throws Exception {

    addDatasetInstance("keyValueTable", "lines");
    addDatasetInstance("keyValueTable", "counts");

    // add some lines to the input dataset
    DataSetManager<KeyValueTable> linesManager = getDataset("lines");
    linesManager.get().write("1", "hello world");
    linesManager.get().write("2", "hi world");
    linesManager.flush();

    // add some counts to the output dataset
    DataSetManager<KeyValueTable> countsManager = getDataset("counts");
    countsManager.get().write("you", Bytes.toBytes(5));
    countsManager.get().write("me", Bytes.toBytes(3));
    countsManager.flush();

    manager = manager.start();
    manager.waitForFinish(60, TimeUnit.SECONDS);

    // validate that there are no counts for "you" and "me", and the the other counts are accurate
    countsManager.flush(); // need to start a new tx to see the output of MR
    Assert.assertEquals(2, Bytes.toInt(countsManager.get().read("world")));
    Assert.assertEquals(1, Bytes.toInt(countsManager.get().read("hello")));
    Assert.assertEquals(1, Bytes.toInt(countsManager.get().read("hi")));
    Assert.assertNull(countsManager.get().read("you"));
    Assert.assertNull(countsManager.get().read("me"));
    countsManager.flush();
  }
}
