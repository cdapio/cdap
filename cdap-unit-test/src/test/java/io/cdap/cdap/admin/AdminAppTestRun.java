/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

package io.cdap.cdap.admin;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Get;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.dataset.table.TableProperties;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.ProgramManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.base.TestFrameworkTestBase;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests whether admin operations work in program contexts.
 */
public class AdminAppTestRun extends TestFrameworkTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  private static final Gson GSON = new Gson();
  private static final ArtifactId ADMIN_APP_ARTIFACT = NamespaceId.DEFAULT.artifact("admin-app", "1.0.0");
  private static final ArtifactSummary ADMIN_ARTIFACT_SUMMARY = new ArtifactSummary(ADMIN_APP_ARTIFACT.getArtifact(),
                                                                                    ADMIN_APP_ARTIFACT.getVersion());

  private ApplicationManager appManager;

  @Before
  public void deploy() throws Exception {
    addAppArtifact(ADMIN_APP_ARTIFACT, AdminApp.class);
    AppRequest<Void> appRequest = new AppRequest<>(ADMIN_ARTIFACT_SUMMARY);
    appManager = deployApplication(NamespaceId.DEFAULT.app("AdminApp"), appRequest);
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

    String namespaceX = "x";
    try {
      URI serviceURI = serviceManager.getServiceURL(10, TimeUnit.SECONDS).toURI();

      // dataset nn should not exist
      HttpResponse response = executeHttp(HttpRequest.get(serviceURI.resolve("exists/nn").toURL()).build());
      Assert.assertEquals(200, response.getResponseCode());
      Assert.assertEquals("false", response.getResponseBodyAsString());

      // create nn as a table
      response = executeHttp(HttpRequest.put(serviceURI.resolve("create/nn/table").toURL()).build());
      Assert.assertEquals(200, response.getResponseCode());

      // now nn should exist
      response = executeHttp(HttpRequest.get(serviceURI.resolve("exists/nn").toURL()).build());
      Assert.assertEquals(200, response.getResponseCode());
      Assert.assertEquals("true", response.getResponseBodyAsString());

      // create it again as a fileset -> should fail with conflict
      response = executeHttp(HttpRequest.put(serviceURI.resolve("create/nn/fileSet").toURL()).build());
      Assert.assertEquals(409, response.getResponseCode());

      // get the type for xx -> not found
      response = executeHttp(HttpRequest.get(serviceURI.resolve("type/xx").toURL()).build());
      Assert.assertEquals(404, response.getResponseCode());

      // get the type for nn -> table
      response = executeHttp(HttpRequest.get(serviceURI.resolve("type/nn").toURL()).build());
      Assert.assertEquals(200, response.getResponseCode());
      Assert.assertEquals("table", response.getResponseBodyAsString());

      // update xx's properties -> should get not-found
      Map<String, String> nnProps = TableProperties.builder().setTTL(1000L).build().getProperties();
      response = executeHttp(HttpRequest.put(serviceURI.resolve("update/xx").toURL())
                                        .withBody(GSON.toJson(nnProps)).build());
      Assert.assertEquals(404, response.getResponseCode());

      // update nn's properties
      response = executeHttp(HttpRequest.put(serviceURI.resolve("update/nn").toURL())
                                        .withBody(GSON.toJson(nnProps)).build());
      Assert.assertEquals(200, response.getResponseCode());

      // get properties for xx -> not found
      response = executeHttp(HttpRequest.get(serviceURI.resolve("props/xx").toURL()).build());
      Assert.assertEquals(404, response.getResponseCode());

      // get properties for nn and validate
      response = executeHttp(HttpRequest.get(serviceURI.resolve("props/nn").toURL()).build());
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
      response = executeHttp(HttpRequest.post(serviceURI.resolve("truncate/xx").toURL()).build());
      Assert.assertEquals(404, response.getResponseCode());

      // truncate nn
      response = executeHttp(HttpRequest.post(serviceURI.resolve("truncate/nn").toURL()).build());
      Assert.assertEquals(200, response.getResponseCode());

      // validate table is empty
      Assert.assertTrue(nnManager.get().get(new Get("x")).isEmpty());
      nnManager.flush();

      // delete nn
      response = executeHttp(HttpRequest.delete(serviceURI.resolve("delete/nn").toURL()).build());
      Assert.assertEquals(200, response.getResponseCode());

      // delete again -> not found
      response = executeHttp(HttpRequest.delete(serviceURI.resolve("delete/nn").toURL()).build());
      Assert.assertEquals(404, response.getResponseCode());

      // delete xx which never existed -> not found
      response = executeHttp(HttpRequest.delete(serviceURI.resolve("delete/xx").toURL()).build());
      Assert.assertEquals(404, response.getResponseCode());

      // exists should now return false for nn
      response = executeHttp(HttpRequest.get(serviceURI.resolve("exists/nn").toURL()).build());
      Assert.assertEquals(200, response.getResponseCode());
      Assert.assertEquals("false", response.getResponseBodyAsString());

      Assert.assertNull(getDataset("nn").get());

      // test Admin.namespaceExists()
      HttpRequest request = HttpRequest.get(serviceURI.resolve("namespaces/y").toURL()).build();
      response = executeHttp(request);
      Assert.assertEquals(404, response.getResponseCode());

      // test Admin.getNamespaceSummary()
      NamespaceMeta namespaceXMeta = new NamespaceMeta.Builder().setName(namespaceX).setGeneration(10L).build();
      getNamespaceAdmin().create(namespaceXMeta);
      request = HttpRequest.get(serviceURI.resolve("namespaces/" + namespaceX).toURL()).build();
      response = executeHttp(request);
      NamespaceSummary namespaceSummary = GSON.fromJson(response.getResponseBodyAsString(), NamespaceSummary.class);
      NamespaceSummary expectedX = new NamespaceSummary(namespaceXMeta.getName(), namespaceXMeta.getDescription(),
                                                        namespaceXMeta.getGeneration());
      Assert.assertEquals(expectedX, namespaceSummary);

      // test ArtifactManager.listArtifacts()
      ArtifactId pluginArtifactId = new NamespaceId(namespaceX).artifact("r1", "1.0.0");

      // add a plugin artifact to namespace X
      addPluginArtifact(pluginArtifactId, ADMIN_APP_ARTIFACT, DummyPlugin.class);
      // no plugins should be listed in the default namespace, but the app artifact should
      request = HttpRequest.get(serviceURI.resolve("namespaces/default/plugins").toURL()).build();
      response = executeHttp(request);
      Assert.assertEquals(200, response.getResponseCode());
      Type setType = new TypeToken<Set<ArtifactSummary>>() { }.getType();
      Assert.assertEquals(Collections.singleton(ADMIN_ARTIFACT_SUMMARY),
                          GSON.fromJson(response.getResponseBodyAsString(), setType));
      // the plugin should be listed in namespace X
      request = HttpRequest.get(serviceURI.resolve("namespaces/x/plugins").toURL()).build();
      response = executeHttp(request);
      Assert.assertEquals(200, response.getResponseCode());
      ArtifactSummary expected = new ArtifactSummary(pluginArtifactId.getArtifact(), pluginArtifactId.getVersion());
      Assert.assertEquals(Collections.singleton(expected), GSON.fromJson(response.getResponseBodyAsString(), setType));

    } finally {
      serviceManager.stop();
      if (getNamespaceAdmin().exists(new NamespaceId(namespaceX))) {
        getNamespaceAdmin().delete(new NamespaceId(namespaceX));
      }
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

  private <T extends ProgramManager<T>> void testAdminBatchProgram(ProgramManager<T> manager) throws Exception {

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
    manager.waitForRun(ProgramRunStatus.COMPLETED, 180, TimeUnit.SECONDS);

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
