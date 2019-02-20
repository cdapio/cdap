/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package co.cask.cdap.runtime;

import co.cask.cdap.DummyAppWithTrackingTable;
import co.cask.cdap.TrackingTable;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespacePathLocator;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.DefaultId;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.master.spi.program.ProgramController;
import co.cask.cdap.master.spi.program.ProgramDescriptor;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.XSlowTests;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests that service and batch jobs close their data sets.
 */
@Category(XSlowTests.class)
public class OpenCloseDataSetTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static Location namespaceHomeLocation;

  private static final Supplier<File> TEMP_FOLDER_SUPPLIER = () -> {
    try {
      return TEMP_FOLDER.newFolder();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  };

  @BeforeClass
  public static void setup() throws Exception {
    NamespacePathLocator namespacePathLocator =
      AppFabricTestHelper.getInjector().getInstance(NamespacePathLocator.class);
    namespaceHomeLocation = namespacePathLocator.get(DefaultId.NAMESPACE);
    NamespaceAdmin namespaceAdmin = AppFabricTestHelper.getInjector().getInstance(NamespaceAdmin.class);
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(DefaultId.NAMESPACE).build());
    Locations.mkdirsIfNotExists(namespaceHomeLocation);
  }

  @Test(timeout = 120000)
  public void testDataSetsAreClosed() throws Exception {
    final String tableName = "foo";

    TrackingTable.resetTracker();
    ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(DummyAppWithTrackingTable.class,
                                                                                   TEMP_FOLDER_SUPPLIER);
    List<ProgramController> controllers = Lists.newArrayList();

    // start the programs
    for (ProgramDescriptor programDescriptor : app.getPrograms()) {
      if (programDescriptor.getProgramId().getType().equals(ProgramType.MAPREDUCE)) {
        continue;
      }

      // Start service with 1 thread
      Map<String, String> args = Collections.singletonMap(SystemArguments.SERVICE_THREADS, "1");
      controllers.add(AppFabricTestHelper.submit(app, programDescriptor.getSpecification().getClassName(),
                                                 new BasicArguments(args), TEMP_FOLDER_SUPPLIER));
    }

    DiscoveryServiceClient discoveryServiceClient = AppFabricTestHelper.getInjector().
      getInstance(DiscoveryServiceClient.class);

    Discoverable discoverable = new RandomEndpointStrategy(() -> discoveryServiceClient.discover(
      String.format("service.%s.%s.%s", DefaultId.NAMESPACE.getEntityName(), "dummy", "DummyService")))
      .pick(5, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverable);

    // write some data to the tracking table through the service
    for (int i = 0; i < 4; i++) {
      String msg = "x" + i;
      URL url = new URL(String.format("http://%s:%d/v3/namespaces/default/apps/%s/services/%s/methods/%s",
                                      discoverable.getSocketAddress().getHostName(),
                                      discoverable.getSocketAddress().getPort(),
                                      "dummy",
                                      "DummyService",
                                      msg));
      HttpRequests.execute(HttpRequest.put(url).build());
    }

    // get the number of writes to the foo table
    Assert.assertEquals(4, TrackingTable.getTracker(tableName, "write"));
    // only 2 "open" calls should be tracked:
    // 1. the service has started with one instance (service is loaded lazily on 1st request)
    // 2. DatasetSystemMetadataWriter also instantiates the dataset because it needs to add some system tags
    // for the dataset
    Assert.assertEquals(2, TrackingTable.getTracker(tableName, "open"));

    // now query data from the service
    URL url = new URL(String.format("http://%s:%d/v3/namespaces/default/apps/%s/services/%s/methods/%s",
                                    discoverable.getSocketAddress().getHostName(),
                                    discoverable.getSocketAddress().getPort(),
                                    "dummy",
                                    "DummyService",
                                    "x1"));
    HttpResponse response = HttpRequests.execute(HttpRequest.get(url).build());

    String responseContent = new Gson().fromJson(response.getResponseBodyAsString(), String.class);

    Assert.assertEquals("x1", responseContent);

    // now the dataset must have a read and another open operation
    Assert.assertEquals(1, TrackingTable.getTracker(tableName, "read"));
    // since the same service instance is used, there shouldn't be any new open
    Assert.assertEquals(2, TrackingTable.getTracker(tableName, "open"));
    // The dataset that was instantiated by the DatasetSystemMetadataWriter should have been closed
    Assert.assertEquals(1, TrackingTable.getTracker(tableName, "close"));

    // stop all programs, they should both close the data set foo
    for (ProgramController controller : controllers) {
      controller.stop().get();
    }
    int timesOpened = TrackingTable.getTracker(tableName, "open");
    Assert.assertTrue(timesOpened >= 2);
    Assert.assertEquals(timesOpened, TrackingTable.getTracker(tableName, "close"));

    // now start the m/r job
    ProgramController controller = null;
    for (ProgramDescriptor programDescriptor : app.getPrograms()) {
      if (programDescriptor.getProgramId().getType().equals(ProgramType.MAPREDUCE)) {
        controller = AppFabricTestHelper.submit(app, programDescriptor.getSpecification().getClassName(),
                                                new BasicArguments(), TEMP_FOLDER_SUPPLIER);
      }
    }
    Assert.assertNotNull(controller);

    while (!controller.getState().equals(ProgramController.State.COMPLETED)) {
      TimeUnit.MILLISECONDS.sleep(100);
    }

    // M/r job is done, one mapper and the m/r client should have opened and closed the data set foo
    // we don't know the exact number of times opened, but it is at least once, and it must be closed the same number
    // of times.
    Assert.assertTrue(timesOpened < TrackingTable.getTracker(tableName, "open"));
    Assert.assertEquals(TrackingTable.getTracker(tableName, "open"),
                        TrackingTable.getTracker(tableName, "close"));
    Assert.assertTrue(0 < TrackingTable.getTracker("bar", "open"));
    Assert.assertEquals(TrackingTable.getTracker("bar", "open"),
                        TrackingTable.getTracker("bar", "close"));

  }

  @AfterClass
  public static void tearDown() {
    Locations.deleteQuietly(namespaceHomeLocation, true);
  }
}
