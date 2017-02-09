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

package co.cask.cdap.internal.app.store.remote;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.RemoteNamespaceQueryClient;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Preconditions;
import com.google.inject.Injector;
import org.apache.tephra.TransactionManager;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.TimeUnit;

/**
 * Tests {@link RemoteNamespaceQueryClient} queries by using it to fetch namespaces.
 */
public class RemoteNamespaceQueryTest {
  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static TransactionManager txManager;
  private static DatasetService datasetService;

  private static NamespaceAdmin namespaceAdmin;
  private static RemoteNamespaceQueryClient queryClient;
  private static NamespacedLocationFactory namespacedLocationFactory;
  private static AppFabricServer appFabricServer;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMPORARY_FOLDER.newFolder().getAbsolutePath());
    Injector injector = AppFabricTestHelper.getInjector(cConf);
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    waitForService(discoveryServiceClient, Constants.Service.DATASET_MANAGER);
    waitForService(discoveryServiceClient, Constants.Service.APP_FABRIC_HTTP);
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    queryClient = injector.getInstance(RemoteNamespaceQueryClient.class);
    namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);
  }

  @AfterClass
  public static void tearDown() {
    appFabricServer.stopAndWait();
    datasetService.stopAndWait();
    txManager.stopAndWait();
  }

  private static void waitForService(DiscoveryServiceClient discoveryService, String discoverableName)
    throws InterruptedException {
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(discoveryService.discover(discoverableName));
    Preconditions.checkNotNull(endpointStrategy.pick(5, TimeUnit.SECONDS),
                               "%s service is not up after 5 seconds", discoverableName);
  }

  @Test
  public void testCustomNS() throws Exception {
    String cdapNamespace = "NS1";
    String hbaseNamespace = "custHBase";
    String rootDirectory = "/directory";
    String hiveDb = "myHive";
    String schedulerQueue = "schQ";
    String description = "Namespace with custom HBase mapping";
    NamespaceConfig namespaceConfig = new NamespaceConfig(schedulerQueue, rootDirectory, hbaseNamespace, hiveDb,
                                                          null, null, null);
    NamespaceMeta meta = new NamespaceMeta.Builder()
      .setName(cdapNamespace)
      .setDescription(description)
      .setSchedulerQueueName(schedulerQueue)
      .setRootDirectory(rootDirectory)
      .setHBaseNamespace(hbaseNamespace)
      .setHiveDatabase(hiveDb)
      .build();
    // create the ns location since admin expect it to exists
    Location nsLocation = namespacedLocationFactory.get(meta);
    nsLocation.mkdirs();
    namespaceAdmin.create(meta);
    NamespaceId namespaceId = new NamespaceId(cdapNamespace);
    Assert.assertTrue(queryClient.exists(namespaceId));
    NamespaceMeta resultMeta = queryClient.get(namespaceId);
    Assert.assertEquals(namespaceConfig, resultMeta.getConfig());

    namespaceAdmin.delete(namespaceId);
    Assert.assertTrue(!queryClient.exists(namespaceId));
  }
}
