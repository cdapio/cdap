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

package co.cask.cdap.internal.app.store.remote;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.namespace.RemoteNamespaceQueryClient;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.gateway.handlers.meta.RemoteNamespaceQueryHandler;
import co.cask.cdap.gateway.handlers.meta.RemoteSystemOperationsService;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.store.NamespaceStore;
import co.cask.tephra.TransactionManager;
import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.TimeUnit;

/**
 * Tests implementation of {@link RemoteNamespaceQueryHandler} by using it fetch namespaces.
 */
public class RemoteNamespaceQueryTest {
  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static TransactionManager txManager;
  private static DatasetService datasetService;
  private static RemoteSystemOperationsService remoteSysOpService;

  private static NamespaceStore namespaceStore;
  private static RemoteNamespaceQueryClient queryClient;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    Injector injector = Guice.createInjector(new AppFabricTestModule(cConf));
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    remoteSysOpService = injector.getInstance(RemoteSystemOperationsService.class);
    remoteSysOpService.startAndWait();
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    waitForService(discoveryServiceClient, Constants.Service.DATASET_MANAGER);
    waitForService(discoveryServiceClient, Constants.Service.REMOTE_SYSTEM_OPERATION);
    namespaceStore = injector.getInstance(NamespaceStore.class);
    queryClient = injector.getInstance(RemoteNamespaceQueryClient.class);
  }

  @AfterClass
  public static void tearDown() {
    remoteSysOpService.stopAndWait();
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
                                                          null, null);
    namespaceStore.create(new NamespaceMeta.Builder()
                            .setName(cdapNamespace)
                            .setDescription(description)
                            .setSchedulerQueueName(schedulerQueue)
                            .setRootDirectory(rootDirectory)
                            .setHBaseNamespace(hbaseNamespace)
                            .setHiveDatabase(hiveDb)
                            .build());
    NamespaceId namespaceId = new NamespaceId(cdapNamespace);
    Assert.assertTrue(queryClient.exists(namespaceId.toId()));
    NamespaceMeta resultMeta = queryClient.get(namespaceId.toId());
    Assert.assertEquals(namespaceConfig, resultMeta.getConfig());

    namespaceStore.delete(namespaceId.toId());
    Assert.assertTrue(!queryClient.exists(namespaceId.toId()));
  }
}
