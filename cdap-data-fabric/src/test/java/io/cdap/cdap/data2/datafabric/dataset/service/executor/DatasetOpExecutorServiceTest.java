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

package io.cdap.cdap.data2.datafabric.dataset.service.executor;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.table.Get;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.KafkaClientModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.TransactionMetricsModule;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.NoOpMetadataServiceClient;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.URIScheme;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.impersonation.UnsupportedUGIProvider;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.DefaultTransactionExecutor;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService}.
 */
public class DatasetOpExecutorServiceTest {

  private static final Gson GSON = new Gson();
  private static final NamespaceId namespace = new NamespaceId("myspace");
  private static final DatasetId bob = namespace.dataset("bob");

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private DatasetOpExecutorService dsOpExecService;
  private DatasetService managerService;
  private DatasetFramework dsFramework;
  private EndpointStrategy endpointStrategy;
  private TransactionManager txManager;
  private NamespaceAdmin namespaceAdmin;

  @Before
  public void setUp() throws Exception {
    Configuration hConf = new Configuration();
    CConfiguration cConf = CConfiguration.create();

    File datasetDir = new File(TMP_FOLDER.newFolder(), "datasetUser");
    Assert.assertTrue(datasetDir.mkdirs());

    cConf.set(Constants.Dataset.Manager.OUTPUT_DIR, datasetDir.getAbsolutePath());
    cConf.set(Constants.Service.MASTER_SERVICES_BIND_ADDRESS, "localhost");

    cConf.set(Constants.Dataset.Executor.ADDRESS, "localhost");
    cConf.setInt(Constants.Dataset.Executor.PORT, Networks.getRandomPort());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new InMemoryDiscoveryModule(),
      new NonCustomLocationUnitTestModule(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetsModules().getStandaloneModules(),
      new DataSetServiceModules().getInMemoryModules(),
      new TransactionMetricsModule(),
      new ExploreClientModule(),
      new NamespaceAdminTestModule(),
      new AuthenticationContextModules().getMasterModule(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
          bind(MetadataServiceClient.class).to(NoOpMetadataServiceClient.class);
        }
      });

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    StructuredTableRegistry structuredTableRegistry = injector.getInstance(StructuredTableRegistry.class);
    structuredTableRegistry.initialize();
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class), structuredTableRegistry);

    dsOpExecService = injector.getInstance(DatasetOpExecutorService.class);
    dsOpExecService.startAndWait();

    managerService = injector.getInstance(DatasetService.class);
    managerService.startAndWait();

    dsFramework = injector.getInstance(DatasetFramework.class);

    // find host
    DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
    endpointStrategy = new RandomEndpointStrategy(() -> discoveryClient.discover(Constants.Service.DATASET_MANAGER));

    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    namespaceAdmin.create(NamespaceMeta.DEFAULT);
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(bob.getParent()).build());
    StructuredTableAdmin tableAdmin = injector.getInstance(StructuredTableAdmin.class);
    StructuredTableRegistry registry = injector.getInstance(StructuredTableRegistry.class);
    try {
      StoreDefinition.createAllTables(tableAdmin, registry);
    } catch (IOException | TableAlreadyExistsException e) {
      throw new RuntimeException("Failed to create the system tables", e);
    }
  }

  @After
  public void tearDown() throws Exception {
    dsFramework = null;

    dsOpExecService.stopAndWait();
    dsOpExecService = null;

    managerService.stopAndWait();
    managerService = null;

    namespaceAdmin.delete(NamespaceId.DEFAULT);
    namespaceAdmin.delete(bob.getParent());
  }

  @Test
  public void testRest() throws Exception {
    // check non-existence with 404
    testAdminOp(bob, "exists", 404, null);

    // add instance, should automatically create an instance
    dsFramework.addInstance("table", bob, DatasetProperties.EMPTY);
    testAdminOp(bob, "exists", 200, true);

    testAdminOp("bob", "exists", 404, null);

    // check truncate
    final Table table = dsFramework.getDataset(bob, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(table);
    TransactionExecutor txExecutor =
      new DefaultTransactionExecutor(new InMemoryTxSystemClient(txManager),
                                     ImmutableList.of((TransactionAware) table));

    // writing smth to table
    txExecutor.execute(() -> table.put(new Put("key1", "col1", "val1")));

    // verify that we can read the data
    txExecutor.execute(() -> Assert.assertEquals("val1", table.get(new Get("key1", "col1")).getString("col1")));

    testAdminOp(bob, "truncate", 200, null);

    // verify that data is no longer there
    txExecutor.execute(() -> Assert.assertTrue(table.get(new Get("key1", "col1")).isEmpty()));

    // check upgrade
    testAdminOp(bob, "upgrade", 200, null);

    // drop and check non-existence
    dsFramework.deleteInstance(bob);
    testAdminOp(bob, "exists", 404, null);
  }

  @Test
  public void testUpdate() throws Exception {
    // check non-existence with 404
    testAdminOp(bob, "exists", 404, null);

    // add instance, should automatically create an instance
    dsFramework.addInstance("table", bob, DatasetProperties.EMPTY);
    testAdminOp(bob, "exists", 200, true);

    dsFramework.updateInstance(bob, DatasetProperties.builder().add("dataset.table.ttl", "10000").build());
    // check upgrade
    testAdminOp(bob, "upgrade", 200, null);

    // drop and check non-existence
    dsFramework.deleteInstance(bob);
    testAdminOp(bob, "exists", 404, null);
  }

  @SuppressWarnings("SameParameterValue")
  private void testAdminOp(String instanceName, String opName, int expectedStatus, Object expectedResult)
    throws IOException {
    testAdminOp(NamespaceId.DEFAULT.dataset(instanceName), opName, expectedStatus,
                expectedResult);
  }

  private void testAdminOp(DatasetId datasetInstanceId, String opName, int expectedStatus,
                           Object expectedResult)
    throws IOException {
    String path = String.format("/namespaces/%s/data/datasets/%s/admin/%s",
                                datasetInstanceId.getNamespace(), datasetInstanceId.getEntityName(), opName);

    URL targetUrl = resolve(path);
    HttpResponse response = HttpRequests.execute(HttpRequest.post(targetUrl).build(),
                                                 new DefaultHttpRequestConfig(false));
    DatasetAdminOpResponse body = getResponse(response.getResponseBody());
    Assert.assertEquals(expectedStatus, response.getResponseCode());
    Assert.assertEquals(expectedResult, body.getResult());
  }

  private URL resolve(String path) throws MalformedURLException {
    Discoverable discoverable = endpointStrategy.pick(1, TimeUnit.SECONDS);
    return URIScheme.createURI(discoverable, "%s%s", Constants.Gateway.API_VERSION_3_TOKEN, path).toURL();
  }

  private DatasetAdminOpResponse getResponse(byte[] body) {
    return Objects.firstNonNull(GSON.fromJson(Bytes.toString(body), DatasetAdminOpResponse.class),
                                new DatasetAdminOpResponse(null, null));
  }
}
