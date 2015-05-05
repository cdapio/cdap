/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service.executor;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.proto.Id;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService}.
 */
public class DatasetOpExecutorServiceTest {

  private static final Gson GSON = new Gson();
  private static final Id.Namespace namespace = Id.Namespace.from("myspace");
  private static final Id.DatasetInstance bob = Id.DatasetInstance.from(namespace, "bob");

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private DatasetService managerService;
  private DatasetFramework dsFramework;
  private EndpointStrategy endpointStrategy;
  private TransactionManager txManager;

  @Before
  public void setUp() throws IOException {
    Configuration hConf = new Configuration();
    CConfiguration cConf = CConfiguration.create();

    File datasetDir = new File(tmpFolder.newFolder(), "datasetUser");
    datasetDir.mkdirs();

    cConf.set(Constants.Dataset.Manager.OUTPUT_DIR, datasetDir.getAbsolutePath());
    cConf.set(Constants.Dataset.Manager.ADDRESS, "localhost");

    cConf.set(Constants.Dataset.Executor.ADDRESS, "localhost");
    cConf.setInt(Constants.Dataset.Executor.PORT, Networks.getRandomPort());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetsModules().getStandaloneModules(),
      new DataSetServiceModules().getInMemoryModules(),
      new AuthModule(),
      new TransactionMetricsModule(),
      new ExploreClientModule());

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    managerService = injector.getInstance(DatasetService.class);
    managerService.startAndWait();

    dsFramework = injector.getInstance(DatasetFramework.class);

    // find host
    DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
    endpointStrategy = new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.DATASET_MANAGER));
  }

  @After
  public void tearDown() {
    dsFramework = null;

    managerService.stopAndWait();
    managerService = null;
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
    TransactionExecutor txExecutor =
      new DefaultTransactionExecutor(new InMemoryTxSystemClient(txManager),
                                     ImmutableList.of((TransactionAware) table));

    // writing smth to table
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        table.put(new Put("key1", "col1", "val1"));
      }
    });

    // verify that we can read the data
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals("val1", table.get(new Get("key1", "col1")).getString("col1"));
      }
    });

    testAdminOp(bob, "truncate", 200, null);

    // verify that data is no longer there
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertTrue(table.get(new Get("key1", "col1")).isEmpty());
      }
    });

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

  private void testAdminOp(String instanceName, String opName, int expectedStatus, Object expectedResult)
    throws URISyntaxException, IOException {
    testAdminOp(Id.DatasetInstance.from(Constants.DEFAULT_NAMESPACE, instanceName), opName, expectedStatus,
                expectedResult);
  }

  private void testAdminOp(Id.DatasetInstance datasetInstanceId, String opName, int expectedStatus,
                           Object expectedResult)
    throws URISyntaxException, IOException {
    String path = String.format("/namespaces/%s/data/datasets/%s/admin/%s",
                                datasetInstanceId.getNamespaceId(), datasetInstanceId.getId(), opName);

    URL targetUrl = resolve(path);
    HttpResponse response = HttpRequests.execute(HttpRequest.post(targetUrl).build());
    DatasetAdminOpResponse body = getResponse(response.getResponseBody());
    Assert.assertEquals(expectedStatus, response.getResponseCode());
    Assert.assertEquals(expectedResult, body.getResult());
  }

  private URL resolve(String path) throws URISyntaxException, MalformedURLException {
    InetSocketAddress socketAddress = endpointStrategy.pick(1, TimeUnit.SECONDS).getSocketAddress();
    return new URL(String.format("http://%s:%d%s%s", socketAddress.getHostName(),
                                 socketAddress.getPort(), Constants.Gateway.API_VERSION_3, path));
  }

  private DatasetAdminOpResponse getResponse(byte[] body) {
    return Objects.firstNonNull(GSON.fromJson(new String(body), DatasetAdminOpResponse.class),
                                new DatasetAdminOpResponse(null, null));
  }

}
