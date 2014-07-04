package com.continuuity.data2.datafabric.dataset.service.executor;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.table.Get;
import com.continuuity.api.dataset.table.Put;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.KafkaClientModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.http.HttpRequests;
import com.continuuity.common.http.HttpResponse;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetServiceModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data2.datafabric.dataset.service.DatasetService;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.gateway.handlers.PingHandler;
import com.continuuity.http.HttpHandler;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
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
 * Test for {@link com.continuuity.data2.datafabric.dataset.service.executor.DatasetOpExecutorService}.
 */
public class DatasetOpExecutorServiceTest {

  private static final Gson GSON = new Gson();

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private DatasetService managerService;
  private DatasetFramework dsFramework;
  private TimeLimitEndpointStrategy endpointStrategy;
  private InMemoryTransactionManager txManager;

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
      new IOModule(), new ZKClientModule(),
      new KafkaClientModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetsModules().getInMemoryModule(),
      Modules.override(new DataSetServiceModules().getInMemoryModule()).with(new AbstractModule() {
        @Override
        protected void configure() {
          Named name = Names.named(Constants.Service.DATASET_EXECUTOR);
          Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class, name);
          handlerBinder.addBinding().to(DatasetAdminOpHTTPHandler.class);
          handlerBinder.addBinding().to(PingHandler.class);

          bind(DatasetOpExecutorService.class).in(Scopes.SINGLETON);

          bind(DatasetOpExecutor.class).to(LocalDatasetOpExecutor.class);
        }
      }),
      new AuthModule(),
      new TransactionMetricsModule());

    txManager = injector.getInstance(InMemoryTransactionManager.class);
    txManager.startAndWait();

    managerService = injector.getInstance(DatasetService.class);
    managerService.startAndWait();

    dsFramework = injector.getInstance(DatasetFramework.class);

    // find host
    DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
    endpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(
        discoveryClient.discover(Constants.Service.DATASET_MANAGER)), 1L, TimeUnit.SECONDS);
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
    testAdminOp("bob", "exists", 404, null);

    // add instance, should automatically create an instance
    dsFramework.addInstance("table", "bob", DatasetProperties.EMPTY);
    testAdminOp("bob", "exists", 200, true);

    testAdminOp("joe", "exists", 404, null);

    // check truncate
    final Table table = dsFramework.getDataset("bob", null);
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

    testAdminOp("bob", "truncate", 200, null);

    // verify that data is no longer there
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertTrue(table.get(new Get("key1", "col1")).isEmpty());
      }
    });

    // check upgrade
    testAdminOp("bob", "upgrade", 200, null);

    // drop and check non-existence
    dsFramework.deleteInstance("bob");
    testAdminOp("bob", "exists", 404, null);
  }

  private void testAdminOp(String instanceName, String opName, int expectedStatus, Object expectedResult)
    throws URISyntaxException, IOException {
    String path = String.format("/data/datasets/%s/admin/%s", instanceName, opName);

    URL targetUrl = resolve(path);
    HttpResponse response = HttpRequests.post(targetUrl);
    DatasetAdminOpResponse body = getResponse(response.getResponseBody());
    Assert.assertEquals(expectedStatus, response.getResponseCode());
    Assert.assertEquals(expectedResult, body.getResult());
  }

  private URL resolve(String path) throws URISyntaxException, MalformedURLException {
    InetSocketAddress socketAddress = endpointStrategy.pick().getSocketAddress();
    return new URL(String.format("http://%s:%d%s%s", socketAddress.getHostName(),
                                 socketAddress.getPort(), Constants.Gateway.GATEWAY_VERSION, path));
  }

  private DatasetAdminOpResponse getResponse(byte[] body) {
    return Objects.firstNonNull(GSON.fromJson(new String(body), DatasetAdminOpResponse.class),
                                new DatasetAdminOpResponse(null, null));
  }

}
