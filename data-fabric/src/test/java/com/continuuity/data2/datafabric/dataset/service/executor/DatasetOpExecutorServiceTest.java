package com.continuuity.data2.datafabric.dataset.service.executor;

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
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.NoOpMetricsCollectionService;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetServiceModules;
import com.continuuity.data2.datafabric.dataset.RemoteDatasetFramework;
import com.continuuity.data2.datafabric.dataset.client.DatasetManagerServiceClient;
import com.continuuity.data2.datafabric.dataset.service.DatasetService;
import com.continuuity.data2.dataset2.DatasetManagementException;
import com.continuuity.data2.dataset2.InMemoryDatasetDefinitionRegistry;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.gateway.handlers.PingHandler;
import com.continuuity.http.HttpHandler;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
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
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link com.continuuity.data2.datafabric.dataset.service.executor.DatasetOpExecutorService}.
 */
public class DatasetOpExecutorServiceTest {

  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(DatasetOpExecutorServiceTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private DatasetService managerService;
  private RemoteDatasetFramework dsFramework;
  private TimeLimitEndpointStrategy endpointStrategy;

  @Before
  public void setUp() throws IOException {
    Configuration hConf = new Configuration();
    CConfiguration cConf = CConfiguration.create();

    File datasetDir = new File(tmpFolder.newFolder(), "datasetUser");
    datasetDir.mkdirs();

    cConf.set(Constants.Dataset.Manager.OUTPUT_DIR, datasetDir.getAbsolutePath());
    cConf.set(Constants.Dataset.Manager.ADDRESS, "localhost");
    cConf.setInt(Constants.Dataset.Manager.PORT, Networks.getRandomPort());

    cConf.set(Constants.Dataset.Executor.ADDRESS, "localhost");
    cConf.setInt(Constants.Dataset.Executor.PORT, Networks.getRandomPort());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(), new ZKClientModule(),
      new KafkaClientModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DataFabricModules(cConf, hConf).getInMemoryModules(),
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
      new AuthModule(), new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
      }
    });

    InMemoryTransactionManager transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    transactionManager.startAndWait();

    managerService = injector.getInstance(DatasetService.class);
    managerService.startAndWait();

    // initialize client
    DatasetOpExecutor opExecutor = injector.getInstance(DatasetOpExecutor.class);

    DatasetManagerServiceClient serviceClient = new DatasetManagerServiceClient(
      injector.getInstance(DiscoveryServiceClient.class));

    dsFramework = new RemoteDatasetFramework(
      serviceClient,
      cConf,
      injector.getInstance(LocationFactory.class),
      new InMemoryDatasetDefinitionRegistry());

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
  public void testRest() throws IOException, URISyntaxException, DatasetManagementException {
    // check non-existence with 404
    testAdminOp("bob", "exists", 404, null);

    // add instance and check non-existence with 200
    dsFramework.addInstance("table", "bob", DatasetInstanceProperties.EMPTY);
    testAdminOp("bob", "exists", 200, false);

    testAdminOp("joe", "exists", 404, null);

    // create and check existence
    testAdminOp("bob", "create", 200, null);
    testAdminOp("bob", "exists", 200, true);

    // check various operations
    testAdminOp("bob", "truncate", 200, null);
    testAdminOp("bob", "upgrade", 200, null);

    // drop and check non-existence
    testAdminOp("bob", "drop", 200, null);
    testAdminOp("bob", "exists", 200, false);
  }

  private void testAdminOp(String instanceName, String opName, int expectedStatus, Object expectedResult)
    throws URISyntaxException, IOException {

    InetSocketAddress socketAddress = endpointStrategy.pick().getSocketAddress();
    URI baseUri = new URI("http://" + socketAddress.getHostName() + ":" + socketAddress.getPort());
    String template =  Constants.Gateway.GATEWAY_VERSION + "/data/instances/%s/admin/%s";
    URL targetUrl = baseUri.resolve(String.format(template, instanceName, opName)).toURL();

    HttpRequests.HttpResponse response = HttpRequests.post(targetUrl);
    DatasetAdminOpResponse body = getResponse(response.getResponseBody());
    Assert.assertEquals(expectedStatus, response.getResponseCode());
    Assert.assertEquals(expectedResult, body.getResult());
  }

  private DatasetAdminOpResponse getResponse(byte[] body) {
    if (body == null) {
      return new DatasetAdminOpResponse(null, null);
    }

    return GSON.fromJson(new String(body), DatasetAdminOpResponse.class);
  }

}
