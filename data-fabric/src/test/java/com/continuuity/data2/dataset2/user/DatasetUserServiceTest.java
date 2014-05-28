package com.continuuity.data2.dataset2.user;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.KafkaClientModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.NoOpMetricsCollectionService;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.datafabric.dataset.DataFabricDatasetManager;
import com.continuuity.data2.datafabric.dataset.client.DatasetManagerServiceClient;
import com.continuuity.data2.datafabric.dataset.service.DatasetManagerService;
import com.continuuity.data2.dataset2.manager.DatasetManagementException;
import com.continuuity.data2.dataset2.manager.inmemory.InMemoryDatasetDefinitionRegistry;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.google.common.base.Optional;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Test for {@link DatasetUserService}.
 */
public class DatasetUserServiceTest {

  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(DatasetUserServiceTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private DatasetManagerService managerService;
  private InetSocketAddress userServiceAddress;
  private DataFabricDatasetManager managerClient;

  @Before
  public void setUp() throws IOException {
    Configuration hConf = new Configuration();
    CConfiguration cConf = CConfiguration.create();

    File datasetDir = new File(tmpFolder.newFolder(), "datasetUser");
    datasetDir.mkdirs();

    cConf.set(Constants.Dataset.Manager.OUTPUT_DIR, datasetDir.getAbsolutePath());
    cConf.set(Constants.Dataset.Manager.ADDRESS, "localhost");
    cConf.setInt(Constants.Dataset.Manager.PORT, Networks.getRandomPort());

    cConf.set(Constants.Dataset.User.ADDRESS, "localhost");
    cConf.setInt(Constants.Dataset.User.PORT, Networks.getRandomPort());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(), new ZKClientModule(),
      new KafkaClientModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DataFabricModules(cConf, hConf).getInMemoryModules(),
      new AuthModule(), new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
      }
    });

    InMemoryTransactionManager transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    transactionManager.startAndWait();

    managerService = injector.getInstance(DatasetManagerService.class);
    managerService.startAndWait();

    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    ServiceDiscovered discover = discoveryServiceClient.discover(Constants.Service.DATASET_USER);
    Discoverable userServiceDiscoverable = discover.iterator().next();
    userServiceAddress = userServiceDiscoverable.getSocketAddress();

    // initialize client
    DatasetManagerServiceClient serviceClient = new DatasetManagerServiceClient(
      injector.getInstance(DiscoveryServiceClient.class));

    managerClient = new DataFabricDatasetManager(
      serviceClient,
      cConf,
      injector.getInstance(LocationFactory.class),
      new InMemoryDatasetDefinitionRegistry());
  }

  @After
  public void tearDown() {
    managerClient = null;

    managerService.stopAndWait();
    managerService = null;

    userServiceAddress = null;
  }

  @Test
  public void testRest() throws IOException, URISyntaxException, DatasetManagementException {
    // check non-existence with 404
    testAdminOp("bob", "exists", 404, null);

    // add instance and check non-existence with 200
    managerClient.addInstance("table", "bob", DatasetInstanceProperties.EMPTY);
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

  private void testAdminOp(String instanceName, String opName, int expectedStatus, Object expectedBody)
    throws URISyntaxException, IOException {

    URI baseUri = new URI("http://" + userServiceAddress.getHostName() + ":" + userServiceAddress.getPort());
    String template =  Constants.Gateway.GATEWAY_VERSION + "/data/instances/%s/admin/%s";
    URI targetUri = baseUri.resolve(String.format(template, instanceName, opName));

    Response<AdminOpResponse> response = doPost(targetUri, AdminOpResponse.class);
    Assert.assertEquals(expectedStatus, response.getStatusCode());
    Assert.assertEquals(expectedBody, response.getBody().or(new AdminOpResponse(null, null)).getResult());
  }

  private <T> Response<T> doPost(URI uri, Class<T> cls) throws IOException {
    try {
      LOG.info("doPost({}, {})", uri.toASCIIString(), cls);
      HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
      connection.setRequestMethod("POST");
      InputStream response = connection.getInputStream();
      T body = GSON.fromJson(new InputStreamReader(response), cls);
      return new Response(connection.getResponseCode(), body);
    } catch (FileNotFoundException e) {
      return new Response(404, null);
    }
  }

  private static final class Response<T> {
    private final int statusCode;
    private final Optional<T> body;

    private Response(int statusCode, T body) {
      this.statusCode = statusCode;
      this.body = Optional.fromNullable(body);
    }

    public int getStatusCode() {
      return statusCode;
    }

    public Optional<T> getBody() {
      return body;
    }
  }

}
