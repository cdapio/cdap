package com.continuuity.internal.app.services.http;

import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AppFabricServiceException;
import com.continuuity.app.services.ProgramDescriptor;
import com.continuuity.app.services.ProgramId;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.utils.Networks;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.handlers.dataset.DataSetInstantiatorFromMetaData;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.internal.app.services.http.handlers.AppFabricHttpHandlerTest;
import com.continuuity.internal.app.services.http.handlers.PingHandlerTest;
import com.continuuity.test.internal.TestHelper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ObjectArrays;
import com.google.inject.Injector;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.thrift.TException;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.util.concurrent.TimeUnit;


/**
 * Test Suite for running all API tests.
 */
@RunWith(value = Suite.class)
@Suite.SuiteClasses(value = {PingHandlerTest.class, AppFabricHttpHandlerTest.class})
public class AppFabricTestsSuite {
  private static final String API_KEY = "SampleTestApiKey";
  private static final String CLUSTER = "SampleTestClusterName";
  private static final Header AUTH_HEADER = new BasicHeader(Constants.Gateway.CONTINUUITY_API_KEY, API_KEY);

  //private static Gateway gateway;
  private static final String hostname = "127.0.0.1";
  private static int port;
  private static CConfiguration conf = CConfiguration.create();

  private static Injector injector;
  private static AppFabricServer appFabricServer;

  private static EndpointStrategy endpointStrategy;
  private static AppFabricService.Iface app;

  @ClassRule
  public static ExternalResource resources = new ExternalResource() {
    @Override
    protected void before() throws Throwable {

      conf.setInt(Constants.AppFabric.SERVER_PORT, 0);
      conf.set(Constants.AppFabric.SERVER_ADDRESS, hostname);
      conf.set(Constants.AppFabric.OUTPUT_DIR, System.getProperty("java.io.tmpdir"));
      conf.set(Constants.AppFabric.TEMP_DIR, System.getProperty("java.io.tmpdir"));
      conf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
      conf.set(Constants.AppFabric.SERVER_PORT, Integer.toString(Networks.getRandomPort()));
      conf.setBoolean(Constants.Gateway.CONFIG_AUTHENTICATION_REQUIRED, true);
      conf.set(Constants.Gateway.CLUSTER_NAME, CLUSTER);

      injector = TestHelper.getInjector();
      injector.getInstance(InMemoryTransactionManager.class).startAndWait();
      appFabricServer = injector.getInstance(AppFabricServer.class);
      appFabricServer.startAndWait();
      DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
      endpointStrategy = new TimeLimitEndpointStrategy(new RandomEndpointStrategy(discoveryClient.discover(
                        Constants.Service.APP_FABRIC_HTTP)), 1L, TimeUnit.SECONDS);
      injector.getInstance(DataSetInstantiatorFromMetaData.class).init(endpointStrategy);
      port = endpointStrategy.pick().getSocketAddress().getPort();
      app =  appFabricServer.getService();

    }

    @Override
    protected void after() {
      stopAppFabricServer(conf);
    }
  };


  public static Injector getInjector() {
    return injector;
  }

  public static Injector startAppFabric(CConfiguration conf) {
    return injector;
  }

  public static void startProgram (ProgramId id) throws TException, AppFabricServiceException {
    app.start(TestHelper.DUMMY_AUTH_TOKEN, new ProgramDescriptor(id, ImmutableMap.<String, String>of()));
  }


  public static void stopProgram (ProgramId id) throws TException, AppFabricServiceException {
    app.stop(TestHelper.DUMMY_AUTH_TOKEN, id);
  }

  public static void stopAppFabricServer(CConfiguration conf) {
    appFabricServer.stopAndWait();
    conf.clear();
  }

  public static HttpResponse doGet(String resource) throws Exception {
    return doGet(resource, null);
  }

  public static HttpResponse doGet(String resource, Header[] headers) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet("http://" + hostname + ":" + port + resource);

    if (headers != null) {
      get.setHeaders(ObjectArrays.concat(AUTH_HEADER, headers));
    } else {

      get.setHeader(AUTH_HEADER);
    }
    return client.execute(get);
  }
}
