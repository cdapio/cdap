package com.continuuity.internal.app.services.http;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.metrics.query.MetricsQueryService;
import com.continuuity.test.internal.guice.AppFabricTestModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

/**
 * AppFabric HttpHandler Test classes can extend this class, this will allow the HttpService be setup before
 * running the handler tests, this also gives the ability to run individual test cases.
 */
public abstract class AppFabricTestBase {
  private static final String API_KEY = "SampleTestApiKey";
  private static final String CLUSTER = "SampleTestClusterName";
  private static final Header AUTH_HEADER = new BasicHeader(Constants.Gateway.CONTINUUITY_API_KEY, API_KEY);
  //private static Gateway gateway;
  private static final String hostname = "127.0.0.1";

  private static int port;
  private static CConfiguration conf;
  private static Injector injector;
  private static AppFabricServer appFabricServer;
  private static EndpointStrategy endpointStrategy;
  private static MetricsQueryService metrics;
  private static TransactionSystemClient txClient;

  @ClassRule
  public static ExternalResource resources = new ExternalResource() {
    @Override
    protected void before() throws Throwable {
      conf = CConfiguration.create();

      conf.set(Constants.AppFabric.SERVER_ADDRESS, hostname);
      conf.set(Constants.AppFabric.OUTPUT_DIR, System.getProperty("java.io.tmpdir"));
      conf.set(Constants.AppFabric.TEMP_DIR, System.getProperty("java.io.tmpdir"));

      conf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
      conf.setBoolean(Constants.Gateway.CONFIG_AUTHENTICATION_REQUIRED, false);
      conf.set(Constants.Gateway.CLUSTER_NAME, CLUSTER);

      injector = Guice.createInjector(new AppFabricTestModule(conf));
      injector.getInstance(InMemoryTransactionManager.class).startAndWait();
      appFabricServer = injector.getInstance(AppFabricServer.class);
      appFabricServer.startAndWait();
      DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
      endpointStrategy = new TimeLimitEndpointStrategy(new RandomEndpointStrategy(discoveryClient.discover(
        Constants.Service.APP_FABRIC_HTTP)), 1L, TimeUnit.SECONDS);
      port = endpointStrategy.pick().getSocketAddress().getPort();
      txClient = injector.getInstance(TransactionSystemClient.class);
      metrics = injector.getInstance(MetricsQueryService.class);
      metrics.startAndWait();
    }

    @Override
    protected void after() {
      stopAppFabricServer(conf);
    }
  };


  public static Injector getInjector() {
    return injector;
  }

  public static TransactionSystemClient getTxClient() {
    return txClient;
  }

  public static Injector startAppFabric(CConfiguration conf) {
    return injector;
  }


  public static void stopAppFabricServer(CConfiguration conf) {
    appFabricServer.stopAndWait();
    metrics.stopAndWait();
    conf.clear();
  }

  public static int getPort() {
    return port;
  }

  public static URI getEndPoint(String path) throws URISyntaxException {
    return new URI("http://" + hostname + ":" + port + path);
  }
}
