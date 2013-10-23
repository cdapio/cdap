package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.utils.Networks;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.collector.NettyFlumeCollectorTest;
import com.continuuity.gateway.util.DataSetInstantiatorFromMetaData;
import com.continuuity.gateway.v2.Gateway;
import com.continuuity.gateway.v2.handlers.v2.AppFabricServiceHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.PingHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.ProcedureHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.dataset.ClearFabricHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.dataset.DatasetHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.dataset.TableHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.hooks.MetricsReporterHookTest;
import com.continuuity.gateway.v2.handlers.v2.log.LogHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.log.MockLogReader;
import com.continuuity.gateway.v2.runtime.GatewayModules;
import com.continuuity.gateway.v2.tools.DataSetClientTest;
import com.continuuity.gateway.v2.tools.StreamClientTest;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.logging.read.LogReader;
import com.continuuity.passport.http.client.PassportClient;
import com.continuuity.test.internal.guice.AppFabricTestModule;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ObjectArrays;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test Suite for running all API tests.
 */
@RunWith(value = Suite.class)
@Suite.SuiteClasses(value = {PingHandlerTest.class, LogHandlerTest.class,
  ProcedureHandlerTest.class, TableHandlerTest.class, DatasetHandlerTest.class, ClearFabricHandlerTest.class,
  DataSetClientTest.class, StreamClientTest.class, AppFabricServiceHandlerTest.class,
  NettyFlumeCollectorTest.class, MetricsReporterHookTest.class})
public class GatewayFastTestsSuite {
  private static final String API_KEY = "SampleTestApiKey";
  private static final String CLUSTER = "SampleTestClusterName";
  private static final Header AUTH_HEADER = new BasicHeader(GatewayAuthenticator.CONTINUUITY_API_KEY, API_KEY);

  private static Gateway gateway;
  private static final String hostname = "127.0.0.1";
  private static int port;
  private static CConfiguration conf = CConfiguration.create();

  private static Injector injector;
  private static AppFabricServer appFabricServer;

  private static EndpointStrategy endpointStrategy;

  @ClassRule
  public static ExternalResource resources = new ExternalResource() {
    @Override
    protected void before() throws Throwable {

      conf.setInt(Constants.Gateway.PORT, 0);
      conf.set(Constants.Gateway.ADDRESS, hostname);
      conf.set(Constants.AppFabric.OUTPUT_DIR, System.getProperty("java.io.tmpdir"));
      conf.set(Constants.AppFabric.TEMP_DIR, System.getProperty("java.io.tmpdir"));
      conf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
      conf.set(Constants.AppFabric.SERVER_PORT, Integer.toString(Networks.getRandomPort()));
      conf.setBoolean(Constants.Gateway.CONFIG_AUTHENTICATION_REQUIRED, true);
      conf.set(Constants.Gateway.CLUSTER_NAME, CLUSTER);

      injector = startGateway(conf);
    }

    @Override
    protected void after() {
      stopGateway(conf);
    }
  };

  public static Injector startGateway(CConfiguration conf) {
    final Map<String, List<String>> keysAndClusters = ImmutableMap.of(API_KEY, Collections.singletonList(CLUSTER));

    // Set up our Guice injections
    injector = Guice.createInjector(Modules.override(
      new GatewayModules().getInMemoryModules(),
      new AppFabricTestModule(conf)
    ).with(new AbstractModule() {
      @Override
      protected void configure() {
        // It's a bit hacky to add it here. Need to refactor these bindings out as it overlaps with
        // AppFabricServiceModule
        bind(LogReader.class).to(MockLogReader.class).in(Scopes.SINGLETON);
        bind(DataSetInstantiatorFromMetaData.class).in(Scopes.SINGLETON);
        bind(PassportClient.class).toInstance(new MockedPassportClient(keysAndClusters));

        MockMetricsCollectionService metricsCollectionService = new MockMetricsCollectionService();
        bind(MetricsCollectionService.class).toInstance(metricsCollectionService);
        bind(MockMetricsCollectionService.class).toInstance(metricsCollectionService);
      }
    }
    ));

    gateway = injector.getInstance(Gateway.class);
    injector.getInstance(InMemoryTransactionManager.class).startAndWait();
    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
    gateway.startAndWait();
    port = gateway.getBindAddress().getPort();

    // initialize the dataset instantiator
    DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
    endpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.APP_FABRIC)), 1L, TimeUnit.SECONDS);
    injector.getInstance(DataSetInstantiatorFromMetaData.class).init(endpointStrategy);

    return injector;
  }

  public static void stopGateway(CConfiguration conf) {
    gateway.stopAndWait();
    appFabricServer.stopAndWait();
    conf.clear();
  }

  public static Injector getInjector() {
    return injector;
  }

  public static int getPort() {
    return port;
  }

  public static Header getAuthHeader() {
    return AUTH_HEADER;
  }

  public static EndpointStrategy getEndpointStrategy() {
    return endpointStrategy;
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

  public static HttpResponse doPut(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut("http://" + hostname + ":" + port + resource);
    put.setHeader(AUTH_HEADER);
    return client.execute(put);
  }

  public static HttpResponse doPut(String resource, String body) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut("http://" + hostname + ":" + port + resource);
    if (body != null) {
      put.setEntity(new StringEntity(body));
    }
    put.setHeader(AUTH_HEADER);
    return client.execute(put);
  }

  public static HttpResponse doPost(HttpPost post) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    post.setHeader(AUTH_HEADER);
    return client.execute(post);
  }

  public static HttpPost getPost(String resource) {
    HttpPost post = new HttpPost("http://" + hostname + ":" + port + resource);
    post.setHeader(AUTH_HEADER);
    return post;
  }

  public static HttpResponse doPost(String resource, String body) throws Exception {
    return doPost(resource, body, null);
  }

  public static HttpResponse doPost(String resource, String body, Header[] headers) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPost post = new HttpPost("http://" + hostname + ":" + port + resource);

    if (body != null) {
      post.setEntity(new StringEntity(body));
    }

    if (headers != null) {
      post.setHeaders(ObjectArrays.concat(AUTH_HEADER, headers));
    } else {
      post.setHeader(AUTH_HEADER);
    }
    return client.execute(post);
  }

  public static HttpResponse doDelete(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpDelete delete = new HttpDelete("http://" + hostname + ":" + port + resource);
    delete.setHeader(AUTH_HEADER);
    return client.execute(delete);
  }

}
