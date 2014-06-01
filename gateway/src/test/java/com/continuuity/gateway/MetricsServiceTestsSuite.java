package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.handlers.dataset.DataSetInstantiatorFromMetaData;
import com.continuuity.gateway.handlers.log.LogHandlerTest;
import com.continuuity.gateway.handlers.log.MockLogReader;
import com.continuuity.gateway.handlers.metrics.MetricsDeleteTest;
import com.continuuity.gateway.handlers.metrics.MetricsDiscoveryQueryTest;
import com.continuuity.gateway.handlers.metrics.MetricsQueryTest;
import com.continuuity.logging.read.LogReader;
import com.continuuity.metrics.query.MetricsQueryService;
import com.continuuity.passport.http.client.PassportClient;
import com.continuuity.test.internal.guice.AppFabricTestModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ObjectArrays;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
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
import org.apache.twill.discovery.DiscoveryServiceClient;
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
@Suite.SuiteClasses(value = {MetricsQueryTest.class, MetricsDeleteTest.class, MetricsDiscoveryQueryTest.class,
  LogHandlerTest.class})
public class MetricsServiceTestsSuite  {
  private static final String API_KEY = "SampleTestApiKey";
  private static final String CLUSTER = "SampleTestClusterName";
  private static final Header AUTH_HEADER = new BasicHeader(Constants.Gateway.CONTINUUITY_API_KEY, API_KEY);

  private static MetricsQueryService metrics;
  private static final String hostname = "127.0.0.1";
  private static int port;
  private static CConfiguration conf = CConfiguration.create();

  private static Injector injector;

  @ClassRule
  public static ExternalResource resources = new ExternalResource() {
    @Override
    protected void before() throws Throwable {

      conf.set(Constants.Metrics.ADDRESS, hostname);
      conf.set(Constants.AppFabric.OUTPUT_DIR, System.getProperty("java.io.tmpdir"));
      conf.set(Constants.AppFabric.TEMP_DIR, System.getProperty("java.io.tmpdir"));
      conf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
      conf.setBoolean(Constants.Metrics.CONFIG_AUTHENTICATION_REQUIRED, true);
      conf.set(Constants.Metrics.CLUSTER_NAME, CLUSTER);

      injector = startMetricsService(conf);
    }

    @Override
    protected void after() {
      stopMetricsService(conf);
    }
  };

  public static Injector startMetricsService(CConfiguration conf) {
    final Map<String, List<String>> keysAndClusters = ImmutableMap.of(API_KEY, Collections.singletonList(CLUSTER));

    // Set up our Guice injections
    injector = Guice.createInjector(Modules.override(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(PassportClient.class).toProvider(new Provider<PassportClient>() {
            @Override
            public PassportClient get() {
              return new MockedPassportClient(keysAndClusters);
            }
          });
        }
      },
      new AppFabricTestModule(conf)
    ).with(new AbstractModule() {
             @Override
             protected void configure() {
               // It's a bit hacky to add it here. Need to refactor
               // these bindings out as it overlaps with
               // AppFabricServiceModule
               bind(LogReader.class).to(MockLogReader.class).in(Scopes.SINGLETON);
               bind(DataSetInstantiatorFromMetaData.class).in(Scopes.SINGLETON);

               MockMetricsCollectionService metricsCollectionService =
                 new MockMetricsCollectionService();
               bind(MetricsCollectionService.class).toInstance(metricsCollectionService);
               bind(MockMetricsCollectionService.class).toInstance(metricsCollectionService);
             }
           }
    ));

    metrics = injector.getInstance(MetricsQueryService.class);
    injector.getInstance(InMemoryTransactionManager.class).startAndWait();
    metrics.startAndWait();

    // initialize the dataset instantiator
    DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);

    EndpointStrategy metricsEndPoints = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.METRICS)), 1L, TimeUnit.SECONDS);

    port = metricsEndPoints.pick().getSocketAddress().getPort();

    return injector;
  }

  public static void stopMetricsService(CConfiguration conf) {
    metrics.stopAndWait();
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
