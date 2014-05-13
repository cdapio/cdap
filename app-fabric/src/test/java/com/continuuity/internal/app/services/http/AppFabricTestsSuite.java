package com.continuuity.internal.app.services.http;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.utils.Networks;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.handlers.dataset.DataSetInstantiatorFromMetaData;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.internal.app.services.http.handlers.AppFabricHttpHandlerTest;
import com.continuuity.internal.app.services.http.handlers.PingHandlerTest;
import com.continuuity.metrics.query.MetricsQueryService;
import com.continuuity.test.internal.guice.AppFabricTestModule;
import com.google.common.collect.ObjectArrays;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
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
  private static MetricsQueryService metrics;
  private static TransactionSystemClient txClient;

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
      conf.setBoolean(Constants.Gateway.CONFIG_AUTHENTICATION_REQUIRED, false);
      conf.set(Constants.Gateway.CLUSTER_NAME, CLUSTER);

      injector = Guice.createInjector(new AppFabricTestModule(conf));
      injector.getInstance(InMemoryTransactionManager.class).startAndWait();
      appFabricServer = injector.getInstance(AppFabricServer.class);
      appFabricServer.startAndWait();
      DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
      endpointStrategy = new TimeLimitEndpointStrategy(new RandomEndpointStrategy(discoveryClient.discover(
        Constants.Service.APP_FABRIC_HTTP)), 1L, TimeUnit.SECONDS);
      injector.getInstance(DataSetInstantiatorFromMetaData.class).init(endpointStrategy);
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

  public static HttpResponse execute(HttpUriRequest request) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    request.setHeader(AUTH_HEADER);
    return client.execute(request);
  }

  public static HttpPost getPost(String resource) {
    HttpPost post = new HttpPost("http://" + hostname + ":" + port + resource);
    post.setHeader(AUTH_HEADER);
    return post;
  }

  public static HttpPut getPut(String resource) {
    HttpPut put = new HttpPut("http://" + hostname + ":" + port + resource);
    put.setHeader(AUTH_HEADER);
    return put;
  }
  public static HttpResponse doPost(String resource) throws Exception {
    return doPost(resource, null, null);
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

  public static HttpResponse doPost(HttpPost post) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    post.setHeader(AUTH_HEADER);
    return client.execute(post);
  }


  public static HttpResponse doPut(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut("http://" + hostname + ":" + port + resource);
    put.setHeader(AUTH_HEADER);
    return doPut(resource, null);
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

  public static HttpResponse doDelete(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpDelete delete = new HttpDelete("http://" + hostname + ":" + port + resource);
    delete.setHeader(AUTH_HEADER);
    return client.execute(delete);
  }
}
