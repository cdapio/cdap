package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.Networks;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.util.DataSetInstantiatorFromMetaData;
import com.continuuity.gateway.v2.Gateway;
import com.continuuity.gateway.v2.handlers.v2.AppFabricServiceHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.MetadataServiceHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.PingHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.ProcedureHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.dataset.ClearFabricHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.dataset.DatasetHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.dataset.TableHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.log.LogHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.log.MockLogReader;
import com.continuuity.gateway.v2.runtime.GatewayModules;
import com.continuuity.gateway.v2.tools.DataSetClientTest;
import com.continuuity.gateway.v2.tools.StreamClientTest;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.logging.read.LogReader;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.test.internal.guice.AppFabricTestModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Test Suite for running all API tests.
 */
@RunWith(value = Suite.class)
@SuiteClasses(value = {PingHandlerTest.class, MetadataServiceHandlerTest.class, LogHandlerTest.class,
  ProcedureHandlerTest.class, TableHandlerTest.class, DatasetHandlerTest.class, ClearFabricHandlerTest.class,
  DataSetClientTest.class, StreamClientTest.class, AppFabricServiceHandlerTest.class})
public class GatewayFastTestsSuite {
  private static Gateway gateway;
  private static final String hostname = "127.0.0.1";
  private static int port;
  private static CConfiguration conf = CConfiguration.create();

  private static Injector injector;
  private static MetadataService.Iface mds;
  private static AppFabricServer appFabricServer;


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

      // Set up our Guice injections
      injector = Guice.createInjector(
        new GatewayModules(conf).getInMemoryModules(),
        new AppFabricTestModule(conf),
        new AbstractModule() {
          @Override
          protected void configure() {
            // It's a bit hacky to add it here. Need to refactor these bindings out as it overlaps with
            // AppFabricServiceModule
            bind(LogReader.class).to(MockLogReader.class).in(Scopes.SINGLETON);
            bind(DataSetInstantiatorFromMetaData.class).in(Scopes.SINGLETON);
          }
        }
      );

      gateway = injector.getInstance(Gateway.class);
      mds = injector.getInstance(MetadataService.Iface.class);
      injector.getInstance(InMemoryTransactionManager.class).init();
      appFabricServer = injector.getInstance(AppFabricServer.class);
      appFabricServer.startAndWait();
      gateway.startAndWait();
      port = gateway.getBindAddress().getPort();
    }

    @Override
    protected void after() {
      gateway.stopAndWait();
      appFabricServer.stopAndWait();
      conf.clear();
    }
  };

  public static Injector getInjector() {
    return injector;
  }

  public static int getPort() {
    return port;
  }

  public static HttpResponse doGet(String resource) throws Exception {
    return doGet(resource, null);
  }

  public static HttpResponse doGet(String resource, Header[] headers) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet("http://" + hostname + ":" + port + resource);

    if (headers != null) {
      get.setHeaders(headers);
    }
    return client.execute(get);
  }

  public static HttpResponse doPut(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut("http://" + hostname + ":" + port + resource);
    return client.execute(put);
  }

  public static HttpResponse doPut(String resource, String body) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut("http://" + hostname + ":" + port + resource);
    if (body != null) {
      put.setEntity(new StringEntity(body));
    }
    return client.execute(put);
  }

  public static HttpResponse doPut(HttpPut put) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    return client.execute(put);
  }

  public static HttpPut getPut(String resource) {
    return new HttpPut("http://" + hostname + ":" + port + resource);
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
      post.setHeaders(headers);
    }
    return client.execute(post);
  }

  public static HttpResponse doDelete(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpDelete delete = new HttpDelete("http://" + hostname + ":" + port + resource);
    return client.execute(delete);
  }

  public static MetadataService.Iface getMds() {
    return mds;
  }

}
