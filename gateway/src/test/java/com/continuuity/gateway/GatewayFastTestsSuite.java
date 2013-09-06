package com.continuuity.gateway;

import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.v2.Gateway;
import com.continuuity.gateway.v2.GatewayConstants;
import com.continuuity.gateway.v2.handlers.v2.PingHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.ProcedureHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.dataset.MetadataServiceHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.log.LogHandlerTest;
import com.continuuity.gateway.v2.handlers.v2.log.MockLogReader;
import com.continuuity.gateway.v2.runtime.GatewayModules;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.logging.read.LogReader;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.continuuity.weave.discovery.InMemoryDiscoveryService;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
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
  ProcedureHandlerTest.class})
public class GatewayFastTestsSuite {
  private static Gateway gateway;
  private static final String hostname = "127.0.0.1";
  private static int port;
  private static CConfiguration conf = CConfiguration.create();

  private static final InMemoryDiscoveryService IN_MEMORY_DISCOVERY_SERVICE = new InMemoryDiscoveryService();
  private static MetadataService.Iface mds;

  @ClassRule
  public static ExternalResource resources = new ExternalResource() {
    @Override
    protected void before() throws Throwable {
      conf.setInt(GatewayConstants.ConfigKeys.PORT, 0);
      conf.set(GatewayConstants.ConfigKeys.ADDRESS, hostname);

      // Set up our Guice injections
      Injector injector = Guice.createInjector(
        new DataFabricModules().getInMemoryModules(),
        new ConfigModule(conf),
        new LocationRuntimeModule().getInMemoryModules(),
        new GatewayModules(conf).getInMemoryModules(),
        new AbstractModule() {
          @Override
          protected void configure() {
            // It's a bit hacky to add it here. Need to refactor these bindings out as it overlaps with
            // AppFabricServiceModule
            bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
            bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
            bind(StoreFactory.class).to(MDSStoreFactory.class);
            bind(LogReader.class).to(MockLogReader.class);
            bind(DiscoveryServiceClient.class).toInstance(IN_MEMORY_DISCOVERY_SERVICE);
          }
        }
      );

      gateway = injector.getInstance(Gateway.class);
      mds = injector.getInstance(MetadataService.Iface.class);
      injector.getInstance(InMemoryTransactionManager.class).init();
      gateway.startAndWait();
      port = gateway.getBindAddress().getPort();
    }

    @Override
    protected void after() {
      gateway.stopAndWait();
      conf.clear();
    }
  };

  public static InMemoryDiscoveryService getInMemoryDiscoveryService() {
    return IN_MEMORY_DISCOVERY_SERVICE;
  }

  public static HttpResponse GET(String resource) throws Exception {
    return GET(resource, null);
  }

  public static HttpResponse GET(String resource, Header[] headers) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet("http://" + hostname + ":" + port + resource);

    if (headers != null) {
      get.setHeaders(headers);
    }
    return client.execute(get);
  }

  public static HttpResponse PUT(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut("http://" + hostname + ":" + port + resource);
    return client.execute(put);
  }

  public static HttpResponse PUT(HttpPut put) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    return client.execute(put);
  }

  public static HttpResponse POST(String resource, String body) throws Exception {
    return POST(resource, body, null);
  }

  public static HttpResponse POST(String resource, String body, Header[] headers) throws Exception {
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

  public static MetadataService.Iface getMds() {
    return mds;
  }

}
