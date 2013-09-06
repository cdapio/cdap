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
import com.continuuity.gateway.v2.handlers.v2.dataset.MetadataServiceHandlerTest;
import com.continuuity.gateway.v2.runtime.GatewayModules;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.continuuity.weave.discovery.InMemoryDiscoveryService;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
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
@SuiteClasses(value = {PingHandlerTest.class, MetadataServiceHandlerTest.class})
public class GatewayFastTestsSuite {
  private static Gateway gateway;
  private static final String hostname = "127.0.0.1";
  private static int port;
  private static CConfiguration conf = CConfiguration.create();
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
            bind(DiscoveryServiceClient.class).to(InMemoryDiscoveryService.class);
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

  public static HttpResponse GET(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet("http://" + hostname + ":" + port + resource);
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

  public static MetadataService.Iface getMds() {
    return mds;
  }

}
