package com.continuuity.gateway.v2.handlers.stream;

import com.continuuity.app.guice.LocationRuntimeModule;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.gateway.v2.Gateway;
import com.continuuity.gateway.v2.GatewayConstants;
import com.continuuity.gateway.v2.handlers.PingHandler;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import junit.framework.Assert;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Test stream handler.
 */
public class StreamHandlerTest {
  private static Gateway gateway;
  private static final String hostname = "127.0.0.1";
  private static int port;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration configuration = CConfiguration.create();
    configuration.setInt(GatewayConstants.ConfigKeys.PORT, 0);
    configuration.set(GatewayConstants.ConfigKeys.ADDRESS, hostname);
    configuration.setInt(GatewayConstants.ConfigKeys.MAX_CACHED_EVENTS_PER_STREAM_NUM, 5);

    // Set up our Guice injections
    final CMetrics cMetrics = new CMetrics(MetricType.System);
    Injector injector = Guice.createInjector(
      new DataFabricModules().getInMemoryModules(),
      new ConfigModule(configuration),
      new LocationRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // It's a bit hacky to add it here. Need to refactor these bindings out as it overlaps with
          // AppFabricServiceModule
          bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
          bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
          bind(StoreFactory.class).to(MDSStoreFactory.class);
          bind(GatewayAuthenticator.class).to(NoAuthenticator.class);
          bind(CMetrics.class).toInstance(cMetrics);

          Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class);
          handlerBinder.addBinding().to(StreamHandler.class).in(Scopes.SINGLETON);
          handlerBinder.addBinding().to(PingHandler.class).in(Scopes.SINGLETON);
        }

        @Provides
        @Named(GatewayConstants.ConfigKeys.ADDRESS)
        public final InetAddress providesHostname(CConfiguration cConf) {
          return Networks.resolve(cConf.get(GatewayConstants.ConfigKeys.ADDRESS),
                                  new InetSocketAddress(hostname, 0).getAddress());
        }
      }
    );

    gateway = injector.getInstance(Gateway.class);
    gateway.startAndWait();
    port = gateway.getBindAddress().getPort();
  }

  @AfterClass
  public static void finish() throws Exception {
    gateway.stopAndWait();
  }

  @Test
  public void testPing() throws Exception {
    DefaultHttpClient httpclient = new DefaultHttpClient();
    HttpGet httpget = new HttpGet(String.format("http://%s:%d/ping", hostname, port));
    HttpResponse response = httpclient.execute(httpget);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals("OK.\n", EntityUtils.toString(response.getEntity()));
  }

  @Test
  public void testStreamCreate() throws Exception {
    // Try to get info on a non-existant stream
    DefaultHttpClient httpclient = new DefaultHttpClient();
    HttpGet httpGet = new HttpGet(String.format("http://%s:%d/stream/test_stream1", hostname, port));
    HttpResponse response = httpclient.execute(httpGet);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());

    // Now, create the new stream.
    HttpPut httpPut = new HttpPut(String.format("http://%s:%d/stream/test_stream1", hostname, port));
    response = httpclient.execute(httpPut);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());

    // getInfo should now return 200
    httpGet = new HttpGet(String.format("http://%s:%d/stream/test_stream1", hostname, port));
    response = httpclient.execute(httpGet);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());

    httpGet = new HttpGet(String.format("http://%s:%d/stream/test_stream1?q=info", hostname, port));
    response = httpclient.execute(httpGet);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());
  }

  @Test
  public void testSimpleStreamEnqueue() throws Exception {
    DefaultHttpClient httpclient = new DefaultHttpClient();

    // Create new stream.
    HttpPut httpPut = new HttpPut(String.format("http://%s:%d/stream/test_stream_enqueue", hostname, port));
    HttpResponse response = httpclient.execute(httpPut);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());

    // Enqueue 10 entries
    for (int i = 0; i < 10; ++i) {
      HttpPost httpPost = new HttpPost(String.format("http://%s:%d/stream/test_stream_enqueue", hostname, port));
      httpPost.setEntity(new StringEntity(Integer.toString(i)));
      httpPost.setHeader("test_stream_enqueue.header1", Integer.toString(i));
      response = httpclient.execute(httpPost);
      Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
      EntityUtils.consume(response.getEntity());
    }

    // Get new consumer id
    HttpGet httpGet = new HttpGet(String.format("http://%s:%d/stream/test_stream_enqueue?q=newConsumer",
                                                hostname, port));
    response = httpclient.execute(httpGet);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals(1, response.getHeaders(Constants.HEADER_STREAM_CONSUMER).length);
    String groupId = response.getFirstHeader(Constants.HEADER_STREAM_CONSUMER).getValue();
    EntityUtils.consume(response.getEntity());

    // Dequeue 10 entries
    for (int i = 0; i < 10; ++i) {
      httpGet = new HttpGet(String.format("http://%s:%d/stream/test_stream_enqueue?q=dequeue", hostname, port));
      httpGet.setHeader(Constants.HEADER_STREAM_CONSUMER, groupId);
      response = httpclient.execute(httpGet);
      Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
      int actual = Integer.parseInt(EntityUtils.toString(response.getEntity()));
      Assert.assertEquals(i, actual);
      Assert.assertEquals(1, response.getHeaders("test_stream_enqueue.header1").length);
      Assert.assertEquals(Integer.toString(i), response.getFirstHeader("test_stream_enqueue.header1").getValue());
    }

    // Dequeue-ing again should give NO_CONTENT
    httpGet = new HttpGet(String.format("http://%s:%d/stream/test_stream_enqueue?q=dequeue", hostname, port));
    httpGet.setHeader(Constants.HEADER_STREAM_CONSUMER, groupId);
    response = httpclient.execute(httpGet);
    Assert.assertEquals(HttpResponseStatus.NO_CONTENT.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());
  }

  @Test
  public void testBatchStreamEnqueue() throws Exception {
    DefaultHttpClient httpclient = new DefaultHttpClient();

    // Create new stream.
    HttpPut httpPut = new HttpPut(String.format("http://%s:%d/stream/test_batch_stream_enqueue", hostname, port));
    HttpResponse response = httpclient.execute(httpPut);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());

    // Get new consumer id
    HttpGet httpGet = new HttpGet(String.format("http://%s:%d/stream/test_batch_stream_enqueue?q=newConsumer",
                                                hostname, port));
    response = httpclient.execute(httpGet);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals(1, response.getHeaders(Constants.HEADER_STREAM_CONSUMER).length);
    String groupId = response.getFirstHeader(Constants.HEADER_STREAM_CONSUMER).getValue();
    EntityUtils.consume(response.getEntity());

    ExecutorService executorService = Executors.newFixedThreadPool(5);
    BatchEnqueue batchEnqueue1 = new BatchEnqueue(true);
    BatchEnqueue batchEnqueue2 = new BatchEnqueue(false);
    Future<?> future1 = executorService.submit(batchEnqueue1);
    Future<?> future2 = executorService.submit(batchEnqueue2);
    future1.get();
    future2.get();
    executorService.shutdown();

    List<Integer> actual = Lists.newArrayList();
    // Dequeue all entries
    for (int i = 0; i < BatchEnqueue.NUM_ELEMENTS; ++i) {
      httpGet = new HttpGet(String.format("http://%s:%d/stream/test_batch_stream_enqueue?q=dequeue", hostname, port));
      httpGet.setHeader(Constants.HEADER_STREAM_CONSUMER, groupId);
      response = httpclient.execute(httpGet);
      Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
      int entry = Integer.parseInt(EntityUtils.toString(response.getEntity()));
      actual.add(entry);
    }

    batchEnqueue1.verify(actual);
    batchEnqueue2.verify(actual);

    Collections.sort(actual);
    for (int i = 0; i < BatchEnqueue.NUM_ELEMENTS; ++i) {
      Assert.assertEquals((Integer) i, actual.get(i));
    }
  }

  private static class BatchEnqueue implements Runnable {
    public static final int NUM_ELEMENTS = 100;  // Should be an even number
    private final boolean evenGenerator;

    private final List<Integer> expected = Lists.newArrayList();

    private BatchEnqueue(boolean evenGenerator) {
      this.evenGenerator = evenGenerator;
    }

    @Override
    public void run() {
      try {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        for (int i = evenGenerator ? 0 : 1; i < NUM_ELEMENTS; i += 2) {
          HttpPost httpPost = new HttpPost(String.format("http://%s:%d/stream/test_batch_stream_enqueue",
                                                         hostname, port));
          httpPost.setEntity(new StringEntity(Integer.toString(i)));
          httpPost.setHeader("test_batch_stream_enqueue1", Integer.toString(i));
          HttpResponse response = httpclient.execute(httpPost);
          Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
          EntityUtils.consume(response.getEntity());
          expected.add(i);
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    public void verify(List<Integer> out) {
      Assert.assertEquals(0, NUM_ELEMENTS % 2);
      Assert.assertEquals(NUM_ELEMENTS, out.size());

      List<Integer> actual = Lists.newArrayList();
      for (Integer i : out) {
        if ((i % 2 == 0) == evenGenerator) {
          actual.add(i);
        }
      }
      Assert.assertEquals(expected, actual);
    }
  }
}
