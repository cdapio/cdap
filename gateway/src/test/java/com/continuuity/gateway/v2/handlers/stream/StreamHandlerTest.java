package com.continuuity.gateway.v2.handlers.stream;

import com.continuuity.app.guice.LocationRuntimeModule;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.v2.GatewayV2;
import com.continuuity.gateway.v2.GatewayV2Constants;
import com.continuuity.gateway.v2.runtime.GatewayV2Modules;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * Test stream handler.
 */
public class StreamHandlerTest {
  private static GatewayV2 gatewayV2;
  private static final String hostname = "127.0.0.1";
  private static int port;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration configuration = CConfiguration.create();
    configuration.setInt(GatewayV2Constants.ConfigKeys.PORT, 0);
    configuration.set(GatewayV2Constants.ConfigKeys.ADDRESS, hostname);
    configuration.setInt(GatewayV2Constants.ConfigKeys.MAX_CACHED_EVENTS_PER_STREAM_NUM, 5);

    // Set up our Guice injections
    Injector injector = Guice.createInjector(
      new DataFabricModules().getInMemoryModules(),
      new ConfigModule(configuration),
      new LocationRuntimeModule().getInMemoryModules(),
      new GatewayV2Modules(configuration).getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // It's a bit hacky to add it here. Need to refactor these bindings out as it overlaps with
          // AppFabricServiceModule
          bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
          bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
          bind(StoreFactory.class).to(MDSStoreFactory.class);
        }
      }
    );

    gatewayV2 = injector.getInstance(GatewayV2.class);
    gatewayV2.startAndWait();
    port = gatewayV2.getBindAddress().getPort();
  }

  @AfterClass
  public static void finish() throws Exception {
    gatewayV2.stopAndWait();
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
    final int concurrencyLevel = 6;
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

    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
    List<ListenableFuture<?>> futureList = Lists.newArrayList();

    List<BatchEnqueue> batchEnqueues = Lists.newArrayList();
    for (int i = 0; i < concurrencyLevel; ++i) {
      BatchEnqueue batchEnqueue = new BatchEnqueue(i * 1000);
      batchEnqueues.add(batchEnqueue);
      futureList.add(executorService.submit(batchEnqueue));
    }

    Futures.allAsList(futureList).get();
    executorService.shutdown();

    List<Integer> actual = Lists.newArrayList();
    // Dequeue all entries
    for (int i = 0; i < concurrencyLevel * BatchEnqueue.NUM_ELEMENTS; ++i) {
      httpGet = new HttpGet(String.format("http://%s:%d/stream/test_batch_stream_enqueue?q=dequeue", hostname, port));
      httpGet.setHeader(Constants.HEADER_STREAM_CONSUMER, groupId);
      response = httpclient.execute(httpGet);
      Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
      int entry = Integer.parseInt(EntityUtils.toString(response.getEntity()));
      actual.add(entry);
    }

    List<Integer> expected = Lists.newArrayList();
    for (BatchEnqueue batchEnqueue : batchEnqueues) {
      expected.addAll(batchEnqueue.expected);
      batchEnqueue.verify(actual);
    }

    Collections.sort(expected);
    Collections.sort(actual);
    Assert.assertEquals(expected, actual);
  }

  private static class BatchEnqueue implements Runnable {
    public static final int NUM_ELEMENTS = 100;
    private final int startElement;

    private final List<Integer> expected = Lists.newArrayList();

    private BatchEnqueue(int startElement) {
      this.startElement = startElement;
    }

    @Override
    public void run() {
      try {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        for (int i = startElement; i < startElement + NUM_ELEMENTS; ++i) {
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
      List<Integer> actual = Lists.newArrayList();
      for (Integer i : out) {
        if (startElement <= i && i < startElement + NUM_ELEMENTS) {
          actual.add(i);
        }
      }
      Assert.assertEquals(expected, actual);
    }
  }
}
