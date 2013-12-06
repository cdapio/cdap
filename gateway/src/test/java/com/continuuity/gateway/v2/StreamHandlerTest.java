package com.continuuity.gateway.v2;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.Networks;
import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;
import com.ning.http.client.providers.netty.NettyAsyncHttpProvider;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Test stream handler. This is not part of GatewayFastTestsSuite because it needs to start the gateway multiple times.
 */
public class StreamHandlerTest {
  private static final Logger LOG = LoggerFactory.getLogger(StreamHandlerTest.class);
  private static final String API_KEY = "SampleTestApiKey";
  private static final String CLUSTER = "SampleTestClusterName";
  private static final Header AUTH_HEADER = new BasicHeader(GatewayAuthenticator.CONTINUUITY_API_KEY, API_KEY);

  private static final String hostname = "127.0.0.1";
  private static int port;
  private static CConfiguration configuration = CConfiguration.create();

  @Before
  public void setupConf() {
    configuration.set(Constants.AppFabric.SERVER_PORT, Integer.toString(Networks.getRandomPort()));
    configuration.setInt(com.continuuity.common.conf.Constants.Gateway.PORT, 0);
    configuration.set(com.continuuity.common.conf.Constants.Gateway.ADDRESS, hostname);
    configuration.set(com.continuuity.common.conf.Constants.CFG_LOCAL_DATA_DIR,
                      System.getProperty("java.io.tmpdir"));
    configuration.setBoolean(Constants.Gateway.CONFIG_AUTHENTICATION_REQUIRED, true);
    configuration.set(Constants.Gateway.CLUSTER_NAME, CLUSTER);
  }

  private void startGateway() throws Exception {
    GatewayFastTestsSuite.startGateway(configuration);
    port = GatewayFastTestsSuite.getPort();
    testPing();
  }

  @After
  public void stopGateway() throws Exception {
    GatewayFastTestsSuite.stopGateway(configuration);
  }

  @Test
  public void testStreamCreate() throws Exception {
    startGateway();

    // Try to get info on a non-existant stream
    DefaultHttpClient httpclient = new DefaultHttpClient();
    HttpGet httpGet = new HttpGet(String.format("http://%s:%d/v2/streams/test_stream1/info", hostname, port));
    httpGet.setHeader(AUTH_HEADER);
    HttpResponse response = httpclient.execute(httpGet);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());

    // Now, create the new stream.
    HttpPut httpPut = new HttpPut(String.format("http://%s:%d/v2/streams/test_stream1", hostname, port));
    httpPut.setHeader(AUTH_HEADER);
    response = httpclient.execute(httpPut);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());

    // getInfo should now return 200
    httpGet = new HttpGet(String.format("http://%s:%d/v2/streams/test_stream1/info", hostname, port));
    httpGet.setHeader(AUTH_HEADER);
    response = httpclient.execute(httpGet);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());
  }

  @Test
  public void testAuth() throws Exception {
    configuration.setBoolean(Constants.Gateway.CONFIG_AUTHENTICATION_REQUIRED, true);
    startGateway();

    // Test create without auth, should return 403
    DefaultHttpClient httpclient = new DefaultHttpClient();
    HttpPut httpPut = new HttpPut(String.format("http://%s:%d/v2/streams/test_stream_no_auth_fail", hostname, port));
    HttpResponse response = httpclient.execute(httpPut);
    Assert.assertEquals(HttpResponseStatus.UNAUTHORIZED.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());
  }

  @Test
  public void testNoAuth() throws Exception {
    configuration.setBoolean(Constants.Gateway.CONFIG_AUTHENTICATION_REQUIRED, false);
    startGateway();

    // Test create without auth, should return 200 since auth is turned off.
    DefaultHttpClient httpclient = new DefaultHttpClient();
    HttpPut httpPut = new HttpPut(String.format("http://%s:%d/v2/streams/test_stream_no_auth_pass", hostname, port));
    HttpResponse response = httpclient.execute(httpPut);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());
  }

  @Test
  public void testSimpleStreamEnqueue() throws Exception {
    startGateway();

    DefaultHttpClient httpclient = new DefaultHttpClient();

    // Create new stream.
    HttpPut httpPut = new HttpPut(String.format("http://%s:%d/v2/streams/test_stream_enqueue", hostname, port));
    httpPut.setHeader(AUTH_HEADER);
    HttpResponse response = httpclient.execute(httpPut);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());

    // Enqueue 10 entries
    for (int i = 0; i < 10; ++i) {
      HttpPost httpPost = new HttpPost(String.format("http://%s:%d/v2/streams/test_stream_enqueue", hostname,
                                                     port));
      httpPost.setEntity(new StringEntity(Integer.toString(i)));
      httpPost.setHeader("test_stream_enqueue.header1", Integer.toString(i));
      httpPost.setHeader(AUTH_HEADER);
      response = httpclient.execute(httpPost);
      Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
      EntityUtils.consume(response.getEntity());
    }

    String groupId = getStreamConsumer(httpclient, "test_stream_enqueue");
    // Dequeue 10 entries
    for (int i = 0; i < 10; ++i) {
      HttpPost httpPost = new HttpPost(String.format("http://%s:%d/v2/streams/test_stream_enqueue/dequeue",
                                                     hostname, port));
      httpPost.setHeader(AUTH_HEADER);
      httpPost.setHeader(Constants.Gateway.HEADER_STREAM_CONSUMER, groupId);
      response = httpclient.execute(httpPost);
      Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
      int actual = Integer.parseInt(EntityUtils.toString(response.getEntity()));
      Assert.assertEquals(i, actual);
      Assert.assertEquals(1, response.getHeaders("test_stream_enqueue.header1").length);
      Assert.assertEquals(Integer.toString(i), response.getFirstHeader("test_stream_enqueue.header1").getValue());
    }

    // Dequeue-ing again should give NO_CONTENT
    HttpPost httpPost = new HttpPost(String.format("http://%s:%d/v2/streams/test_stream_enqueue/dequeue",
                                                   hostname, port));
    httpPost.setHeader(AUTH_HEADER);
    httpPost.setHeader(Constants.Gateway.HEADER_STREAM_CONSUMER, groupId);
    response = httpclient.execute(httpPost);
    Assert.assertEquals(HttpResponseStatus.NO_CONTENT.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());
  }

  private String getStreamConsumer(DefaultHttpClient httpclient, String streamName) throws IOException {
    HttpPost httpPost = new HttpPost(String.format("http://%s:%d/v2/streams/%s/consumer-id",
                                                   hostname, port, streamName));
    httpPost.setHeader(AUTH_HEADER);
    HttpResponse response = httpclient.execute(httpPost);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals(1, response.getHeaders(Constants.Gateway.HEADER_STREAM_CONSUMER).length);
    String groupId = response.getFirstHeader(Constants.Gateway.HEADER_STREAM_CONSUMER).getValue();
    EntityUtils.consume(response.getEntity());
    return groupId;
  }

  @Test
  public void testStreamTruncate() throws Exception {
    startGateway();

    DefaultHttpClient httpclient = new DefaultHttpClient();

    // Create new stream.
    HttpPut httpPut = new HttpPut(String.format("http://%s:%d/v2/streams/test_stream_truncate", hostname, port));
    httpPut.setHeader(AUTH_HEADER);
    HttpResponse response = httpclient.execute(httpPut);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());

    // Enqueue 10 entries
    for (int i = 0; i < 10; ++i) {
      HttpPost httpPost = new HttpPost(String.format("http://%s:%d/v2/streams/test_stream_truncate", hostname,
                                                     port));
      httpPost.setEntity(new StringEntity("truncate-elem-" + Integer.toString(i)));
      httpPost.setHeader("test_stream_enqueue.header1", Integer.toString(i));
      httpPost.setHeader(AUTH_HEADER);
      response = httpclient.execute(httpPost);
      Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
      EntityUtils.consume(response.getEntity());
    }

    // Dequeue 1 entry
    String groupId = getStreamConsumer(httpclient, "test_stream_truncate");
    HttpPost httpPost = new HttpPost(String.format("http://%s:%d/v2/streams/test_stream_truncate/dequeue",
                                                   hostname, port));
    httpPost.setHeader(AUTH_HEADER);
    httpPost.setHeader(Constants.Gateway.HEADER_STREAM_CONSUMER, groupId);
    response = httpclient.execute(httpPost);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String actual = EntityUtils.toString(response.getEntity());
    Assert.assertEquals("truncate-elem-0", actual);

    // truncate stream
    httpPost = new HttpPost(String.format("http://%s:%d/v2/streams/test_stream_truncate/truncate", hostname,
                                                   port));
    httpPost.setHeader(AUTH_HEADER);
    response = httpclient.execute(httpPost);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());

    // now dequeue should give NO_CONTENT
    httpPost = new HttpPost(String.format("http://%s:%d/v2/streams/test_stream_truncate/dequeue",
                                          hostname, port));
    httpPost.setHeader(AUTH_HEADER);
    httpPost.setHeader(Constants.Gateway.HEADER_STREAM_CONSUMER, groupId);
    response = httpclient.execute(httpPost);
    Assert.assertEquals(HttpResponseStatus.NO_CONTENT.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());
  }

  @Test
  public void testBatchStreamEnqueue() throws Exception {
    startGateway();

    DefaultHttpClient httpclient = new DefaultHttpClient();

    // Create new stream.
    HttpPut httpPut = new HttpPut(String.format("http://%s:%d/v2/streams/test_batch_stream_enqueue", hostname, port));
    httpPut.setHeader(AUTH_HEADER);
    HttpResponse response = httpclient.execute(httpPut);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());

    // Get new consumer id
    HttpPost httpPost = new HttpPost(String.format("http://%s:%d/v2/streams/test_batch_stream_enqueue/consumer-id",
                                                   hostname, port));
    httpPost.setHeader(AUTH_HEADER);
    response = httpclient.execute(httpPost);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals(1, response.getHeaders(Constants.Gateway.HEADER_STREAM_CONSUMER).length);
    String groupId = response.getFirstHeader(Constants.Gateway.HEADER_STREAM_CONSUMER).getValue();
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
      httpPost = new HttpPost(String.format("http://%s:%d/v2/streams/test_batch_stream_enqueue/dequeue", hostname,
                                            port));
      httpPost.setHeader(AUTH_HEADER);
      httpPost.setHeader(Constants.Gateway.HEADER_STREAM_CONSUMER, groupId);
      response = httpclient.execute(httpPost);
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
    public static final int NUM_ELEMENTS = 50; // Should be an even number
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
          HttpPost httpPost = new HttpPost(String.format("http://%s:%d/v2/streams/test_batch_stream_enqueue",
                                                         hostname, port));
          httpPost.setEntity(new StringEntity(Integer.toString(i)));
          httpPost.setHeader("test_batch_stream_enqueue1", Integer.toString(i));
          httpPost.setHeader(AUTH_HEADER);
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

  @Test
  public void testAsyncBatchStreamEnqueueLiimitEventsPerStream() throws Exception {
    configuration.setInt(Constants.Gateway.MAX_CACHED_EVENTS_PER_STREAM_NUM,
                         BatchAsyncEnqueue.NUM_ELEMENTS / 3);
    testAsyncBatchStreamEnqueue();
  }

  @Test
  public void testAsyncBatchStreamEnqueueLimitBytes() throws Exception {
    configuration.setInt(Constants.Gateway.MAX_CACHED_STREAM_EVENTS_BYTES, 1000);
    testAsyncBatchStreamEnqueue();
  }

  @Test
  public void testAsyncBatchStreamEnqueueLimitFlushInterval() throws Exception {
    configuration.setInt(Constants.Gateway.STREAM_EVENTS_FLUSH_INTERVAL_MS, 100);
    testAsyncBatchStreamEnqueue();
  }

  @Test
  public void testAsyncBatchStreamEnqueueLimitNumEvents() throws Exception {
    configuration.setInt(Constants.Gateway.MAX_CACHED_STREAM_EVENTS_NUM, BatchAsyncEnqueue.NUM_ELEMENTS / 3);
    testAsyncBatchStreamEnqueue();
  }

  private void testAsyncBatchStreamEnqueue() throws Exception {
    startGateway();

    final int concurrencyLevel = 6;
    DefaultHttpClient httpclient = new DefaultHttpClient();

    // Create new stream.
    HttpPut httpPut = new HttpPut(String.format("http://%s:%d/v2/streams/test_batch_stream_enqueue", hostname, port));
    httpPut.setHeader(AUTH_HEADER);
    HttpResponse response = httpclient.execute(httpPut);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());

    // Get new consumer id
    HttpPost httpPost = new HttpPost(String.format("http://%s:%d/v2/streams/test_batch_stream_enqueue/consumer-id",
                                                   hostname, port));
    httpPost.setHeader(AUTH_HEADER);
    response = httpclient.execute(httpPost);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals(1, response.getHeaders(Constants.Gateway.HEADER_STREAM_CONSUMER).length);
    String groupId = response.getFirstHeader(Constants.Gateway.HEADER_STREAM_CONSUMER).getValue();
    EntityUtils.consume(response.getEntity());

    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
    List<ListenableFuture<?>> futureList = Lists.newArrayList();

    List<BatchAsyncEnqueue> batchAsyncEnqueues = Lists.newArrayList();
    for (int i = 0; i < concurrencyLevel; ++i) {
      BatchAsyncEnqueue batchAsyncEnqueue = new BatchAsyncEnqueue(i * 1000);
      batchAsyncEnqueues.add(batchAsyncEnqueue);
      futureList.add(executorService.submit(batchAsyncEnqueue));
    }

    Futures.allAsList(futureList).get();
    executorService.shutdown();

    List<Integer> actual = Lists.newArrayList();
    // Dequeue all entries
    for (int i = 0; i < concurrencyLevel * BatchEnqueue.NUM_ELEMENTS; ++i) {
      httpPost
        = new HttpPost(String.format("http://%s:%d/v2/streams/test_batch_stream_enqueue/dequeue", hostname,
                                     port));
      httpPost.setHeader(AUTH_HEADER);
      httpPost.setHeader(Constants.Gateway.HEADER_STREAM_CONSUMER, groupId);
      response = httpclient.execute(httpPost);
      Assert.assertEquals("Failed for entry number " + i,
                          HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
      int entry = Integer.parseInt(EntityUtils.toString(response.getEntity()));
      actual.add(entry);
    }

    List<Integer> expected = Lists.newArrayList();
    for (BatchAsyncEnqueue batchAsyncEnqueue : batchAsyncEnqueues) {
      expected.addAll(batchAsyncEnqueue.expected);
      batchAsyncEnqueue.verify(actual);
    }

    Collections.sort(expected);
    Collections.sort(actual);
    Assert.assertEquals(expected, actual);
  }

  private static class BatchAsyncEnqueue implements Runnable {
    public static final int NUM_ELEMENTS = 50;
    private final int startElement;

    private final BlockingQueue<Integer> expected = Queues.newArrayBlockingQueue(NUM_ELEMENTS);

    private BatchAsyncEnqueue(int startElement) {
      this.startElement = startElement;
    }

    @Override
    public void run() {
      try {
        AsyncHttpClientConfig.Builder configBuilder = new AsyncHttpClientConfig.Builder();

        final AsyncHttpClient asyncHttpClient = new AsyncHttpClient(
          new NettyAsyncHttpProvider(configBuilder.build()),
          configBuilder.build());

        final CountDownLatch latch = new CountDownLatch(NUM_ELEMENTS);
        for (int i = startElement; i < startElement + NUM_ELEMENTS; ++i) {
          final int elem = i;
          final Request request = getPostRequest(Integer.toString(elem));
          asyncHttpClient.executeRequest(request,
                                         new AsyncCompletionHandler<Void>() {
                                           @Override
                                           public Void onCompleted(Response response) throws Exception {
                                             expected.add(elem);
                                             latch.countDown();
                                             Assert.assertEquals(HttpResponseStatus.OK.getCode(),
                                                                 response.getStatusCode());
                                             return null;
                                           }

                                           @Override
                                           public void onThrowable(Throwable t) {
                                             LOG.error("Got exception while posting {}", elem, t);
                                             expected.add(-1);
                                             latch.countDown();
                                           }
                                         });

          // Sleep so as not to overrun the server.
          TimeUnit.MILLISECONDS.sleep(10);
        }
        latch.await();
        asyncHttpClient.close();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    private Request getPostRequest(String body) {
      RequestBuilder requestBuilder = new RequestBuilder("POST");
      return requestBuilder
        .setUrl(String.format("http://%s:%d/v2/streams/test_batch_stream_enqueue", hostname, port))
        .setBody(body)
        .setHeader(AUTH_HEADER.getName(), AUTH_HEADER.getValue())
        .build();

    }

    public void verify(List<Integer> out) {
      List<Integer> actual = Lists.newArrayList();
      for (Integer i : out) {
        if (startElement <= i && i < startElement + NUM_ELEMENTS) {
          actual.add(i);
        }
      }
      List<Integer> expecetedList = Lists.newArrayListWithCapacity(NUM_ELEMENTS);
      expected.drainTo(expecetedList);
      Collections.sort(expecetedList);
      Collections.sort(actual);
      Assert.assertEquals(expecetedList, actual);
    }
  }

  private static void testPing() throws Exception {
    DefaultHttpClient httpclient = new DefaultHttpClient();
    HttpGet httpget = new HttpGet(String.format("http://%s:%d/ping", hostname, port));
    httpget.setHeader(AUTH_HEADER);
    HttpResponse response = httpclient.execute(httpget);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals("OK.\n", EntityUtils.toString(response.getEntity()));
  }
}
