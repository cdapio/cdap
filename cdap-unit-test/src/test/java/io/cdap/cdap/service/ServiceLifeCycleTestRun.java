/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.service;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.metrics.MetricDataQuery;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.service.http.AbstractServiceHttpServer;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.app.ServiceLifecycleApp;
import io.cdap.cdap.test.base.TestFrameworkTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for testing service handler lifecycle.
 */
public class ServiceLifeCycleTestRun extends TestFrameworkTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration();

  private static final Gson GSON = new Gson();
  private static final Type STATES_TYPE = new TypeToken<List<ImmutablePair<Integer, String>>>() { }.getType();
  private static File artifactJar;
  private ServiceManager serviceManager;

  @BeforeClass
  public static void init() throws IOException {
    artifactJar = createArtifactJar(ServiceLifecycleApp.class);
  }

  @Test
  public void testLifecycleWithThreadTerminates() throws Exception {
    // Set the http server properties to speed up test
    System.setProperty(AbstractServiceHttpServer.HANDLER_CLEANUP_PERIOD_MILLIS, "100");

    try {
      ApplicationManager appManager = deployWithArtifact(ServiceLifecycleApp.class, artifactJar);

      serviceManager = appManager.getServiceManager("test")
        .start(ImmutableMap.of(SystemArguments.SERVICE_THREADS, "1",
                               SystemArguments.SERVICE_THREAD_KEEPALIVE_SECS, "1"));

      // Make a call to the service, expect an init state
      Multimap<Integer, String> states = getStates(serviceManager);
      Assert.assertEquals(1, states.size());
      int handlerHashCode = states.keySet().iterator().next();
      Assert.assertEquals(ImmutableList.of("INIT"), ImmutableList.copyOf(states.get(handlerHashCode)));

      // Sleep for 3 seconds for the thread going IDLE, gets terminated and cleanup
      TimeUnit.SECONDS.sleep(3);

      states = getStates(serviceManager);

      // Size of states keys should be two, since the old instance must get destroy and there is a new
      // one created to handle the getStates request.
      Assert.assertEquals(2, states.keySet().size());

      // For the state changes for the old handler, it should have INIT, DESTROY
      Assert.assertEquals(ImmutableList.of("INIT", "DESTROY"), ImmutableList.copyOf(states.get(handlerHashCode)));

      // For the state changes for the new handler, it should be INIT
      for (int key : states.keys()) {
        if (key != handlerHashCode) {
          Assert.assertEquals(ImmutableList.of("INIT"), ImmutableList.copyOf(states.get(key)));
        }
      }
    } finally {
      serviceManager.stop();
      serviceManager.waitForStopped(10, TimeUnit.SECONDS);
      // Reset the http server properties to speed up test
      System.clearProperty(AbstractServiceHttpServer.HANDLER_CLEANUP_PERIOD_MILLIS);
    }
  }

  @Test
  public void testLifecycleWithGC() throws Exception {
    // Set the http server properties to speed up test
    System.setProperty(AbstractServiceHttpServer.HANDLER_CLEANUP_PERIOD_MILLIS, "100");

    try {
      ApplicationManager appManager = deployWithArtifact(ServiceLifecycleApp.class, artifactJar);

      serviceManager = appManager.getServiceManager("test")
        .start(ImmutableMap.of(SystemArguments.SERVICE_THREADS, "1",
                               SystemArguments.SERVICE_THREAD_KEEPALIVE_SECS, "1"));

      // Make 5 consecutive calls, there should be one handler instance being created,
      // since there is only one handler thread.
      Multimap<Integer, String> states = null;
      for (int i = 0; i < 5; i++) {
        states = getStates(serviceManager);

        // There should only be one instance created
        Assert.assertEquals(1, states.size());

        // For the instance, there should only be INIT state.
        Assert.assertEquals(ImmutableList.of("INIT"),
                            ImmutableList.copyOf(states.get(states.keySet().iterator().next())));
      }

      // Capture the current state
      final Multimap<Integer, String> lastStates = states;


      // TTL for the thread is 1 second, hence sleep for 2 second to make sure the thread is gone
      TimeUnit.SECONDS.sleep(2);

      Tasks.waitFor(true, () -> {
        // Force a gc to have weak references cleanup
        System.gc();
        Multimap<Integer, String> newStates = getStates(serviceManager);

        // Should expect size be 3. An INIT and a DESTROY from the collected handler
        // and an INIT for the new handler that just handle the getState call
        if (newStates.size() != 3) {
          return false;
        }

        // A INIT and a DESTROY is expected for the old handler
        return ImmutableList.of("INIT", "DESTROY")
          .equals(ImmutableList.copyOf(newStates.get(lastStates.keySet().iterator().next())));
      }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    } finally {
      serviceManager.stop();
      serviceManager.waitForStopped(10, TimeUnit.SECONDS);
      // Reset the http server properties to speed up test
      System.clearProperty(AbstractServiceHttpServer.HANDLER_CLEANUP_PERIOD_MILLIS);
    }
  }

  @Test
  public void testContentConsumerLifecycle() throws Exception {
    try {
      ApplicationManager appManager = deployWithArtifact(ServiceLifecycleApp.class, artifactJar);

      // Set to have one thread only for testing context capture and release
      serviceManager = appManager.getServiceManager("test")
        .start(ImmutableMap.of(SystemArguments.SERVICE_THREADS, "1"));
      CountDownLatch uploadLatch = new CountDownLatch(1);

      // Create five concurrent upload
      List<ListenableFuture<Integer>> completions = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        completions.add(slowUpload(serviceManager, "PUT", "upload", uploadLatch));
      }

      // Get the states, there should be six handler instances initialized.
      // Five for the in-progress upload, one for the getStates call
      Tasks.waitFor(6, () -> getStates(serviceManager).size(), 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // Finish the upload
      uploadLatch.countDown();
      Futures.successfulAsList(completions).get(10, TimeUnit.SECONDS);

      // Verify the result
      for (ListenableFuture<Integer> future : completions) {
        Assert.assertEquals(200, future.get().intValue());
      }

      // Get the states, there should still be six handler instances initialized.
      final Multimap<Integer, String> states = getStates(serviceManager);
      Assert.assertEquals(6, states.size());

      // Do another round of six concurrent upload. It should reuse all of the existing six contexts
      completions.clear();
      uploadLatch = new CountDownLatch(1);
      for (int i = 0; i < 6; i++) {
        completions.add(slowUpload(serviceManager, "PUT", "upload", uploadLatch));
      }

      // Get the states, there should be seven handler instances initialized.
      // Six for the in-progress upload, one for the getStates call
      // Out of the 7 states, six of them should be the same as the old one
      Tasks.waitFor(true, () -> {
        Multimap<Integer, String> newStates = getStates(serviceManager);
        if (newStates.size() != 7) {
          return false;
        }

        for (Map.Entry<Integer, String> entry : states.entries()) {
          if (!newStates.containsEntry(entry.getKey(), entry.getValue())) {
            return false;
          }
        }

        return true;
      }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // Complete the upload
      uploadLatch.countDown();
      Futures.successfulAsList(completions).get(10, TimeUnit.SECONDS);

      // Verify the result
      for (ListenableFuture<Integer> future : completions) {
        Assert.assertEquals(200, future.get().intValue());
      }

      // Query the queue size metrics. Expect the maximum be 6.
      // This is because only the six from the concurrent upload will get captured added back to the queue,
      // while the one created for the getState() call will be stated in the thread cache, but not in the queue.
      Tasks.waitFor(6L, () -> {
        Map<String, String> context = ImmutableMap.of(
          Constants.Metrics.Tag.NAMESPACE, NamespaceId.DEFAULT.getNamespace(),
            Constants.Metrics.Tag.APP, ServiceLifecycleApp.class.getSimpleName(),
            Constants.Metrics.Tag.SERVICE, "test");
        MetricDataQuery metricQuery = new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE,
                                                          "system.context.pool.size", AggregationFunction.MAX,
                                                          context, Collections.emptyList());
        Iterator<MetricTimeSeries> result = getMetricsManager().query(metricQuery).iterator();
        return result.hasNext() ? result.next().getTimeValues().get(0).getValue() : 0L;
      }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    } finally {
      serviceManager.stop();
      serviceManager.waitForStopped(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testContentProducerLifecycle() throws Exception {
    try {
      ApplicationManager appManager = deployWithArtifact(ServiceLifecycleApp.class, artifactJar);

      // Set to have one thread only for testing context capture and release
      serviceManager = appManager.getServiceManager("test")
        .start(ImmutableMap.of(SystemArguments.SERVICE_THREADS, "1"));
      final DataSetManager<KeyValueTable> datasetManager = getDataset(ServiceLifecycleApp.HANDLER_TABLE_NAME);
      // Clean up the dataset first to avoid being affected by other tests
      datasetManager.get().delete(Bytes.toBytes("called"));
      datasetManager.get().delete(Bytes.toBytes("completed"));
      datasetManager.flush();

      // Starts 5 concurrent downloads
      List<ListenableFuture<String>> completions = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        completions.add(download(serviceManager));
      }

      // Make sure all producers has produced something
      Tasks.waitFor(true, () -> {
        byte[] value = datasetManager.get().read("called");
        datasetManager.flush();
        return value != null && value.length == Bytes.SIZEOF_LONG && Bytes.toLong(value) > 5;
      }, 10L , TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // Get the states, there should be 6 handler instances instantiated, 5 the downloads, one for getState.
      Multimap<Integer, String> states = getStates(serviceManager);
      Assert.assertEquals(6, states.size());

      // Set the complete flag in the dataset
      datasetManager.get().write("completed", Bytes.toBytes(true));
      datasetManager.flush();

      // Wait for download to complete
      Futures.allAsList(completions).get(10L, TimeUnit.SECONDS);

      // Get the states again, it should still be 6 same instances
      Assert.assertEquals(states, getStates(serviceManager));

    } finally {
      serviceManager.stop();
      serviceManager.waitForStopped(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testContentConsumerProducerLifecycle() throws Exception {
    try {
      ApplicationManager appManager = deployWithArtifact(ServiceLifecycleApp.class, artifactJar);

      // Set to have one thread only for testing context capture and release
      serviceManager = appManager.getServiceManager("test")
        .start(ImmutableMap.of(SystemArguments.SERVICE_THREADS, "1"));
      final DataSetManager<KeyValueTable> datasetManager = getDataset(ServiceLifecycleApp.HANDLER_TABLE_NAME);
      // Clean up the dataset first to avoid being affected by other tests
      datasetManager.get().delete(Bytes.toBytes("called"));
      datasetManager.get().delete(Bytes.toBytes("completed"));
      datasetManager.flush();

      CountDownLatch uploadLatch = new CountDownLatch(1);

      // Create five concurrent upload
      List<ListenableFuture<Integer>> completions = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        completions.add(slowUpload(serviceManager, "POST", "uploadDownload", uploadLatch));
      }

      // Get the states, there should be six handler instances initialized.
      // Five for the in-progress upload, one for the getStates call
      Tasks.waitFor(6, () -> getStates(serviceManager).size(), 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // Complete the upload
      uploadLatch.countDown();

      // Make sure the download through content producer has started
      Tasks.waitFor(true, () -> {
        byte[] value = datasetManager.get().read("called");
        datasetManager.flush();
        return value != null && value.length == Bytes.SIZEOF_LONG && Bytes.toLong(value) > 5;
      }, 10L , TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // Get the states, there should still be six handler instances since the ContentConsumer should
      // be passing it's captured context to the ContentProducer without creating new one.
      Multimap<Integer, String> states = getStates(serviceManager);
      Assert.assertEquals(6, states.size());

      // Set the complete flag in the dataset
      datasetManager.get().write("completed", Bytes.toBytes(true));
      datasetManager.flush();

      // Wait for completion
      Futures.successfulAsList(completions).get(10, TimeUnit.SECONDS);

      // Verify the upload result
      for (ListenableFuture<Integer> future : completions) {
        Assert.assertEquals(200, future.get().intValue());
      }

      // Get the states again, it should still be 6 same instances
      Assert.assertEquals(states, getStates(serviceManager));

    } finally {
      serviceManager.stop();
      serviceManager.waitForStopped(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testInvalidResponder() throws Exception {
    ApplicationManager appManager = deployWithArtifact(ServiceLifecycleApp.class, artifactJar);
    serviceManager = appManager.getServiceManager("test").start();

    CountDownLatch uploadLatch = new CountDownLatch(1);
    ListenableFuture<Integer> completion = slowUpload(serviceManager, "PUT", "invalid", uploadLatch);

    uploadLatch.countDown();
    Assert.assertEquals(500, completion.get().intValue());
    serviceManager.stop();
    serviceManager.waitForStopped(10, TimeUnit.SECONDS);
  }

  @Test
  public void testInvalidContentProducer() throws Exception {
    ApplicationManager appManager = deployWithArtifact(ServiceLifecycleApp.class, artifactJar);
    serviceManager = appManager.getServiceManager("test").start();

    URL serviceURL = serviceManager.getServiceURL(10, TimeUnit.SECONDS);
    URL url = serviceURL.toURI().resolve("invalid?methods=getContentLength").toURL();
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    try {
      Assert.assertEquals(500, urlConn.getResponseCode());
    } finally {
      urlConn.disconnect();
    }

    // Exception from both nextChunk and onError
    url = serviceURL.toURI().resolve("invalid?methods=nextChunk&methods=onError").toURL();
    urlConn = (HttpURLConnection) url.openConnection();
    try {
      // 200 will be the status code
      Assert.assertEquals(200, urlConn.getResponseCode());
      // Expect IOException when trying to read since the server closed the connection
      try {
        ByteStreams.toByteArray(urlConn.getInputStream());
        Assert.fail("Expected IOException");
      } catch (IOException e) {
        // expected
      }
    } finally {
      urlConn.disconnect();
    }

    // Exception from both onFinish
    url = serviceURL.toURI().resolve("invalid?methods=onFinish").toURL();
    urlConn = (HttpURLConnection) url.openConnection();
    try {
      // 200 will be the status code. Since the response is completed, from the client perspective, there is no error.
      Assert.assertEquals(200, urlConn.getResponseCode());
      Assert.assertEquals("0123456789", new String(ByteStreams.toByteArray(urlConn.getInputStream()),
                                                   StandardCharsets.UTF_8));
    } finally {
      serviceManager.stop();
      serviceManager.waitForStopped(10, TimeUnit.SECONDS);
      urlConn.disconnect();
    }
  }

  /**
   * Returns the handler state change as a Multimap. The key is the hashcode of the handler instance,
   * the value is a list of state changes for that handler instance.
   */
  private Multimap<Integer, String> getStates(ServiceManager serviceManager) throws Exception {
    URL url = serviceManager.getServiceURL(10, TimeUnit.SECONDS).toURI().resolve("states").toURL();

    Multimap<Integer, String> result = LinkedListMultimap.create();
    try (InputStream is = url.openConnection().getInputStream()) {
      List<ImmutablePair<Integer, String>> states = GSON.fromJson(new InputStreamReader(is, Charsets.UTF_8),
                                                                  STATES_TYPE);
      for (ImmutablePair<Integer, String> pair : states) {
        result.put(pair.getFirst(), pair.getSecond());
      }
    }
    return result;
  }

  private ListenableFuture<Integer> slowUpload(final ServiceManager serviceManager,
                                               final String method,
                                               final String endpoint,
                                               final CountDownLatch latch) throws Exception {
    final SettableFuture<Integer> completion = SettableFuture.create();
    Thread t = new Thread(() -> {
      try {
        URL url = serviceManager.getServiceURL(10, TimeUnit.SECONDS).toURI().resolve(endpoint).toURL();
        HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
        try {
          urlConn.setChunkedStreamingMode(5);
          urlConn.setDoOutput(true);
          urlConn.setRequestMethod(method);

          try (OutputStream os = urlConn.getOutputStream()) {
            // Write some chunks
            os.write("Testing".getBytes(Charsets.UTF_8));
            os.flush();
            latch.await();
          }

          int sc = urlConn.getResponseCode();
          if (sc == 200) {
            ByteStreams.toByteArray(urlConn.getInputStream());
          } else {
            ByteStreams.toByteArray(urlConn.getErrorStream());
          }
          completion.set(sc);

        } finally {
          urlConn.disconnect();
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    });
    t.start();

    return completion;
  }

  private ListenableFuture<String> download(final ServiceManager serviceManager) {
    final SettableFuture<String> completion = SettableFuture.create();
    Thread t = new Thread(() -> {
      try {
        URL url = serviceManager.getServiceURL(10, TimeUnit.SECONDS).toURI().resolve("download").toURL();
        HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
        try {
          completion.set(new String(ByteStreams.toByteArray(urlConn.getInputStream()), Charsets.UTF_8));
        } finally {
          urlConn.disconnect();
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    });
    t.start();
    return completion;
  }
}
