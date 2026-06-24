/*
 * Copyright © 2021-2023 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import com.google.common.util.concurrent.ListenableFuture;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.service.worker.RemoteExecutionException;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.encryption.AeadCipher;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.security.spi.encryption.CipherException;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for RemoteTaskExecutor
 */
public class RemoteTaskExecutorTest {

  private static RemoteClientFactory remoteClientFactory;
  private static CConfiguration cConf;
  private static NettyHttpService httpService;
  private static InMemoryDiscoveryService discoveryService;
  private static AeadCipher mockAeadCipher;
  private static TaskWorkerHttpHandlerInternal taskWorkerHandler;
  Map<Map<String, String>, Map<String, Long>> metricCollectors;
  private MetricsCollectionService mockMetricsCollector;
  private Cancellable registered;

  @BeforeClass
  public static void init() throws Exception {
    cConf = CConfiguration.create();
    discoveryService = new InMemoryDiscoveryService();
    mockAeadCipher = createMockAeadCipher();
    remoteClientFactory = new RemoteClientFactory(discoveryService, new NoOpInternalAuthenticator());
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    taskWorkerHandler = new TaskWorkerHttpHandlerInternal(cConf, discoveryService, discoveryService, className -> {
    }, new NoOpMetricsCollectionService());
    httpService = new CommonNettyHttpServiceBuilder(cConf, "test", new NoOpMetricsCollectionService(), false,
                                                     auditLogContexts -> {}, mockAeadCipher)
      .setHttpHandlers(taskWorkerHandler)
      .setChannelPipelineModifier(new ChannelPipelineModifier() {
        @Override
        public void modify(ChannelPipeline pipeline) {
          pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
        }
      })
      .build();
    cConf.setInt(Constants.ArtifactLocalizer.PORT, -1);
    httpService.start();
  }

  @Before
  public void beforeTest() {
    metricCollectors = new HashMap<>();
    mockMetricsCollector = createMockMetricsCollectionService();
    mockMetricsCollector.startAndWait();
    registered = discoveryService.register(URIScheme.createDiscoverable(Constants.Service.TASK_WORKER, httpService));
  }

  private static AeadCipher createMockAeadCipher() {
    return new AeadCipher() {
      @Override
      public byte[] encrypt(byte[] plainData, byte[] associatedData) throws CipherException {
        return new byte[0];
      }

      @Override
      public byte[] decrypt(byte[] cipherData, byte[] associatedData) throws CipherException {
        return new byte[0];
      }
    };
  }

  private MetricsCollectionService createMockMetricsCollectionService() {
    return new MetricsCollectionService() {

      @Override
      public ListenableFuture<State> start() {
        return null;
      }

      @Override
      public State startAndWait() {
        return null;
      }

      @Override
      public boolean isRunning() {
        return false;
      }

      @Override
      public State state() {
        return null;
      }

      @Override
      public ListenableFuture<State> stop() {
        return null;
      }

      @Override
      public State stopAndWait() {
        return null;
      }

      @Override
      public void addListener(final Listener listener, final Executor executor) {}

      @Override
      public MetricsContext getContext(Map<String, String> context) {
        return new MetricsContext() {
          @Override
          public void increment(String metricName, long value) {
            metricCollectors.putIfAbsent(context, new HashMap<>());
            metricCollectors.get(context).merge(metricName, value, Long::sum);
          }

          @Override
          public void gauge(String metricName, long value) {
            metricCollectors.putIfAbsent(context, new HashMap<>());
            metricCollectors.get(context).put(metricName, value);
          }

          @Override
          public void event(String metricName, long value) {
            // no-op
          }

          @Override
          public MetricsContext childContext(Map<String, String> tags) {
            return this;
          }

          @Override
          public MetricsContext childContext(String tagName, String tagValue) {
            return this;
          }

          @Override
          public Map<String, String> getTags() {
            return Collections.emptyMap();
          }
        };
      }
    };
  }

  @After
  public void afterTest() {
    registered.cancel();
  }

  @AfterClass
  public static void cleanup() throws Exception {
    httpService.stop();
  }

  @Test
  public void testFailedMetrics() throws Exception {
    RemoteTaskExecutor remoteTaskExecutor = new RemoteTaskExecutor(cConf, mockMetricsCollector, remoteClientFactory,
                                                                   RemoteTaskExecutor.Type.TASK_WORKER, mockAeadCipher);
    RunnableTaskRequest runnableTaskRequest = RunnableTaskRequest.getBuilder(InValidRunnableClass.class.getName()).
      withParam("param").withNamespace("testNamespace").build();
    try {
      remoteTaskExecutor.runTask(runnableTaskRequest);
    } catch (RemoteExecutionException e) {
      // Exception thrown in the task executor should be in the exception message in the caller
      Assert.assertEquals("Invalid", e.getMessage());
    }
    mockMetricsCollector.stopAndWait();
    Assert.assertSame(1, metricCollectors.size());

    //check the metrics are present
    Map<String, String> metricKeys = metricCollectors.keySet().iterator().next();
    Map<String, Long> metricValues = metricCollectors.get(metricKeys);
    Assert.assertTrue(hasMetric(metricValues, Constants.Metrics.TaskWorker.CLIENT_REQUEST_LATENCY_MS));
    Assert.assertTrue(hasMetric(metricValues, Constants.Metrics.TaskWorker.CLIENT_REQUEST_COUNT));
    //check the clz tag is set correctly
    Assert.assertEquals(InValidRunnableClass.class.getName(), metricKeys.get("clz"));
  }

  @Test
  public void testSuccessMetrics() throws Exception {
    RemoteTaskExecutor remoteTaskExecutor = new RemoteTaskExecutor(cConf, mockMetricsCollector, remoteClientFactory,
        RemoteTaskExecutor.Type.TASK_WORKER, mockAeadCipher);
    RunnableTaskRequest runnableTaskRequest = RunnableTaskRequest.getBuilder(ValidRunnableClass.class.getName()).
      withParam("param").withNamespace("testNamespace").build();
    remoteTaskExecutor.runTask(runnableTaskRequest);
    mockMetricsCollector.stopAndWait();
    Assert.assertSame(1, metricCollectors.size());

    //check the metrics are present
    Map<String, String> metricsKey = metricCollectors.keySet().iterator().next();
    Map<String, Long> metricsValue = metricCollectors.get(metricsKey);
    Assert.assertTrue(hasMetric(metricsValue, Constants.Metrics.TaskWorker.CLIENT_REQUEST_LATENCY_MS));
    Assert.assertTrue(hasMetric(metricsValue, Constants.Metrics.TaskWorker.CLIENT_REQUEST_COUNT));
    //check the clz tag is set correctly
    Assert.assertEquals(ValidRunnableClass.class.getName(), metricsKey.get("clz"));
  }

  @Test
  public void testRetryMetrics() throws Exception {
    // Remove the service registration
    registered.cancel();
    RemoteTaskExecutor remoteTaskExecutor = new RemoteTaskExecutor(cConf, mockMetricsCollector, remoteClientFactory,
        RemoteTaskExecutor.Type.TASK_WORKER, mockAeadCipher);
    RunnableTaskRequest runnableTaskRequest = RunnableTaskRequest.getBuilder(ValidRunnableClass.class.getName()).
      withParam("param").withNamespace("testNamespace").build();
    try {
      remoteTaskExecutor.runTask(runnableTaskRequest);
    } catch (Exception e) {
      // expected
    }
    mockMetricsCollector.stopAndWait();
    Assert.assertSame(1, metricCollectors.size());

    //check the metrics are present
    Map<String, String> metricsKey = metricCollectors.keySet().iterator().next();
    Map<String, Long> metricsValue = metricCollectors.get(metricsKey);
    Assert.assertTrue(hasMetric(metricsValue, Constants.Metrics.TaskWorker.CLIENT_REQUEST_LATENCY_MS));
    Assert.assertTrue(hasMetric(metricsValue, Constants.Metrics.TaskWorker.CLIENT_REQUEST_COUNT));
    Assert.assertEquals("failure", metricsKey.get(Constants.Metrics.Tag.STATUS));
    int retryCount = Integer.parseInt(metricsKey.get(Constants.Metrics.Tag.TRIES));
    Assert.assertTrue(retryCount > 1);
  }

  private boolean hasMetric(Map<String, Long> metricValues, String metricName) {
    for (String metricValue : metricValues.keySet()) {
      if (metricValue.equals(metricName)) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void testSessionAffinityLeaseEnforcementAndReclamation() throws Exception {
    // Reset the lease manager to idle state to clear any lease left by previous tests
    taskWorkerHandler.getStickyLeaseManager().releaseLease("Reset for integration test");

    // 1. Create a custom CConfiguration where we set the retry policy type to "none"
    CConfiguration customConf = CConfiguration.copy(cConf);
    customConf.set("task.worker.retry.policy.type", "none");

    // 2. Create RemoteTaskExecutor instances using this customConf
    RemoteTaskExecutor executor = new RemoteTaskExecutor(customConf, mockMetricsCollector, remoteClientFactory,
                                                         RemoteTaskExecutor.Type.TASK_WORKER, mockAeadCipher);

    // 3. Namespace "ns_sticky_developer" runs a task. It should succeed and lease the pod.
    RunnableTaskRequest req1 = RunnableTaskRequest.getBuilder(ValidRunnableClass.class.getName())
        .withParam("param").withNamespace("ns_sticky_developer").build();
    byte[] result = executor.runTask(req1);
    Assert.assertEquals("success", new String(result, StandardCharsets.UTF_8));

    // 4. A different namespace "ns_sticky_2" runs a task. It should be rejected due to lease mismatch.
    RunnableTaskRequest req2 = RunnableTaskRequest.getBuilder(ValidRunnableClass.class.getName())
        .withParam("param").withNamespace("ns_sticky_2").build();
    try {
      executor.runTask(req2);
      Assert.fail("Expected namespace mismatch error");
    } catch (Exception e) {
      // Expected rejection with 429 / RetryableException / ServiceException
      Assert.assertTrue("Exception should contain '429' or 'RetryableException' or 'ServiceException', got: " + e,
                        e.toString().contains("429") || e.toString().contains("RetryableException")
                            || e.toString().contains("ServiceException"));
    }

    // 5. Wait 6 seconds for the Developer tier's 5-second inactivity lease reclamation to trigger.
    java.util.concurrent.TimeUnit.SECONDS.sleep(6);

    // 6. Now, "ns_sticky_2" runs a task again. It should succeed because the lease was reclaimed!
    byte[] result2 = executor.runTask(req2);
    Assert.assertEquals("success", new String(result2, StandardCharsets.UTF_8));
  }

  static class ValidRunnableClass implements RunnableTask {
    @Override
    public void run(RunnableTaskContext context) throws Exception {
      context.writeResult("success".getBytes(StandardCharsets.UTF_8));
    }
  }

  static class InValidRunnableClass implements RunnableTask {
    @Override
    public void run(RunnableTaskContext context) throws Exception {
      throw new RuntimeException("Invalid");
    }
  }
}
