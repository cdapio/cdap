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
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
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
  Map<Map<String, String>, Map<String, Long>> metricCollectors;
  private MetricsCollectionService mockMetricsCollector;
  private Cancellable registered;

  @BeforeClass
  public static void init() throws Exception {
    cConf = CConfiguration.create();
    discoveryService = new InMemoryDiscoveryService();
    remoteClientFactory = new RemoteClientFactory(discoveryService, new NoOpInternalAuthenticator());
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    httpService = new CommonNettyHttpServiceBuilder(cConf, "test", new NoOpMetricsCollectionService(), null)
      .setHttpHandlers(
        new TaskWorkerHttpHandlerInternal(cConf, discoveryService, discoveryService, className -> {
        }, new NoOpMetricsCollectionService())
      )
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
                                                                   RemoteTaskExecutor.Type.TASK_WORKER);
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
        RemoteTaskExecutor.Type.TASK_WORKER);
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
        RemoteTaskExecutor.Type.TASK_WORKER);
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
