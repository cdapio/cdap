/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.metrics.collect.AggregatedMetricsCollectionService;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Tests for TaskWorker Metrics
 */
public class TaskWorkerMetricsTest {

  private static final Gson GSON = new Gson();

  private TaskWorkerService taskWorkerService;
  private List<MetricValues> published;
  private URI uri;
  private CompletableFuture<Service.State> taskWorkerStateFuture;

  private CConfiguration createCConf() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorker.ADDRESS, "localhost");
    cConf.setInt(Constants.TaskWorker.PORT, 0);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);
    cConf.set(Constants.TaskWorker.PRELOAD_ARTIFACTS, "");
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 1);
    return cConf;
  }

  @Before
  public void beforeTest() {
    CConfiguration cConf = createCConf();
    SConfiguration sConf = SConfiguration.create();
    published = new ArrayList<>();

    AggregatedMetricsCollectionService mockMetricsCollector = new AggregatedMetricsCollectionService(1000L) {
      @Override
      protected void publish(Iterator<MetricValues> metrics) {
        Iterators.addAll(published, metrics);
      }
    };
    mockMetricsCollector.startAndWait();
    taskWorkerService = new TaskWorkerService(cConf, sConf, new InMemoryDiscoveryService(),
                                                                (namespaceId, retryStrategy) -> null,
                                                                mockMetricsCollector);
    taskWorkerStateFuture = TaskWorkerTestUtil.getServiceCompletionFuture(taskWorkerService);
    // start the service
    taskWorkerService.startAndWait();
    InetSocketAddress addr = taskWorkerService.getBindAddress();
    this.uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));
  }

  @After
  public void afterTest() {
    if (taskWorkerService != null) {
      taskWorkerService.stopAndWait();
    }
  }

  @Test
  public void testSimpleRequest() throws IOException {
    String taskClassName = TaskWorkerServiceTest.TestRunnableClass.class.getName();
    RunnableTaskRequest req = RunnableTaskRequest.getBuilder(taskClassName)
      .withParam("100")
      .build();
    String reqBody = GSON.toJson(req);
    HttpResponse response = HttpRequests.execute(
      HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
        .withBody(reqBody).build(),
      new DefaultHttpRequestConfig(false));
    TaskWorkerTestUtil.waitForServiceCompletion(taskWorkerStateFuture);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertSame(1, published.size());

    //check the metrics are present
    MetricValues metricValues = published.get(0);
    Assert.assertTrue(hasMetric(metricValues, Constants.Metrics.TaskWorker.REQUEST_LATENCY_MS));
    Assert.assertTrue(hasMetric(metricValues, Constants.Metrics.TaskWorker.REQUEST_COUNT));
    //check the clz tag is set correctly
    Assert.assertEquals(taskClassName, metricValues.getTags().get("clz"));
  }

  @Test
  public void testWrappedRequest() throws IOException {
    String taskClassName = TaskWorkerServiceTest.TestRunnableClass.class.getName();
    String wrappedClassName = "testClassName";
    RunnableTaskRequest req = RunnableTaskRequest.getBuilder(
      taskClassName).withParam("100")
      .withEmbeddedTaskRequest(RunnableTaskRequest.getBuilder(wrappedClassName).build()).build();
    String reqBody = GSON.toJson(req);
    HttpResponse response = HttpRequests.execute(
      HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
        .withBody(reqBody).build(),
      new DefaultHttpRequestConfig(false));
    TaskWorkerTestUtil.waitForServiceCompletion(taskWorkerStateFuture);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertSame(1, published.size());

    //check the metrics are present
    MetricValues metricValues = published.get(0);
    Assert.assertTrue(hasMetric(metricValues, Constants.Metrics.TaskWorker.REQUEST_COUNT));
    Assert.assertTrue(hasMetric(metricValues, Constants.Metrics.TaskWorker.REQUEST_LATENCY_MS));
    //check the clz tag is set correctly
    Assert.assertEquals(wrappedClassName, metricValues.getTags().get(Constants.Metrics.Tag.CLASS));
  }

  private boolean hasMetric(MetricValues metricValues, String metricName) {
    for (MetricValue metricValue : metricValues.getMetrics()) {
      if (metricValue.getName().equals(metricName)) {
        return true;
      }
    }
    return false;
  }

  public static class TestFailingRunnableClass implements RunnableTask {
    @Override
    public void run(RunnableTaskContext context) throws Exception {
      throw new RuntimeException("unexpected error");
    }
  }
}
