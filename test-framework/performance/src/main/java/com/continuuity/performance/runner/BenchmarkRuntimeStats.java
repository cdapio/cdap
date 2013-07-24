/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.runner;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.performance.application.BenchmarkRuntimeMetrics;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.ning.http.client.Response;
import com.ning.http.client.SimpleAsyncHttpClient;
import com.ning.http.client.generators.InputStreamBodyGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runtime statistics of Reactor App during a performance test.
 */
public final class BenchmarkRuntimeStats {

  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkRuntimeStats.class);

  private static final String FLOWLET_EVENTS = "/process/events/{appId}/flows/{flowId}/{flowletId}?aggregate=true";
  private static final String FLOWLET_INPUTS = "/process/events/{appId}/flows/{flowId}/{flowletId}/ins?aggregate=true";

  private static final MetricsClient metricsClient = getMetricsClient();

  /**
   * Timeout to get response from metrics system.
   */
  private static final long METRICS_SERVER_RESPONSE_TIMEOUT = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  private static final int REQUEST_TIMEOUT = 1000;

  // Read runtime metric counters for a given flowlet.
  public static BenchmarkRuntimeMetrics getFlowletMetrics(final String applicationId,
                                                          final String flowId, final String flowletId) {
    final Metric aggregatedFlowletInputs = new Metric(FLOWLET_INPUTS,
                                                      ImmutableMap.of(Metric.Parameter.APPLICATION_ID, applicationId,
                                                      Metric.Parameter.FLOW_ID, flowId,
                                                      Metric.Parameter.FLOWLET_ID, flowletId));
    final Metric aggregatedFlowletEvents = new Metric(FLOWLET_EVENTS,
                                                      ImmutableMap.of(Metric.Parameter.APPLICATION_ID, applicationId,
                                                      Metric.Parameter.FLOW_ID, flowId,
                                                      Metric.Parameter.FLOWLET_ID, flowletId));

    return new BenchmarkRuntimeMetrics() {
      @Override
      public long getInput() {
        Counter counter = getCounter(aggregatedFlowletInputs);
        if (counter == null) {
          return 0L;
        } else {
          return counter.getValue();
        }
      }

      @Override
      public long getProcessed() {
        Counter counter = getCounter(aggregatedFlowletEvents);
        if (counter == null) {
          return 0L;
        } else {
          return counter.getValue();
        }
      }

      @Override
      public void waitForinput(long count, long timeout, TimeUnit timeoutUnit)
        throws TimeoutException, InterruptedException {
        waitFor(aggregatedFlowletEvents, count, timeout, timeoutUnit);
      }

      @Override
      public void waitForProcessed(long count, long timeout, TimeUnit timeoutUnit)
        throws TimeoutException, InterruptedException {
        waitFor(aggregatedFlowletEvents, count, timeout,
                timeoutUnit);
      }

      // Waits until metrics counter has reached given count number.
      private void waitFor(Metric metric, long count, long timeout, TimeUnit timeoutUnit)
        throws TimeoutException, InterruptedException {
        Long value = getCounter(metric).getValue();
        while (timeout > 0 && (value == null || value.longValue() < count)) {
          timeoutUnit.sleep(1);
          value = getCounter(metric).getValue();
          timeout--;
        }
        if (timeout == 0 && (value == null || value.longValue() < count)) {
          throw new TimeoutException("Time limit reached.");
        }
      }

      @Override
      public String toString() {
        return String.format("%s; input=%d, processed=%d, exception=%d", flowletId, getInput(), getProcessed());
      }
    };
  }

  /**
   * Waits until metrics counter for flowlet has reached the given count number.
   * @param pattern Counter pattern
   * @param type Metrics type {@link Metric.Type}
   * @param applicationId Application id
   * @param flowId Flow id
   * @param flowletId Flowlet id
   * @param count Count to wait for
   * @param timeout Maximum time to wait for
   * @param timeoutUnit {@link TimeUnit} for the timeout time.
   * @throws TimeoutException if the timeout time passed and still not seeing that many counts.
   */
  @SuppressWarnings(value = "unused")
  public static void waitForCounter(String pattern, Metric.Type type, String applicationId,
                                    String flowId, String flowletId, long count, long timeout, TimeUnit timeoutUnit)
    throws TimeoutException, InterruptedException {

    Counter c = getCounter(pattern, type, new String[] {applicationId, flowId, flowletId});
    if (c == null) {
      throw new RuntimeException("No counter with pattern " + pattern + " found for application " + applicationId
                                   + " ,flow " + flowId + " and flowlet " + flowletId + ".");
    }
    Long value = c.getValue();
    while (timeout > 0 && (value < count)) {
      timeoutUnit.sleep(1);
      value = getCounter(pattern, type, new String[] {applicationId, flowId, flowletId}).getValue();
      timeout--;
    }
    if (timeout == 0 && (value < count)) {
      throw new TimeoutException("Time limit reached.");
    }
  }

  /**
   * Waits until metrics counter has reached the given count number.
   * @param counterPath Counter path
   * @param count Count to wait for
   * @param timeout Maximum time to wait for
   * @param timeoutUnit {@link TimeUnit} for the timeout time.
   * @throws TimeoutException if the timeout time passed and still not seeing that many count.
   */
  @SuppressWarnings(value = "unused")
  public static void waitForCounter(String counterPath, long count, long timeout, TimeUnit timeoutUnit)
    throws TimeoutException, InterruptedException {

    Counter c = getCounter(counterPath);
    if (c == null) {
      throw new RuntimeException("No counter with path " + counterPath + " found.");
    }
    long value = c.getValue();
    while (timeout > 0 && (value < count)) {
      timeoutUnit.sleep(1);
      value = getCounter(counterPath).getValue();
      timeout--;
    }
    if (timeout == 0 && (value < count)) {
      throw new TimeoutException("Time limit reached.");
    }
  }

  /**
   * Gets metrics counter object for a given counter path.
   * @param counterPath Full path of Counter
   * @return Counter
   */
  public static Counter getCounter(String counterPath) {
    return metricsClient.getCounter(counterPath);
  }

  /**
   * Gets metrics counter object for a given counter path.
   * @param metric Full path of Counter
   * @return Counter
   */
  public static Counter getCounter(Metric metric) {
    return metricsClient.getCounter(metric);
  }

  public static Counter getCounter(String pattern, Metric.Type type, String applicationId, String flowId,
                                   String flowletId) {
    return metricsClient.getCounter(pattern, type, new String[] {applicationId, flowId, flowletId});
  }

  public static Counter getCounter(String pattern, Metric.Type type, String[] params) {
    return metricsClient.getCounter(pattern, type, params);
  }

  private static MetricsClient getMetricsClient() {
    CConfiguration config = CConfiguration.create();

    String hostname = config.get(MetricsConstants.ConfigKeys.SERVER_ADDRESS, "localhost");
    int port = config.getInt(MetricsConstants.ConfigKeys.SERVER_PORT, 45005);
    return new MetricsClient(hostname, port);
  }

  /**
   * Counter class.
   */
  public static class Counter {
    private final String path;
    private final long value;

    public Counter(String path, long value) {
      this.path = path;
      this.value = value;
    }
    public String getPath() {
      return path;
    }
    public long getValue() {
      return value;
    }
  }

  static class MetricsClient {

    private static final Gson GSON = new Gson();

    private final String hostname;
    private final int port;
    private final String url;

    MetricsClient(String hostname, int port) {
      this.hostname = hostname;
      this.port = port;
      this.url = String.format("http://%s:%d/metrics", hostname, port);
    }

    public Counter getCounter(Metric metric) {

      if (metric.getType() == Metric.Type.AGGREGATE) {
        String json = "[ \"" + metric.getPath() + "\" ]";
        String response = sendJSonPostRequest(url, json, null);
        List<MetricsResponse> responseData = GSON.fromJson(response,
                                                           new TypeToken<Collection<MetricsResponse>>(){}.getType());
        if (responseData == null || responseData.size() == 0) {
          throw new RuntimeException(String.format("No metric received from metrics system for request %s",
                                                   metric.getPath()));
        }
        if (responseData.size() > 1) {
          throw new RuntimeException(String.format("More than one metric received from metrics system for request %s",
                                                   metric.getPath()));
        }
        return new Counter(responseData.get(0).getPath(), responseData.get(0).getResult().getData());
      } else {
        throw new RuntimeException(String.format("Metrics request type %s is not supported.", metric.getType()));
      }
    }

    public Counter getCounter(String pattern, Metric.Type type, String[] params) {
      if (type == Metric.Type.AGGREGATE) {
        String json = "[ \"" + String.format(pattern, (Object[]) params) + "\" ]";
        String response = sendJSonPostRequest(url, json, null);
        List<MetricsResponse> responseData = GSON.fromJson(response,
                                                           new TypeToken<Collection<MetricsResponse>>(){}.getType());
        if (responseData == null || responseData.size() == 0) {
          throw new RuntimeException(String.format("No metric received from metrics system for request %s",
                                                   String.format(pattern, (Object[]) params)));
        }
        if (responseData.size() > 1) {
          throw new RuntimeException(String.format("More than one metric received from metrics system for request %s",
                                                   String.format(pattern, (Object[]) params)));
        }
        return new Counter(responseData.get(0).getPath(), responseData.get(0).getResult().getData());
      } else {
        throw new RuntimeException(String.format("Metrics request type %s is not supported.", type.name()));
      }
    }

    public Counter getCounter(String counterPath) {
      String json = "[ \"" + counterPath + "\" ]";
      LOG.info(" JSON:" + json);
      String response = sendJSonPostRequest(String.format("http://%s:%d/metrics", hostname, port), json, null);
      List<MetricsResponse> responseData = GSON.fromJson(response,
                                                         new TypeToken<Collection<MetricsResponse>>(){}.getType());

      if (responseData == null || responseData.size() == 0) {
        throw new RuntimeException(String.format("No metric received from metrics system for request %s", counterPath));
      }
      if (responseData.size() > 1) {
        throw new RuntimeException(String.format("More than one metric received from metrics system for request %s",
                                                 counterPath));
      }
      return new Counter(responseData.get(0).getPath(), responseData.get(0).getResult().getData());
    }

    static final class MetricsResponse {
      private final String path;
      private final AggregateResult result;

      MetricsResponse(String path, AggregateResult result) {
        this.path = path;
        this.result = result;
      }
      public String getPath() {
        return path;
      }
      public AggregateResult getResult() {
        return result;
      }
    }
    static final class AggregateResult {
      private final long data;
      AggregateResult(long data) {
        this.data = data;
      }
      public long getData() {
        return data;
      }
    }
  }

  private static String sendJSonPostRequest(String url, String json, Map<String, String> headers) {
    SimpleAsyncHttpClient.Builder builder = new SimpleAsyncHttpClient.Builder()
      .setUrl(url)
      .setRequestTimeoutInMs((int) METRICS_SERVER_RESPONSE_TIMEOUT)
      .setHeader("Content-Type", "application/json");
    if (headers != null) {
      for (Map.Entry<String, String> header: headers.entrySet()) {
        builder.addHeader(header.getKey(), header.getValue());
      }
    }
    SimpleAsyncHttpClient asyncClient = builder.build();

    try {
      Future<Response> future =
        asyncClient.post(new InputStreamBodyGenerator(new ByteArrayInputStream(json.getBytes())));
      Response response = future.get(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
      String body =  response.getResponseBody();
      if (response.getStatusCode() != 200) {
        throw new RuntimeException(body);
      }
      return body;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    } catch (TimeoutException e) {
      throw Throwables.propagate(e);
    } finally {
      asyncClient.close();
    }
  }
}

