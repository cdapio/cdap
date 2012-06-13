package com.continuuity.metrics;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.ServiceDiscoveryClientException;
import com.continuuity.metrics.service.MetricsClient;
import com.continuuity.metrics.stubs.FlowMetric;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class FlowMetricsReporter extends AbstractPollingReporter implements MetricProcessor<String> {
  private static final Logger Log = LoggerFactory.getLogger(FlowMetricsReporter.class);
  private final MetricPredicate predicate;
  private MetricsClient client;
  private boolean hasConnected = false;

  /**
   * Enables the console reporter for the default metrics registry, and causes it to print to
   * STDOUT with the specified period.
   *
   * @param period the period between successive outputs
   * @param unit   the time unit of {@code period}
   */
  public static void enable(long period, TimeUnit unit) {
    enable(Metrics.defaultRegistry(), period, unit, null);
  }

  /**
   * Enables the console reporter for the default metrics registry, and causes it to print to
   * STDOUT with the specified period.
   *
   * @param period the period between successive outputs
   * @param unit   the time unit of {@code period}
   */
  public static void enable(long period, TimeUnit unit, CConfiguration configuraton) {
    enable(Metrics.defaultRegistry(), period, unit, configuraton);
  }

  /**
   * Enables the console reporter for the given metrics registry, and causes it to print to STDOUT
   * with the specified period and unrestricted output.
   *
   * @param metricsRegistry the metrics registry
   * @param period          the period between successive outputs
   * @param unit            the time unit of {@code period}
   */
  public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, CConfiguration configuration) {
    final FlowMetricsReporter reporter = new FlowMetricsReporter(metricsRegistry, configuration);
    reporter.start(period, unit);
  }

  /**
   * @param metricsRegistry
   */
  public FlowMetricsReporter(MetricsRegistry metricsRegistry, CConfiguration configuration) {
    super(Metrics.defaultRegistry(), "flow-monitor-reporter");
    this.predicate = MetricPredicate.ALL;
    try {
      if(configuration == null) {
        int port = Integer.parseInt(Constants.DEFAULT_FLOW_MONITOR_SERVER_PORT);
        this.client = new MetricsClient("localhost", port );
      } else {
        this.client = new MetricsClient(configuration);
      }
      hasConnected = true;
    } catch (ServiceDiscoveryClientException e) {
      Log.error("Unable to connect to flow monitor. Reason : {}.", e.getMessage());
    }
  }

  /**
   *
   */
  public FlowMetricsReporter(CConfiguration configuration) {
    this(Metrics.defaultRegistry(), configuration);
  }

  /**
   * The method called when a a poll is scheduled to occur.
   */
  @Override
  public void run() {
    if (!hasConnected) {
      return;
    }
    for (Map.Entry<MetricName, Metric> entry : getMetricsRegistry().allMetrics().entrySet()) {
      Metric metric = entry.getValue();
      try {
        metric.processWith(this, entry.getKey(), null);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Process the given {@link com.yammer.metrics.core.Metered} instance.
   *
   * @param name    the name of the meter
   * @param meter   the meter
   * @param context the context of the meter
   * @throws Exception if something goes wrong
   */
  @Override
  public void processMeter(MetricName name, Metered meter, String context) throws Exception {
    throw new NotImplementedException();
  }

  /**
   * Process the given counter.
   *
   * @param name    the name of the counter
   * @param counter the counter
   * @param context the context of the meter
   * @throws Exception if something goes wrong
   */
  @Override
  public void processCounter(MetricName name, Counter counter, String context) throws Exception {
    if (!("flow".equals(name.getGroup()))) {
      return;
    }

    FlowMetric metric = getFlowMetric(name.getScope(), name.getName());
    metric.setValue(counter.count());
    Log.trace("Sending metric {} to Flow Monitor.", metric.toString());
    client.add(metric);
  }

  private FlowMetric getFlowMetric(String scope, String name) {
    Log.trace("Extracting flow metrics from scope {} and name {}", scope, name);
    FlowMetric metric = null;

    String[] scopeParts = scope.split("\\.");
    if (scopeParts.length != 4) {
      return metric;
    }

    String[] nameParts = name.split("\\.");
    if (nameParts.length != 4) {
      return metric;
    }

    metric = new FlowMetric();
    int timestamp = (int) (System.currentTimeMillis() / 1000);
    metric.setTimestamp(timestamp);
    metric.setAccountId(scopeParts[0]);
    metric.setApplication(scopeParts[1]);
    metric.setVersion(scopeParts[2]);
    metric.setRid(scopeParts[3]);
    metric.setFlow(nameParts[0]);
    metric.setFlowlet(nameParts[1]);
    metric.setInstance(nameParts[2]);
    metric.setMetric(nameParts[3]);

    Log.info("Adding metrics {}.", metric.toString());

    return metric;

  }

  /**
   * Process the given histogram.
   *
   * @param name      the name of the histogram
   * @param histogram the histogram
   * @param context   the context of the meter
   * @throws Exception if something goes wrong
   */
  @Override
  public void processHistogram(MetricName name, Histogram histogram, String context) throws Exception {
    throw new NotImplementedException();
  }

  /**
   * Process the given timer.
   *
   * @param name    the name of the timer
   * @param timer   the timer
   * @param context the context of the meter
   * @throws Exception if something goes wrong
   */
  @Override
  public void processTimer(MetricName name, Timer timer, String context) throws Exception {
    throw new NotImplementedException();
  }

  /**
   * Process the given gauge.
   *
   * @param name    the name of the gauge
   * @param gauge   the gauge
   * @param context the context of the meter
   * @throws Exception if something goes wrong
   */
  @Override
  public void processGauge(MetricName name, Gauge<?> gauge, String context) throws Exception {
    throw new NotImplementedException();
  }
}
