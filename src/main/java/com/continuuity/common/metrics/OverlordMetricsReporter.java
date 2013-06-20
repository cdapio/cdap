package com.continuuity.common.metrics;

import com.continuuity.common.conf.CConfiguration;
import com.google.common.base.Preconditions;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * OverlordMetricsReporter is a concrete implementation of AbstractPollingReporter
 * that is responsible for programmatically sending the metrics to Overlord
 * metric collection server.
 *
 * <p>
 *   It reports following types of metrics to overlord.
 *   <ul>
 *     <li>Meter</li>
 *     <li>Counter</li>
 *     <li>Guage</li>
 *     <li>Histogram</li>
 *   </ul>
 * </p>
 *
 * <p>
 *   Following is how the OverlordMetricsReporter and the metrics can be
 *   reported.
 *
 *   Somewhere in your daemon or class where you are starting the service
 *   initialize and start the overlord metrics reporter as follows:
 *
 *   <code>
 *     OverlordMetricsReporter.enable(1, TimeUnit.SECONDS,
 *                CConfiguration.create());
 *   </code>
 *
 *   Once you have done that you have setup the reporter that is responsible
 *   for looking at the metrics that you are creating and reporting them
 *   to overlord.
 *
 * </p>
 */
public final class OverlordMetricsReporter extends AbstractPollingReporter implements MetricProcessor<String> {

  private static final Logger LOG = LoggerFactory.getLogger(OverlordMetricsReporter.class);

  /**
   * Stores the timestamp at which the metrics time are computed.
   */
  private long timestamp;

  /**
   * Locale for String.format().
   */
  private final Locale locale = Locale.US;

  /**
   * Gets virtual machine metrics.
   */
  private final VirtualMachineMetrics vm;

  /**
   * Hostname the metrics are collected from.
   */
  private final String hostname;

  /**
   * Handler for sending metrics to overlord.
   */
  private final MetricsClient client;

  /**
   * Single instance of metrics overlord report.
   */
  private static OverlordMetricsReporter reporter = null;

  /**
   * Specifies the backoff minimum time to be used for backing off.
   */
  private static final int BACKOFF_MIN_TIME = 1;

  /**
   * Specifies the backoff maximum time.
   */
  private static int BACKOFF_MAX_TIME = 10;

  /**
   * Specifies the exponent for increasing the backoff by.
   */
  private static final int BACKOFF_EXPONENT = 2;

  /**
   * Current interval to sleep during back-off.
   */
  private int interval = BACKOFF_MIN_TIME;

  /**
   * Enables the overlord reporter for the default metrics registry, and causes
   * it to send reports to overlord.
   *
   * @param period the period between successive outputs
   * @param unit   the time unit of {@code period}
   * @param configuration the configuration to use
   */
  public static synchronized void enable(long period, TimeUnit unit, CConfiguration configuration) {

    // In order to prevent from all the hosts checking to see if the
    // service is backup we add some amount of randomization that allows
    // the clients to do the check at random max times when max backoff
    // is reached.
    BACKOFF_MAX_TIME = BACKOFF_MIN_TIME +
        (int) (Math.random() * (BACKOFF_MAX_TIME - BACKOFF_MIN_TIME) + 1);

    if (reporter == null) {
      reporter = new OverlordMetricsReporter(Metrics.defaultRegistry(), configuration);
      reporter.start(period, unit);
    }
  }

  /**
   * Clears metrics for a given name.
   */
  public static synchronized void clear(String name) {
    for (Map.Entry<MetricName, Metric> entry :
      Metrics.defaultRegistry().allMetrics().entrySet()) {
      if (entry.getKey().getGroup().contains(name)) {
        Metrics.defaultRegistry().removeMetric(entry.getKey());
      }
    }
  }

  /**
   * Disables the overlord metric reporter.
   */
  public static synchronized void disable() {
    if (reporter != null) {
      reporter.shutdown();
      reporter = null;
    }
  }

  /**
   * Creates a new AbstractPollingReporter instance
   *
   * @param registry the MetricRegistry containing the metrics this reporter will
   *                 report
   * @param configuration instance of configuration object.
   */
  private OverlordMetricsReporter(MetricsRegistry registry, CConfiguration configuration) {
    super(registry, "overlord-metric-reporter");
    Preconditions.checkNotNull(configuration);
    Preconditions.checkNotNull(registry);
    this.vm = VirtualMachineMetrics.getInstance();
    this.hostname = getDefaultHostLabel();
    this.client = new MetricsClient(configuration);
  }

  /**
   * Returns the metrics registry associated with
   * @return MetricsRegistry
   */
  public static MetricsRegistry getRegistry() {
    if (reporter != null) {
      reporter.getMetricsRegistry();
    }
    return null;
  }

  @Override
  public void start(long period, TimeUnit unit) {
    this.client.startAndWait();
    super.start(period, unit);
  }

  @Override
  public void shutdown(long timeout, TimeUnit unit) throws InterruptedException {
    super.shutdown(timeout, unit);
    client.stopAndWait();
  }

  @Override
  public void shutdown() {
    super.shutdown();
    client.stopAndWait();
  }

  /**
   * Part of the thread that is invoked based on the period and unit
   * specified for collecting metrics.
   */
  @Override
  public void run() {
    // Ensures that the timestamp is the same for all the metrics
    // that are processed.
    this.timestamp = System.currentTimeMillis() / 1000;

    // Iterate through all the metrics that we have collected.
    for (Map.Entry<MetricName, Metric> entry :
          getMetricsRegistry().allMetrics().entrySet()) {
      Metric metric = entry.getValue();
      try {
        metric.processWith(this, entry.getKey(), null);
      } catch (Exception e) {
        LOG.warn("Issue processing metric {}. Reason : {}",
                 metric.toString(), e.getMessage());
      }
    }
  }

  /**
   * Constructs and sends the command to overlord to register the
   * metric.
   *
   * <p>
   *   Currently we send one metric at a time to overlord over the
   *   connection held. in future the plan is to send a batch of
   *   metrics to overlord for processing.
   * </p>
   *
   * @param metricName
   * @param metricValue
   */
  private void sendToOverlord(String metricName, String scope,
                              String metricValue) {
    Preconditions.checkNotNull(metricName);
    Preconditions.checkNotNull(metricValue);

    String tags = null;
    if (hostname != null && !hostname.isEmpty()) {
      tags = String.format("host=%s", hostname);
    }

    if (scope != null && !scope.isEmpty()) {
      tags = String.format("%s scope=%s", tags, scope);
    }

    String command = "";
    if (tags != null) {
      command = String.format("put %s %s %s %s", metricName,
                     timestamp, metricValue, tags);
    } else {
      command = String.format("put %s %s %s", metricName,
                              timestamp, metricValue);
    }

    // Write the command into the client queue.
    if (!client.write(command)) {
      // If we fail then we back-off to a max of BACKOFF_MAX_TIME.
      // While this thread is blocked, a thread in the client is
      // dequeing and trying to make space for more stuff to be add
      // later.
      interval = Math.min(BACKOFF_MAX_TIME, interval * BACKOFF_EXPONENT) * 1000;
      try {
        Thread.sleep(interval);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void sendToOverlord(String type, String group,
                              String name, String scope, String value) {
    Preconditions.checkNotNull(type);
    Preconditions.checkNotNull(group);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(value);

    String metricName = "";
    if (group == null || group.isEmpty()) {
      metricName = String.format(locale, "%s:%s", type, name);
    } else {
      metricName = String.format(locale, "%s:%s.%s", type, group, name);
    }
    sendToOverlord(metricName, scope, value);
  }

  private void sendLong(String type, String group, String name, String scope,
                        long value) {
    sendToOverlord(type, group, name, scope, String.format(locale, "%d", value));
  }

  private void sendLong(String metricName, String scope, long value) {
    sendToOverlord(
      String.format("%s:%s", MetricType.System.name(), metricName),
      scope,
      String.format(locale, "%d", value)
    );
  }

  private void sendDouble(String type, String group, String name,
                          String scope, double value) {
    sendToOverlord(type, group, name, scope,
                   String.format(locale, "%4.6f", value));
  }

  private void sendDouble(String metricName, String scope, double value) {
    sendToOverlord(
      String.format("%s:%s", MetricType.System.name(), metricName),
      scope,
      String.format(locale, "%4.6f", value)
    );
  }

  @Override
  public void processMeter(MetricName name, Metered meter, String context)
    throws Exception {
    final String metricName = name.getName();
    final String type = name.getType();
    final String group = name.getGroup();
    final String scope = name.getScope();

    sendLong(type, group, metricName + ".count", scope, meter.count());
    sendDouble(type, group, metricName + ".meanRate", scope, meter.meanRate());
    sendDouble(type, group, metricName + ".1MinRate", scope, meter.oneMinuteRate());
    sendDouble(type, group, metricName + ".5MinRate", scope, meter.fiveMinuteRate());
    sendDouble(type, group, metricName + ".15MinRate", scope, meter.fifteenMinuteRate());
  }

  @Override
  public void processCounter(MetricName name, Counter counter,
                             String context) throws Exception {
    sendLong(name.getType(), name.getGroup(), name.getName() + ".count",
             name.getScope(), counter.count());
  }

  @Override
  public void processHistogram(MetricName name, Histogram histogram,
                               String context) throws Exception {
    // Take a snapshot for computing percentiles.
    final Snapshot snapshot = histogram.getSnapshot();

    // Simplification
    final String metricName = name.getName();
    final String type = name.getType();
    final String group = name.getGroup();
    final String scope = name.getScope();

    // Send metrics from histogram.
    sendDouble(type, group, metricName + ".min", scope, histogram.min());
    sendDouble(type, group, metricName + ".max", scope, histogram.max());
    sendDouble(type, group, metricName + ".mean", scope, histogram.mean());
    sendDouble(type, group, metricName + ".stddev", scope, histogram.stdDev());
    sendDouble(type, group, metricName + ".median", scope, snapshot.getMedian());
    sendDouble(type, group, metricName + ".75percentile", scope,
               snapshot.get75thPercentile());
    sendDouble(type, group, metricName + ".95percentile", scope,
               snapshot.get95thPercentile());
    sendDouble(type, group, metricName + ".98percentile", scope,
               snapshot.get98thPercentile());
    sendDouble(type, group, metricName + ".99percentile", scope,
               snapshot.get99thPercentile());
    sendDouble(type, group, metricName + ".999percentile", scope,
               snapshot.get999thPercentile());
  }

  @Override
  public void processGauge(MetricName name, Gauge<?> gauge, String context) throws Exception {
    sendToOverlord(name.getType(), name.getGroup(), name.getName(),
                   name.getScope(), String.format(locale, "%s", gauge.value()));
  }

  @Override
  public void processTimer(MetricName name, Timer timer, String context)
    throws Exception {
    // Not implemented.
  }

  private String getDefaultHostLabel() {
    try {
      final InetAddress addr = InetAddress.getLocalHost();
      return addr.getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to get local  name: ", e);
    }
    return null;
  }

  private void sendJVMMetrics() {
    sendDouble("jvm.memory.heap_usage", "jvm", vm.heapUsage());
    sendDouble("jvm.memory.non_heap_usage", "jvm", vm.nonHeapUsage());
    for (Map.Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
      sendDouble("jvm.memory.memory_pool_usages." + pool.getKey(), "jvm",
                       pool.getValue()
                       );
    }

    sendDouble("jvm.daemon_thread_count", "jvm", vm.daemonThreadCount());
    sendDouble("jvm.thread_count", "jvm", vm.threadCount());
    sendDouble("jvm.uptime", "jvm", vm.uptime());
    sendDouble("jvm.fd_usage", "jvm", vm.fileDescriptorUsage());

    for (Map.Entry<Thread.State, Double> entry :
      vm.threadStatePercentages().entrySet()) {
      sendDouble("jvm.thread-states." + entry.getKey().toString().toLowerCase(),
                      "jvm",
                       entry.getValue());
    }
  }
}
