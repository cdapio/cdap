package com.continuuity.performance.benchmark;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.performance.util.MensaMetricsDispatcher;
import com.continuuity.performance.util.MensaUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

class MensaMetricsCollector extends MetricsCollector {

  /**
   * Metrics collector that uploads metrics to mensa server.
   */
  private static final Logger LOG = LoggerFactory.getLogger(MensaMetricsCollector.class);

  /**
   * Metric metricsDispatcher.
   */
  private final MensaMetricsDispatcher metricsDispatcher;

  private static final int MENSA_METRIC_INTERVAL = 10;
  private static final String OPS_PER_SEC_10_SEC = "benchmark.ops.per_sec.10s";

  /**
   * Name of benchmark that is currently running.
   */
  private String benchmarkName;

  /**
   * Hostname of mensa server to connect to.
   */
  private String mensaHost;

  /**
   * Port of mensa server to connect to.
   */
  private int mensaPort;

  /**
   * Extra mensa tags that get uploaded with each mensa metric.
   */
  private String mensaTags = new String();

  /**
   * Mensa mensaMetrics to be uploaded.
   */
  private Map<String, ArrayList<Double>> mensaMetrics;

  @Override
  protected int getInterval() {
    return MENSA_METRIC_INTERVAL;
  }

  /**
   * Queue that holds metrics information.
   */
  private final LinkedBlockingDeque<String> queue;

  /**
   * Executor service for running the metricsDispatcher thread.
   * Overkill right now -- the idea is that in future if
   * we find that we need to more threads to send then
   * it can be extended.
   */
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  private Future futureDispatcher;

  private void parseConfig(CConfiguration config) {
    String mensa = config.get("mensa");
    if (StringUtils.isNotEmpty(mensa)) {
      String[] hostPort = mensa.split(":");
      mensaHost = hostPort[0];
      mensaPort = Integer.valueOf(hostPort[1]);
    }
    String extraTags = config.get("extratags");
    if (StringUtils.isNotEmpty(extraTags)) {
      extraTags = extraTags.replace(",", " ");
      appendExtraTags(extraTags);
    }
  }

  private void appendExtraTags(String extraTags) {
    mensaTags = Joiner.on(" ").join(mensaTags, extraTags);
  }

  private String getShortBenchmarkName(String benchmarkName) {
    if (StringUtils.isEmpty(benchmarkName)) {
      return benchmarkName;
    }
    int pos = benchmarkName.lastIndexOf(".");
    if (pos != -1) {
      return benchmarkName.substring(pos + 1, benchmarkName.length());
    } else {
      return benchmarkName;
    }
  }

  protected MensaMetricsCollector(String benchmarkName, AgentGroup[] groups, BenchmarkMetric[] metrics,
                               CConfiguration config, String extraTags) {
    super(groups, metrics);
    mensaTags = new String();
    this.benchmarkName = getShortBenchmarkName(benchmarkName);

    parseConfig(config);
    appendExtraTags(extraTags);

    mensaMetrics = new HashMap<String, ArrayList<Double>>(groups.length);
    for (AgentGroup group : groups) {
      mensaMetrics.put(group.getName(), new ArrayList<Double>());
    }

    // Creates the queue that holds the mensaMetrics to be dispatched
    // to the overlord.
    queue = new LinkedBlockingDeque<String>(50000);

    metricsDispatcher = new MensaMetricsDispatcher(mensaHost, mensaPort, queue);

    // Start the metricsDispatcher thread.
    futureDispatcher = executorService.submit(metricsDispatcher);

  }

  @Override
  protected void processGroupMetricsInterval(long unixTime, AgentGroup group, long previousMillis, long millis,
                                          Map<String, Long> prevMetrics, Map<String, Long> latestMetrics,
                                          boolean interrupt) throws BenchmarkException {
    if (prevMetrics != null && !interrupt) {
      LOG.debug("Processing group metrics at Unix time {}.", unixTime);
      for (Map.Entry<String, Long> singleMetric : latestMetrics.entrySet()) {
        String metricName = singleMetric.getKey();
        long latestValue = singleMetric.getValue();
        Long previousValue = prevMetrics.get(metricName);
        if (previousValue == null) {
          previousValue = 0L;
        }
        long valueSince = latestValue - previousValue;
        long millisSince = millis - previousMillis;
        mensaMetrics.get(group.getName()).add(valueSince * 1000.0 / millisSince);
        String metricValue = String.format("%1.2f", valueSince * 1000.0 / millisSince);
        String metricPut = MensaUtils.buildMetric(OPS_PER_SEC_10_SEC, Long.toString(unixTime), metricValue,
                                                  benchmarkName, group.getName(),
                                                  Integer.toString(group.getNumAgents()), mensaTags);

        if (write(metricPut) == false) {
          throw new BenchmarkException("Error when trying to upload group metric "
                                         + metricPut + " to mensa at " + mensaHost + ":" + mensaPort + ".");
        } else {
          LOG.debug("Uploaded metric put operation \"{}\" to mensa at {}:{}", metricPut, mensaHost, mensaPort);
        }
      }
    }
  }

  @Override
  protected void processGroupMetricsFinal(long unixTime, AgentGroup group) throws BenchmarkException {
  }

  @Override
  protected void shutdown() {
    // Shutdown the metricsDispatcher thread.
    LOG.debug("Stopping metrics dispatcher thread.");
    metricsDispatcher.stop();
    futureDispatcher.cancel(true);

    LOG.debug("Shutting down executor service of metrics dispatcher.");
    // Shutdown executor service.
    executorService.shutdown();
  }

  /**
   * Writes the metric to be sent to queue.
   *
   * @param metricPut contains the metric put command to be sent to the server.
   * @return true if successfully put on the queue else false.
   */
  private boolean write(String metricPut) {
    Preconditions.checkNotNull(metricPut);
    return queue.offer(metricPut);
  }
}
