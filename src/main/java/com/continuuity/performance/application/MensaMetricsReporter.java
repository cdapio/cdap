package com.continuuity.performance.application;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.performance.util.MensaMetricsDispatcher;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

/**
 *
 */
public final class MensaMetricsReporter  {
  static final Logger LOG = LoggerFactory.getLogger(MensaMetricsReporter.class);

  private final String mensaHost;
  private final int mensaPort;
  private final RuntimeMetricsCollector collector;
  private final LinkedBlockingDeque<String> dispatchQueue;
  private final MensaMetricsDispatcher dispatcher;
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private Future futureCollector;
  private Future futureDispatcher;

  public MensaMetricsReporter(CConfiguration config, List<String> metricNames, String tags) {
    String mensa = config.get("mensa");
    if (StringUtils.isNotEmpty(mensa)) {
      String[] hostPort = mensa.split(":");
      mensaHost = hostPort[0];
      mensaPort = Integer.valueOf(hostPort[1]);
    } else {
      mensaHost = null;
      mensaPort = -1;
    }
    String extraTags = config.get("extratags");
    if (StringUtils.isNotEmpty(extraTags)) {
      extraTags = extraTags.replace(",", " ");
      //appendExtraTags(extraTags);
    }
    dispatchQueue = new LinkedBlockingDeque<String>(50000);
    collector = new RuntimeMetricsCollector(dispatchQueue, 10, metricNames, tags);
    dispatcher = new MensaMetricsDispatcher(mensaHost, mensaPort, dispatchQueue);
    init();
  }

  public MensaMetricsReporter(String mensaHost, int mensaPort, List<String> metricNames, String tags, int interval) {
    this.mensaHost = mensaHost;
    this.mensaPort = mensaPort;
    dispatchQueue = new LinkedBlockingDeque<String>(50000);
    collector = new RuntimeMetricsCollector(dispatchQueue, interval, metricNames, tags);
    dispatcher = new MensaMetricsDispatcher(mensaHost, mensaPort, dispatchQueue);
    init();
  }

  private void init() {
    // Start the metrics collector thread.
    futureCollector = executorService.submit(collector);
    // Start the metrics dispatcher thread.
    futureDispatcher = executorService.submit(dispatcher);
  }

  public void reportNow(String metricName, double value) {
    collector.enqueueMetric(metricName, value);
  }

  public void reportNow(String metricName) {
    collector.enqueueMetric(metricName);
  }

  public final void shutdown() {
    // Shutdown the metricsDispatcher thread.
    LOG.debug("Stopping metrics collector thread.");
    collector.stop();
    futureCollector.cancel(true);

    LOG.debug("Stopping metrics dispatcher thread.");
    dispatcher.stop();
    futureDispatcher.cancel(true);

    LOG.debug("Shutting down executor service of metrics collector and dispatcher.");
    executorService.shutdown();
  }


}
