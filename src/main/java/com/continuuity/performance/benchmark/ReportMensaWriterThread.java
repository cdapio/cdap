package com.continuuity.performance.benchmark;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.performance.util.MensaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ReportMensaWriterThread extends ReportThread {

  private static final Logger LOG = LoggerFactory.getLogger(ReportMensaWriterThread.class);

  private static final String OPS_PER_SEC_ONE_MIN = "benchmark.ops.per_sec.1m";
  private static final String OPS_PER_SEC_AVG = "benchmark.ops.per_sec.avg";

  String fileName;
  String benchmarkName;
  String mensaHost;
  String mensaTags;
  int mensaPort;
  Map<String, ArrayList<Double>> metrics;

  public ReportMensaWriterThread(String benchmarkName, AgentGroup[] groups, BenchmarkMetric[] metrics,
                                 CConfiguration config, String extraTags) {
    this.groupMetrics = metrics;
    this.groups = groups;
    this.reportInterval = config.getInt("report", reportInterval);
    this.fileName = config.get("reportfile");
    int pos=benchmarkName.lastIndexOf(".");
    if (pos != -1) {
      this.benchmarkName=benchmarkName.substring(pos + 1, benchmarkName.length());
    } else {
      this.benchmarkName=benchmarkName;
    }
    String mensa = config.get("mensa");
    if (mensa != null && mensa.length() != 0) {
      String[] hostPort = mensa.split(":");
      this.mensaHost = hostPort[0];
      this.mensaPort = Integer.valueOf(hostPort[1]);
    }
    mensaTags = config.get("extratags");
    if (mensaTags != null) {
      mensaTags = mensaTags.replace(","," ");
    }
    if (extraTags != null && extraTags.length() != 0) {
      if (mensaTags != null && mensaTags.length() != 0) {
        mensaTags = mensaTags + " " + extraTags;
      } else {
        mensaTags = extraTags;
      }
    }
    this.metrics = new HashMap<String,ArrayList<Double>>(groups.length);
    for (int i = 0; i < groups.length; i++) {
      this.metrics.put(groups[i].getName(), new ArrayList<Double>());
    }
  }

  @Override
  protected void init() {
  }

  @Override
  public void processGroupMetricsInterval(long unixTime,
                                          AgentGroup group,
                                          long previousMillis,
                                          long millis,
                                          Map<String, Long> prevMetrics,
                                          Map<String, Long> latestMetrics,
                                          boolean interrupt) {
    if (prevMetrics != null) {
      for (Map.Entry<String, Long> singleMetric : prevMetrics.entrySet()) {
        String key = singleMetric.getKey();
        long value = singleMetric.getValue();
        if (!interrupt) {
          Long previousValue = prevMetrics.get(key);
          if (previousValue == null) previousValue = 0L;
          long valueSince = value - previousValue;
          long millisSince = millis - previousMillis;
          metrics.get(group.getName()).add(valueSince * 1000.0 / millisSince);
          String metricValue = String.format("%1.2f", valueSince * 1000.0 / millisSince);
          String metric = MensaUtils.buildMetric(OPS_PER_SEC_ONE_MIN, Long.toString(unixTime), metricValue,
                                                 benchmarkName, group.getName(),
                                                 Integer.toString(group.getNumAgents()), mensaTags);
          try {
            MensaUtils.uploadMetric(mensaHost, mensaPort, metric);
          } catch (IOException e) {
            LOG.error("Error during upload of metric to Mensa", e);
          }
          LOG.info("Sent metric with operation \"{}\" to mensa at {}:{}", metric, mensaHost, mensaPort);
        }
      }
    }
  }

  @Override
  protected void processGroupMetricsFinal(long unixTime, AgentGroup group) {
  }

  @Override
  protected void shutdown() {
  }
}
