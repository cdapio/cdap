package com.continuuity.performance.benchmark;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.performance.util.MensaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metrics collector that writes metrics to file.
 */
class FileMetricReporter extends MetricsCollector {

  private static final Logger LOG = LoggerFactory.getLogger(FileMetricReporter.class);

  private static final int FILE_APPENDER_METRIC_INTERVAL = 60;
  private static final String OPS_PER_SEC_ONE_MIN = "benchmark.ops.per_sec.1m";
  private static final String OPS_PER_SEC_AVG = "benchmark.ops.per_sec.avg";

  String fileName;
  File reportFile;
  String benchmarkName;
  Map<String, ArrayList<Double>> metrics;
  BufferedWriter bw;

  @Override
  protected int getInterval() {
    return FILE_APPENDER_METRIC_INTERVAL;
  }

  protected FileMetricReporter(String benchmarkName, AgentGroup[] groups, BenchmarkMetric[] metrics,
                            CConfiguration config) {
    super(groups, metrics);
    this.fileName = config.get("reportfile");
    int pos = benchmarkName.lastIndexOf(".");
    if (pos != -1) {
      this.benchmarkName = benchmarkName.substring(pos + 1, benchmarkName.length());
    } else {
      this.benchmarkName = benchmarkName;
    }
    this.metrics = new HashMap<String, ArrayList<Double>>(groups.length);
    for (AgentGroup group : groups) {
      this.metrics.put(group.getName(), new ArrayList<Double>());
    }
  }

  @Override
  protected void init() throws BenchmarkException {
    reportFile = new File(fileName);
    try {
      bw = new BufferedWriter(new FileWriter(reportFile, true));
    } catch (IOException e) {
      throw new BenchmarkException("Error during init of report file appender when trying to open report file "
                                     + fileName + ".", e);
    }
  }

  @Override
  protected void processGroupMetricsInterval(long unixTime, AgentGroup group, long previousMillis, long millis,
                                          Map<String, Long> prevMetrics, Map<String, Long> latestMetrics,
                                          boolean interrupt) {
    if (prevMetrics != null && !interrupt) {
      for (Map.Entry<String, Long> singleMetric : latestMetrics.entrySet()) {
        String key = singleMetric.getKey();
        long value = singleMetric.getValue();
        Long previousValue = prevMetrics.get(key);
        if (previousValue == null) {
          previousValue = 0L;
        }
        long valueSince = value - previousValue;
        long millisSince = millis - previousMillis;
        metrics.get(group.getName()).add(valueSince * 1000.0 / millisSince);
        String metricValue = String.format("%1.2f", valueSince * 1000.0 / millisSince);
        String metric = MensaUtils.buildMetric(OPS_PER_SEC_ONE_MIN, Long.toString(unixTime), metricValue,
                                               benchmarkName, group.getName(),
                                               Integer.toString(group.getNumAgents()), "");
        LOG.debug("Collected metric {} in memory ", metric);
      }
    }
  }


  @Override
  protected void processGroupMetricsFinal(long unixTime, AgentGroup group) throws BenchmarkException {
    List<Double> grpVals = metrics.get(group.getName());
    if (grpVals.size() != 0) {
      double sum = 0;
      for (Double grpVal : grpVals) {
        sum += grpVal;
      }
      double avg = sum / grpVals.size();
      String metricValue = String.format("%1.2f", avg);
      String metric = MensaUtils.buildMetric(OPS_PER_SEC_AVG,
                                             Long.toString(unixTime),
                                             metricValue,
                                             benchmarkName,
                                             group.getName(),
                                             Integer.toString(group.getNumAgents()),
                                             "");
      LOG.debug("Writing {} to file {}", metric, fileName);
      try {
        bw.write(metric);
        bw.write("\n");
        bw.flush();
      } catch (IOException e) {
        throw new BenchmarkException("Error when trying to write final group metrics to report file" + fileName + ".",
                                     e);
      }
    }
  }

  @Override
  protected void shutdown() {
    try {
      bw.close();
    } catch (IOException e) {
      LOG.error("Error during shutdown of report file appender when trying to close report file " + fileName + ".", e);
    }
  }
}
