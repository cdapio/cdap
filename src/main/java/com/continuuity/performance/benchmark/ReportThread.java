package com.continuuity.performance.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

public abstract class ReportThread extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(ReportThread.class);

//  private int reportInterval = 60;
  private AgentGroup[] groups;
  private BenchmarkMetric[] groupMetrics;

  public abstract int getInterval();


  public ReportThread(AgentGroup[] groups, BenchmarkMetric[] metrics) {
    this.groupMetrics = metrics;
    this.groups = groups;
  }

  protected abstract void processGroupMetricsInterval(long unixTime,
                                                      AgentGroup group,
                                                      long previousMillis,
                                                      long millis,
                                                      Map<String, Long> prevMetrics,
                                                      Map<String, Long> latestMetrics,
                                                      boolean interrupt) throws BenchmarkException;

  protected abstract void processGroupMetricsFinal(long unixTime, AgentGroup group) throws BenchmarkException;

  public final void run() {
    try {
      init();
      long start = System.currentTimeMillis();
      long unixTime;
      boolean interrupt = false;
      ArrayList<Map<String, Long>> previousMetrics = new ArrayList<Map<String, Long>>(groups.length);
      for (int i = 0; i < groups.length; i++) {
        previousMetrics.add(null);
      }
      long[] previousMillis = new long[groups.length];
      // wake up every interval (i.e. every minute) to report the metrics
      int interval = getInterval();
      for (int seconds = interval; !interrupt; seconds += interval) {
        long wakeup = start + (seconds * 1000);
        long currentTime = System.currentTimeMillis();
        unixTime = currentTime / 1000L;
        try {
          if (wakeup > currentTime) {
            Thread.sleep(wakeup - currentTime);
          }
        } catch (InterruptedException e) {
          interrupt = true;
        }
        long latestMillis;
        if (interrupt) {
          latestMillis = System.currentTimeMillis() - start;
        } else {
          latestMillis = seconds * 1000L;
        }
        LOG.debug("{} elapsed: ", time2String(latestMillis));
        for (int i = 0; i < groups.length; i++) {
          Map<String, Long> latestGrpMetrics = groupMetrics[i].list();
          Map<String, Long> previousGrpMetrics = previousMetrics.get(i);

          processGroupMetricsInterval(unixTime, groups[i], previousMillis[i], latestMillis,
                                      previousGrpMetrics, latestGrpMetrics, interrupt);

          previousMetrics.set(i, latestGrpMetrics);
          previousMillis[i] = latestMillis;
        }
      } // each interval
      LOG.debug("Summarizing collected metrics...");
      unixTime = System.currentTimeMillis() / 1000L;
      for (AgentGroup group : groups) {
        processGroupMetricsFinal(unixTime, group);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      shutdown();
    }
}

  protected void init() throws BenchmarkException {
  }

  protected void shutdown() {
  }

  private String time2String(long millis) {
    final long second = 1000;
    final long minute = 60 * second;
    final long hour = 60 * minute;
    final long day = 24 * hour;

    StringBuilder builder = new StringBuilder();
    if (millis > day) {
      long days = millis / day;
      millis = millis % day;
      builder.append(days);
      builder.append('d');
    }
    long hours = millis / hour;
    millis = millis % hour;
    long minutes = millis / minute;
    millis = millis % minute;
    long seconds = millis / second;
    millis = millis % second;

    builder.append(String.format("%02d:%02d:%02d", hours, minutes, seconds));
    if (millis > 0) builder.append(String.format(".%03d", millis));
    return builder.toString();
  }
}
