package com.continuuity.performance.benchmark;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Abstract metrics collector class.
 */
abstract class MetricsCollector implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsCollector.class);

  /**
   *
   */
  private volatile boolean interrupt = false;

  /**
   *
   */
  private AgentGroup[] groups;

  /**
   *
   */
  private BenchmarkMetric[] groupMetrics;

  /**
   *
   */
  private final Stopwatch stopwatch = new Stopwatch();
  private long elapsedMillis = 0L;
  private final ArrayList<Map<String, Long>> previousMetrics;

  protected abstract int getInterval();

  protected MetricsCollector(AgentGroup[] groups, BenchmarkMetric[] metrics) {
    this.groupMetrics = metrics;
    this.groups = groups;
    this.previousMetrics = new ArrayList<Map<String, Long>>(groups.length);
    for (int i = 0; i < groups.length; i++) {
      previousMetrics.add(i, null);
    }
  }

  protected final void stop() {
    interrupt = true;
  }

  protected abstract void processGroupMetricsInterval(long unixTime, AgentGroup group, long previousMillis, long millis,
                                          Map<String, Long> prevMetrics, Map<String, Long> latestMetrics,
                                          boolean interrupt) throws BenchmarkException;

  protected abstract void processGroupMetricsFinal(long unixTime, AgentGroup group) throws BenchmarkException;

  public final void run() {
    try {
      LOG.debug("Initializing metrics collector.");
      init();
      stopwatch.start();

      long[] previousElapsedMillis = new long[groups.length];
      // wake up every interval (i.e. every minute) to report the metrics
      int interval = getInterval();

      LOG.debug("Starting to collect metrics every {} seconds.", interval);
      for (int seconds = interval; !interrupt; seconds += interval) {
        long nextWakeupMillis = seconds * 1000L;
        elapsedMillis = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
        try {
          if (nextWakeupMillis > elapsedMillis) {
            Thread.sleep(nextWakeupMillis - elapsedMillis);
          }
          elapsedMillis = nextWakeupMillis;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          interrupt = true;
          elapsedMillis = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
        }

        //either next wake-up time has been reached or an interrupt happened before that
        LOG.debug("{} elapsed: ", time2String(elapsedMillis));

        for (int i = 0; i < groups.length; i++) {
          Map<String, Long> latestGrpMetrics = groupMetrics[i].list();
          Map<String, Long> previousGrpMetrics = previousMetrics.get(i);

          processGroupMetricsInterval(getCurrentUnixTime(), groups[i], previousElapsedMillis[i], elapsedMillis,
                                      previousGrpMetrics, latestGrpMetrics, interrupt);

          previousMetrics.set(i, latestGrpMetrics);
          previousElapsedMillis[i] = elapsedMillis;
        }
      } // each interval
      LOG.debug("Summarizing collected metrics...");
      for (AgentGroup group : groups) {
        processGroupMetricsFinal(getCurrentUnixTime(), group);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      LOG.debug("Shutting down metrics collector.");
      stopwatch.stop();
      shutdown();
    }
  }

  protected void init() throws BenchmarkException {
  }

  protected void shutdown() {
  }

  private static String time2String(long millis) {
    final long second = 1000L;
    final long minute = 60L * second;
    final long hour = 60L * minute;
    final long day = 24L * hour;

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
    if (millis > 0) {
      builder.append(String.format(".%03d", millis));
    }
    return builder.toString();
  }

  private static long getCurrentUnixTime() {
    return System.currentTimeMillis() / 1000L;
  }

  public void getResults(final BenchmarkResult result) {
    result.setRuntimeMillis(stopwatch.elapsedMillis());
    for (int i = 0; i < groups.length; i++) {
      result.add(new BenchmarkResult.
        GroupResult(groups[i].getName(), groups[i].getNumAgents(), groupMetrics[i].list()));
    }
  }
}
