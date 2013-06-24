package com.continuuity.performance.application;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.metrics2.thrift.Counter;
import com.continuuity.performance.runner.PerformanceTestRunner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import org.apache.commons.lang.StringUtils;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Class for collecting runtime metrics while performance test is being executed.
 */
public final class RuntimeMetricsCollector implements MetricsCollector {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMetricsCollector.class);

  private final int interval;
  private final List<CounterName> counterNames;
  private final String tags;
  private final LinkedBlockingDeque<String> queue;

  private final Stopwatch stopwatch = new Stopwatch();
  private volatile boolean interrupt = false;

  public RuntimeMetricsCollector(LinkedBlockingDeque<String> queue, int interval, List<String> counterNames,
                                 String tags) {
    this.counterNames = new ArrayList<CounterName>(counterNames.size());
    for (String counterName : counterNames) {
      CounterName cn = new CounterName(counterName);
      this.counterNames.add(new CounterName(counterName));
      Log.debug("Added counter {} to list of counters of metrics collector.", cn);
    }
    this.tags = tags;
    this.queue = queue;
    this.interval = interval;
  }

  public void configure(CConfiguration config) {
  }

  @Override
  public void run() {
    try {
      LOG.debug("Initializing metrics collector.");
      stopwatch.start();

      // wake up every interval (i.e. every minute) to report the metrics
      LOG.debug("Starting to collect metrics every {} seconds.", interval);

      for (int seconds = interval; !interrupt; seconds += interval) {
        long nextWakeupMillis = seconds * 1000L;
        long elapsedMillis = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
        try {
          if (nextWakeupMillis > elapsedMillis) {
            Thread.sleep(nextWakeupMillis - elapsedMillis);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          interrupt = true;
        }
        LOG.debug("Metrics collector woke up.");

        if (!interrupt) {
          final long unixTime = getCurrentUnixTime();
          for (CounterName counterName : counterNames) {
            enqueueMetric(counterName, unixTime);
          }
        }
      } // each interval
    } catch (Exception e) {
      Throwables.propagate(e);
    } finally {
      LOG.debug("Shutting down runtime metrics collector.");
      stopwatch.stop();
    }
  }

  @Override
  public void stop() {
    interrupt = true;
  }

  public void enqueueMetric(String counterName) {
    enqueueMetric(new CounterName(counterName), getCurrentUnixTime());
  }

  public void enqueueMetric(String counterName, double value) {
    enqueueMetric(counterName, getCurrentUnixTime(),  value);
  }

  public void enqueueMetric(String counterName, long unixTime, double value) {
    enqueueMetricCommand(new CounterName(counterName).getName(), unixTime, value, tags);
  }

  public void  enqueueMetric(CounterName counterName, long unixTime) {
    final Counter counter;
    if (counterName.getLevel() == CounterLevel.Account) {
      counter = PerformanceTestRunner.BenchmarkRuntimeStats.getCounter(counterName.getName());
    } else {
      counter = PerformanceTestRunner.BenchmarkRuntimeStats.getCounter(counterName.getAccountId(),
                                                                       counterName.getApplicationId(),
                                                                       counterName.getFlowId(),
                                                                       counterName.getFlowletId(),
                                                                       counterName.getName());
    }
    if (counter != null) {
      StringBuilder metricTags = new StringBuilder(tags);
      if (StringUtils.isNotEmpty(counterName.getApplicationId())) {
        metricTags.append("application=" + counterName.getApplicationId());
        metricTags.append(" ");
      }
      if (StringUtils.isNotEmpty(counterName.getFlowId())) {
        metricTags.append("flow=" + counterName.getFlowId());
        metricTags.append(" ");
      }
      if (StringUtils.isNotEmpty(counterName.getFlowletId())) {
        metricTags.append("flowlet=" + counterName.getFlowletId());
      }
      enqueueMetricCommand(counterName.getName(), unixTime, counter.getValue(), metricTags.toString().trim());
    }
  }

  private void enqueueMetricCommand(String metricName, long unixTime, double value, String tags) {
    String cmd = getMetricCommand(metricName, unixTime, value, tags);
    queue.add(cmd);
    LOG.debug("Added metric command '{}' to the metric collector's dispatch queue.", cmd);
  }

  private static long getCurrentUnixTime() {
    return System.currentTimeMillis() / 1000L;
  }

  private static String getMetricCommand(String metricName, long unixTime, double value, String tags) {
    return "put" + " " + metricName + " " + unixTime + " " + value + " " + tags;
  }

  /**
   * Class for account or flowlet level counter names.
   */
  final class CounterName {
    private CounterLevel level;
    private String accountId;
    private String applicationId;
    private String flowId;
    private String flowletId;
    private String name;

    CounterName(String accountId, String applicationId, String flowId, String flowletId, String name) {
      this.accountId = accountId;
      this.applicationId = applicationId;
      this.flowId = flowId;
      this.flowletId = flowletId;
      this.name = name;
    }

    CounterName(String name) {
      String[] parts = name.split(":");
      switch (parts.length) {
        case 1:
          this.level = CounterLevel.Account;
          this.name = name;
          break;
        case 2:
          this.level = CounterLevel.Flowlet;
          this.flowletId = parts[0];
          this.name = parts[1];
          break;
        case 5:
          this.level = CounterLevel.Flowlet;
          this.accountId = parts[0];
          this.applicationId = parts[1];
          this.flowId = parts[2];
          this.flowletId = parts[3];
          this.name = parts[4];
          break;
      }
    }

    CounterLevel getLevel() {
      return level;
    }

    String getAccountId() {
      return accountId;
    }

    String getApplicationId() {
      return applicationId;
    }

    String getFlowId() {
      return flowId;
    }

    String getFlowletId() {
      return flowletId;
    }

    String getName() {
      switch (level) {
        case Account:
          return name;
        case Flowlet:
          return name;
      }
      return name;
    }
  }

  /**
   * Enum for level of counter.
   */
  enum CounterLevel {
    Account(1, "Account"),
    Flowlet(2, "Flowlet");

    private final int id;
    private final String name;

    private CounterLevel(int id, String name) {
      this.id = id;
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
