package com.continuuity.performance.runner;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.metrics2.thrift.Counter;
import com.continuuity.metrics2.thrift.CounterRequest;
import com.continuuity.metrics2.thrift.FlowArgument;
import com.continuuity.metrics2.thrift.MetricsFrontendService;
import com.continuuity.performance.application.BenchmarkRuntimeMetrics;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.hsqldb.lib.StringUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runtime statistics of Reactor App during a performance test.
 */
public final class BenchmarkRuntimeStats {
  private static String ACCOUNT_ID = "developer";

  private static final MetricsFrontendService.Client metricsClient = getMetricsClient();

  /*
   * Read runtime metric counters for a given flowlet.
   */
  public static BenchmarkRuntimeMetrics getFlowletMetrics(final String applicationId,
                                                          final String flowId, final String flowletId) {
    final String inputName = String.format("%s.tuples.read.count", flowletId);
    final String processedName = String.format("%s.processed.count", flowletId);

    return new BenchmarkRuntimeMetrics() {
      @Override
      public long getInput() {
        Double input = getCounters(applicationId, flowId, flowletId).get(inputName);
        if (input == null) {
          return 0L;
        } else {
          return input.longValue();
        }
      }

      @Override
      public long getProcessed() {
        Double processed = getCounters(applicationId, flowId, flowletId).get(processedName);
        if (processed == null) {
          return 0L;
        } else {
          return processed.longValue();
        }
      }

      @Override
      public void waitForinput(long count, long timeout, TimeUnit timeoutUnit)
        throws TimeoutException, InterruptedException {
        waitFor(inputName, count, timeout, timeoutUnit);
      }

      @Override
      public void waitForProcessed(long count, long timeout, TimeUnit timeoutUnit)
        throws TimeoutException, InterruptedException {
        waitFor(processedName, count, timeout, timeoutUnit);
      }

      /*
       * Waits until metrics counter has reached given count number.
       * @param name Counter name
       * @param count Count to wait for
       * @param timeout Maximum time to wait for
       * @param timeoutUnit {@link java.util.concurrent.TimeUnit} for the timeout time.
       * @throws java.util.concurrent.TimeoutException if the timeout time passed and still not seeing that
       * many counts.
       */
      private void waitFor(String name, long count, long timeout, TimeUnit timeoutUnit)
        throws TimeoutException, InterruptedException {
        Double value = getCounters(applicationId, flowId, flowletId).get(name);
        while (timeout > 0 && (value == null || value.longValue() < count)) {
          timeoutUnit.sleep(1);
          value = getCounters(applicationId, flowId, flowletId).get(name);
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
   * @param applicationId Application id
   * @param flowName Flow name
   * @param flowletName Flowlet name
   * @param counterName Counter name
   * @param count Count to wait for
   * @param timeout Maximum time to wait for
   * @param timeoutUnit {@link java.util.concurrent.TimeUnit} for the timeout time.
   * @throws java.util.concurrent.TimeoutException if the timeout time passed and still not seeing that many counts.
   */
  @SuppressWarnings("unused")
  public static void waitForCounter(String applicationId, String flowName, String flowletName,
                                    String counterName, long count, long timeout, TimeUnit timeoutUnit)
    throws TimeoutException, InterruptedException {

    Counter c = getCounter(applicationId, flowName, flowletName, counterName);
    if (c == null) {
      throw new RuntimeException("No counter with name " + counterName + " found for application " + applicationId
                                   + " ,flow " + flowName + " and flowlet " + flowletName + ".");
    }
    double value = c.getValue();
    while (timeout > 0 && (value < count)) {
      timeoutUnit.sleep(1);
      value = getCounter(counterName).getValue();
      timeout--;
    }
    if (timeout == 0 && (value < count)) {
      throw new TimeoutException("Time limit reached.");
    }
  }

  /**
   * Waits until metrics counter has reached the given count number.
   * @param counterName Counter name
   * @param count Count to wait for
   * @param timeout Maximum time to wait for
   * @param timeoutUnit {@link java.util.concurrent.TimeUnit} for the timeout time.
   * @throws java.util.concurrent.TimeoutException if the timeout time passed and still not seeing that many count.
   */
  @SuppressWarnings("unused")
  public static void waitForCounter(String counterName, long count, long timeout, TimeUnit timeoutUnit)
    throws TimeoutException, InterruptedException {

    Counter c = getCounter(counterName);
    if (c == null) {
      throw new RuntimeException("No counter with name " + counterName + " found.");
    }
    double value = c.getValue();
    while (timeout > 0 && (value < count)) {
      timeoutUnit.sleep(1);
      value = getCounter(counterName).getValue();
      timeout--;
    }
    if (timeout == 0 && (value < count)) {
      throw new TimeoutException("Time limit reached.");
    }
  }

  /**
   * Gets metrics counter object for a given flowlet.
   * @param applicationId Application id
   * @param flowName Flow name
   * @param flowletName Flowlet name
   * @param counterName Counter name
   */
  public static Counter getCounter(String applicationId, String flowName, String flowletName,
                                   String counterName) {
    FlowArgument arg = new FlowArgument(ACCOUNT_ID, applicationId, flowName);
    try {
      List<Counter> counters = metricsClient.getCounters(new CounterRequest(arg));
      for (Counter counter : counters) {
        if (counter.getQualifier().equals(flowletName) && counter.getName().equals(counterName)) {
          return counter;
        }
      }
      return null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets metrics counter object for a given counter name.
   * @param counterName Counter name
   */
  public static Counter getCounter(String counterName) {
    FlowArgument arg = new FlowArgument("-", "-", "-");
    arg.setFlowletId("-");
    CounterRequest req = new CounterRequest(arg);
    req.setName(ImmutableList.of(counterName));
    try {
      List<Counter> counters = metricsClient.getCounters(req);
      if (counters != null && counters.size() != 0) {
        return counters.get(0);
      }
      return null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets map with metric counter values for all flowlets of a given flow.
   * @param applicationId Application id
   * @param flowName Flow name
   */
  @SuppressWarnings("unused")
  public static Map<String, Double> getCounters(String applicationId, String flowName) {
    return getCounters(applicationId, flowName, null);
  }

  /*
   * Gets map with metric counter values for a given flowlet or all flowlets of a given flow if name of flowlet
   * is null or an empty String.
   */
  private static Map<String, Double> getCounters(String applicationId, String flowName,
                                                 String flowletName) {
    FlowArgument arg = new FlowArgument(ACCOUNT_ID, applicationId, flowName);
    try {
      List<Counter> counters = metricsClient.getCounters(new CounterRequest(arg));
      Map<String, Double> counterMap = new HashMap<String, Double>(counters.size());
      if (StringUtil.isEmpty(flowletName)) {
        for (Counter counter : counters) {
          counterMap.put(counter.getQualifier() + "." + counter.getName(), counter.getValue());
        }
      } else {
        for (Counter counter : counters) {
          if (counter.getQualifier().equals(flowletName)) {
            counterMap.put(counter.getQualifier() + "." + counter.getName(), counter.getValue());
          }
        }
      }
      return counterMap;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /*
   * Gets metrics client based on current Reactor configuration.
   */
  private static MetricsFrontendService.Client getMetricsClient() {
    CConfiguration config = CConfiguration.create();
    try {
      return
        new MetricsFrontendService.Client(
          getThriftProtocol(config.get(Constants.CFG_METRICS_FRONTEND_SERVER_ADDRESS,
                                       Constants.DEFAULT_OVERLORD_SERVER_ADDRESS),
                            config.getInt(Constants.CFG_METRICS_FRONTEND_SERVER_PORT,
                                          Constants.DEFAULT_METRICS_FRONTEND_SERVER_PORT)));
    } catch (TTransportException e) {
      Throwables.propagate(e);
    }
    return null;
  }

  /*
   * Gets thrift protocol for communication with thrift server.
   */
  private static TProtocol getThriftProtocol(String serviceHost, int servicePort) throws TTransportException {
    TTransport transport = new TFramedTransport(new TSocket(serviceHost, servicePort));
    transport.open();
    // now try to connect the thrift client
    return new TBinaryProtocol(transport);
  }
}

