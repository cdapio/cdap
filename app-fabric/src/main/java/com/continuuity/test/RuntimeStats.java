package com.continuuity.test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public final class RuntimeStats {

  private static ConcurrentMap<String, AtomicLong> counters = Maps.newConcurrentMap();

  public static void count(String name, int count) {
    AtomicLong oldValue = counters.putIfAbsent(name, new AtomicLong(count));
    if (oldValue != null) {
      oldValue.addAndGet(count);
    }
  }

  public static RuntimeMetrics getFlowletMetrics(String applicationId, String flowId, String flowletId) {
    String prefix = String.format("%s.f.%s.%s", applicationId, flowId, flowletId);
    String inputName = String.format("%s.process.tuples.read", prefix);
    String processedName = String.format("%s.process.events.processed", prefix);
    String exceptionName = String.format("%s.process.errors", prefix);

    return getMetrics(prefix, inputName, processedName, exceptionName);
  }

  public static RuntimeMetrics getProcedureMetrics(String applicationId, String procedureId) {
    String prefix = String.format("%s.p.%s", applicationId, procedureId);
    String inputName = String.format("%s.query.requests", prefix);
    String processedName = String.format("%s.query.processed", prefix);
    String exceptionName = String.format("%s.query.failures", prefix);

    return getMetrics(prefix, inputName, processedName, exceptionName);
  }

  private static RuntimeMetrics getMetrics(final String prefix,
                                           final String inputName,
                                           final String processedName,
                                           final String exceptionName) {
    return new RuntimeMetrics() {
      @Override
      public long getInput() {
        AtomicLong input = counters.get(inputName);
        return input == null ? 0 : input.get();
      }

      @Override
      public long getProcessed() {
        AtomicLong processed = counters.get(processedName);
        return processed == null ? 0 : processed.get();

      }

      @Override
      public long getException() {
        AtomicLong exception = counters.get(exceptionName);
        return exception == null ? 0 : exception.get();
      }

      @Override
      public void waitForinput(long count, long timeout, TimeUnit timeoutUnit)
                                          throws TimeoutException, InterruptedException {
        doWaitFor(inputName, count, timeout, timeoutUnit);
      }

      @Override
      public void waitForProcessed(long count, long timeout, TimeUnit timeoutUnit)
                                          throws TimeoutException, InterruptedException {
        doWaitFor(processedName, count, timeout, timeoutUnit);
      }

      @Override
      public void waitForException(long count, long timeout, TimeUnit timeoutUnit)
                                          throws TimeoutException, InterruptedException {
        doWaitFor(exceptionName, count, timeout, timeoutUnit);
      }

      @Override
      public void waitFor(String name, long count,
                          long timeout, TimeUnit timeoutUnit) throws TimeoutException, InterruptedException {
        doWaitFor(prefix + "." + name, count, timeout, timeoutUnit);
      }

      private void doWaitFor(String name, long count, long timeout, TimeUnit timeoutUnit)
                                          throws TimeoutException, InterruptedException {
        AtomicLong value = counters.get(name);
        while (timeout > 0 && (value == null || value.get() < count)) {
          timeoutUnit.sleep(1);
          value = counters.get(name);
          timeout--;
        }

        if (timeout == 0 && (value == null || value.get() < count)) {
          throw new TimeoutException("Time limit reached.");
        }
      }

      @Override
      public String toString() {
        return String.format("%s; input=%d, processed=%d, exception=%d",
                             prefix, getInput(), getProcessed(), getException());
      }
    };
  }

  public static void clearStats(final String prefix) {
    Iterators.removeIf(counters.entrySet().iterator(), new Predicate<Map.Entry<String, AtomicLong>>() {
      @Override
      public boolean apply(Map.Entry<String, AtomicLong> input) {
        return input.getKey().startsWith(prefix);
      }
    });
  }

  private RuntimeStats() {
  }
}
