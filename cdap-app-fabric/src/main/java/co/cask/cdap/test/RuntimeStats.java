/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.test;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.program.TypeId;
import co.cask.cdap.proto.ProgramType;
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

  private static final String EMPTY_STRING = "";

  private static ConcurrentMap<String, AtomicLong> counters = Maps.newConcurrentMap();

  public static void resetAll() {
    counters.clear();
  }

  public static void count(String name, long count) {
    AtomicLong oldValue = counters.putIfAbsent(name, new AtomicLong(count));
    if (oldValue != null) {
      oldValue.addAndGet(count);
    }
  }

  public static RuntimeMetrics getFlowletMetrics(String applicationId, String flowId, String flowletId) {
    String prefix = String.format("%s.%s.f.%s.%s", Constants.DEFAULT_NAMESPACE, applicationId, flowId, flowletId);
    String inputName = String.format("%s.process.tuples.read", prefix);
    String processedName = String.format("%s.process.events.processed", prefix);
    String exceptionName = String.format("%s.process.errors", prefix);

    return getMetrics(prefix, inputName, processedName, exceptionName);
  }

  public static RuntimeMetrics getProcedureMetrics(String applicationId, String procedureId) {
    String prefix = String.format("%s.%s.p.%s", Constants.DEFAULT_NAMESPACE, applicationId, procedureId);
    String inputName = String.format("%s.query.requests", prefix);
    String processedName = String.format("%s.query.processed", prefix);
    String exceptionName = String.format("%s.query.failures", prefix);

    return getMetrics(prefix, inputName, processedName, exceptionName);
  }

  public static RuntimeMetrics getServiceMetrics(String applicationId, String serviceId) {
    String prefix = String.format("%s.%s.%s.%s", Constants.DEFAULT_NAMESPACE, applicationId,
                                  TypeId.getMetricContextId(ProgramType.SERVICE), serviceId);
    String inputName = String.format("%s.requests.count", prefix);
    String processedName = String.format("%s.response.successful.count", prefix);
    String exceptionName = String.format("%s.response.server.error.count", prefix);

    return getMetrics(prefix, inputName, processedName, exceptionName);
  }

  public static long getSparkMetrics(String applicationId, String procedureId, String keyEnding) {
    String keyStarting = String.format("%s.%s.%s.%s", Constants.DEFAULT_NAMESPACE, applicationId,
                                       TypeId.getMetricContextId(ProgramType.SPARK), procedureId);
    String inputName = getMetricsKey(keyStarting, keyEnding);
    AtomicLong input = counters.get(inputName);
    return input == null ? 0 : input.get();
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

  /**
   * Returns the metrics key having the given starting and ending parts. If no such key is found, returns an 
   * empty string.
   *
   * @param starting the starting part of the key
   * @param ending the ending part of the key
   * @return the complete key if found else an empyty string
   */
  public static String getMetricsKey(String starting, String ending) {
    for (String key : counters.keySet()) {
      if (key.startsWith(starting) && key.endsWith(ending)) {
        return key;
      }
    }
    return EMPTY_STRING;
  }
}
