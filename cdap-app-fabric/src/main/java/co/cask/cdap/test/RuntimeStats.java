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

import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.api.metrics.TimeValue;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsConstants;
import co.cask.cdap.common.metrics.MetricsContexts;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.clearspring.analytics.util.Preconditions;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 *
 */
public final class RuntimeStats {

  // ugly attempt to suport existing APIs
  // todo: non-thread safe? or fine as long as in-memory datasets underneath are used?
  public static MetricStore metricStore;

  private RuntimeStats() {
  }

  public static void resetAll() throws Exception {
    metricStore.deleteBefore(System.currentTimeMillis() / 1000);
  }

  public static RuntimeMetrics getFlowletMetrics(String namespace, String applicationId,
                                                 String flowId, String flowletId) {
    Id.Program id = Id.Program.from(namespace, applicationId, ProgramType.FLOW, flowId);
    return getMetrics(MetricsContexts.forFlowlet(id, flowletId),
                      MetricsConstants.FLOWLET_INPUT, MetricsConstants.FLOWLET_PROCESSED,
                      MetricsConstants.FLOWLET_EXCEPTIONS);
  }

  public static RuntimeMetrics getFlowletMetrics(String applicationId, String flowId, String flowletId) {
    return getFlowletMetrics(Constants.DEFAULT_NAMESPACE, applicationId, flowId, flowletId);
  }

  public static RuntimeMetrics getProcedureMetrics(String namespace, String applicationId, String procedureId) {
    Id.Program id = Id.Program.from(namespace, applicationId, ProgramType.PROCEDURE, procedureId);
    return getMetrics(MetricsContexts.forProcedure(id),
                      MetricsConstants.PROCEDURE_INPUT, MetricsConstants.PROCEDURE_PROCESSED,
                      MetricsConstants.PROCEDURE_EXCEPTIONS);
  }

  public static RuntimeMetrics getProcedureMetrics(String applicationId, String procedureId) {
    return getProcedureMetrics(Constants.DEFAULT_NAMESPACE, applicationId, procedureId);
  }

  public static RuntimeMetrics getServiceMetrics(String namespace, String applicationId, String serviceId) {
    Id.Program id = Id.Program.from(namespace, applicationId, ProgramType.SERVICE, serviceId);
    return getMetrics(MetricsContexts.forService(id),
                      MetricsConstants.SERVICE_INPUT, MetricsConstants.SERVICE_PROCESSED,
                      MetricsConstants.SERVICE_EXCEPTIONS);
  }

  public static RuntimeMetrics getServiceMetrics(String applicationId, String serviceId) {
    return getServiceMetrics(Constants.DEFAULT_NAMESPACE, applicationId, serviceId);
  }

  @Deprecated
  public static void clearStats(final String applicationId) {
    try {
      // null for "all metric names"
      metricStore.delete(
        new MetricDeleteQuery(0, System.currentTimeMillis() / 1000, null,
                              ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE,
                                              Constants.Metrics.Tag.APP, applicationId)));
    } catch (Exception e) {
      // Should never happen in unit test
      throw Throwables.propagate(e);
    }
  }

  private static RuntimeMetrics getMetrics(final Map<String, String> context,
                                           final String inputName,
                                           final String processedName,
                                           @Nullable final String exceptionName) {
    return new RuntimeMetrics() {
      @Override
      public long getInput() {
        return getTotalCounter(context, inputName);
      }

      @Override
      public long getProcessed() {
        return getTotalCounter(context, processedName);
      }

      @Override
      public long getException() {
        Preconditions.checkArgument(exceptionName != null, "exception count not supported");
        return getTotalCounter(context, exceptionName);
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
        doWaitFor(name, count, timeout, timeoutUnit);
      }

      private void doWaitFor(String name, long count, long timeout, TimeUnit timeoutUnit)
                                          throws TimeoutException, InterruptedException {
        long value = getTotalCounter(context, name);

        // Min sleep time is 10ms, max sleep time is 1 seconds
        long sleepMillis = Math.max(10, Math.min(timeoutUnit.toMillis(timeout) / 10, TimeUnit.SECONDS.toMillis(1)));
        Stopwatch stopwatch = new Stopwatch().start();
        while (value < count && stopwatch.elapsedTime(timeoutUnit) < timeout) {
          TimeUnit.MILLISECONDS.sleep(sleepMillis);
          value = getTotalCounter(context, name);
        }

        if (value < count) {
          throw new TimeoutException("Time limit reached.");
        }
      }

      @Override
      public String toString() {
        return String.format("%s; input=%d, processed=%d, exception=%d",
                             Joiner.on(",").withKeyValueSeparator(":").join(context),
                             getInput(), getProcessed(), getException());
      }
    };
  }

  private static long getTotalCounter(Map<String, String> context, String metricName) {
    MetricDataQuery query = getTotalCounterQuery(context, metricName);
    try {
      Collection<MetricTimeSeries> result = metricStore.query(query);
      if (result.isEmpty()) {
        return 0;
      }
      // since it is totals query and not groupBy specified, we know there's one time series
      List<TimeValue> timeValues = result.iterator().next().getTimeValues();
      if (timeValues.isEmpty()) {
        return 0;
      }

      // since it is totals, we know there's one value only
      return timeValues.get(0).getValue();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static MetricDataQuery getTotalCounterQuery(Map<String, String> context, String metricName) {
    return new MetricDataQuery(0, 0, Integer.MAX_VALUE, metricName, MetricType.COUNTER,
                         context, new ArrayList<String>());
  }
}
