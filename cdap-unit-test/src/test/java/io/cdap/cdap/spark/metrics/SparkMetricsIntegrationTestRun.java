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

package io.cdap.cdap.spark.metrics;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.lib.cube.TimeValue;
import io.cdap.cdap.api.metrics.MetricDataQuery;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.XSlowTests;
import io.cdap.cdap.test.base.TestFrameworkTestBase;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Test Spark program metrics.
 */
@Category(XSlowTests.class)
public class SparkMetricsIntegrationTestRun extends TestFrameworkTestBase {
  @ClassRule
  public static final TestConfiguration CONFIG =
    new TestConfiguration(Constants.Metrics.SPARK_METRICS_ENABLED, true,
                          Constants.CLUSTER_NAME, "testCluster");

  @Test
  public void testSparkMetrics() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestSparkMetricsIntegrationApp.class);
    SparkManager sparkManager =
      applicationManager.getSparkManager(TestSparkMetricsIntegrationApp.APP_SPARK_NAME).start();
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 120, TimeUnit.SECONDS);
    List<RunRecord> history = sparkManager.getHistory(ProgramRunStatus.COMPLETED);
    Assert.assertEquals(1, history.size());

    // Wait for the metrics to get updated
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return getSparkMetric(TestSparkMetricsIntegrationApp.APP_NAME,
                              TestSparkMetricsIntegrationApp.APP_SPARK_NAME,
                              "system.driver.BlockManager.memory.remainingMem_MB") > 0;
      }
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    Tasks.waitFor(2L, new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return getSparkMetric(TestSparkMetricsIntegrationApp.APP_NAME,
                              TestSparkMetricsIntegrationApp.APP_SPARK_NAME,
                              "user.more.than.30");
      }
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  private long getSparkMetric(String applicationId, String sparkId, String metricName) throws Exception {
    Map<String, String> context = ImmutableMap.of(
      Constants.Metrics.Tag.NAMESPACE, NamespaceId.DEFAULT.getEntityName(),
      Constants.Metrics.Tag.APP, applicationId,
      Constants.Metrics.Tag.SPARK, sparkId);

    return getTotalCounter(context, metricName);
  }


  private long getTotalCounter(Map<String, String> context, String metricName) throws Exception {
    MetricDataQuery query = new MetricDataQuery(0, 0, Integer.MAX_VALUE, metricName, AggregationFunction.SUM,
                                                context, new ArrayList<String>());
    try {
      Collection<MetricTimeSeries> result = getMetricsManager().query(query);
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
}
