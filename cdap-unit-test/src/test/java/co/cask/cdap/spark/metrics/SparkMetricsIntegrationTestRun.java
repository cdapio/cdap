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

package co.cask.cdap.spark.metrics;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.program.TypeId;
import co.cask.cdap.metrics.store.cube.CubeExploreQuery;
import co.cask.cdap.metrics.store.cube.CubeQuery;
import co.cask.cdap.metrics.store.cube.TimeSeries;
import co.cask.cdap.metrics.store.timeseries.MeasureType;
import co.cask.cdap.metrics.store.timeseries.TagValue;
import co.cask.cdap.metrics.store.timeseries.TimeValue;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test Spark program metrics.
 */
@Category(XSlowTests.class)
public class SparkMetricsIntegrationTestRun extends TestFrameworkTestBase {

  public static final String METRICS_KEY = ".BlockManager.memory.remainingMem_MB";

  @Test
  public void testSparkMetrics() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestSparkMetricsIntegrationApp.class);
    SparkManager sparkManager = applicationManager.startSpark(TestSparkMetricsIntegrationApp.APP_SPARK_NAME);
    sparkManager.waitForFinish(120, TimeUnit.SECONDS);

    Assert.assertTrue(getSparkMetric(TestSparkMetricsIntegrationApp.APP_NAME,
                                     TestSparkMetricsIntegrationApp.APP_SPARK_NAME, METRICS_KEY) > 0);
    //TODO: Add test to check user metrics once the support is added: CDAP-765
  }

  private static long getSparkMetric(String applicationId, String sparkId, String metricName) throws Exception {
    Map<String, String> context = ImmutableMap.of(
      Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE,
      Constants.Metrics.Tag.APP, applicationId,
      Constants.Metrics.Tag.PROGRAM_TYPE, TypeId.getMetricContextId(ProgramType.SPARK),
      Constants.Metrics.Tag.PROGRAM, sparkId);

    return getTotalCounterByPrefix(context, metricName);
  }

  private static long getTotalCounterByPrefix(Map<String, String> context, String metricNameSuffix) throws Exception {
    CubeQuery query = getTotalCounterQuery(context);

    // todo: allow group by metric name when querying Cube instead
    String metricName = findMetricName(context, query.getStartTs(), query.getEndTs(),
                                       query.getResolution(), metricNameSuffix);
    query = new CubeQuery(query, metricName);

    try {
      Collection<TimeSeries> result = RuntimeStats.metricStore.query(query);
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

  private static String findMetricName(Map<String, String> context, long startTs, long endTs,
                                       int resolution, String metricNameSuffix) throws Exception {

    List<TagValue> tagValues = Lists.newArrayList();
    // note: we know the order is good
    for (Map.Entry<String, String> tagValue : context.entrySet()) {
      tagValues.add(new TagValue(tagValue.getKey(), tagValue.getValue()));
    }

    Collection<String> metricNames =
      RuntimeStats.metricStore.findMetricNames(
        new CubeExploreQuery(startTs, endTs, resolution, Integer.MAX_VALUE, tagValues));

    String metricName = null;
    for (String name : metricNames) {
      if (name.endsWith(metricNameSuffix)) {
        metricName = name;
        break;
      }
    }
    return metricName;
  }

  private static CubeQuery getTotalCounterQuery(Map<String, String> context) {
    return new CubeQuery(0, 0, Integer.MAX_VALUE, null, MeasureType.COUNTER, context, new ArrayList<String>());
  }

}
