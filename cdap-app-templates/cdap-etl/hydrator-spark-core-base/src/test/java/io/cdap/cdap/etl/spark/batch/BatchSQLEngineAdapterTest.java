/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.batch;

import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.etl.api.StageMetrics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class BatchSQLEngineAdapterTest {

  StageMetrics stageMetrics;
  Map<Long, Integer> invocationCounts;

  @Before
  public void setUp() {
    invocationCounts = new HashMap<>();

    stageMetrics = new StageMetrics() {
      @Override
      public void count(String metricName, int delta) {
        throw new UnsupportedOperationException("not implemented");
      }

      @Override
      public void countLong(String metricName, long delta) {
        invocationCounts.compute(delta, (k, v) -> (v == null) ? 1 : v + 1);
      }

      @Override
      public void gauge(String metricName, long value) {
        throw new UnsupportedOperationException("not implemented");
      }

      @Override
      public void pipelineCount(String metricName, int delta) {
        throw new UnsupportedOperationException("not implemented");
      }

      @Override
      public void pipelineGauge(String metricName, long value) {
        throw new UnsupportedOperationException("not implemented");
      }

      @Override
      public Metrics child(Map<String, String> tags) {
        throw new UnsupportedOperationException("not implemented");
      }

      @Override
      public Map<String, String> getTags() {
        throw new UnsupportedOperationException("not implemented");
      }
    };
  }

  @Test
  public void testCountStageMetrics() {
    // Answer should be INTEGER.MAX_VALUE called 2 times, 123456 called 1 time.
    long count = (long) Integer.MAX_VALUE + (long) Integer.MAX_VALUE + (long) 123456;

    BatchSQLEngineAdapter.countStageMetrics(stageMetrics, "some-metric", count);
    Assert.assertEquals(1, invocationCounts.size());
    Assert.assertEquals(1, (int) invocationCounts.get(count));

    BatchSQLEngineAdapter.countStageMetrics(stageMetrics, "some-metric", 123456);
    Assert.assertEquals(2, invocationCounts.size());
    Assert.assertEquals(1, (int) invocationCounts.get(count));
    Assert.assertEquals(1, (int) invocationCounts.get(123456L));

    BatchSQLEngineAdapter.countStageMetrics(stageMetrics, "some-metric", 123456);
    Assert.assertEquals(2, invocationCounts.size());
    Assert.assertEquals(1, (int) invocationCounts.get(count));
    Assert.assertEquals(2, (int) invocationCounts.get(123456L));

    BatchSQLEngineAdapter.countStageMetrics(stageMetrics, "some-metric", 9876);
    Assert.assertEquals(3, invocationCounts.size());
    Assert.assertEquals(1, (int) invocationCounts.get(count));
    Assert.assertEquals(2, (int) invocationCounts.get(123456L));
    Assert.assertEquals(1, (int) invocationCounts.get(9876L));

    BatchSQLEngineAdapter.countStageMetrics(stageMetrics, "some-metric", 0);
    Assert.assertEquals(4, invocationCounts.size());
    Assert.assertEquals(1, (int) invocationCounts.get(count));
    Assert.assertEquals(2, (int) invocationCounts.get(123456L));
    Assert.assertEquals(1, (int) invocationCounts.get(9876L));
    Assert.assertEquals(1, (int) invocationCounts.get(0L));
  }
}
