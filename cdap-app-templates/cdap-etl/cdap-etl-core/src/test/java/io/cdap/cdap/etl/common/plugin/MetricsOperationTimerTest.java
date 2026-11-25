/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.etl.common.plugin;

import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultStageMetrics;
import io.cdap.cdap.etl.common.MockMetrics;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link MetricsOperationTimer}.
 */
public class MetricsOperationTimerTest {

  @Test(expected = IllegalStateException.class)
  public void testStopBeforeStartThrowsIllegalStateException() {
    MockMetrics mockMetrics = new MockMetrics();
    StageMetrics stageMetrics = new DefaultStageMetrics(mockMetrics, "stage1");
    MetricsOperationTimer timer = new MetricsOperationTimer(stageMetrics);
    timer.stop();
  }

  @Test
  public void testStartStopResetEmitsMetricsAndAllowsRestart() throws InterruptedException {
    MockMetrics mockMetrics = new MockMetrics();
    StageMetrics stageMetrics = new DefaultStageMetrics(mockMetrics, "stage1");
    MetricsOperationTimer timer = new MetricsOperationTimer(stageMetrics);

    timer.start();
    Thread.sleep(5L);
    timer.stop();
    timer.reset();

    String prefix = "stage1.";
    long firstTotal = mockMetrics.getCount(prefix + Constants.Metrics.TOTAL_TIME);
    Assert.assertTrue("Expected positive elapsed microseconds, got " + firstTotal, firstTotal > 0);
    Assert.assertEquals(firstTotal, mockMetrics.getGauge(prefix + Constants.Metrics.MAX_TIME));
    Assert.assertEquals(firstTotal, mockMetrics.getGauge(prefix + Constants.Metrics.AVG_TIME));

    // After reset(), stopwatch is unstarted again and can run another cycle
    timer.start();
    Thread.sleep(2L);
    timer.stop();
    timer.reset();

    long secondTotal = mockMetrics.getCount(prefix + Constants.Metrics.TOTAL_TIME);
    Assert.assertTrue(secondTotal > firstTotal);
  }
}
