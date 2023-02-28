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

package io.cdap.cdap.internal.events;

import io.cdap.cdap.common.MetricRetrievalException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.events.ExecutionMetrics;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for {@link SparkProgramStatusMetricsProvider}
 */
public class SparkProgramStatusMetricsProviderTest {

  private static SparkProgramStatusMetricsProvider metricsProvider;
  private final String mockedRunId = "6354a561-886c-11ec-8688-42010a8e0035";
  private final String mockedAttemptId = "1";
  private final String mockedApplicationJson = "mocked_spark_applications_response.json";
  private final String mockedStagesJson = "mocked_spark_stages_response.json";
  private final String mockedStagesSeveralStagesJson = "mocked_spark_stages_response_several_stages.json";
  private final ProgramRunId mockProgramRunId =
    new ProgramRunId("ns", "app", ProgramType.SPARK, "test", mockedRunId);

  @BeforeClass
  public static void setupClass() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set("spark.metrics.host", "http://mocked-sparkhistory:18080");
    metricsProvider = new SparkProgramStatusMetricsProvider(cConf, new NoOpMetricsCollectionService());
  }

  @Test
  public void testExtractAttemptId() {
    String responseStr = loadMockedResponseAsString(mockedApplicationJson);
    String attemptId = metricsProvider.extractAttemptId(responseStr, mockedRunId);
    Assert.assertEquals(attemptId, mockedAttemptId);
  }

  @Test
  public void testExtractMetrics() {
    String responseStr = loadMockedResponseAsString(mockedStagesJson);
    ExecutionMetrics[] metrics = metricsProvider.extractMetrics(responseStr);
    Assert.assertArrayEquals(metrics, getMockedMetrics());
  }

  @Test
  public void testExtractMetricsSeveralStages() {
    String responseStr = loadMockedResponseAsString(mockedStagesSeveralStagesJson);
    ExecutionMetrics[] metrics = metricsProvider.extractMetrics(responseStr);
    Assert.assertArrayEquals(metrics, getMockedMetricsSeveralStages());
  }

  @Test
  public void testRetriveMetricsFail() throws MetricRetrievalException {
    try {
      ExecutionMetrics[] metrics = metricsProvider.retrieveMetrics(mockProgramRunId);
      Assert.fail("Expected an Exception");
    } catch (MetricRetrievalException e) {
      //expected
    }
  }

  private String loadMockedResponseAsString(String mockedFile) {
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    InputStream responseIS = classLoader.getResourceAsStream(mockedFile);
    Assert.assertNotNull(responseIS);
    return new BufferedReader(new InputStreamReader(responseIS))
      .lines().collect(Collectors.joining(System.lineSeparator()));
  }

  private ExecutionMetrics[] getMockedMetrics() {
    return new ExecutionMetrics[]{
      new ExecutionMetrics(
        "0",
        10195,
        22,
        6046096,
        6237,
        0,
        0,
        0,
        0
      )};
  }

  private ExecutionMetrics[] getMockedMetricsSeveralStages() {
    return new ExecutionMetrics[]{
      new ExecutionMetrics("4", 0, 1354, 0, 275330, 2324, 285592, 0, 0),
      new ExecutionMetrics("1", 1162, 0, 132206, 0, 0, 0, 1162, 142796),
      new ExecutionMetrics("0", 1162, 0, 132206, 0, 0, 0, 1162, 142796)
    };
  }
}
