/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.datastreams.preview;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.app.preview.PreviewManager;
import co.cask.cdap.app.preview.PreviewRunner;
import co.cask.cdap.app.preview.PreviewStatus;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.datastreams.DataStreamsApp;
import co.cask.cdap.datastreams.DataStreamsSparkLauncher;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.spark.streaming.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.mock.transform.IdentityTransform;
import co.cask.cdap.etl.proto.v2.DataStreamsConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.artifact.preview.PreviewConfig;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for preview for data streams app
 */
public class PreviewDataStreamsTest extends HydratorTestBase {

  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("app", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("app", "1.0.0");
  private static int startCount = 0;
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);
  private static final String DATA_TRACER_PROPERTY = "records.out";

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    setupStreamingArtifacts(APP_ARTIFACT_ID, DataStreamsApp.class);
  }

  @Test
  public void testDataStreamsPreviewRun() throws Exception {
    PreviewManager previewManager = getPreviewManager();

    String sinkTableName = "singleOutput";
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    List<StructuredRecord> records = new ArrayList<>();
    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();
    StructuredRecord recordTest = StructuredRecord.builder(schema).set("name", "test").build();
    records.add(recordSamuel);
    records.add(recordBob);
    records.add(recordTest);

    /*
     * source --> transform -> sink
     */
    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(schema, records)))
      .addStage(new ETLStage("transform", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkTableName)))
      .addConnection("source", "transform")
      .addConnection("transform", "sink")
      .setNumOfRecordsPreview(100)
      .setBatchInterval("1s")
      .build();

    // Construct the preview config with the program name and program type, also, mark the mock table as a real dataset.
    // Otherwise, no data will be emitted in the preview run.
    PreviewConfig previewConfig = new PreviewConfig(DataStreamsSparkLauncher.NAME, ProgramType.SPARK,
                                                    Collections.<String>emptySet(),
                                                    Collections.<String, String>emptyMap(), 1);

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig, previewConfig);

    // Start the preview and get the corresponding PreviewRunner.
    ApplicationId previewId = previewManager.start(NamespaceId.DEFAULT, appRequest);
    final PreviewRunner previewRunner = previewManager.getRunner(previewId);

    // Wait for the preview to be running and wait until the records are processed in the sink.
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Map<String, List<JsonElement>> data = previewRunner.getData("sink");
        return data != null && data.get(DATA_TRACER_PROPERTY) != null && data.get(DATA_TRACER_PROPERTY).size() == 3;
      }
    }, 1, TimeUnit.MINUTES);

    // check data in source and transform
    checkPreviewStore(previewRunner, "source", 3);
    checkPreviewStore(previewRunner, "transform", 3);

    // Wait for the pipeline to be shutdown by timer.
    TimeUnit.MINUTES.sleep(1);
    Tasks.waitFor(PreviewStatus.Status.KILLED_BY_TIMER, new Callable<PreviewStatus.Status>() {
      @Override
      public PreviewStatus.Status call() throws Exception {
        return previewRunner.getStatus().getStatus();
      }
    }, 1, TimeUnit.MINUTES);

    // Validate the metrics for preview
    validateMetric(3, previewId, "source.records.out", previewRunner);
    validateMetric(3, previewId, "transform.records.in", previewRunner);
    validateMetric(3, previewId, "transform.records.out", previewRunner);
    validateMetric(3, previewId, "sink.records.in", previewRunner);
    validateMetric(3, previewId, "sink.records.out", previewRunner);

    // Check the sink table is not created in the real space.
    DataSetManager<Table> sinkManager = getDataset(sinkTableName);
    Assert.assertNull(sinkManager.get());
  }

  private void checkPreviewStore(PreviewRunner previewRunner, String tracerName, int expectedNumber) {
    Map<String, List<JsonElement>> result = previewRunner.getData(tracerName);
    Assert.assertTrue(!result.isEmpty());
    Assert.assertEquals(expectedNumber, result.get(DATA_TRACER_PROPERTY).size());
  }

  private void validateMetric(long expected, ApplicationId previewId,
                              String metric, PreviewRunner runner) throws TimeoutException, InterruptedException {
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, previewId.getNamespace(),
                                               Constants.Metrics.Tag.APP, previewId.getEntityName(),
                                               Constants.Metrics.Tag.SPARK, DataStreamsSparkLauncher.NAME);
    String metricName = "user." + metric;
    long value = getTotalMetric(tags, metricName, runner);

    // Min sleep time is 10ms, max sleep time is 1 seconds
    long sleepMillis = TimeUnit.SECONDS.toMillis(1);
    Stopwatch stopwatch = new Stopwatch().start();
    while (value < expected && stopwatch.elapsedTime(TimeUnit.SECONDS) < 5) {
      TimeUnit.MILLISECONDS.sleep(sleepMillis);
      value = getTotalMetric(tags, metricName, runner);
    }
    // wait for won't throw an exception if the metric count is greater than expected
    Assert.assertEquals(expected, value);
  }

  private long getTotalMetric(Map<String, String> tags, String metricName, PreviewRunner runner) {
    MetricDataQuery query = new MetricDataQuery(0, 0, Integer.MAX_VALUE, metricName, AggregationFunction.SUM,
                                                tags, new ArrayList<String>());
    Collection<MetricTimeSeries> result = runner.getMetricsQueryHelper().getMetricStore().query(query);
    if (result.isEmpty()) {
      return 0;
    }
    List<TimeValue> timeValues = result.iterator().next().getTimeValues();
    if (timeValues.isEmpty()) {
      return 0;
    }
    return timeValues.get(0).getValue();
  }
}
