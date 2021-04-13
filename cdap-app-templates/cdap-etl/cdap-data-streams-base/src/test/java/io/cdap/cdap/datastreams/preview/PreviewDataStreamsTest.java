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

package io.cdap.cdap.datastreams.preview;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.lib.cube.TimeValue;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.metrics.MetricDataQuery;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.app.preview.PreviewManager;
import io.cdap.cdap.app.preview.PreviewRunner;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.datastreams.DataStreamsApp;
import io.cdap.cdap.datastreams.DataStreamsSparkLauncher;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.spark.streaming.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.mock.transform.IdentityTransform;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.spark.Compat;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.preview.PreviewConfig;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
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
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                                                                       Constants.AppFabric.SPARK_COMPAT,
                                                                       Compat.SPARK_COMPAT);
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
      .setCheckpointDir("file://" + TMP_FOLDER.getRoot().toPath().toString())
      .build();

    // Construct the preview config with the program name and program type.
    PreviewConfig previewConfig = new PreviewConfig(DataStreamsSparkLauncher.NAME, ProgramType.SPARK,
                                                    Collections.<String, String>emptyMap(), 1);

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig, previewConfig);

    // Start the preview and get the corresponding PreviewRunner.
    ApplicationId previewId = previewManager.start(NamespaceId.DEFAULT, appRequest);

    // Wait for the preview to be running and wait until the records are processed in the sink.
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Map<String, List<JsonElement>> data = previewManager.getData(previewId, "sink");
        return data != null && data.get(DATA_TRACER_PROPERTY) != null && data.get(DATA_TRACER_PROPERTY).size() == 3;
      }
    }, 1, TimeUnit.MINUTES);

    // check data in source and transform
    checkPreviewStore(previewManager, previewId, "source", 3);
    checkPreviewStore(previewManager, previewId, "transform", 3);

    // Wait for the pipeline to be shutdown by timer.
    TimeUnit.MINUTES.sleep(1);
    Tasks.waitFor(PreviewStatus.Status.KILLED_BY_TIMER, new Callable<PreviewStatus.Status>() {
      @Override
      public PreviewStatus.Status call() throws Exception {
        return previewManager.getStatus(previewId).getStatus();
      }
    }, 1, TimeUnit.MINUTES);

    // Validate the metrics for preview
    validateMetric(3, previewId, "source.records.out", previewManager);
    validateMetric(3, previewId, "transform.records.in", previewManager);
    validateMetric(3, previewId, "transform.records.out", previewManager);
    validateMetric(3, previewId, "sink.records.in", previewManager);
    validateMetric(3, previewId, "sink.records.out", previewManager);

    // Check the sink table is not created in the real space.
    DataSetManager<Table> sinkManager = getDataset(sinkTableName);
    Assert.assertNull(sinkManager.get());
  }

  private void checkPreviewStore(PreviewManager previewManager, ApplicationId previewId, String tracerName,
                                 int expectedNumber) throws NotFoundException {
    Map<String, List<JsonElement>> result = previewManager.getData(previewId, tracerName);
    Assert.assertTrue(!result.isEmpty());
    Assert.assertEquals(expectedNumber, result.get(DATA_TRACER_PROPERTY).size());
  }

  private void validateMetric(long expected, ApplicationId previewId,
                              String metric, PreviewManager previewManager)
    throws TimeoutException, InterruptedException {
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, previewId.getNamespace(),
                                               Constants.Metrics.Tag.APP, previewId.getEntityName(),
                                               Constants.Metrics.Tag.SPARK, DataStreamsSparkLauncher.NAME);
    String metricName = "user." + metric;
    long value = getTotalMetric(tags, metricName, previewManager);

    // Min sleep time is 10ms, max sleep time is 1 seconds
    long sleepMillis = TimeUnit.SECONDS.toMillis(1);
    Stopwatch stopwatch = new Stopwatch().start();
    while (value < expected && stopwatch.elapsedTime(TimeUnit.SECONDS) < 5) {
      TimeUnit.MILLISECONDS.sleep(sleepMillis);
      value = getTotalMetric(tags, metricName, previewManager);
    }
    // wait for won't throw an exception if the metric count is greater than expected
    Assert.assertEquals(expected, value);
  }

  private long getTotalMetric(Map<String, String> tags, String metricName, PreviewManager previewManager) {
    MetricDataQuery query = new MetricDataQuery(0, 0, Integer.MAX_VALUE, metricName, AggregationFunction.SUM,
                                                tags, new ArrayList<String>());
    Collection<MetricTimeSeries> result = previewManager.getMetricsQueryHelper().getMetricStore().query(query);
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
