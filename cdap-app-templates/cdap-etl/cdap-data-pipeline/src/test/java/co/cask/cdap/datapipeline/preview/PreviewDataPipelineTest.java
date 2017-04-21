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

package co.cask.cdap.datapipeline.preview;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
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
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.mock.transform.ExceptionTransform;
import co.cask.cdap.etl.mock.transform.IdentityTransform;
import co.cask.cdap.etl.proto.Engine;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
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
import com.google.common.collect.ImmutableList;
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
 * Test for preview for data pipeline app
 */
public class PreviewDataPipelineTest extends HydratorTestBase {

  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("app", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("app", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT_RANGE = new ArtifactSummary("app", "[0.1.0,1.1.0)");
  private static int startCount = 0;
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                                                                       Constants.Security.Store.PROVIDER, "file");
  private static final String DATA_TRACER_PROPERTY = "records.out";

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }
    setupBatchArtifacts(APP_ARTIFACT_ID, DataPipelineApp.class);
  }

  @Test
  public void testDataPipelinePreviewRuns() throws Exception {
    testDataPipelinePreviewRun(Engine.MAPREDUCE);
    testDataPipelinePreviewRun(Engine.SPARK);
  }

  private void testDataPipelinePreviewRun(Engine engine) throws Exception {
    PreviewManager previewManager = getPreviewManager();

    String sourceTableName = "singleInput";
    String sinkTableName = "singleOutput";
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    /*
     * source --> transform -> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceTableName, schema)))
      .addStage(new ETLStage("transform", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkTableName)))
      .addConnection("source", "transform")
      .addConnection("transform", "sink")
      .setEngine(engine)
      .setNumOfRecordsPreview(100)
      .build();


    // Construct the preview config with the program name and program type
    PreviewConfig previewConfig = new PreviewConfig(SmartWorkflow.NAME, ProgramType.WORKFLOW,
                                                    Collections.<String, String>emptyMap(), 10);

    // Create the table for the mock source
    addDatasetInstance(Table.class.getName(), sourceTableName,
                       DatasetProperties.of(ImmutableMap.of("schema", schema.toString())));
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(sourceTableName));
    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob));

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT_RANGE, etlConfig, previewConfig);

    // Start the preview and get the corresponding PreviewRunner.
    ApplicationId previewId = previewManager.start(NamespaceId.DEFAULT, appRequest);
    final PreviewRunner previewRunner = previewManager.getRunner(previewId);

    // Wait for the preview status go into COMPLETED.
    Tasks.waitFor(PreviewStatus.Status.COMPLETED, new Callable<PreviewStatus.Status>() {
      @Override
      public PreviewStatus.Status call() throws Exception {
        PreviewStatus status = previewRunner.getStatus();
        return status == null ? null : status.getStatus();
      }
    }, 5, TimeUnit.MINUTES);

    // Get the data for stage "source" in the PreviewStore, should contain two records.
    checkPreviewStore(previewRunner, "source", 2);

    // Get the data for stage "transform" in the PreviewStore, should contain two records.
    checkPreviewStore(previewRunner, "transform", 2);

    // Get the data for stage "sink" in the PreviewStore, should contain two records.
    checkPreviewStore(previewRunner, "sink", 2);

    // Validate the metrics for preview
    validateMetric(2, previewId, "source.records.in", previewRunner);
    validateMetric(2, previewId, "source.records.out", previewRunner);
    validateMetric(2, previewId, "transform.records.in", previewRunner);
    validateMetric(2, previewId, "transform.records.out", previewRunner);
    validateMetric(2, previewId, "sink.records.out", previewRunner);
    validateMetric(2, previewId, "sink.records.in", previewRunner);

    // Check the sink table is not created in the real space.
    DataSetManager<Table> sinkManager = getDataset(sinkTableName);
    Assert.assertNull(sinkManager.get());
    deleteDatasetInstance(NamespaceId.DEFAULT.dataset(sourceTableName));
  }

  @Test
  public void testPreviewFailedRun() throws Exception {
    testPreviewFailedRun(Engine.MAPREDUCE);
    testPreviewFailedRun(Engine.SPARK);
  }

  private void testPreviewFailedRun(Engine engine) throws Exception {
    PreviewManager previewManager = getPreviewManager();

    String sourceTableName = "singleInput";
    String sinkTableName = "singleOutput";
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    /*
     * source --> transform -> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceTableName, schema)))
      .addStage(new ETLStage("transform", ExceptionTransform.getPlugin("name", "samuel")))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkTableName)))
      .addConnection("source", "transform")
      .addConnection("transform", "sink")
      .setNumOfRecordsPreview(100)
      .setEngine(engine)
      .build();

    // Construct the preview config with the program name and program type.
    PreviewConfig previewConfig = new PreviewConfig(SmartWorkflow.NAME, ProgramType.WORKFLOW,
                                                    Collections.<String, String>emptyMap(), 10);

    // Create the table for the mock source
    addDatasetInstance(Table.class.getName(), sourceTableName,
                       DatasetProperties.of(ImmutableMap.of("schema", schema.toString())));
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(sourceTableName));
    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();
    MockSource.writeInput(inputManager, "1", recordSamuel);
    MockSource.writeInput(inputManager, "2", recordBob);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig, previewConfig);

    // Start the preview and get the corresponding PreviewRunner.
    ApplicationId previewId = previewManager.start(NamespaceId.DEFAULT, appRequest);
    final PreviewRunner previewRunner = previewManager.getRunner(previewId);

    // Wait for the preview status go into FAILED.
    Tasks.waitFor(PreviewStatus.Status.RUN_FAILED, new Callable<PreviewStatus.Status>() {
      @Override
      public PreviewStatus.Status call() throws Exception {
        PreviewStatus status = previewRunner.getStatus();
        return status == null ? null : status.getStatus();
      }
    }, 5, TimeUnit.MINUTES);

    // Get the data for stage "source" in the PreviewStore.
    checkPreviewStore(previewRunner, "source", 2);

    // Get the data for stage "transform" in the PreviewStore, should contain one less record than source.
    checkPreviewStore(previewRunner, "transform", 1);

    // Get the data for stage "sink" in the PreviewStore, should contain one less record than source.
    checkPreviewStore(previewRunner, "sink", 1);

    // Validate the metrics for preview
    validateMetric(2, previewId, "source.records.in", previewRunner);
    validateMetric(2, previewId, "source.records.out", previewRunner);
    validateMetric(2, previewId, "transform.records.in", previewRunner);
    validateMetric(1, previewId, "transform.records.out", previewRunner);
    validateMetric(1, previewId, "sink.records.out", previewRunner);
    validateMetric(1, previewId, "sink.records.in", previewRunner);

    // Check the sink table is not created in the real space.
    DataSetManager<Table> sinkManager = getDataset(sinkTableName);
    Assert.assertNull(sinkManager.get());
    deleteDatasetInstance(NamespaceId.DEFAULT.dataset(sourceTableName));
  }

  private void checkPreviewStore(PreviewRunner previewRunner, String tracerName, int numExpectedRecords) {
    Map<String, List<JsonElement>> result = previewRunner.getData(tracerName);
    List<JsonElement> data = result.get(DATA_TRACER_PROPERTY);
    if (data == null) {
      Assert.assertEquals(numExpectedRecords, 0);
    } else {
      Assert.assertEquals(numExpectedRecords, data.size());
    }
  }

  private void validateMetric(long expected, ApplicationId previewId,
                              String metric, PreviewRunner runner) throws TimeoutException, InterruptedException {
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, previewId.getNamespace(),
                                               Constants.Metrics.Tag.APP, previewId.getEntityName(),
                                               Constants.Metrics.Tag.WORKFLOW, SmartWorkflow.NAME);
    String metricName = "user." + metric;
    long value = getTotalMetric(tags, metricName, runner);

    // Min sleep time is 10ms, max sleep time is 1 seconds
    long sleepMillis = TimeUnit.SECONDS.toMillis(1);
    Stopwatch stopwatch = new Stopwatch().start();
    while (value < expected && stopwatch.elapsedTime(TimeUnit.SECONDS) < 20) {
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
