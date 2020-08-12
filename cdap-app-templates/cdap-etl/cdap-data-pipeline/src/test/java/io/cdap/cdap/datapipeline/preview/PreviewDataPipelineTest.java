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

package io.cdap.cdap.datapipeline.preview;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.lib.cube.TimeValue;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.metrics.MetricDataQuery;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.app.preview.PreviewManager;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.batch.joiner.MockJoiner;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.mock.transform.ExceptionTransform;
import io.cdap.cdap.etl.mock.transform.IdentityTransform;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
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
import javax.annotation.Nullable;

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
                                                                       Constants.Security.Store.PROVIDER, "file",
                                                                       Constants.AppFabric.SPARK_COMPAT,
                                                                       Compat.SPARK_COMPAT);
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
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
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

    // Wait for the preview status go into COMPLETED.
    Tasks.waitFor(PreviewStatus.Status.COMPLETED, new Callable<PreviewStatus.Status>() {
      @Override
      public PreviewStatus.Status call() throws Exception {
        PreviewStatus status = previewManager.getStatus(previewId);
        return status == null ? null : status.getStatus();
      }
    }, 5, TimeUnit.MINUTES);

    // Get the data for stage "source" in the PreviewStore, should contain two records.
    checkPreviewStore(previewManager, previewId, "source", 2);

    // Get the data for stage "transform" in the PreviewStore, should contain two records.
    checkPreviewStore(previewManager, previewId, "transform", 2);

    // Get the data for stage "sink" in the PreviewStore, should contain two records.
    checkPreviewStore(previewManager, previewId, "sink", 2);

    // Validate the metrics for preview
    validateMetric(2, previewId, "source.records.in", previewManager);
    validateMetric(2, previewId, "source.records.out", previewManager);
    validateMetric(2, previewId, "transform.records.in", previewManager);
    validateMetric(2, previewId, "transform.records.out", previewManager);
    validateMetric(2, previewId, "sink.records.out", previewManager);
    validateMetric(2, previewId, "sink.records.in", previewManager);

    // Check the sink table is not created in the real space.
    DataSetManager<Table> sinkManager = getDataset(sinkTableName);
    Assert.assertNull(sinkManager.get());
    deleteDatasetInstance(NamespaceId.DEFAULT.dataset(sourceTableName));
    Assert.assertNotNull(previewManager.getRunId(previewId));
  }

  @Test
  public void testPreviewStop() throws Exception {
    testDataPipelinePreviewStop(Engine.MAPREDUCE, null);
    testDataPipelinePreviewStop(Engine.SPARK, null);
    testDataPipelinePreviewStop(Engine.MAPREDUCE, 60000L);
    testDataPipelinePreviewStop(Engine.SPARK, 60000L);
  }

  private void testDataPipelinePreviewStop(Engine engine, @Nullable Long sleepInMillis) throws Exception {
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
    ETLPlugin sourcePlugin = sleepInMillis == null ? MockSource.getPlugin(sourceTableName, schema)
      : MockSource.getPlugin(sourceTableName, schema, sleepInMillis);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", sourcePlugin))
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

    if (sleepInMillis != null) {
      // Wait for the preview status go into RUNNING.
      Tasks.waitFor(PreviewStatus.Status.RUNNING, () -> {
        PreviewStatus status = previewManager.getStatus(previewId);
        return status == null ? null : status.getStatus();
      }, 5, TimeUnit.MINUTES);
    }

    previewManager.stopPreview(previewId);
    // Wait for the preview status go into KILLED.
    Tasks.waitFor(PreviewStatus.Status.KILLED, () -> {
      PreviewStatus status = previewManager.getStatus(previewId);
      return status == null ? null : status.getStatus();
    }, 5, TimeUnit.MINUTES);

    // Check the sink table is not created in the real space.
    DataSetManager<Table> sinkManager = getDataset(sinkTableName);
    Assert.assertNull(sinkManager.get());
    deleteDatasetInstance(NamespaceId.DEFAULT.dataset(sourceTableName));
    Assert.assertTrue(previewManager.getRunId(previewId) == null || sleepInMillis != null);
  }

  @Test
  public void testMultiPhasePreview() throws Exception {
    testMultiplePhase(Engine.MAPREDUCE);
    testMultiplePhase(Engine.SPARK);
  }

  private void testMultiplePhase(Engine engine) throws Exception {
    /*
     * source1 ----> t1 ------
     *                        | --> innerjoin ----> t4 ------
     * source2 ----> t2 ------                                 |
     *                                                         | ---> outerjoin --> sink1
     *                                                         |
     * source3 -------------------- t3 ------------------------
     */

    PreviewManager previewManager = getPreviewManager();

    Schema inputSchema1 = Schema.recordOf(
      "customerRecord",
      Schema.Field.of("customer_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("customer_name", Schema.of(Schema.Type.STRING))
    );

    Schema inputSchema2 = Schema.recordOf(
      "itemRecord",
      Schema.Field.of("item_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item_price", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("cust_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("cust_name", Schema.of(Schema.Type.STRING))
    );

    Schema inputSchema3 = Schema.recordOf(
      "transactionRecord",
      Schema.Field.of("t_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("c_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("i_id", Schema.of(Schema.Type.STRING))
    );

    Schema outSchema2 = Schema.recordOf(
      "join.output",
      Schema.Field.of("t_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("c_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("i_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("customer_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("customer_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("item_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("item_price", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("cust_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("cust_name", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    String source1MulitJoinInput = "multiJoinSource1-" + engine;
    String source2MultiJoinInput = "multiJoinSource2-" + engine;
    String source3MultiJoinInput = "multiJoinSource3-" + engine;
    String outputName = "multiJoinOutput-" + engine;
    String sinkName = "multiJoinOutputSink-" + engine;
    String outerJoinName = "multiJoinOuter-" + engine;
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source1", MockSource.getPlugin(source1MulitJoinInput, inputSchema1)))
      .addStage(new ETLStage("source2", MockSource.getPlugin(source2MultiJoinInput, inputSchema2)))
      .addStage(new ETLStage("source3", MockSource.getPlugin(source3MultiJoinInput, inputSchema3)))
      .addStage(new ETLStage("t1", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t2", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t3", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t4", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("innerjoin", MockJoiner.getPlugin("t1.customer_id=t2.cust_id",
                                                               "t1,t2", "")))
      .addStage(new ETLStage(outerJoinName, MockJoiner.getPlugin("t4.item_id=t3.i_id",
                                                                 "", "")))
      .addStage(new ETLStage(sinkName, MockSink.getPlugin(outputName)))
      .addConnection("source1", "t1")
      .addConnection("source2", "t2")
      .addConnection("source3", "t3")
      .addConnection("t1", "innerjoin")
      .addConnection("t2", "innerjoin")
      .addConnection("innerjoin", "t4")
      .addConnection("t3", outerJoinName)
      .addConnection("t4", outerJoinName)
      .addConnection(outerJoinName, sinkName)
      .setEngine(engine)
      .setNumOfRecordsPreview(100)
      .build();

    // Construct the preview config with the program name and program type
    PreviewConfig previewConfig = new PreviewConfig(SmartWorkflow.NAME, ProgramType.WORKFLOW,
                                                    Collections.<String, String>emptyMap(), 10);

    // Create the table for the mock source
    addDatasetInstance(Table.class.getName(), source1MulitJoinInput,
                       DatasetProperties.of(ImmutableMap.of("schema", inputSchema1.toString())));
    addDatasetInstance(Table.class.getName(), source2MultiJoinInput,
                       DatasetProperties.of(ImmutableMap.of("schema", inputSchema2.toString())));
    addDatasetInstance(Table.class.getName(), source3MultiJoinInput,
                       DatasetProperties.of(ImmutableMap.of("schema", inputSchema3.toString())));

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig, previewConfig);
    // Start the preview and get the corresponding PreviewRunner.
    ApplicationId previewId = previewManager.start(NamespaceId.DEFAULT, appRequest);

    ingestData(inputSchema1, inputSchema2, inputSchema3, source1MulitJoinInput, source2MultiJoinInput,
               source3MultiJoinInput);

    // Wait for the preview status go into COMPLETED.
    Tasks.waitFor(PreviewStatus.Status.COMPLETED, new Callable<PreviewStatus.Status>() {
      @Override
      public PreviewStatus.Status call() throws Exception {
        PreviewStatus status = previewManager.getStatus(previewId);
        return status == null ? null : status.getStatus();
      }
    }, 5, TimeUnit.MINUTES);


    checkPreviewStore(previewManager, previewId, sinkName, 3);
    validateMetric(3L, previewId, sinkName + ".records.in", previewManager);
  }

  private void ingestData(Schema inputSchema1, Schema inputSchema2, Schema inputSchema3, String source1MulitJoinInput,
                          String source2MultiJoinInput, String source3MultiJoinInput) throws Exception {
    StructuredRecord recordSamuel = StructuredRecord.builder(inputSchema1).set("customer_id", "1")
      .set("customer_name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(inputSchema1).set("customer_id", "2")
      .set("customer_name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(inputSchema1).set("customer_id", "3")
      .set("customer_name", "jane").build();

    StructuredRecord recordCar = StructuredRecord.builder(inputSchema2).set("item_id", "11").set("item_price", 10000L)
      .set("cust_id", "1").set("cust_name", "samuel").build();
    StructuredRecord recordBike = StructuredRecord.builder(inputSchema2).set("item_id", "22").set("item_price", 100L)
      .set("cust_id", "3").set("cust_name", "jane").build();

    StructuredRecord recordTrasCar = StructuredRecord.builder(inputSchema3).set("t_id", "1").set("c_id", "1")
      .set("i_id", "11").build();
    StructuredRecord recordTrasBike = StructuredRecord.builder(inputSchema3).set("t_id", "2").set("c_id", "3")
      .set("i_id", "22").build();
    StructuredRecord recordTrasPlane = StructuredRecord.builder(inputSchema3).set("t_id", "3").set("c_id", "4")
      .set("i_id", "33").build();

    // write one record to each source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(source1MulitJoinInput));
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob, recordJane));
    inputManager = getDataset(NamespaceId.DEFAULT.dataset(source2MultiJoinInput));
    MockSource.writeInput(inputManager, ImmutableList.of(recordCar, recordBike));
    inputManager = getDataset(NamespaceId.DEFAULT.dataset(source3MultiJoinInput));
    MockSource.writeInput(inputManager, ImmutableList.of(recordTrasCar, recordTrasBike, recordTrasPlane));
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
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
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

    // Wait for the preview status go into FAILED.
    Tasks.waitFor(PreviewStatus.Status.RUN_FAILED, new Callable<PreviewStatus.Status>() {
      @Override
      public PreviewStatus.Status call() throws Exception {
        PreviewStatus status = previewManager.getStatus(previewId);
        return status == null ? null : status.getStatus();
      }
    }, 5, TimeUnit.MINUTES);

    // Get the data for stage "source" in the PreviewStore.
    checkPreviewStore(previewManager, previewId, "source", 2);

    // Get the data for stage "transform" in the PreviewStore, should contain one less record than source.
    checkPreviewStore(previewManager, previewId, "transform", 1);

    // Get the data for stage "sink" in the PreviewStore, should contain one less record than source.
    checkPreviewStore(previewManager, previewId, "sink", 1);

    // Validate the metrics for preview
    validateMetric(2, previewId, "source.records.in", previewManager);
    validateMetric(2, previewId, "source.records.out", previewManager);
    validateMetric(2, previewId, "transform.records.in", previewManager);
    validateMetric(1, previewId, "transform.records.out", previewManager);
    validateMetric(1, previewId, "sink.records.out", previewManager);
    validateMetric(1, previewId, "sink.records.in", previewManager);

    // Check the sink table is not created in the real space.
    DataSetManager<Table> sinkManager = getDataset(sinkTableName);
    Assert.assertNull(sinkManager.get());
    deleteDatasetInstance(NamespaceId.DEFAULT.dataset(sourceTableName));
  }

  private void checkPreviewStore(PreviewManager previewManager, ApplicationId previewId, String tracerName,
                                 int numExpectedRecords) throws Exception {
    Tasks.waitFor(numExpectedRecords, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        Map<String, List<JsonElement>> result = previewManager.getData(previewId, tracerName);
        List<JsonElement> data = result.get(DATA_TRACER_PROPERTY);
        if (data == null) {
          return 0;
        } else {
          return data.size();
        }
      }
    }, 60, TimeUnit.SECONDS);
  }

  private void validateMetric(long expected, ApplicationId previewId,
                              String metric, PreviewManager previewManager)
    throws TimeoutException, InterruptedException {
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, previewId.getNamespace(),
                                               Constants.Metrics.Tag.APP, previewId.getEntityName(),
                                               Constants.Metrics.Tag.WORKFLOW, SmartWorkflow.NAME);
    String metricName = "user." + metric;
    long value = getTotalMetric(tags, metricName, previewManager);

    // Min sleep time is 10ms, max sleep time is 1 seconds
    long sleepMillis = TimeUnit.SECONDS.toMillis(1);
    Stopwatch stopwatch = new Stopwatch().start();
    while (value < expected && stopwatch.elapsedTime(TimeUnit.SECONDS) < 20) {
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
