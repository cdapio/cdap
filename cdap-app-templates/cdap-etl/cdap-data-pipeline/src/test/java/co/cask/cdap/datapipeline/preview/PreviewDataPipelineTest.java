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
import co.cask.cdap.api.dataset.table.Table;
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
import co.cask.cdap.etl.mock.transform.IdentityTransform;
import co.cask.cdap.etl.proto.Engine;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.artifact.preview.PreviewConfig;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonElement;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Test for preview for data pipeline app
 */
public class PreviewDataPipelineTest extends HydratorTestBase {

  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("app", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("app", "1.0.0");
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


    // Construct the preview config with the program name and program type, also, mark the mock table as a real dataset.
    // Otherwise, no data will be emitted in the preview run.
    PreviewConfig previewConfig = new PreviewConfig(SmartWorkflow.NAME, ProgramType.WORKFLOW,
                                                    ImmutableSet.of(sourceTableName),
                                                    Collections.<String, String>emptyMap(), 10);

    // Create the table for the mock source
    addDatasetInstance(Table.class.getName(), sourceTableName,
                       DatasetProperties.of(ImmutableMap.of("schema", schema.toString())));
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(sourceTableName));
    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob));

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig, previewConfig);

    // Start the preview and get the corresponding PreviewRunner.
    final PreviewRunner previewRunner = previewManager.getRunner(previewManager.start(NamespaceId.DEFAULT, appRequest));

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

    // Check the sink table is not created in the real space.
    DataSetManager<Table> sinkManager = getDataset(sinkTableName);
    Assert.assertNull(sinkManager.get());
    deleteDatasetInstance(NamespaceId.DEFAULT.dataset(sourceTableName));
  }

  private void checkPreviewStore(PreviewRunner previewRunner, String tracerName, int expectedNumber) {
    Map<String, List<JsonElement>> result = previewRunner.getData(tracerName);
    Assert.assertTrue(!result.isEmpty());
    Assert.assertEquals(expectedNumber, result.get(DATA_TRACER_PROPERTY).size());
  }
}
