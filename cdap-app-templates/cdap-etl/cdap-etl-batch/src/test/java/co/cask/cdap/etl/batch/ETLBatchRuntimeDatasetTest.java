/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.batch;

import co.cask.cdap.etl.mock.batch.MockRuntimeDatasetSink;
import co.cask.cdap.etl.mock.batch.MockRuntimeDatasetSource;
import co.cask.cdap.etl.proto.Engine;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for runtime dataset creation of batch plugins.
 */
public class ETLBatchRuntimeDatasetTest extends ETLBatchTestBase {

  @Test
  public void testMacrosMapReducePipeline() throws Exception {
    /*
     * Trivial MapReduce pipeline from batch source to batch sink.
     *
     * source --------- sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockRuntimeDatasetSource.getPlugin("mrinput", "${runtime${source}}")))
      .addStage(new ETLStage("sink", MockRuntimeDatasetSink.getPlugin("mroutput", "${runtime}${sink}")))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MRApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // set runtime arguments for macro substitution
    Map<String, String> runtimeArguments = ImmutableMap.<String, String>of("runtime", "mockRuntime",
                                                                           "sink", "MRSinkDataset",
                                                                           "source", "Source",
                                                                           "runtimeSource",
                                                                           "mockRuntimeMRSourceDataset");

    // make sure the datasets don't exist beforehand
    Assert.assertNull(getDataset("mockRuntimeMRSourceDataset").get());
    Assert.assertNull(getDataset("mockRuntimeMRSinkDataset").get());

    WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    workflowManager.setRuntimeArgs(runtimeArguments);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    // now the datasets should exist
    Assert.assertNotNull(getDataset("mockRuntimeMRSourceDataset").get());
    Assert.assertNotNull(getDataset("mockRuntimeMRSinkDataset").get());
  }

  /**
   * Tests that if macros are provided
   */
  @Test
  public void testMacrosSparkPipeline() throws Exception {
    /*
     * Trivial Spark pipeline from batch source to batch sink.
     *
     * source --------- sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setEngine(Engine.SPARK)
      .addStage(new ETLStage("source", MockRuntimeDatasetSource.getPlugin("sparkinput", "${runtime${source}}")))
      .addStage(new ETLStage("sink", MockRuntimeDatasetSink.getPlugin("sparkoutput", "${runtime}${sink}")))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SparkApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // set runtime arguments for macro substitution
    Map<String, String> runtimeArguments = ImmutableMap.<String, String>of("runtime", "mockRuntime",
                                                                           "sink", "SparkSinkDataset",
                                                                           "source", "Source",
                                                                           "runtimeSource",
                                                                           "mockRuntimeSparkSourceDataset");

    WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    workflowManager.setRuntimeArgs(runtimeArguments);

    // make sure the datasets don't exist beforehand
    Assert.assertNull(getDataset("mockRuntimeSparkSourceDataset").get());
    Assert.assertNull(getDataset("mockRuntimeSparkSinkDataset").get());

    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    // now the datasets should exist
    Assert.assertNotNull(getDataset("mockRuntimeSparkSourceDataset").get());
    Assert.assertNotNull(getDataset("mockRuntimeSparkSinkDataset").get());
  }


  /**
   * Tests that if no macro is provided to the dataset name property, datasets will be created at config time.
   */
  @Test
  public void testNoMacroMapReduce() throws Exception {
    /*
     * Trivial MapReduce pipeline from batch source to batch sink.
     *
     * source --------- sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockRuntimeDatasetSource.getPlugin("mrinput", "configTimeMockSourceDataset")))
      .addStage(new ETLStage("sink", MockRuntimeDatasetSink.getPlugin("mroutput", "configTimeMockSinkDataset")))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MRApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // set runtime arguments for macro substitution
    Map<String, String> runtimeArguments = ImmutableMap.<String, String>of("runtime", "mockRuntime",
                                                                           "sink", "SinkDataset",
                                                                           "source", "Source",
                                                                           "runtimeSource", "mockRuntimeSourceDataset");

    WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);

    // make sure the datasets were created at configure time
    Assert.assertNotNull(getDataset("configTimeMockSourceDataset").get());
    Assert.assertNotNull(getDataset("configTimeMockSinkDataset").get());

    workflowManager.setRuntimeArgs(runtimeArguments);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
  }
}
