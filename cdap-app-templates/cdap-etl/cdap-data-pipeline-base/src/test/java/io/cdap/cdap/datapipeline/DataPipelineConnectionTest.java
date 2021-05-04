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
 *
 */

package io.cdap.cdap.datapipeline;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.connection.ConnectionCreationRequest;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.spark.Compat;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test for data pipeline using connections
 */
public class DataPipelineConnectionTest extends HydratorTestBase {
  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.SYSTEM.artifact("cdap-data-pipeline", "6.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("cdap-data-pipeline", "6.0.0",
                                                                          ArtifactScope.SYSTEM);
  private static final Gson GSON = new Gson();

  private static int startCount = 0;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                                                                       Constants.Security.Store.PROVIDER, "file",
                                                                       Constants.AppFabric.SPARK_COMPAT,
                                                                       Compat.SPARK_COMPAT);

  private static ServiceManager serviceManager;
  private static ApplicationManager appManager;
  static URI serviceURI;

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }
    setupBatchArtifacts(APP_ARTIFACT_ID, DataPipelineApp.class);

    enableCapability("pipeline");
    ApplicationId pipeline = NamespaceId.SYSTEM.app("pipeline");
    appManager = getApplicationManager(pipeline);
    waitForAppToDeploy(appManager, pipeline);
    serviceManager = appManager.getServiceManager(io.cdap.cdap.etl.common.Constants.STUDIO_SERVICE_NAME);
    serviceManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 2, TimeUnit.MINUTES);
    serviceURI = serviceManager.getServiceURL(1, TimeUnit.MINUTES).toURI();
  }

  @Test
  public void testUsingConnections() throws Exception {
    testUsingConnections(Engine.SPARK);
    testUsingConnections(Engine.MAPREDUCE);
  }

  private void testUsingConnections(Engine engine) throws Exception {
    String sourceConnName = "sourceConn" + engine;
    String sinkConnName = "sinkConn" + engine;
    String srcTableName = "src" + engine;
    String sinkTableName = "sink" + engine;

    addConnection(
      sourceConnName, new ConnectionCreationRequest(
        "", new PluginInfo("test", "dummy", null, Collections.singletonMap("tableName", srcTableName),
                           new ArtifactSelectorConfig())));
    addConnection(
      sinkConnName, new ConnectionCreationRequest(
        "", new PluginInfo("test", "dummy", null, Collections.singletonMap("tableName", sinkTableName),
                           new ArtifactSelectorConfig())));

    // source -> sink
    ETLBatchConfig config = ETLBatchConfig.builder()
                              .setEngine(engine)
                              .addStage(new ETLStage("source", MockSource.getPluginUsingConnection(sourceConnName)))
                              .addStage(new ETLStage("sink", MockSink.getPluginUsingConnection(sinkConnName)))
                              .addConnection("source", "sink")
                              .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("testApp" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    Schema schema = Schema.recordOf("x", Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    StructuredRecord samuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord dwayne = StructuredRecord.builder(schema).set("name", "dwayne").build();

    // add the dataset by the test, the source won't create it since table name is macro enabled
    addDatasetInstance(NamespaceId.DEFAULT.dataset(srcTableName), Table.TYPE);
    DataSetManager<Table> sourceTable = getDataset(srcTableName);
    MockSource.writeInput(sourceTable, ImmutableList.of(samuel, dwayne));

    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    DataSetManager<Table> sinkTable = getDataset(sinkTableName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(sinkTable);
    Assert.assertEquals(ImmutableSet.of(dwayne, samuel), new HashSet<>(outputRecords));

    // modify the connection to use a new table name for source and sink
    String newSrcTableName = "new" + srcTableName;
    String newSinkTableName = "new" + sinkTableName;
    addConnection(
      sourceConnName, new ConnectionCreationRequest(
        "", new PluginInfo("test", "dummy", null, Collections.singletonMap("tableName", newSrcTableName),
                           new ArtifactSelectorConfig())));
    addConnection(
      sinkConnName, new ConnectionCreationRequest(
        "", new PluginInfo("test", "dummy", null, Collections.singletonMap("tableName", newSinkTableName),
                           new ArtifactSelectorConfig())));

    addDatasetInstance(NamespaceId.DEFAULT.dataset(newSrcTableName), Table.TYPE);
    StructuredRecord newRecord1 = StructuredRecord.builder(schema).set("name", "john").build();
    StructuredRecord newRecord2 = StructuredRecord.builder(schema).set("name", "tom").build();
    sourceTable = getDataset(newSrcTableName);
    MockSource.writeInput(sourceTable, ImmutableList.of(newRecord1, newRecord2));

    // run the program again, it should use the new table to read and write
    manager.start();
    manager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 3, TimeUnit.MINUTES);

    sinkTable = getDataset(newSinkTableName);
    outputRecords = MockSink.readOutput(sinkTable);
    Assert.assertEquals(ImmutableSet.of(newRecord1, newRecord2), new HashSet<>(outputRecords));
  }

  private void addConnection(String connection, ConnectionCreationRequest creationRequest) throws IOException {
    URL validatePipelineURL =
      serviceURI.resolve(String.format("v1/contexts/%s/connections/%s", NamespaceId.DEFAULT.getNamespace(),
                                       connection)).toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.PUT, validatePipelineURL)
                            .withBody(GSON.toJson(creationRequest))
                            .build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    Assert.assertEquals(200, response.getResponseCode());
  }
}
