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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.app.preview.PreviewManager;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.connector.FileConnector;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.mock.transform.IdentityTransform;
import io.cdap.cdap.etl.mock.transform.PluginValidationTransform;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.connection.ConnectionCreationRequest;
import io.cdap.cdap.etl.proto.connection.ConnectorDetail;
import io.cdap.cdap.etl.proto.connection.PluginDetail;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.cdap.etl.proto.connection.SampleResponse;
import io.cdap.cdap.etl.proto.connection.SampleResponseCodec;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.spark.Compat;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.preview.PreviewConfig;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.SearchResponse;
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
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Test for data pipeline using connections
 */
public class DataPipelineConnectionTest extends HydratorTestBase {
  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.SYSTEM.artifact("cdap-data-pipeline", "6.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("cdap-data-pipeline", "6.0.0",
                                                                          ArtifactScope.SYSTEM);
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(SampleResponse.class, new SampleResponseCodec()).setPrettyPrinting().create();

  private static int startCount = 0;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                                                                       Constants.Security.Store.PROVIDER, "file",
                                                                       Constants.AppFabric.SPARK_COMPAT,
                                                                       Compat.SPARK_COMPAT);

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

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
    serviceManager.startAndWaitForGoodRun(ProgramRunStatus.RUNNING, 2, TimeUnit.MINUTES);
    serviceURI = serviceManager.getServiceURL(1, TimeUnit.MINUTES).toURI();
  }

  @Test
  public void testBrowseSample() throws Exception {
    List<BrowseEntity> entities = new ArrayList<>();
    // add directory and files, odd to create file, even to create folder
    File directory = TEMP_FOLDER.newFolder();
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        File folder = new File(directory, "file" + i);
        folder.mkdir();
        entities.add(BrowseEntity.builder(folder.getName(), folder.getCanonicalPath(), "directory")
                       .canSample(true).canBrowse(true).build());
        continue;
      }

      // generate file with 100 lines
      File file = new File(directory, "file" + i + ".txt");
      try (BufferedWriter writer = Files.newBufferedWriter(file.toPath())) {
        for (int j = 0; j < 100; j++) {
          writer.write(i + "");
          if (j != 99) {
            writer.newLine();
          }
        }
      }
      entities.add(BrowseEntity.builder(file.getName(), file.getCanonicalPath(), "file").canSample(true).build());
    }

    String conn = "BrowseSample";
    addConnection(
      conn, new ConnectionCreationRequest(
        "", new PluginInfo(
        FileConnector.NAME, Connector.PLUGIN_TYPE, null, Collections.emptyMap(),
        // in set up we add "-mocks" as the suffix for the artifact id
        new ArtifactSelectorConfig("system", APP_ARTIFACT_ID.getArtifact() + "-mocks",
                                   APP_ARTIFACT_ID.getVersion()))));

    // get all 10 results back
    BrowseDetail browseDetail = browseConnection(conn, directory.getCanonicalPath(), 10);
    BrowseDetail expected = BrowseDetail.builder().setTotalCount(10).setEntities(entities).build();
    Assert.assertEquals(expected, browseDetail);

    // only retrieve 5 back, count should still be 10
    browseDetail = browseConnection(conn, directory.getCanonicalPath(), 5);
    expected = BrowseDetail.builder().setTotalCount(10).setEntities(entities.subList(0, 5)).build();
    Assert.assertEquals(expected, browseDetail);

    // browse the created directory, should give empty result
    browseDetail = browseConnection(conn, entities.get(0).getPath(), 10);
    expected = BrowseDetail.builder().setTotalCount(0).build();
    Assert.assertEquals(expected, browseDetail);

    // browse the file, since it is not browsable, it should return itself
    browseDetail = browseConnection(conn, entities.get(1).getPath(), 10);
    expected = BrowseDetail.builder().setTotalCount(1).addEntity(entities.get(1)).build();
    Assert.assertEquals(expected, browseDetail);

    List<StructuredRecord> records = new ArrayList<>();
    Schema schema = Schema.recordOf(
      "schema",
      Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    for (int i = 0; i < 100; i++) {
      records.add(StructuredRecord.builder(schema).set("offset", i * 2L).set("body", "1").build());
    }
    ArtifactSelectorConfig artifact = new ArtifactSelectorConfig("SYSTEM",
                                                                 APP_ARTIFACT_ID.getArtifact() + "-mocks",
                                                                 APP_ARTIFACT_ID.getVersion());
    Map<String, String> properties = ImmutableMap.of("path", entities.get(1).getPath(),
                                                     "useConnection", "true",
                                                     "connection", String.format("${conn(%s)}", conn));
    ConnectorDetail detail = new ConnectorDetail(
      ImmutableSet.of(new PluginDetail("file", "batchsource", properties, artifact, schema),
                      new PluginDetail("file", "streamingsource", properties, artifact, schema)));
    SampleResponse expectedSample = new SampleResponse(detail, schema, records);

    // sample the file, the file has 100 lines, so 200 should retrieve all lines
    SampleResponse sampleResponse = sampleConnection(conn, entities.get(1).getPath(), 200);
    Assert.assertEquals(expectedSample, sampleResponse);

    // sample 100, should get all
    sampleResponse = sampleConnection(conn, entities.get(1).getPath(), 100);
    Assert.assertEquals(expectedSample, sampleResponse);

    // sample 50, should only get 50
    sampleResponse = sampleConnection(conn, entities.get(1).getPath(), 50);
    expectedSample = new SampleResponse(detail, schema, records.subList(0, 50));
    Assert.assertEquals(expectedSample, sampleResponse);

    deleteConnection(conn);
  }

  @Test
  public void testConnectionsRegistry() throws Exception {
    // source -> sink
    ETLBatchConfig conf1 = ETLBatchConfig.builder()
                             .addStage(new ETLStage("source", MockSource.getPluginUsingConnection("conn 1")))
                             .addStage(new ETLStage("sink", MockSink.getPluginUsingConnection("conn 3")))
                             .addConnection("source", "sink")
                             .build();

    // 3 sources -> identity -> 2 sinks
    ETLBatchConfig conf2 = ETLBatchConfig.builder()
                             .addStage(new ETLStage("src1", MockSource.getPluginUsingConnection("conn 1")))
                             .addStage(new ETLStage("src2", MockSource.getPluginUsingConnection("conn 2")))
                             .addStage(new ETLStage("src3", MockSource.getPluginUsingConnection("conn 3")))
                             .addStage(new ETLStage("sink1", MockSink.getPluginUsingConnection("conn 4")))
                             .addStage(new ETLStage("sink2", MockSink.getPluginUsingConnection("conn 5")))
                             .addStage(new ETLStage("identity", IdentityTransform.getPlugin()))
                             .addConnection("src1", "identity")
                             .addConnection("src2", "identity")
                             .addConnection("src3", "identity")
                             .addConnection("identity", "sink1")
                             .addConnection("identity", "sink2")
                             .build();

    // deploy apps
    AppRequest<ETLBatchConfig> appRequest1 = new AppRequest<>(APP_ARTIFACT, conf1);
    ApplicationId appId1 = NamespaceId.DEFAULT.app("app1");
    ApplicationManager appManager1 = deployApplication(appId1, appRequest1);

    AppRequest<ETLBatchConfig> appRequest2 = new AppRequest<>(APP_ARTIFACT, conf2);
    ApplicationId appId2 = NamespaceId.DEFAULT.app("app2");
    ApplicationManager appManager2 = deployApplication(appId2, appRequest2);

    // Assert metadata
    Metadata app1Actual = getMetadataAdmin().getMetadata(appId1.toMetadataEntity(), MetadataScope.SYSTEM);
    Set<String> app1ExpectedTags = ImmutableSet.of("_conn_1", "_conn_3");
    // here assert actual tags contain all the tags about connections
    Assert.assertTrue(app1Actual.getTags(MetadataScope.SYSTEM).containsAll(app1ExpectedTags));
    // user metadata should be empty
    Assert.assertEquals(Metadata.EMPTY, getMetadataAdmin().getMetadata(appId1.toMetadataEntity(), MetadataScope.USER));

    Metadata app2Actual = getMetadataAdmin().getMetadata(appId2.toMetadataEntity(), MetadataScope.SYSTEM);
    Set<String> app2ExpectedTags = ImmutableSet.of("_conn_1", "_conn_2", "_conn_3", "_conn_4", "_conn_5");
    // here assert actual tags contain all the tags about connections
    Assert.assertTrue(app2Actual.getTags(MetadataScope.SYSTEM).containsAll(app2ExpectedTags));
    // user metadata should be empty
    Assert.assertEquals(Metadata.EMPTY, getMetadataAdmin().getMetadata(appId2.toMetadataEntity(), MetadataScope.USER));

    // using search query to find out the related apps
    Set<MetadataEntity> appsRelated = ImmutableSet.of(appId1.toMetadataEntity(), appId2.toMetadataEntity());
    assertMetadataSearch(appsRelated, "tags:_conn_1");
    assertMetadataSearch(Collections.singleton(appId2.toMetadataEntity()), "tags:_conn_2");
    assertMetadataSearch(appsRelated, "tags:_conn_3");
    assertMetadataSearch(Collections.singleton(appId2.toMetadataEntity()), "tags:_conn_4");
    assertMetadataSearch(Collections.singleton(appId2.toMetadataEntity()), "tags:_conn_5");
  }

  private void assertMetadataSearch(Set<MetadataEntity> appsRelated, String query) throws Exception {
    SearchResponse search = getMetadataAdmin().search(SearchRequest.of(query).build());
    Set<MetadataEntity> actual =
      search.getResults().stream().map(record -> record.getEntity()).collect(Collectors.toSet());
    Assert.assertEquals(appsRelated, actual);
  }

  @Test
  public void testUsingConnections() throws Exception {
    testUsingConnections(Engine.SPARK);
    testUsingConnections(Engine.MAPREDUCE);
  }

  private void testUsingConnections(Engine engine) throws Exception {
    String sourceConnName = "sourceConn " + engine;
    String sinkConnName = "sinkConn " + engine;
    String srcTableName = "src" + engine;
    String sinkTableName = "sink" + engine;

    // add some bad json object to the property
    addConnection(
      sourceConnName, new ConnectionCreationRequest(
        "", new PluginInfo("test", "dummy", null, ImmutableMap.of("tableName", "${srcTable}",
                                                                  "key1", "${badval}"),
                           new ArtifactSelectorConfig())));
    addConnection(
      sinkConnName, new ConnectionCreationRequest(
        "", new PluginInfo("test", "dummy", null, ImmutableMap.of("tableName", "${sinkTable}",
                                                                  "key1", "${badval}"),
                           new ArtifactSelectorConfig())));
    // add json string to the runtime arguments to ensure plugin can get instantiated under such condition
    Map<String, String> runtimeArguments = ImmutableMap.of("badval", "{\"a\" : 1}", "srcTable", srcTableName,
                                                           "sinkTable", sinkTableName);

    // source -> sink
    ETLBatchConfig config = ETLBatchConfig.builder()
                              .setEngine(engine)
                              .addStage(new ETLStage("source", MockSource.getPluginUsingConnection(sourceConnName)))
                              .addStage(new ETLStage("sink", MockSink.getPluginUsingConnection(sinkConnName)))
                              .addConnection("source", "sink")
                              .build();

    Schema schema = Schema.recordOf("x", Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    StructuredRecord samuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord dwayne = StructuredRecord.builder(schema).set("name", "dwayne").build();

    // add the dataset by the test, the source won't create it since table name is macro enabled
    addDatasetInstance(NamespaceId.DEFAULT.dataset(srcTableName), Table.class.getName());
    DataSetManager<Table> sourceTable = getDataset(srcTableName);
    MockSource.writeInput(sourceTable, ImmutableList.of(samuel, dwayne));

    // verify preview can run successfully using connections
    PreviewManager previewManager = getPreviewManager();
    PreviewConfig previewConfig = new PreviewConfig(SmartWorkflow.NAME, ProgramType.WORKFLOW,
                                                    runtimeArguments, 10);
    // Start the preview and get the corresponding PreviewRunner.
    ApplicationId previewId = previewManager.start(NamespaceId.DEFAULT,
                                                   new AppRequest<>(APP_ARTIFACT, config, previewConfig));

    // Wait for the preview status go into COMPLETED.
    Tasks.waitFor(PreviewStatus.Status.COMPLETED, new Callable<PreviewStatus.Status>() {
      @Override
      public PreviewStatus.Status call() throws Exception {
        PreviewStatus status = previewManager.getStatus(previewId);
        return status == null ? null : status.getStatus();
      }
    }, 5, TimeUnit.MINUTES);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("testApp" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // start the actual pipeline run
    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.startAndWaitForGoodRun(runtimeArguments, ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    DataSetManager<Table> sinkTable = getDataset(sinkTableName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(sinkTable);
    Assert.assertEquals(ImmutableSet.of(dwayne, samuel), new HashSet<>(outputRecords));

    // in 6.5 we cannot update the connections, so update the runtime arguments so the connection value will get
    // updated
    String newSrcTableName = "new" + srcTableName;
    String newSinkTableName = "new" + sinkTableName;
    runtimeArguments = ImmutableMap.of("badval", "{\"a\" : 1}", "srcTable", newSrcTableName,
                                       "sinkTable", newSinkTableName);

    addDatasetInstance(NamespaceId.DEFAULT.dataset(newSrcTableName), Table.class.getName());
    StructuredRecord newRecord1 = StructuredRecord.builder(schema).set("name", "john").build();
    StructuredRecord newRecord2 = StructuredRecord.builder(schema).set("name", "tom").build();
    sourceTable = getDataset(newSrcTableName);
    MockSource.writeInput(sourceTable, ImmutableList.of(newRecord1, newRecord2));

    // run the program again, it should use the new table to read and write
    manager.start(runtimeArguments);
    manager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 3, TimeUnit.MINUTES);

    sinkTable = getDataset(newSinkTableName);
    outputRecords = MockSink.readOutput(sinkTable);
    Assert.assertEquals(ImmutableSet.of(newRecord1, newRecord2), new HashSet<>(outputRecords));

    deleteConnection(sourceConnName);
    deleteConnection(sinkConnName);

    deleteDatasetInstance(NamespaceId.DEFAULT.dataset(srcTableName));
    deleteDatasetInstance(NamespaceId.DEFAULT.dataset(sinkTableName));
    deleteDatasetInstance(NamespaceId.DEFAULT.dataset(newSrcTableName));
    deleteDatasetInstance(NamespaceId.DEFAULT.dataset(newSinkTableName));
  }

  @Test
  public void testUsingConnectionsWithPluginMacros() throws Exception {
    testConnectionsWithPluginMacros(Engine.SPARK);
    testConnectionsWithPluginMacros(Engine.MAPREDUCE);
  }

  private void testConnectionsWithPluginMacros(Engine engine) throws Exception {
    String sourceConnName = "sourceConnPluginMacros " + engine;
    String transformConnName = "transformConnPluginMacros " + engine;
    String sinkConnName = "sinkConnPluginMacros " + engine;
    String srcTableName = "srcPluginMacros" + engine;
    String sinkTableName = "sinkPluginMacros" + engine;

    addConnection(
      sourceConnName, new ConnectionCreationRequest(
        "", new PluginInfo("test", "dummy", null, Collections.singletonMap("tableName", "${srcTable}"),
                           new ArtifactSelectorConfig())));
    addConnection(
      transformConnName, new ConnectionCreationRequest(
        "", new PluginInfo("test", "dummy", null, ImmutableMap.of("plugin1", "${plugin1}",
                                                                  "plugin1Type", "${plugin1Type}"),
                           new ArtifactSelectorConfig())));
    addConnection(
      sinkConnName, new ConnectionCreationRequest(
        "", new PluginInfo("test", "dummy", null, Collections.singletonMap("tableName", "${sinkTable}"),
                           new ArtifactSelectorConfig())));

    // source -> pluginValidation transform -> sink
    ETLBatchConfig config = ETLBatchConfig.builder()
                              .setEngine(engine)
                              .addStage(new ETLStage("source", MockSource.getPluginUsingConnection(sourceConnName)))
                              .addStage(new ETLStage(
                                "transform",
                                PluginValidationTransform.getPluginUsingConnection(
                                  transformConnName, "${plugin2}", "${plugin2Type}")))
                              .addStage(new ETLStage("sink", MockSink.getPluginUsingConnection(sinkConnName)))
                              .addConnection("source", "transform")
                              .addConnection("transform", "sink")
                              .build();

    // runtime arguments
    Map<String, String> runtimeArguments = ImmutableMap.<String, String>builder().put("srcTable", srcTableName)
                                             .put("sinkTable", sinkTableName)
                                             .put("plugin1", "Identity")
                                             .put("plugin1Type", Transform.PLUGIN_TYPE)
                                             .put("plugin2", "Double")
                                             .put("plugin2Type", Transform.PLUGIN_TYPE).build();

    Schema schema = Schema.recordOf("x", Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    StructuredRecord samuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord dwayne = StructuredRecord.builder(schema).set("name", "dwayne").build();

    addDatasetInstance(NamespaceId.DEFAULT.dataset(srcTableName), Table.class.getName());
    DataSetManager<Table> sourceTable = getDataset(srcTableName);
    MockSource.writeInput(sourceTable, ImmutableList.of(samuel, dwayne));

    // verify preview can run successfully using connections
    PreviewManager previewManager = getPreviewManager();
    PreviewConfig previewConfig = new PreviewConfig(SmartWorkflow.NAME, ProgramType.WORKFLOW,
                                                    runtimeArguments, 10);
    // Start the preview and get the corresponding PreviewRunner.
    ApplicationId previewId = previewManager.start(NamespaceId.DEFAULT,
                                                   new AppRequest<>(APP_ARTIFACT, config, previewConfig));

    // Wait for the preview status go into COMPLETED.
    Tasks.waitFor(PreviewStatus.Status.COMPLETED, new Callable<PreviewStatus.Status>() {
      @Override
      public PreviewStatus.Status call() throws Exception {
        PreviewStatus status = previewManager.getStatus(previewId);
        return status == null ? null : status.getStatus();
      }
    }, 5, TimeUnit.MINUTES);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("testConnectionsWithPluginMacros" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // start the actual pipeline run
    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.startAndWaitForGoodRun(runtimeArguments, ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    DataSetManager<Table> sinkTable = getDataset(sinkTableName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(sinkTable);
    Assert.assertEquals(ImmutableSet.of(dwayne, samuel), new HashSet<>(outputRecords));

    deleteConnection(sourceConnName);
    deleteConnection(sinkConnName);
    deleteConnection(transformConnName);

    deleteDatasetInstance(NamespaceId.DEFAULT.dataset(srcTableName));
    deleteDatasetInstance(NamespaceId.DEFAULT.dataset(sinkTableName));
  }

  private void addConnection(String connection, ConnectionCreationRequest creationRequest) throws IOException {
    String url = URLEncoder.encode(
      String.format("v1/contexts/%s/connections/%s", NamespaceId.DEFAULT.getNamespace(), connection),
      StandardCharsets.UTF_8.name());
    URL validatePipelineURL = serviceURI.resolve(url).toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.PUT, validatePipelineURL)
                            .withBody(GSON.toJson(creationRequest))
                            .build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    Assert.assertEquals(200, response.getResponseCode());
  }

  private void deleteConnection(String connection) throws IOException {
    String url = URLEncoder.encode(
      String.format("v1/contexts/%s/connections/%s", NamespaceId.DEFAULT.getNamespace(),
                    connection), StandardCharsets.UTF_8.name());
    URL validatePipelineURL = serviceURI.resolve(url).toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.DELETE, validatePipelineURL).build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    Assert.assertEquals(200, response.getResponseCode());
  }

  private BrowseDetail browseConnection(String connection, String path, int limit) throws IOException {
    String url = URLEncoder.encode(
      String.format("v1/contexts/%s/connections/%s/browse", NamespaceId.DEFAULT.getNamespace(),
                    connection), StandardCharsets.UTF_8.name());
    URL validatePipelineURL =
      serviceURI.resolve(String.format("%s?path=%s&limit=%s", url, path, limit)).toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.POST, validatePipelineURL)
                            .withBody(GSON.toJson(BrowseRequest.builder(path).setLimit(limit).build()))
                            .build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), BrowseDetail.class);
  }

  private SampleResponse sampleConnection(String connection, String path, int limit) throws IOException {
    String url = URLEncoder.encode(
      String.format("v1/contexts/%s/connections/%s/sample", NamespaceId.DEFAULT.getNamespace(),
                    connection), StandardCharsets.UTF_8.name());
    URL validatePipelineURL =
      serviceURI.resolve(String.format("%s?path=%s&limit=%s", url, path, limit)).toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.POST, validatePipelineURL)
                            .withBody(GSON.toJson(SampleRequest.builder(limit).setPath(path).build()))
                            .build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), SampleResponse.class);
  }
}
