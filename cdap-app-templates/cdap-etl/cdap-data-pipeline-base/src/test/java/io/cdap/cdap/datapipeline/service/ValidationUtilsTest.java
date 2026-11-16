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

package io.cdap.cdap.datapipeline.service;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.api.service.http.HttpServiceHandlerSpecification;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.api.service.worker.SystemAppTaskContext;
import io.cdap.cdap.etl.api.PipelineConfigurable;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ConnectionConfig;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.proto.v2.validation.StageSchema;
import io.cdap.cdap.etl.proto.v2.validation.StageValidationRequest;
import io.cdap.cdap.etl.proto.v2.validation.StageValidationResponse;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests for ValidationUtils
 */
public class ValidationUtilsTest {

  private static final FeatureFlagsProvider MOCK_FEATURE_FLAGS_PROVIDER =
      new FeatureFlagsProvider() { };

  @Test
  public void testValidateNoException() {
    Schema testSchema = Schema.recordOf("a",
        Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
    StageSchema testStageSchema = new StageSchema("source", testSchema);
    ETLStage etlStage = new ETLStage("source", MockSource.getPlugin("testtable"));
    StageValidationRequest validationRequest = new StageValidationRequest(
      etlStage, Collections
      .singletonList(testStageSchema), false);
    StageValidationResponse stageValidationResponse = ValidationUtils
      .validate(NamespaceId.DEFAULT.getNamespace(),
                validationRequest, getPluginConfigurer(MockSource.PLUGIN_CLASS),
                properties -> properties, MOCK_FEATURE_FLAGS_PROVIDER);
    Assert.assertTrue(stageValidationResponse.getFailures().isEmpty());
  }

  @Test
  public void testValidateException() {
    Schema testSchema = Schema.recordOf("a",
        Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
    StageSchema testStageSchema = new StageSchema("source", testSchema);
    ETLStage etlStage = new ETLStage("source", MockSource.getPlugin(null));
    StageValidationRequest validationRequest = new StageValidationRequest(
      etlStage, Collections
      .singletonList(testStageSchema), false);
    StageValidationResponse stageValidationResponse = ValidationUtils
      .validate(NamespaceId.DEFAULT.getNamespace(),
                validationRequest, getPluginConfigurer(MockSource.PLUGIN_CLASS),
                properties -> properties, MOCK_FEATURE_FLAGS_PROVIDER);
    Assert.assertEquals(1, stageValidationResponse.getFailures().size());
  }

  @Test
  public void testMacroSubstitutionNotReturned() {
    Schema testSchema = Schema.recordOf("a",
        Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
    StageSchema testStageSchema = new StageSchema("source", testSchema);
    ETLStage etlStage = new ETLStage("source", MockSource.getPlugin("@{tName}"));
    StageValidationRequest validationRequest = new StageValidationRequest(
      etlStage, Collections
      .singletonList(testStageSchema), false);
    String testtable = "testtable";
    StageValidationResponse stageValidationResponse = ValidationUtils
      .validate(NamespaceId.DEFAULT.getNamespace(), validationRequest,
                getPluginConfigurer(MockSource.PLUGIN_CLASS), properties -> {
        Map<String, String> propertiesCopy = new HashMap<>(properties);
        if (propertiesCopy.getOrDefault("tableName", "").equals("@{tName}")) {
          propertiesCopy.put("tableName", testtable);
        }
        return propertiesCopy;
      }, MOCK_FEATURE_FLAGS_PROVIDER);
    Assert.assertTrue(stageValidationResponse.getFailures().isEmpty());
    Assert.assertEquals("@{tName}",
        stageValidationResponse.getSpec().getPlugin().getProperties().get("tableName"));
  }

  @Test
  public void testRemoteValidationTaskPreservesNonAsciiCharacters() throws Exception {
    Gson gson = new GsonBuilder()
        .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
        .serializeNulls()
        .create();
    String nonAsciiTable = "parttwo_samlet_købere_total_gads_ä_é";
    Schema testSchema = Schema.recordOf("a",
        Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
    StageSchema testStageSchema = new StageSchema("source", testSchema);
    ETLStage etlStage = new ETLStage("source", MockSource.getPlugin(nonAsciiTable));
    StageValidationRequest validationRequest = new StageValidationRequest(
        etlStage, Collections.singletonList(testStageSchema), false);
    RemoteValidationRequest remoteRequest = new RemoteValidationRequest(
        NamespaceId.DEFAULT.getNamespace(), gson.toJson(validationRequest));

    SystemAppTaskContext systemAppTaskContext = Mockito.mock(SystemAppTaskContext.class);
    Mockito.when(systemAppTaskContext.createPluginConfigurer(Mockito.anyString()))
        .thenReturn(getPluginConfigurer(MockSource.PLUGIN_CLASS));
    Mockito.when(systemAppTaskContext.evaluateMacros(
        Mockito.anyString(), Mockito.anyMap(), Mockito.any(), Mockito.any()))
        .thenAnswer(invocation -> invocation.getArgument(1));

    RunnableTaskRequest taskRequest = RunnableTaskRequest.getBuilder(
        RemoteValidationTask.class.getName()).withParam(gson.toJson(remoteRequest)).build();
    RunnableTaskContext context = new RunnableTaskContext(taskRequest, systemAppTaskContext);

    new RemoteValidationTask().run(context);

    String responseJson = StandardCharsets.UTF_8.decode(context.getResult()).toString();
    StageValidationResponse response = gson.fromJson(responseJson, StageValidationResponse.class);
    Assert.assertTrue(response.getFailures().isEmpty());
    Assert.assertEquals(nonAsciiTable,
        response.getSpec().getPlugin().getProperties().get("tableName"));
  }

  @Test
  public void testValidateDisabledPluginRejectedBeforeInstantiation() {
    ETLStage etlStage = new ETLStage("source", MockSource.getPlugin("testtable"));
    StageValidationRequest validationRequest = new StageValidationRequest(
        etlStage, Collections.emptyList(), false);

    PluginConfigurer pluginConfigurer = Mockito.mock(PluginConfigurer.class);
    AtomicBoolean macroEvaluated = new AtomicBoolean(false);
    StageValidationResponse response = ValidationUtils.validate(
        NamespaceId.DEFAULT.getNamespace(), validationRequest, pluginConfigurer,
        properties -> {
          macroEvaluated.set(true);
          return properties;
        },
        MOCK_FEATURE_FLAGS_PROVIDER,
        ImmutableSet.of(MockSource.PLUGIN_CLASS.getName().toUpperCase()));

    Assert.assertNull(response.getSpec());
    Assert.assertEquals(1, response.getFailures().size());
    Assert.assertTrue(response.getFailures().get(0).getMessage().contains("disabled plugin"));
    Assert.assertFalse(macroEvaluated.get());
    Mockito.verifyZeroInteractions(pluginConfigurer);
  }

  @Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = "File")})
  private static class FakeFileAssociatedPlugin implements PipelineConfigurable {
    private final AtomicBoolean configured;

    FakeFileAssociatedPlugin(AtomicBoolean configured) {
      this.configured = configured;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
      configured.set(true);
    }
  }

  @Test
  public void testValidateDisabledConnectorMetadataPropertyRejectedBeforeConfigurePipeline() {
    AtomicBoolean configurePipelineCalled = new AtomicBoolean(false);
    PluginClass fakePluginClass = PluginClass.builder()
        .setName("CustomFileBatchSource")
        .setType(BatchSource.PLUGIN_TYPE)
        .setDescription("custom file source")
        .setClassName(FakeFileAssociatedPlugin.class.getName())
        .build();

    PluginConfigurer pluginConfigurer = new PluginConfigurer() {
      @Override
      public <T> T usePlugin(String pluginType, String pluginName, String pluginId,
          PluginProperties properties, PluginSelector selector) {
        selector.select(ImmutableSortedMap.of(
            new ArtifactId("mock", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
            fakePluginClass));
        return (T) new FakeFileAssociatedPlugin(configurePipelineCalled);
      }

      @Override
      public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
          PluginProperties properties, PluginSelector selector) {
        return null;
      }

      @Override
      public Map<String, String> evaluateMacros(Map<String, String> properties,
          MacroEvaluator evaluator, MacroParserOptions options) throws InvalidMacroException {
        return properties;
      }
    };

    ETLStage etlStage = new ETLStage("FileStage",
        new ETLPlugin("CustomFileBatchSource", BatchSource.PLUGIN_TYPE, Collections.emptyMap()));
    StageValidationRequest validationRequest = new StageValidationRequest(
        etlStage, Collections.emptyList(), false);

    StageValidationResponse response = ValidationUtils.validate(
        NamespaceId.DEFAULT.getNamespace(), validationRequest, pluginConfigurer,
        properties -> properties, MOCK_FEATURE_FLAGS_PROVIDER, ImmutableSet.of("file"));

    Assert.assertNull(response.getSpec());
    Assert.assertEquals(1, response.getFailures().size());
    Assert.assertTrue(response.getFailures().get(0).getMessage().contains("disabled plugin"));
    Assert.assertFalse(configurePipelineCalled.get());
  }

  @Test
  public void testRemoteValidationTaskRejectsDisabledPlugin() throws Exception {
    Gson gson = new GsonBuilder()
        .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
        .serializeNulls()
        .create();
    ETLStage etlStage = new ETLStage("File",
        new ETLPlugin("File", BatchSource.PLUGIN_TYPE, Collections.emptyMap()));
    StageValidationRequest validationRequest = new StageValidationRequest(
        etlStage, Collections.emptyList(), false);
    RemoteValidationRequest remoteRequest = new RemoteValidationRequest(
        NamespaceId.DEFAULT.getNamespace(), gson.toJson(validationRequest),
        ImmutableSet.of("File"));

    PluginConfigurer pluginConfigurer = Mockito.mock(PluginConfigurer.class);
    SystemAppTaskContext systemAppTaskContext = Mockito.mock(SystemAppTaskContext.class);
    Mockito.when(systemAppTaskContext.createPluginConfigurer(Mockito.anyString()))
        .thenReturn(pluginConfigurer);

    RunnableTaskRequest taskRequest = RunnableTaskRequest.getBuilder(
        RemoteValidationTask.class.getName()).withParam(gson.toJson(remoteRequest)).build();
    RunnableTaskContext context = new RunnableTaskContext(taskRequest, systemAppTaskContext);

    new RemoteValidationTask().run(context);

    String responseJson = StandardCharsets.UTF_8.decode(context.getResult()).toString();
    StageValidationResponse response = gson.fromJson(responseJson, StageValidationResponse.class);
    Assert.assertNull(response.getSpec());
    Assert.assertEquals(1, response.getFailures().size());
    Assert.assertTrue(response.getFailures().get(0).getMessage().contains("disabled plugin"));
    Mockito.verifyZeroInteractions(pluginConfigurer);
  }

  @Test
  public void testValidationHandlerRejectsDisabledStageAndPipeline() throws Exception {
    Gson gson = new GsonBuilder()
        .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
        .serializeNulls()
        .create();
    ConnectionConfig connectionConfig = new ConnectionConfig(
        ImmutableSet.of("File"), Collections.emptySet(), null);
    ValidationHandler handler = new ValidationHandler(connectionConfig);

    SystemHttpServiceContext serviceContext = Mockito.mock(SystemHttpServiceContext.class);
    Admin admin = Mockito.mock(Admin.class);
    Mockito.when(admin.namespaceExists(NamespaceId.DEFAULT.getNamespace())).thenReturn(true);
    Mockito.when(serviceContext.getAdmin()).thenReturn(admin);
    HttpServiceHandlerSpecification spec = new HttpServiceHandlerSpecification(
        ValidationHandler.class.getName(), "ValidationHandler", "desc",
        Collections.singletonMap("disabledTypes", gson.toJson(ImmutableSet.of("File"))),
        Collections.emptySet(), Collections.emptyList());
    Mockito.when(serviceContext.getSpecification()).thenReturn(spec);
    handler.initialize(serviceContext);

    // 1. validateStage with disabled plugin "File"
    ETLStage fileStage = new ETLStage("FileSource",
        new ETLPlugin("File", BatchSource.PLUGIN_TYPE,
            Collections.singletonMap("path", "/tmp/input")));
    StageValidationRequest stageReq = new StageValidationRequest(
        fileStage, Collections.emptyList(), false);
    HttpServiceRequest httpStageReq = Mockito.mock(HttpServiceRequest.class);
    Mockito.when(httpStageReq.getContent()).thenReturn(
        ByteBuffer.wrap(gson.toJson(stageReq).getBytes(StandardCharsets.UTF_8)));
    HttpServiceResponder stageResponder = Mockito.mock(HttpServiceResponder.class);

    handler.validateStage(httpStageReq, stageResponder, NamespaceId.DEFAULT.getNamespace());

    ArgumentCaptor<String> bodyCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(stageResponder).sendString(bodyCaptor.capture());
    StageValidationResponse stageResp =
        gson.fromJson(bodyCaptor.getValue(), StageValidationResponse.class);
    Assert.assertNull(stageResp.getSpec());
    Assert.assertEquals(1, stageResp.getFailures().size());
    Assert.assertTrue(stageResp.getFailures().get(0).getMessage().contains("disabled plugin"));
    Mockito.verify(serviceContext, Mockito.never()).runTask(Mockito.any());
    Mockito.verify(serviceContext, Mockito.never()).createServicePluginConfigurer(Mockito.anyString());

    // 2. validatePipeline with disabled plugin "File"
    ETLBatchConfig batchConfig = ETLBatchConfig.builder()
        .addStage(fileStage)
        .addStage(new ETLStage("sink", MockSink.getPlugin("out")))
        .addConnection("FileSource", "sink")
        .build();
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(
        new ArtifactSummary(StudioUtil.ARTIFACT_BATCH_NAME, "6.0.0"), batchConfig);
    HttpServiceRequest httpPipeReq = Mockito.mock(HttpServiceRequest.class);
    Mockito.when(httpPipeReq.getContent()).thenReturn(
        ByteBuffer.wrap(gson.toJson(appRequest).getBytes(StandardCharsets.UTF_8)));
    HttpServiceResponder pipeResponder = Mockito.mock(HttpServiceResponder.class);

    handler.validatePipeline(httpPipeReq, pipeResponder, NamespaceId.DEFAULT.getNamespace());

    Mockito.verify(pipeResponder).sendError(
        Mockito.eq(HttpURLConnection.HTTP_BAD_REQUEST),
        Mockito.contains("Plugin 'File' is disabled"));
  }

  //Mock PluginConfigurer
  private PluginConfigurer getPluginConfigurer(PluginClass pluginClass) {
    return new PluginConfigurer() {
      @Override
      public <T> T usePlugin(String pluginType, String pluginName, String pluginId,
          PluginProperties properties, PluginSelector selector) {
        selector.select(ImmutableSortedMap.of(
            new ArtifactId("mock", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
            pluginClass));
        String tableName = properties.getProperties().get("tableName");
        if (tableName == null || tableName.isEmpty()) {
          throw new InvalidPluginConfigException(pluginClass, Collections.singleton("tableName"),
              new HashSet<>());
        }
        MockSource.Config config = new MockSource.Config();
        MockSource.ConnectionConfig connectionConfig = new MockSource.ConnectionConfig();
        String schema = properties.getProperties().get("schema");
        String sleep = properties.getProperties().get("sleepInMillis");
        connectionConfig.setTableName(tableName);
        config.setConfig(connectionConfig, schema, null,
            sleep == null ? null : Long.parseLong(sleep));
        return (T) new MockSource(config);
      }

      @Override
      public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
          PluginProperties properties, PluginSelector selector) {
        return null;
      }

      @Override
      public Map<String, String> evaluateMacros(Map<String, String> properties,
          MacroEvaluator evaluator, MacroParserOptions options) throws
        InvalidMacroException {
        return null;
      }
    };
  }
}
