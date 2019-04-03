/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.datapipeline;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.datapipeline.service.StudioService;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.mock.action.FileMoveAction;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.mock.transform.SleepTransform;
import co.cask.cdap.etl.mock.transform.StringValueFilterTransform;
import co.cask.cdap.etl.proto.ArtifactSelectorConfig;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.etl.proto.v2.validation.InvalidConfigPropertyError;
import co.cask.cdap.etl.proto.v2.validation.PluginNotFoundError;
import co.cask.cdap.etl.proto.v2.validation.StageSchema;
import co.cask.cdap.etl.proto.v2.validation.StageValidationError;
import co.cask.cdap.etl.proto.v2.validation.StageValidationRequest;
import co.cask.cdap.etl.proto.v2.validation.StageValidationResponse;
import co.cask.cdap.etl.proto.v2.validation.ValidationError;
import co.cask.cdap.etl.proto.v2.validation.ValidationErrorSerDe;
import co.cask.cdap.etl.spark.Compat;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestConfiguration;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests for the data pipeline service.
 */
public class DataPipelineServiceTest extends HydratorTestBase {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(ValidationError.class, new ValidationErrorSerDe())
    .create();
  private static ServiceManager serviceManager;
  private static URI serviceURI;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                                                                       Constants.Security.Store.PROVIDER, "file",
                                                                       Constants.AppFabric.SPARK_COMPAT,
                                                                       Compat.SPARK_COMPAT);

  @BeforeClass
  public static void setupTest() throws Exception {
    ArtifactId appArtifactId = NamespaceId.DEFAULT.artifact("app", "1.0.0");
    setupBatchArtifacts(appArtifactId, DataPipelineApp.class);

    ArtifactSummary artifactSummary = new ArtifactSummary(appArtifactId.getArtifact(), appArtifactId.getVersion());
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(artifactSummary, ETLBatchConfig.forSystemService());
    ApplicationManager appManager = deployApplication(NamespaceId.DEFAULT.app("datapipeline"), appRequest);
    serviceManager = appManager.getServiceManager(StudioService.NAME);
    serviceManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 2, TimeUnit.MINUTES);
    serviceURI = serviceManager.getServiceURL(1, TimeUnit.MINUTES).toURI();
  }

  @AfterClass
  public static void teardownTests() throws InterruptedException, ExecutionException, TimeoutException {
    serviceManager.stop();
    serviceManager.waitForStopped(2, TimeUnit.MINUTES);
  }

  @Test
  public void testValidatePipelineBadPipelineArtifact() throws IOException {
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("src", MockSource.getPlugin("dummy1")))
      .addStage(new ETLStage("sink", MockSink.getPlugin("dummy2")))
      .addConnection("src", "sink")
      .build();
    ArtifactSummary badArtifact = new ArtifactSummary("ghost", "1.0.0");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(badArtifact, config);

    URL validatePipelineURL = serviceURI
      .resolve(String.format("v1/contexts/%s/validations/pipeline", NamespaceId.DEFAULT.getNamespace()))
      .toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.POST, validatePipelineURL)
      .withBody(GSON.toJson(appRequest))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(400, response.getResponseCode());
  }

  @Test
  public void testValidatePipelineChecksNamespaceExistence() throws IOException {
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("src", MockSource.getPlugin("dummy1")))
      .addStage(new ETLStage("sink", MockSink.getPlugin("dummy2")))
      .addConnection("src", "sink")
      .build();
    ArtifactSummary badArtifact = new ArtifactSummary("ghost", "1.0.0");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(badArtifact, config);

    URL validatePipelineURL = serviceURI.resolve("v1/contexts/ghost/validations/pipeline").toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.POST, validatePipelineURL)
      .withBody(GSON.toJson(appRequest))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(404, response.getResponseCode());
  }

  @Test
  public void testValidateStagePluginNotFound() throws Exception {
    String name = MockSource.NAME;
    String type = BatchSource.PLUGIN_TYPE;
    ArtifactSelectorConfig requestedArtifact = new ArtifactSelectorConfig(ArtifactScope.USER.name(),
                                                                          batchMocksArtifactId.getArtifact() + "-ghost",
                                                                          batchMocksArtifactId.getVersion());

    String stageName = "src";
    ETLStage stage = new ETLStage(stageName, new ETLPlugin(name, type, Collections.emptyMap(), requestedArtifact));
    StageValidationResponse actual = sendRequest(new StageValidationRequest(stage, Collections.emptyList()));

    ArtifactSelectorConfig expectedSuggestion = new ArtifactSelectorConfig(ArtifactScope.USER.name(),
                                                                           batchMocksArtifactId.getArtifact(),
                                                                           batchMocksArtifactId.getVersion());
    PluginNotFoundError error = new PluginNotFoundError(stageName, type, name, requestedArtifact, expectedSuggestion);
    StageValidationResponse expected = new StageValidationResponse(Collections.singletonList(error));
    Assert.assertEquals(expected, actual);
  }

  // test that InvalidConfigPropertyExceptions are captured as InvalidConfigPropertyErrors
  @Test
  public void testValidateStageSingleInvalidConfigProperty() throws Exception {
    // StringValueFilterTransform will be configured to filter records where field x has value 'y'
    // it will be invalid because the type of field x will be an int instead of the required string
    String stageName = "tx";
    Map<String, String> properties = new HashMap<>();
    properties.put("field", "x");
    properties.put("value", "y");
    ETLStage stage = new ETLStage(stageName, new ETLPlugin(StringValueFilterTransform.NAME, Transform.PLUGIN_TYPE,
                                                           properties));

    Schema inputSchema = Schema.recordOf("x", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    StageValidationRequest requestBody =
      new StageValidationRequest(stage, Collections.singletonList(new StageSchema("input", inputSchema)));
    StageValidationResponse actual = sendRequest(requestBody);

    Assert.assertNull(actual.getSpec());
    Assert.assertEquals(1, actual.getErrors().size());
    ValidationError error = actual.getErrors().iterator().next();
    Assert.assertTrue(error instanceof InvalidConfigPropertyError);
    Assert.assertEquals("field", ((InvalidConfigPropertyError) error).getProperty());
    Assert.assertEquals(stageName, ((InvalidConfigPropertyError) error).getStage());
  }

  // test that multiple exceptions set in an InvalidStageException are captured as errors
  @Test
  public void testValidateStageMultipleErrors() throws Exception {
    // configure an invalid regex and a set the source and destination to the same value, which should generate 2 errors
    String stageName = "stg";
    Map<String, String> properties = new HashMap<>();
    properties.put("filterRegex", "[");
    properties.put("sourceFileset", "files");
    properties.put("destinationFileset", "files");
    ETLStage stage = new ETLStage(stageName, new ETLPlugin(FileMoveAction.NAME, Action.PLUGIN_TYPE, properties));

    StageValidationRequest request = new StageValidationRequest(stage, Collections.emptyList());
    StageValidationResponse actual = sendRequest(request);

    Assert.assertNull(actual.getSpec());
    Assert.assertEquals(2, actual.getErrors().size());

    ValidationError error1 = actual.getErrors().get(0);
    ValidationError error2 = actual.getErrors().get(1);

    Assert.assertTrue(error1 instanceof InvalidConfigPropertyError);
    Assert.assertEquals("filterRegex", ((InvalidConfigPropertyError) error1).getProperty());
    Assert.assertEquals(stageName, ((InvalidConfigPropertyError) error1).getStage());

    Assert.assertTrue(error2 instanceof StageValidationError);
    Assert.assertEquals(stageName, ((StageValidationError) error2).getStage());
  }

  // tests that exceptions thrown by configurePipeline that are not InvalidStageExceptions are
  // captured as a StageValidationError.
  @Test
  public void testValidateStageOtherExceptionsCaptured() throws Exception {
    // SleepTransform throws an IllegalArgumentException if the sleep time is less than 1
    String stageName = "tx";
    ETLStage stage = new ETLStage(stageName, new ETLPlugin(SleepTransform.NAME, Transform.PLUGIN_TYPE,
                                                           Collections.singletonMap("millis", "-1")));
    StageValidationResponse actual = sendRequest(new StageValidationRequest(stage, Collections.emptyList()));

    Assert.assertNull(actual.getSpec());
    Assert.assertEquals(1, actual.getErrors().size());
    ValidationError error = actual.getErrors().iterator().next();
    Assert.assertTrue(error instanceof StageValidationError);
    Assert.assertEquals(stageName, ((StageValidationError) error).getStage());
  }


  // tests that plugins that cannot be instantiated due to missing required properties are captured
  @Test
  public void testValidateStageMissingRequiredProperty() throws Exception {
    String stageName = "tx";
    // string filter requires the field name and the value
    ETLStage stage = new ETLStage(stageName, new ETLPlugin(StringValueFilterTransform.NAME, Transform.PLUGIN_TYPE,
                                                           Collections.emptyMap()));
    StageValidationResponse actual = sendRequest(new StageValidationRequest(stage, Collections.emptyList()));

    Assert.assertNull(actual.getSpec());
    Assert.assertEquals(1, actual.getErrors().size());
    ValidationError error = actual.getErrors().iterator().next();
    Assert.assertTrue(error instanceof StageValidationError);
    Assert.assertEquals(stageName, ((StageValidationError) error).getStage());
  }

  private StageValidationResponse sendRequest(StageValidationRequest requestBody) throws IOException {
    URL validatePipelineURL = serviceURI
      .resolve(String.format("v1/contexts/%s/validations/stage", NamespaceId.DEFAULT.getNamespace()))
      .toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.POST, validatePipelineURL)
      .withBody(GSON.toJson(requestBody))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), StageValidationResponse.class);
  }
}
