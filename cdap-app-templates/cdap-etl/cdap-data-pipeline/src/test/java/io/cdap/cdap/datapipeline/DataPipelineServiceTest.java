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

package io.cdap.cdap.datapipeline;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.datapipeline.service.StudioService;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.action.FileMoveAction;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.batch.NullErrorTransform;
import io.cdap.cdap.etl.mock.batch.aggregator.DistinctAggregator;
import io.cdap.cdap.etl.mock.batch.joiner.MockJoiner;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.mock.transform.SleepTransform;
import io.cdap.cdap.etl.mock.transform.StringValueFilterTransform;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.proto.v2.validation.StageSchema;
import io.cdap.cdap.etl.proto.v2.validation.StageValidationRequest;
import io.cdap.cdap.etl.proto.v2.validation.StageValidationResponse;
import io.cdap.cdap.etl.spark.Compat;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.TestConfiguration;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests for the data pipeline service.
 */
public class DataPipelineServiceTest extends HydratorTestBase {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  public static final String STAGE = "stage";

  private static ServiceManager serviceManager;
  private static URI serviceURI;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                                                                       Constants.Security.Store.PROVIDER, "file",
                                                                       Constants.AppFabric.SPARK_COMPAT,
                                                                       Compat.SPARK_COMPAT);

  @BeforeClass
  public static void setupTest() throws Exception {
    ArtifactId appArtifactId = NamespaceId.SYSTEM.artifact("app", "1.0.0");
    setupBatchArtifacts(appArtifactId, DataPipelineApp.class);

    ArtifactSummary artifactSummary = new ArtifactSummary(appArtifactId.getArtifact(), appArtifactId.getVersion());
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(artifactSummary, ETLBatchConfig.forSystemService());
    ApplicationManager appManager = deployApplication(NamespaceId.SYSTEM.app("datapipeline"), appRequest);
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
  public void testSecureMacroSubstitution() throws Exception {
    // StringValueFilterTransform checks that the field exists in the input schema
    String stageName = "tx";
    Map<String, String> properties = new HashMap<>();
    properties.put("field", "${secure(field)}");
    properties.put("value", "y");
    ETLStage stage = new ETLStage(stageName, new ETLPlugin(StringValueFilterTransform.NAME, Transform.PLUGIN_TYPE,
                                                           properties));
    Schema inputSchema = Schema.recordOf("x", Schema.Field.of("x", Schema.of(Schema.Type.INT)));

    // this call happens when no value for 'field' is in the secure store, which should not result in
    // any failures because the macro could not be enabled so the check is skipped
    StageValidationRequest requestBody =
      new StageValidationRequest(stage, Collections.singletonList(new StageSchema("input", inputSchema)));
    StageValidationResponse actual = sendRequest(requestBody);
    Assert.assertTrue(actual.getFailures().isEmpty());

    // now set the value of 'field' to be 'z', which is not in the input schema
    // this call should result in a failure
    getSecureStoreManager().put("default", "field", "z", "desc", Collections.emptyMap());
    actual = sendRequest(requestBody);
    Assert.assertNull(actual.getSpec());
    Assert.assertEquals(1, actual.getFailures().size());
    ValidationFailure failure = actual.getFailures().iterator().next();
    // the stage will add 2 causes for invalid input field failure. One is related to input field and the other is
    // related to config property.
    Assert.assertEquals(1, failure.getCauses().size());
    Assert.assertEquals("field", failure.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(stageName, failure.getCauses().get(0).getAttribute(STAGE));
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
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
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
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
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
    Assert.assertEquals(1, actual.getFailures().size());
    ValidationFailure failure = actual.getFailures().iterator().next();
    Assert.assertEquals(stageName, failure.getCauses().get(0).getAttribute(CauseAttributes.PLUGIN_ID));
    Assert.assertEquals(type, failure.getCauses().get(0).getAttribute(CauseAttributes.PLUGIN_TYPE));
    Assert.assertEquals(name, failure.getCauses ().get(0).getAttribute(CauseAttributes.PLUGIN_NAME));
    Assert.assertEquals(requestedArtifact.getName(), failure.getCauses().get(0)
      .getAttribute(CauseAttributes.REQUESTED_ARTIFACT_NAME));
    Assert.assertEquals(requestedArtifact.getScope(), failure.getCauses().get(0)
      .getAttribute(CauseAttributes.REQUESTED_ARTIFACT_SCOPE));
    Assert.assertEquals(requestedArtifact.getVersion(), failure.getCauses().get(0)
      .getAttribute(CauseAttributes.REQUESTED_ARTIFACT_VERSION));
    Assert.assertEquals(batchMocksArtifactId.getArtifact(), failure.getCauses().get(0)
      .getAttribute(CauseAttributes.SUGGESTED_ARTIFACT_NAME));
    Assert.assertEquals(ArtifactScope.SYSTEM.name(), failure.getCauses().get(0)
      .getAttribute(CauseAttributes.SUGGESTED_ARTIFACT_SCOPE));
    Assert.assertEquals(batchMocksArtifactId.getVersion(), failure.getCauses().get(0)
      .getAttribute(CauseAttributes.SUGGESTED_ARTIFACT_VERSION));
  }

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
    Assert.assertEquals(1, actual.getFailures().size());
    ValidationFailure failure = actual.getFailures().iterator().next();
    // the stage will add 2 causes for invalid input field failure. One is related to input field and the other is
    // related to config property.
    Assert.assertEquals(2, failure.getCauses().size());
    Assert.assertEquals("field", failure.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(stageName, failure.getCauses().get(0).getAttribute(STAGE));
    Assert.assertEquals("x", failure.getCauses().get(1).getAttribute(CauseAttributes.INPUT_SCHEMA_FIELD));
    Assert.assertEquals("input", failure.getCauses().get(1).getAttribute(CauseAttributes.INPUT_STAGE));
    Assert.assertEquals(stageName, failure.getCauses().get(1).getAttribute(STAGE));
  }

  @Test
  public void testValidateMultiInputInvalidInputField() throws Exception {
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
      new StageValidationRequest(stage, ImmutableList.of(new StageSchema("input1", inputSchema),
                                                         new StageSchema("input2", inputSchema)));
    StageValidationResponse actual = sendRequest(requestBody);

    List<String> expectedInputs = ImmutableList.of("input1", "input2");
    Assert.assertNull(actual.getSpec());
    Assert.assertEquals(1, actual.getFailures().size());
    ValidationFailure failure = actual.getFailures().iterator().next();
    // the stage will add 3 causes. Two are related to input field and one is related to config property.
    Assert.assertEquals(3, failure.getCauses().size());
    Assert.assertEquals("field", failure.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(stageName, failure.getCauses().get(0).getAttribute(STAGE));
    Assert.assertEquals(stageName, failure.getCauses().get(1).getAttribute(STAGE));
    Assert.assertEquals(stageName, failure.getCauses().get(2).getAttribute(STAGE));
    Assert.assertTrue(expectedInputs.contains(failure.getCauses().get(1).getAttribute(CauseAttributes.INPUT_STAGE)));
    Assert.assertTrue(expectedInputs.contains(failure.getCauses().get(2).getAttribute(CauseAttributes.INPUT_STAGE)));
  }

  // test that multiple exceptions set in an InvalidStageException are captured as failures
  @Test
  public void testValidateStageMultipleErrors() throws Exception {
    // configure an invalid regex and a set the source and destination to the same value,
    // which should generate 2 errors
    String stageName = "stg";
    Map<String, String> properties = new HashMap<>();
    properties.put("filterRegex", "[");
    properties.put("sourceFileset", "files");
    properties.put("destinationFileset", "files");
    ETLStage stage = new ETLStage(stageName, new ETLPlugin(FileMoveAction.NAME, Action.PLUGIN_TYPE, properties));

    StageValidationRequest request = new StageValidationRequest(stage, Collections.emptyList());
    StageValidationResponse actual = sendRequest(request);

    Assert.assertNull(actual.getSpec());
    Assert.assertEquals(2, actual.getFailures().size());

    ValidationFailure failure1 = actual.getFailures().get(0);
    Assert.assertEquals("filterRegex", failure1.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(stageName, failure1.getCauses().get(0).getAttribute(STAGE));

    // failure 2 should have 2 causes one for each config property
    ValidationFailure failure2 = actual.getFailures().get(1);
    Assert.assertEquals(2, failure2.getCauses().size());
  }

  // tests that exceptions thrown by configurePipeline that are not InvalidStageExceptions are
  // captured as a ValidationFailure.
  @Test
  public void testValidateStageOtherExceptionsCaptured() throws Exception {
    // SleepTransform throws an IllegalArgumentException if the sleep time is less than 1
    String stageName = "tx";
    ETLStage stage = new ETLStage(stageName, new ETLPlugin(SleepTransform.NAME, Transform.PLUGIN_TYPE,
                                                           Collections.singletonMap("millis", "-1")));
    StageValidationResponse actual = sendRequest(new StageValidationRequest(stage, Collections.emptyList()));

    Assert.assertNull(actual.getSpec());
    Assert.assertEquals(1, actual.getFailures().size());
    ValidationFailure failure = actual.getFailures().iterator().next();
    Assert.assertEquals(stageName, failure.getCauses().get(0).getAttribute(STAGE));
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
    Assert.assertEquals(2, actual.getFailures().size());
    Set<String> properties = new HashSet<>();
    properties.add(actual.getFailures().get(0).getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    properties.add(actual.getFailures().get(1).getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    Set<String> expected = new HashSet<>();
    expected.add("field");
    expected.add("value");
    Assert.assertEquals(expected, properties);
  }

  @Test
  public void testValidationFailureForAggregator() throws Exception {
    String stageName = "ag";
    ETLStage stage = new ETLStage(stageName, DistinctAggregator.getPlugin("id,name"));
    // input schema does not contain name field
    Schema inputSchema = Schema.recordOf("id", Schema.Field.of("id", Schema.of(Schema.Type.STRING)));

    StageValidationRequest requestBody =
      new StageValidationRequest(stage, Collections.singletonList(new StageSchema("input", inputSchema)));
    StageValidationResponse actual = sendRequest(requestBody);

    Assert.assertNull(actual.getSpec());
    Assert.assertEquals(1, actual.getFailures().size());
    ValidationFailure failure = actual.getFailures().iterator().next();
    Assert.assertEquals(stageName, failure.getCauses().get(0).getAttribute(STAGE));
    Assert.assertEquals("fields", failure.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals("name", failure.getCauses().get(0).getAttribute(CauseAttributes.CONFIG_ELEMENT));
  }

  @Test
  public void testValidationFailureForJoiner() throws Exception {
    String stageName = "joiner";
    // join key field t2_cust_name does not exist
    ETLStage stage = new ETLStage(stageName, MockJoiner.getPlugin("t1.customer_id=t2.cust_id&" +
                                                                    "t1.customer_name=t2.t2_cust_name", "t1,t2", ""));
    StageSchema inputSchema1 = new StageSchema(
      "t1", Schema.recordOf("id", Schema.Field.of("customer_id", Schema.of(Schema.Type.STRING)),
                            Schema.Field.of("customer_name", Schema.of(Schema.Type.STRING))));
    // t1.customer_id type string does not match t2.cust_id type int
    StageSchema inputSchema2 = new StageSchema(
      "t2", Schema.recordOf("id", Schema.Field.of("cust_id", Schema.of(Schema.Type.INT)),
                            Schema.Field.of("cust_name", Schema.of(Schema.Type.STRING))));
    StageValidationRequest requestBody =
      new StageValidationRequest(stage, ImmutableList.of(inputSchema1, inputSchema2));

    StageValidationResponse actual = sendRequest(requestBody);

    Assert.assertNull(actual.getSpec());
    Assert.assertEquals(2, actual.getFailures().size());

    ValidationFailure fieldDoesNotExist = actual.getFailures().get(0);
    Assert.assertEquals(stageName, fieldDoesNotExist.getCauses().get(0).getAttribute(STAGE));
    Assert.assertEquals("t1.customer_id=t2.cust_id",
                        fieldDoesNotExist.getCauses().get(0).getAttribute(CauseAttributes.CONFIG_ELEMENT));

    ValidationFailure typeMismatch = actual.getFailures().get(1);
    Assert.assertEquals(stageName, typeMismatch.getCauses().get(0).getAttribute(STAGE));
    Assert.assertEquals("t1.customer_name=t2.t2_cust_name",
                        typeMismatch.getCauses().get(0).getAttribute(CauseAttributes.CONFIG_ELEMENT));
  }

  @Test
  public void testValidationFailureWithNPE() throws Exception {
    String stageName = "npe";
    ETLStage stage = new ETLStage(stageName, NullErrorTransform.getPlugin());

    StageValidationResponse actual = sendRequest(new StageValidationRequest(stage, Collections.emptyList()));

    Assert.assertNull(actual.getSpec());
    Assert.assertEquals(1, actual.getFailures().size());
    ValidationFailure failure = actual.getFailures().iterator().next();
    Assert.assertEquals(stageName, failure.getCauses().get(0).getAttribute(STAGE));
    Assert.assertNotNull(failure.getCauses().get(0).getAttribute(CauseAttributes.STACKTRACE));
  }

  private StageValidationResponse sendRequest(StageValidationRequest requestBody) throws IOException {
    URL validatePipelineURL = serviceURI
      .resolve(String.format("v1/contexts/%s/validations/stage", NamespaceId.DEFAULT.getNamespace()))
      .toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.POST, validatePipelineURL)
      .withBody(GSON.toJson(requestBody))
      .build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), StageValidationResponse.class);
  }
}
