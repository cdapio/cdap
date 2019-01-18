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

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.datapipeline.service.StudioService;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.etl.spark.Compat;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests for the data pipeline service.
 */
public class DataPipelineServiceTest extends HydratorTestBase {
  private static final Gson GSON = new Gson();
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
  public void testValidateBadPipelineArtifact() throws IOException {
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
  public void testValidateChecksNamespaceExistence() throws IOException {
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
}
