/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.cli;

import co.cask.cdap.StandaloneTester;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.cli.command.NamespaceCommandUtils;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.DatasetTypeClient;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.client.app.ConfigTestApp;
import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.app.FakeDataset;
import co.cask.cdap.client.app.FakeFlow;
import co.cask.cdap.client.app.FakeSpark;
import co.cask.cdap.client.app.FakeWorkflow;
import co.cask.cdap.client.app.PingService;
import co.cask.cdap.client.app.PrefixedEchoHandler;
import co.cask.cdap.common.DatasetTypeNotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.WorkflowTokenDetail;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.XSlowTests;
import co.cask.common.cli.CLI;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Test for {@link CLIMain}.
 */
@Category(XSlowTests.class)
public class CLIMainTest extends CLITestBase {

  @ClassRule
  public static final StandaloneTester STANDALONE = new StandaloneTester();

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final Logger LOG = LoggerFactory.getLogger(CLIMainTest.class);
  private static final Joiner SPACE_JOINER = Joiner.on(" ");
  private static final Gson GSON = new Gson();
  private static final String PREFIX = "123ff1_";
  private static final String V1 = "1.0";
  private static final String V1_SNAPSHOT = "v1.0.0-SNAPSHOT";

  private static final ArtifactId fakeArtifactId = NamespaceId.DEFAULT.artifact(FakeApp.NAME, V1);
  private static final ApplicationId fakeAppId = NamespaceId.DEFAULT.app(FakeApp.NAME);
  private static final ApplicationId fakeAppIdV1 = NamespaceId.DEFAULT.app(FakeApp.NAME, V1_SNAPSHOT);
  private final ProgramId fakeWorkflowId = fakeAppId.workflow(FakeWorkflow.NAME);
  private final ProgramId fakeFlowId = fakeAppId.flow(FakeFlow.NAME);
  private final ProgramId fakeSparkId = fakeAppId.spark(FakeSpark.NAME);
  private final ServiceId pingServiceId = fakeAppId.service(PingService.NAME);
  private final ServiceId prefixedEchoHandlerId = fakeAppId.service(PrefixedEchoHandler.NAME);
  private final DatasetId fakeDsId = NamespaceId.DEFAULT.dataset(FakeApp.DS_NAME);
  private final StreamId fakeStreamId = NamespaceId.DEFAULT.stream(FakeApp.STREAM_NAME);

  private static ProgramClient programClient;
  private static QueryClient queryClient;
  private static CLIConfig cliConfig;
  private static CLIMain cliMain;
  private static CLI cli;

  @BeforeClass
  public static void setUpClass() throws Exception {
    cliConfig = createCLIConfig(STANDALONE.getBaseURI());
    LaunchOptions launchOptions = new LaunchOptions(LaunchOptions.DEFAULT.getUri(), true, true, false);
    cliMain = new CLIMain(launchOptions, cliConfig);
    programClient = new ProgramClient(cliConfig.getClientConfig());
    queryClient = new QueryClient(cliConfig.getClientConfig());

    cli = cliMain.getCLI();

    testCommandOutputContains(cli, "connect " + STANDALONE.getBaseURI(), "Successfully connected");
    testCommandOutputNotContains(cli, "list apps", FakeApp.NAME);

    File appJarFile = createAppJarFile(FakeApp.class);

    testCommandOutputContains(cli, String.format("load artifact %s name %s version %s", appJarFile.getAbsolutePath(),
                                                 FakeApp.NAME, V1),
                              "Successfully added artifact");
    testCommandOutputContains(cli, "deploy app " + appJarFile.getAbsolutePath(), "Successfully deployed app");
    testCommandOutputContains(cli, String.format("create app %s version %s %s %s %s",
                                                 FakeApp.NAME, V1_SNAPSHOT, FakeApp.NAME, V1, ArtifactScope.USER),
                              "Successfully created application");
    if (!appJarFile.delete()) {
      LOG.warn("Failed to delete temporary app jar file: {}", appJarFile.getAbsolutePath());
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    testCommandOutputContains(cli, "delete app " + FakeApp.NAME, "Successfully deleted app");
    testCommandOutputContains(cli, String.format("delete app %s version %s", FakeApp.NAME, V1_SNAPSHOT),
                              "Successfully deleted app");
  }

  @Test
  public void testConnect() throws Exception {
    testCommandOutputContains(cli, "connect fakehost", "could not be reached");
    testCommandOutputContains(cli, "connect " + STANDALONE.getBaseURI(), "Successfully connected");
  }

  @Test
  public void testList() throws Exception {
    testCommandOutputContains(cli, "list app versions " + FakeApp.NAME, V1_SNAPSHOT);
    testCommandOutputContains(cli, "list app versions " + FakeApp.NAME, ApplicationId.DEFAULT_VERSION);
    testCommandOutputContains(cli, "list dataset instances", FakeApp.DS_NAME);
    testCommandOutputContains(cli, "list streams", FakeApp.STREAM_NAME);
    testCommandOutputContains(cli, "list flows", FakeApp.FLOWS.get(0));
  }

  @Test
  public void testPrompt() throws Exception {
    String prompt = cliMain.getPrompt(cliConfig.getConnectionConfig());
    Assert.assertFalse(prompt.contains("@"));
    Assert.assertTrue(prompt.contains(STANDALONE.getBaseURI().getHost()));
    Assert.assertTrue(prompt.contains(cliConfig.getCurrentNamespace().getEntityName()));

    CLIConnectionConfig oldConnectionConfig = cliConfig.getConnectionConfig();
    CLIConnectionConfig authConnectionConfig = new CLIConnectionConfig(
      oldConnectionConfig, NamespaceId.DEFAULT, "test-username");
    cliConfig.setConnectionConfig(authConnectionConfig);
    prompt = cliMain.getPrompt(cliConfig.getConnectionConfig());
    Assert.assertTrue(prompt.contains("test-username@"));
    Assert.assertTrue(prompt.contains(STANDALONE.getBaseURI().getHost()));
    Assert.assertTrue(prompt.contains(cliConfig.getCurrentNamespace().getEntityName()));
    cliConfig.setConnectionConfig(oldConnectionConfig);
  }

  @Test
  public void testProgram() throws Exception {
    String flowId = FakeApp.FLOWS.get(0);
    ProgramId flow = fakeAppId.flow(flowId);

    String qualifiedFlowId = FakeApp.NAME + "." + flowId;
    testCommandOutputContains(cli, "start flow " + qualifiedFlowId, "Successfully started flow");
    assertProgramStatus(programClient, flow, "RUNNING");
    testCommandOutputContains(cli, "stop flow " + qualifiedFlowId, "Successfully stopped flow");
    assertProgramStatus(programClient, flow, "STOPPED");
    testCommandOutputContains(cli, "get flow status " + qualifiedFlowId, "STOPPED");
    testCommandOutputContains(cli, "get flow runs " + qualifiedFlowId, "KILLED");
    testCommandOutputContains(cli, "get flow live " + qualifiedFlowId, flowId);
  }

  @Test
  public void testAppDeploy() throws Exception {
    testDeploy(null);
    testDeploy(new ConfigTestApp.ConfigClass("testStream", "testTable"));
  }

  private void testDeploy(ConfigTestApp.ConfigClass config) throws Exception {
    String streamId = ConfigTestApp.DEFAULT_STREAM;
    String datasetId = ConfigTestApp.DEFAULT_TABLE;
    if (config != null) {
      streamId = config.getStreamName();
      datasetId = config.getTableName();
    }

    File appJarFile = createAppJarFile(ConfigTestApp.class);
    if (config != null) {
      String appConfig = GSON.toJson(config);
      testCommandOutputContains(cli, String.format("deploy app %s %s", appJarFile.getAbsolutePath(), appConfig),
                                "Successfully deployed app");
    } else {
      testCommandOutputContains(cli, String.format("deploy app %s", appJarFile.getAbsolutePath()), "Successfully");
    }

    if (!appJarFile.delete()) {
      LOG.warn("Failed to delete temporary app jar file: {}", appJarFile.getAbsolutePath());
    }
    testCommandOutputContains(cli, "list streams", streamId);
    testCommandOutputContains(cli, "list dataset instances", datasetId);
    testCommandOutputContains(cli, "delete app " + ConfigTestApp.NAME, "Successfully");
  }

  @Test
  public void testStream() throws Exception {
    String streamId = PREFIX + "sdf123";

    File file = new File(TMP_FOLDER.newFolder(), "test1.txt");
    StreamProperties streamProperties = new StreamProperties(2L, null, 10, "Golden Stream");
    try (BufferedWriter writer = Files.newWriter(file, Charsets.UTF_8)) {
      writer.write(GSON.toJson(streamProperties));
    }
    testCommandOutputContains(cli, "create stream " + streamId + " " + file.getAbsolutePath(),
                              "Successfully created stream");
    testCommandOutputContains(cli, "describe stream " + streamId, "Golden Stream");
    testCommandOutputContains(cli, "set stream description " + streamId + " 'Silver Stream'",
                              "Successfully set stream description");
    testCommandOutputContains(cli, "describe stream " + streamId, "Silver Stream");
    testCommandOutputContains(cli, "delete stream " + streamId, "Successfully deleted stream");

    testCommandOutputContains(cli, "create stream " + streamId, "Successfully created stream");
    testCommandOutputContains(cli, "list streams", streamId);
    testCommandOutputNotContains(cli, "get stream " + streamId, "helloworld");
    testCommandOutputContains(cli, "send stream " + streamId + " helloworld", "Successfully sent stream event");
    testCommandOutputContains(cli, "get stream " + streamId, "helloworld");
    testCommandOutputContains(cli, "get stream " + streamId + " -10m -0s 1", "helloworld");
    testCommandOutputContains(cli, "get stream " + streamId + " -10m -0s", "helloworld");
    testCommandOutputContains(cli, "get stream " + streamId + " -10m", "helloworld");
    testCommandOutputContains(cli, "truncate stream " + streamId, "Successfully truncated stream");
    testCommandOutputNotContains(cli, "get stream " + streamId, "helloworld");
    testCommandOutputContains(cli, "set stream ttl " + streamId + " 100000", "Successfully set TTL of stream");
    testCommandOutputContains(cli, "set stream notification-threshold " + streamId + " 1",
                              "Successfully set notification threshold of stream");
    testCommandOutputContains(cli, "describe stream " + streamId, "100000");

    file = new File(TMP_FOLDER.newFolder(), "test2.txt");
    // If the file not exist or not a file, upload should fails with an error.
    testCommandOutputContains(cli, "load stream " + streamId + " " + file.getAbsolutePath(), "Not a file");
    testCommandOutputContains(cli,
                              "load stream " + streamId + " " + file.getParentFile().getAbsolutePath(),
                              "Not a file");

    // Generate a file to send
    try (BufferedWriter writer = Files.newWriter(file, Charsets.UTF_8)) {
      for (int i = 0; i < 10; i++) {
        writer.write(String.format("%s, Event %s", i, i));
        writer.newLine();
      }
    }
    testCommandOutputContains(cli, "load stream " + streamId + " " + file.getAbsolutePath(),
                              "Successfully loaded file to stream");
    testCommandOutputContains(cli, "get stream " + streamId, "9, Event 9");
    testCommandOutputContains(cli, "get stream-stats " + streamId,
                              String.format("No schema found for stream '%s'", streamId));
    testCommandOutputContains(cli, "set stream format " + streamId + " csv 'body string'",
                              String.format("Successfully set format of stream '%s'", streamId));
    testCommandOutputContains(cli, "execute 'show tables'", String.format("stream_%s", streamId));
    testCommandOutputContains(cli, "get stream-stats " + streamId,
                              "Analyzed 10 Stream events in the time range [0, 9223372036854775807]");
    testCommandOutputContains(cli, "get stream-stats " + streamId + " limit 5 start 5 end 10",
                              "Analyzed 0 Stream events in the time range [5, 10]");
  }

  @Test
  public void testSchedule() throws Exception {
    String scheduleId = FakeApp.NAME + "." + FakeApp.SCHEDULE_NAME;
    String workflowId = FakeApp.NAME + "." + FakeWorkflow.NAME;
    testCommandOutputContains(cli, "get schedule status " + scheduleId, "SUSPENDED");
    testCommandOutputContains(cli, "resume schedule " + scheduleId, "Successfully resumed");
    testCommandOutputContains(cli, "get schedule status " + scheduleId, "SCHEDULED");
    testCommandOutputContains(cli, "suspend schedule " + scheduleId, "Successfully suspended");
    testCommandOutputContains(cli, "get schedule status " + scheduleId, "SUSPENDED");
    testCommandOutputContains(cli, "get workflow schedules " + workflowId, FakeApp.SCHEDULE_NAME);
  }

  @Test
  public void testDataset() throws Exception {
    String datasetName = PREFIX + "sdf123lkj";

    DatasetTypeClient datasetTypeClient = new DatasetTypeClient(cliConfig.getClientConfig());
    DatasetTypeMeta datasetType = datasetTypeClient.list(NamespaceId.DEFAULT.toId()).get(0);
    testCommandOutputContains(cli, "create dataset instance " + datasetType.getName() + " " + datasetName + " \"a=1\"",
                              "Successfully created dataset");
    testCommandOutputContains(cli, "list dataset instances", FakeDataset.class.getSimpleName());
    testCommandOutputContains(cli, "get dataset instance properties " + datasetName, "\"a\":\"1\"");

    NamespaceClient namespaceClient = new NamespaceClient(cliConfig.getClientConfig());
    NamespaceId barspace = new NamespaceId("bar");
    namespaceClient.create(new NamespaceMeta.Builder().setName(barspace.toId()).build());
    cliConfig.setNamespace(barspace);
    // list of dataset instances is different in 'foo' namespace
    testCommandOutputNotContains(cli, "list dataset instances", FakeDataset.class.getSimpleName());

    // also can not create dataset instances if the type it depends on exists only in a different namespace.
    DatasetTypeId datasetType1 = barspace.datasetType(datasetType.getName());
    testCommandOutputContains(cli, "create dataset instance " + datasetType.getName() + " " + datasetName,
                              new DatasetTypeNotFoundException(datasetType1).getMessage());

    testCommandOutputContains(cli, "use namespace default", "Now using namespace 'default'");
    try {
      testCommandOutputContains(cli, "truncate dataset instance " + datasetName, "Successfully truncated");
    } finally {
      testCommandOutputContains(cli, "delete dataset instance " + datasetName, "Successfully deleted");
    }

    String datasetName2 = PREFIX + "asoijm39485";
    String description = "test-description-for-" + datasetName2;
    testCommandOutputContains(cli, "create dataset instance " + datasetType.getName() + " " + datasetName2 +
                                " \"a=1\"" + " " + description,
                              "Successfully created dataset");
    testCommandOutputContains(cli, "list dataset instances", description);
    testCommandOutputContains(cli, "delete dataset instance " + datasetName2, "Successfully deleted");
  }

  @Test
  public void testService() throws Exception {
    ServiceId service = fakeAppId.service(PrefixedEchoHandler.NAME);
    ServiceId serviceV1 = fakeAppIdV1.service(PrefixedEchoHandler.NAME);
    String serviceName = String.format("%s.%s", FakeApp.NAME, PrefixedEchoHandler.NAME);
    String serviceArgument = String.format("%s version %s", serviceName, ApplicationId.DEFAULT_VERSION);
    String serviceV1Argument = String.format("%s version %s", serviceName, V1_SNAPSHOT);
    try {
      // Test service commands with no optional version argument
      testCommandOutputContains(cli, "start service " + serviceName, "Successfully started service");
      testCommandOutputContains(cli, "get endpoints service " + serviceName, "POST");
      testCommandOutputContains(cli, "get endpoints service " + serviceName, "/echo");
      testCommandOutputContains(cli, "check service availability " + serviceName, "Service is available");
      testCommandOutputContains(cli, "call service " + serviceName
        + " POST /echo body \"testBody\"", ":testBody");
      testCommandOutputContains(cli, "stop service " + serviceName, "Successfully stopped service");
      assertProgramStatus(programClient, service, "STOPPED");
      // Test service commands with version argument when two versions of the service are running
      testCommandOutputContains(cli, "start service " + serviceArgument, "Successfully started service");
      testCommandOutputContains(cli, "start service " + serviceV1Argument, "Successfully started service");
      assertProgramStatus(programClient, service, "RUNNING");
      assertProgramStatus(programClient, serviceV1, "RUNNING");
      testCommandOutputContains(cli, "get endpoints service " + serviceArgument, "POST");
      testCommandOutputContains(cli, "get endpoints service " + serviceV1Argument, "POST");
      testCommandOutputContains(cli, "get endpoints service " + serviceArgument, "/echo");
      testCommandOutputContains(cli, "get endpoints service " + serviceV1Argument, "/echo");
      testCommandOutputContains(cli, "check service availability " + serviceArgument, "Service is available");
      testCommandOutputContains(cli, "check service availability " + serviceV1Argument, "Service is available");
      testCommandOutputContains(cli, "call service " + serviceArgument
        + " POST /echo body \"testBody\"", ":testBody");
      testCommandOutputContains(cli, "call service " + serviceV1Argument
        + " POST /echo body \"testBody\"", ":testBody");
    } finally {
      // Stop all running services
      programClient.stopAll(NamespaceId.DEFAULT.toId());
    }
  }

  @Test
  public void testRuntimeArgs() throws Exception {
    ServiceId service = fakeAppId.service(PrefixedEchoHandler.NAME);
    ServiceId serviceV1 = fakeAppIdV1.service(PrefixedEchoHandler.NAME);
    String serviceName = String.format("%s.%s", FakeApp.NAME, PrefixedEchoHandler.NAME);
    String serviceArgument = String.format("%s version %s", serviceName, ApplicationId.DEFAULT_VERSION);
    String serviceV1Argument = String.format("%s version %s", serviceName, V1_SNAPSHOT);

    // Start service with default version
    Map<String, String> runtimeArgs = ImmutableMap.of("sdf", "bacon");
    String runtimeArgsJson = GSON.toJson(runtimeArgs);
    String runtimeArgsKV = Joiner.on(",").withKeyValueSeparator("=").join(runtimeArgs);
    testCommandOutputContains(cli, "start service " + serviceArgument + " '" + runtimeArgsKV + "'",
                              "Successfully started service");
    assertProgramStatus(programClient, service, "RUNNING");

    try {
      // Use commands with no optional version argument to call service and stop it
      testCommandOutputContains(cli, "call service " + serviceName + " POST /echo body \"testBody\"",
                                "bacon:testBody");
      testCommandOutputContains(cli, "stop service " + serviceName, "Successfully stopped service");
      assertProgramStatus(programClient, service, "STOPPED");
      
      // Start serviceV1
      Map<String, String> runtimeArgs1 = ImmutableMap.of("sdf", "chickenz");
      String runtimeArgs1Json = GSON.toJson(runtimeArgs1);
      String runtimeArgs1KV = Joiner.on(",").withKeyValueSeparator("=").join(runtimeArgs1);
      testCommandOutputContains(cli, "start service " + serviceV1Argument + " '" + runtimeArgs1KV + "'",
                                "Successfully started service");
      assertProgramStatus(programClient, serviceV1, "RUNNING");

      // Call serviceV1 and stop it
      testCommandOutputContains(cli, "call service " + serviceV1Argument + " POST /echo body \"testBody\"",
                                "chickenz:testBody");
      testCommandOutputContains(cli, "stop service " + serviceV1Argument, "Successfully stopped service");
      assertProgramStatus(programClient, serviceV1, "STOPPED");

      // Use commands with no optional version argument to set runtimeargs1 for service, get runtimeargs1, call service
      testCommandOutputContains(cli, "set service runtimeargs " + serviceName + " '" + runtimeArgs1KV + "'",
                                "Successfully set runtime args");
      testCommandOutputContains(cli, "start service " + serviceName, "Successfully started service");
      testCommandOutputContains(cli, "get service runtimeargs " + serviceName, runtimeArgs1Json);
      testCommandOutputContains(cli, "call service " + serviceName + " POST /echo body \"testBody\"",
                                "chickenz:testBody");

      // Use commands with version argument to set runtimeargs for serviceV1, get runtimeargs, call service when both
      // versions of the service are running
      testCommandOutputContains(cli, "set service runtimeargs " + serviceV1Argument + " '" + runtimeArgsKV + "'",
                                "Successfully set runtime args");
      testCommandOutputContains(cli, "start service " + serviceV1Argument, "Successfully started service");
      testCommandOutputContains(cli, "get service runtimeargs " + serviceV1Argument, runtimeArgsJson);
      testCommandOutputContains(cli, "call service " + serviceV1Argument + " POST /echo body \"testBody\"",
                                "bacon:testBody");
    } finally {
      // Stop all running services
      programClient.stopAll(NamespaceId.DEFAULT.toId());
    }
  }

  @Test
  public void testRouteConfig() throws Exception {
    ServiceId service = fakeAppId.service(PrefixedEchoHandler.NAME);
    ServiceId serviceV1 = fakeAppIdV1.service(PrefixedEchoHandler.NAME);
    String serviceName = String.format("%s.%s", FakeApp.NAME, PrefixedEchoHandler.NAME);
    String serviceArgument = String.format("%s version %s", serviceName, ApplicationId.DEFAULT_VERSION);
    String serviceV1Argument = String.format("%s version %s", serviceName, V1_SNAPSHOT);

    try {
      // Start service with default version
      Map<String, String> runtimeArgs = ImmutableMap.of("sdf", ApplicationId.DEFAULT_VERSION);
      String runtimeArgsKV = SPACE_JOINER.withKeyValueSeparator("=").join(runtimeArgs);
      testCommandOutputContains(cli, "start service " + serviceArgument + " '" + runtimeArgsKV + "'",
                                "Successfully started service");
      assertProgramStatus(programClient, service, "RUNNING");
      // Verify that RouteConfig is empty initially
      testCommandOutputContains(cli, "get route-config for service " + serviceName, GSON.toJson(ImmutableMap.of()));
      // Call non-version service and get response from service with Default version
      testCommandOutputContains(cli, "call service " + serviceName + " POST /echo body \"testBody\"",
                                String.format("%s:testBody", ApplicationId.DEFAULT_VERSION));

      // Start serviceV1
      Map<String, String> runtimeArgs1 = ImmutableMap.of("sdf", V1_SNAPSHOT);
      String runtimeArgs1KV = SPACE_JOINER.withKeyValueSeparator("=").join(runtimeArgs1);
      testCommandOutputContains(cli, "start service " + serviceV1Argument + " '" + runtimeArgs1KV + "'",
                                "Successfully started service");
      assertProgramStatus(programClient, serviceV1, "RUNNING");

      // Set RouteConfig to route all traffic to service with default version
      Map<String, Integer> routeConfig = ImmutableMap.of(ApplicationId.DEFAULT_VERSION, 100, V1_SNAPSHOT, 0);
      String routeConfigKV = SPACE_JOINER.withKeyValueSeparator("=").join(routeConfig);
      testCommandOutputContains(cli, "set route-config for service " + serviceName + " '" + routeConfigKV + "'",
                                "Successfully set route configuration");
      for (int i = 0; i < 10; i++) {
        testCommandOutputContains(cli, "call service " + serviceName + " POST /echo body \"testBody\"",
                                  String.format("%s:testBody", ApplicationId.DEFAULT_VERSION));
      }
      // Get the RouteConfig set previously
      testCommandOutputContains(cli, "get route-config for service " + serviceName, GSON.toJson(routeConfig));

      // Set RouteConfig to route all traffic to serviceV1
      routeConfig = ImmutableMap.of(ApplicationId.DEFAULT_VERSION, 0, V1_SNAPSHOT, 100);
      routeConfigKV = SPACE_JOINER.withKeyValueSeparator("=").join(routeConfig);
      testCommandOutputContains(cli, "set route-config for service " + serviceName + " '" + routeConfigKV + "'",
                                "Successfully set route configuration");
      for (int i = 0; i < 10; i++) {
        testCommandOutputContains(cli, "call service " + serviceName + " POST /echo body \"testBody\"",
                                  String.format("%s:testBody", V1_SNAPSHOT));
      }
      // Get the RouteConfig set previously
      testCommandOutputContains(cli, "get route-config for service " + serviceName, GSON.toJson(routeConfig));
      // Delete the RouteConfig and verify the RouteConfig is empty
      testCommandOutputContains(cli, "delete route-config for service " + serviceName,
                                "Successfully deleted route configuration");
      testCommandOutputContains(cli, "get route-config for service " + serviceName, GSON.toJson(ImmutableMap.of()));
    } finally {
      // Stop all running services
      programClient.stopAll(NamespaceId.DEFAULT.toId());
    }
  }

  @Test
  public void testSpark() throws Exception {
    String sparkId = FakeApp.SPARK.get(0);
    String qualifiedSparkId = FakeApp.NAME + "." + sparkId;
    ProgramId spark = NamespaceId.DEFAULT.app(FakeApp.NAME).spark(sparkId);

    testCommandOutputContains(cli, "list spark", sparkId);
    testCommandOutputContains(cli, "start spark " + qualifiedSparkId, "Successfully started Spark");
    assertProgramStatus(programClient, spark, "RUNNING");
    assertProgramStatus(programClient, spark, "STOPPED");
    testCommandOutputContains(cli, "get spark status " + qualifiedSparkId, "STOPPED");
    testCommandOutputContains(cli, "get spark runs " + qualifiedSparkId, "COMPLETED");
    testCommandOutputContains(cli, "get spark logs " + qualifiedSparkId, "HelloFakeSpark");
  }

  @Test
  public void testPreferences() throws Exception {
    testPreferencesOutput(cli, "get preferences instance", ImmutableMap.<String, String>of());
    Map<String, String> propMap = Maps.newHashMap();
    propMap.put("key", "newinstance");
    propMap.put("k1", "v1");
    testCommandOutputContains(cli, "delete preferences instance", "successfully");
    testCommandOutputContains(cli, "set preferences instance 'key=newinstance k1=v1'",
                              "successfully");
    testPreferencesOutput(cli, "get preferences instance", propMap);
    testPreferencesOutput(cli, "get resolved preferences instance", propMap);
    testCommandOutputContains(cli, "delete preferences instance", "successfully");
    propMap.clear();
    testPreferencesOutput(cli, "get preferences instance", propMap);
    propMap.put("key", "flow");
    testCommandOutputContains(cli, String.format("set preferences flow 'key=flow' %s.%s",
                                                 FakeApp.NAME, FakeFlow.NAME), "successfully");
    testPreferencesOutput(cli, String.format("get preferences flow %s.%s", FakeApp.NAME, FakeFlow.NAME), propMap);
    testCommandOutputContains(cli, String.format("delete preferences flow %s.%s", FakeApp.NAME, FakeFlow.NAME),
                              "successfully");
    propMap.clear();
    testPreferencesOutput(cli, String.format("get preferences app %s", FakeApp.NAME), propMap);
    testPreferencesOutput(cli, "get preferences namespace", propMap);
    testCommandOutputContains(cli, "get preferences app invalidapp", "not found");

    File file = new File(TMP_FOLDER.newFolder(), "prefFile.txt");
    // If the file not exist or not a file, upload should fails with an error.
    testCommandOutputContains(cli, "load preferences instance " + file.getAbsolutePath() + " json", "Not a file");
    testCommandOutputContains(cli, "load preferences instance " + file.getParentFile().getAbsolutePath() + " json",
                              "Not a file");
    // Generate a file to load
    BufferedWriter writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      writer.write("{'key':'somevalue'}");
    } finally {
      writer.close();
    }
    testCommandOutputContains(cli, "load preferences instance " + file.getAbsolutePath() + " json", "successful");
    propMap.clear();
    propMap.put("key", "somevalue");
    testPreferencesOutput(cli, "get preferences instance", propMap);
    testCommandOutputContains(cli, "delete preferences namespace", "successfully");
    testCommandOutputContains(cli, "delete preferences instance", "successfully");

    //Try invalid Json
    file = new File(TMP_FOLDER.newFolder(), "badPrefFile.txt");
    writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      writer.write("{'key:'somevalue'}");
    } finally {
      writer.close();
    }
    testCommandOutputContains(cli, "load preferences instance " + file.getAbsolutePath() + " json", "invalid");
    testCommandOutputContains(cli, "load preferences instance " + file.getAbsolutePath() + " xml", "Unsupported");

    testCommandOutputContains(cli, "set preferences namespace 'k1=v1'", "successfully");
    testCommandOutputContains(cli, "set preferences namespace 'k1=v1' name",
                              "Error: Expected format: set preferences namespace <runtime-args>");
    testCommandOutputContains(cli, "set preferences instance 'k1=v1' name",
                              "Error: Expected format: set preferences instance <runtime-args>");

  }

  @Test
  public void testNamespaces() throws Exception {
    final String name = PREFIX + "testNamespace";
    final String description = "testDescription";
    final String keytab = "keytab";
    final String principal = "principal";
    final String hbaseNamespace = "hbase";
    final String hiveDatabase = "hiveDB";
    final String schedulerQueueName = "queue";
    File rootdir = TEMPORARY_FOLDER.newFolder("rootdir");
    final String rootDirectory = rootdir.getAbsolutePath();
    final String defaultFields = PREFIX + "defaultFields";
    final String doesNotExist = "doesNotExist";
    createHiveDB(hiveDatabase);
    // initially only default namespace should be present
    NamespaceMeta defaultNs = new NamespaceMeta.Builder()
      .setName("default").setDescription("Default Namespace").build();
    List<NamespaceMeta> expectedNamespaces = Lists.newArrayList(defaultNs);
    testNamespacesOutput(cli, "list namespaces", expectedNamespaces);

    // describe non-existing namespace
    testCommandOutputContains(cli, String.format("describe namespace %s", doesNotExist),
                              String.format("Error: 'namespace:%s' was not found", doesNotExist));
    // delete non-existing namespace
    // TODO: uncomment when fixed - this makes build hang since it requires confirmation from user
//    testCommandOutputContains(cli, String.format("delete namespace %s", doesNotExist),
//                              String.format("Error: namespace '%s' was not found", doesNotExist));

    // create a namespace
    String command = String.format("create namespace %s description %s principal %s keytab-URI %s " +
                                     "hbase-namespace %s hive-database %s root-directory %s scheduler-queue-name %s",
                                   name, description, principal, keytab, hbaseNamespace,
                                   hiveDatabase, rootDirectory, schedulerQueueName);
    testCommandOutputContains(cli, command, String.format("Namespace '%s' created successfully.", name));

    NamespaceMeta expected = new NamespaceMeta.Builder()
      .setName(name).setDescription(description).setPrincipal(principal).setKeytabURI(keytab)
      .setHBaseNamespace(hbaseNamespace).setSchedulerQueueName(schedulerQueueName)
      .setHiveDatabase(hiveDatabase).setRootDirectory(rootDirectory).build();
    expectedNamespaces = Lists.newArrayList(defaultNs, expected);
    // list namespaces and verify
    testNamespacesOutput(cli, "list namespaces", expectedNamespaces);

    // get namespace details and verify
    expectedNamespaces = Lists.newArrayList(expected);
    command = String.format("describe namespace %s", name);
    testNamespacesOutput(cli, command, expectedNamespaces);

    // try creating a namespace with existing id
    command = String.format("create namespace %s", name);
    testCommandOutputContains(cli, command, String.format("Error: 'namespace:%s' already exists\n", name));

    // create a namespace with default name and description
    command = String.format("create namespace %s", defaultFields);
    testCommandOutputContains(cli, command, String.format("Namespace '%s' created successfully.", defaultFields));

    NamespaceMeta namespaceDefaultFields = new NamespaceMeta.Builder()
      .setName(defaultFields).setDescription("").build();
    // test that there are 3 namespaces including default
    expectedNamespaces = Lists.newArrayList(defaultNs, namespaceDefaultFields, expected);
    testNamespacesOutput(cli, "list namespaces", expectedNamespaces);
    // describe namespace with default fields
    expectedNamespaces = Lists.newArrayList(namespaceDefaultFields);
    testNamespacesOutput(cli, String.format("describe namespace %s", defaultFields), expectedNamespaces);

    // delete namespace and verify
    // TODO: uncomment when fixed - this makes build hang since it requires confirmation from user
//    command = String.format("delete namespace %s", name);
//    testCommandOutputContains(cli, command, String.format("Namespace '%s' deleted successfully.", name));
    dropHiveDb(hiveDatabase);
  }

  @Test
  public void testWorkflows() throws Exception {
    String workflow = String.format("%s.%s", FakeApp.NAME, FakeWorkflow.NAME);
    File doneFile = TMP_FOLDER.newFile("fake.done");
    Map<String, String> runtimeArgs = ImmutableMap.of("done.file", doneFile.getAbsolutePath());
    String runtimeArgsKV = Joiner.on(",").withKeyValueSeparator("=").join(runtimeArgs);
    testCommandOutputContains(cli, "start workflow " + workflow + " '" + runtimeArgsKV + "'",
                              "Successfully started workflow");
    assertProgramStatus(programClient, fakeWorkflowId, "STOPPED");
    testCommandOutputContains(cli, "cli render as csv", "Now rendering as CSV");
    String commandOutput = getCommandOutput(cli, "get workflow runs " + workflow);
    String[] lines = commandOutput.split("\\r?\\n");
    Assert.assertEquals(2, lines.length);
    String[] split = lines[1].split(",");
    String runId = split[0];
    // Test entire workflow token
    List<WorkflowTokenDetail.NodeValueDetail> tokenValues = new ArrayList<>();
    tokenValues.add(new WorkflowTokenDetail.NodeValueDetail(FakeWorkflow.FakeAction.class.getSimpleName(),
                                                            FakeWorkflow.FakeAction.TOKEN_VALUE));
    tokenValues.add(new WorkflowTokenDetail.NodeValueDetail(FakeWorkflow.FakeAction.ANOTHER_FAKE_NAME,
                                                            FakeWorkflow.FakeAction.TOKEN_VALUE));
    testCommandOutputContains(cli, String.format("get workflow token %s %s", workflow, runId),
                              Joiner.on(",").join(FakeWorkflow.FakeAction.TOKEN_KEY, GSON.toJson(tokenValues)));
    testCommandOutputNotContains(cli, String.format("get workflow token %s %s scope system", workflow, runId),
                                 Joiner.on(",").join(FakeWorkflow.FakeAction.TOKEN_KEY, GSON.toJson(tokenValues)));
    testCommandOutputContains(
      cli, String.format("get workflow token %s %s scope user key %s", workflow, runId,
                         FakeWorkflow.FakeAction.TOKEN_KEY),
      Joiner.on(",").join(FakeWorkflow.FakeAction.TOKEN_KEY, GSON.toJson(tokenValues)));

    // Test with node name
    String fakeNodeValue = Joiner.on(",").join(FakeWorkflow.FakeAction.TOKEN_KEY, FakeWorkflow.FakeAction.TOKEN_VALUE);
    testCommandOutputContains(
      cli, String.format("get workflow token %s %s at node %s", workflow, runId,
                         FakeWorkflow.FakeAction.class.getSimpleName()), fakeNodeValue);
    testCommandOutputNotContains(
      cli, String.format("get workflow token %s %s at node %s scope system", workflow, runId,
                         FakeWorkflow.FakeAction.ANOTHER_FAKE_NAME), fakeNodeValue);
    testCommandOutputContains(
      cli, String.format("get workflow token %s %s at node %s scope user key %s", workflow, runId,
                         FakeWorkflow.FakeAction.ANOTHER_FAKE_NAME, FakeWorkflow.FakeAction.TOKEN_KEY), fakeNodeValue);

    testCommandOutputContains(cli, "get workflow logs " + workflow, FakeWorkflow.FAKE_LOG);

    // stop workflow
    testCommandOutputContains(cli, "stop workflow " + workflow,
                              String.format("400: Program '%s' is not running", fakeWorkflowId));
  }

  @Test
  public void testMetadata() throws Exception {
    testCommandOutputContains(cli, "cli render as csv", "Now rendering as CSV");
    // verify system metadata
    testCommandOutputContains(cli, String.format("get metadata %s scope system", fakeAppId),
                              FakeApp.class.getSimpleName());
    testCommandOutputContains(cli, String.format("get metadata-tags %s scope system", fakeWorkflowId),
                              FakeWorkflow.FakeAction.class.getSimpleName());
    testCommandOutputContains(cli, String.format("get metadata-tags %s scope system", fakeWorkflowId),
                              FakeWorkflow.FakeAction.ANOTHER_FAKE_NAME);
    testCommandOutputContains(cli, String.format("get metadata-tags %s scope system", fakeDsId),
                              "batch");
    testCommandOutputContains(cli, String.format("get metadata-tags %s scope system", fakeDsId),
                              "explore");
    testCommandOutputContains(cli, String.format("get metadata-tags %s scope system", fakeStreamId),
                              FakeApp.STREAM_NAME);
    testCommandOutputContains(cli, String.format("add metadata-properties %s appKey=appValue", fakeAppId),
                              "Successfully added metadata properties");
    testCommandOutputContains(cli, String.format("get metadata-properties %s", fakeAppId), "appKey,appValue");
    testCommandOutputContains(cli, String.format("add metadata-tags %s 'wfTag1 wfTag2'", fakeWorkflowId),
                              "Successfully added metadata tags");
    String output = getCommandOutput(cli, String.format("get metadata-tags %s", fakeWorkflowId));
    List<String> lines = Arrays.asList(output.split("\\r?\\n"));
    Assert.assertTrue(lines.contains("wfTag1") && lines.contains("wfTag2"));
    testCommandOutputContains(cli, String.format("add metadata-properties %s dsKey=dsValue", fakeDsId),
                              "Successfully added metadata properties");
    testCommandOutputContains(cli, String.format("get metadata-properties %s", fakeDsId), "dsKey,dsValue");
    testCommandOutputContains(cli, String.format("add metadata-tags %s 'streamTag1 streamTag2'", fakeStreamId),
                              "Successfully added metadata tags");
    output = getCommandOutput(cli, String.format("get metadata-tags %s", fakeStreamId));
    lines = Arrays.asList(output.split("\\r?\\n"));
    Assert.assertTrue(lines.contains("streamTag1") && lines.contains("streamTag2"));
    // test search
    testCommandOutputContains(cli, String.format("search metadata %s filtered by target-type artifact",
                                                 FakeApp.class.getSimpleName()), fakeArtifactId.toString());
    testCommandOutputContains(cli, "search metadata appKey:appValue", fakeAppId.toString());
    testCommandOutputContains(cli, "search metadata fake* filtered by target-type app", fakeAppId.toString());
    output = getCommandOutput(cli, "search metadata fake* filtered by target-type program");
    lines = Arrays.asList(output.split("\\r?\\n"));
    List<String> expected = ImmutableList.of("Entity", fakeWorkflowId.toString(), fakeSparkId.toString(),
                                             fakeFlowId.toString());
    Assert.assertTrue(lines.containsAll(expected) && expected.containsAll(lines));
    testCommandOutputContains(cli, "search metadata fake* filtered by target-type dataset", fakeDsId.toString());
    testCommandOutputContains(cli, "search metadata fake* filtered by target-type stream", fakeStreamId.toString());
    testCommandOutputContains(cli, String.format("search metadata %s", FakeApp.SCHEDULE_NAME), fakeAppId.toString());
    testCommandOutputContains(cli, String.format("search metadata %s", FakeApp.STREAM_SCHEDULE_NAME),
                              fakeAppId.toString());
    testCommandOutputContains(cli, String.format("search metadata %s filtered by target-type app", PingService.NAME),
                              fakeAppId.toString());
    testCommandOutputContains(cli, String.format("search metadata %s filtered by target-type program",
                                                 PrefixedEchoHandler.NAME), prefixedEchoHandlerId.toString());
    testCommandOutputContains(cli, "search metadata batch* filtered by target-type dataset", fakeDsId.toString());
    testCommandOutputNotContains(cli, "search metadata batchwritable filtered by target-type dataset",
                                 fakeDsId.toString());
    testCommandOutputContains(cli, "search metadata bat* filtered by target-type dataset", fakeDsId.toString());
    output = getCommandOutput(cli, "search metadata batch filtered by target-type program");
    lines = Arrays.asList(output.split("\\r?\\n"));
    expected = ImmutableList.of("Entity", fakeSparkId.toString(), fakeWorkflowId.toString());
    Assert.assertTrue(lines.containsAll(expected) && expected.containsAll(lines));
    output = getCommandOutput(cli, "search metadata realtime filtered by target-type program");
    lines = Arrays.asList(output.split("\\r?\\n"));
    expected = ImmutableList.of("Entity", fakeFlowId.toString(), pingServiceId.toString(),
                                prefixedEchoHandlerId.toString());
    Assert.assertTrue(lines.containsAll(expected) && expected.containsAll(lines));
    output = getCommandOutput(cli, "search metadata fake* filtered by target-type dataset,stream");
    lines = Arrays.asList(output.split("\\r?\\n"));
    expected = ImmutableList.of("Entity", fakeDsId.toString(), fakeStreamId.toString());
    Assert.assertTrue(lines.containsAll(expected) && expected.containsAll(lines));
    output = getCommandOutput(cli, "search metadata fake* filtered by target-type dataset,stream,app");
    lines = Arrays.asList(output.split("\\r?\\n"));
    expected = ImmutableList.of("Entity", fakeDsId.toString(), fakeStreamId.toString(), fakeAppId.toString());
    Assert.assertTrue(lines.containsAll(expected) && expected.containsAll(lines));
  }

  private static File createAppJarFile(Class<?> cls) throws IOException {
    File tmpFolder = TMP_FOLDER.newFolder();
    LocationFactory locationFactory = new LocalLocationFactory(tmpFolder);
    Location deploymentJar = AppJarHelper.createDeploymentJar(locationFactory, cls);
    File appJarFile =
      new File(tmpFolder, String.format("%s-1.0.%d.jar", cls.getSimpleName(), System.currentTimeMillis()));
    Files.copy(Locations.newInputSupplier(deploymentJar), appJarFile);
    return appJarFile;
  }

  protected void assertProgramStatus(ProgramClient programClient, ProgramId programId, String programStatus, int tries)
    throws IOException, ProgramNotFoundException, UnauthenticatedException, UnauthorizedException {

    String status;
    int numTries = 0;
    do {
      status = programClient.getStatus(programId);
      numTries++;
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        // NO-OP
      }
    } while (!status.equals(programStatus) && numTries <= tries);
    Assert.assertEquals(programStatus, status);
  }

  protected void assertProgramStatus(ProgramClient programClient, ProgramId programId, String programStatus)
    throws IOException, ProgramNotFoundException, UnauthenticatedException, UnauthorizedException {

    assertProgramStatus(programClient, programId, programStatus, 180);
  }

  private static void testNamespacesOutput(CLI cli, String command, final List<NamespaceMeta> expected)
    throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream output = new PrintStream(outputStream);
    Table table = Table.builder()
      .setHeader("name", "description", "config")
      .setRows(expected, new RowMaker<NamespaceMeta>() {
        @Override
        public List<?> makeRow(NamespaceMeta object) {
          return Lists.newArrayList(object.getName(), object.getDescription(),
                                    NamespaceCommandUtils.prettyPrintNamespaceConfigCLI(object.getConfig()));
        }
      }).build();
    cliMain.getTableRenderer().render(cliConfig, output, table);
    final String expectedOutput = outputStream.toString();

    testCommand(cli, command, new Function<String, Void>() {
      @Nullable
      @Override
      public Void apply(@Nullable String output) {
        Assert.assertNotNull(output);
        Assert.assertEquals(expectedOutput, output);
        return null;
      }
    });
  }

  private static void testPreferencesOutput(CLI cli, String command, final Map<String, String> expected)
    throws Exception {
    testCommand(cli, command, new Function<String, Void>() {
      @Nullable
      @Override
      public Void apply(@Nullable String output) {
        Assert.assertNotNull(output);
        Map<String, String> outputMap = Splitter.on(System.lineSeparator())
          .omitEmptyStrings().withKeyValueSeparator("=").split(output);
        Assert.assertEquals(expected, outputMap);
        return null;
      }
    });
  }

  private static void createHiveDB(String hiveDb) throws Exception {
    ListenableFuture<ExploreExecutionResult> future =
      queryClient.execute(NamespaceId.DEFAULT.toId(), "create database " + hiveDb);
    assertExploreQuerySuccess(future);
    future = queryClient.execute(NamespaceId.DEFAULT.toId(), "describe database " + hiveDb);
    assertExploreQuerySuccess(future);
  }

  private static void dropHiveDb(String hiveDb) throws Exception {
    assertExploreQuerySuccess(queryClient.execute(NamespaceId.DEFAULT.toId(), "drop database " + hiveDb));
  }

  private static void assertExploreQuerySuccess(
    ListenableFuture<ExploreExecutionResult> dbCreationFuture) throws Exception {
    ExploreExecutionResult exploreExecutionResult = dbCreationFuture.get(10, TimeUnit.SECONDS);
    QueryStatus status = exploreExecutionResult.getStatus();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, status.getStatus());
  }
}
