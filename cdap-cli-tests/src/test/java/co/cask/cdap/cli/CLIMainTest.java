/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.client.app.FakePlugin;
import co.cask.cdap.client.app.FakeSpark;
import co.cask.cdap.client.app.FakeWorkflow;
import co.cask.cdap.client.app.PingService;
import co.cask.cdap.client.app.PluginConfig;
import co.cask.cdap.client.app.PrefixedEchoHandler;
import co.cask.cdap.common.DatasetTypeNotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.metadata.system.AbstractSystemMetadataWriter;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.RunRecord;
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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Test for {@link CLIMain}.
 */
@Category(XSlowTests.class)
public class CLIMainTest extends CLITestBase {

  @ClassRule
  public static final StandaloneTester STANDALONE = new StandaloneTester(
    // Turn off the log event delay to make log event flush on every write to eliminate race condition.
    Constants.Logging.PIPELINE_EVENT_DELAY_MS, 0
  );

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final Logger LOG = LoggerFactory.getLogger(CLIMainTest.class);
  private static final Joiner.MapJoiner SPACE_EQUALS_JOINER = Joiner.on(" ").withKeyValueSeparator("=");
  private static final Gson GSON = new Gson();
  private static final String PREFIX = "123ff1_";
  private static final String V1 = "1.0";
  private static final String V1_SNAPSHOT = "v1.0.0-SNAPSHOT";

  private static final ArtifactId FAKE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact(FakeApp.NAME, V1);
  private static final ApplicationId FAKE_APP_ID = NamespaceId.DEFAULT.app(FakeApp.NAME);
  private static final ApplicationId FAKE_APP_ID_V_1 = NamespaceId.DEFAULT.app(FakeApp.NAME, V1_SNAPSHOT);
  private static final ArtifactId FAKE_PLUGIN_ID = NamespaceId.DEFAULT.artifact(FakePlugin.NAME, V1);
  private static final ProgramId FAKE_WORKFLOW_ID = FAKE_APP_ID.workflow(FakeWorkflow.NAME);
  private static final ProgramId FAKE_FLOW_ID = FAKE_APP_ID.flow(FakeFlow.NAME);
  private static final ProgramId FAKE_SPARK_ID = FAKE_APP_ID.spark(FakeSpark.NAME);
  private static final ServiceId PING_SERVICE_ID = FAKE_APP_ID.service(PingService.NAME);
  private static final ServiceId PREFIXED_ECHO_HANDLER_ID = FAKE_APP_ID.service(PrefixedEchoHandler.NAME);
  private static final DatasetId FAKE_DS_ID = NamespaceId.DEFAULT.dataset(FakeApp.DS_NAME);
  private static final StreamId FAKE_STREAM_ID = NamespaceId.DEFAULT.stream(FakeApp.STREAM_NAME);

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

    File pluginJarFile = createPluginJarFile(FakePlugin.class);
    testCommandOutputContains(cli, String.format("load artifact %s config-file %s", pluginJarFile.getAbsolutePath(),
                                                 createPluginConfig().getAbsolutePath()), "Successfully");
    if (!appJarFile.delete()) {
      LOG.warn("Failed to delete temporary app jar file: {}", appJarFile.getAbsolutePath());
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    programClient.stopAll(NamespaceId.DEFAULT);
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
    final ProgramId flow = FAKE_APP_ID.flow(flowId);

    String qualifiedFlowId = FakeApp.NAME + "." + flowId;
    testCommandOutputContains(cli, "start flow " + qualifiedFlowId, "Successfully started flow");
    assertProgramStatus(programClient, flow, "RUNNING");
    testCommandOutputContains(cli, "stop flow " + qualifiedFlowId, "Successfully stopped flow");
    assertProgramStatus(programClient, flow, "STOPPED");
    testCommandOutputContains(cli, "get flow status " + qualifiedFlowId, "STOPPED");
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        List<RunRecord> output = programClient.getProgramRuns(flow, "KILLED", 0L, Long.MAX_VALUE, Integer.MAX_VALUE);
        return output != null && output.size() != 0;
      }
    }, 5, TimeUnit.SECONDS);
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
    testCommandOutputContains(cli, "delete dataset instance " + datasetId, "Successfully deleted");
    testCommandOutputContains(cli, "delete stream " + streamId, "Successfully deleted stream");
  }

  @Test
  public void testGetParentArtifact() throws Exception {
    testCommandOutputContains(cli, String.format("describe artifact %s %s", FAKE_PLUGIN_ID.getArtifact(),
                                                 FAKE_PLUGIN_ID.getVersion()), FakeApp.NAME);
  }

  @Test
  public void testDeployAppWithConfigFile() throws Exception {
    String streamId = "testStream";
    String datasetId = "testTable";

    File appJarFile = createAppJarFile(ConfigTestApp.class);
    File configFile = new File(TMP_FOLDER.newFolder(), "testConfigFile.txt");
    try (BufferedWriter writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      writer.write(String.format("{\n\"streamName\":\"%s\",\n\"tableName\":\"%s\"\n}", streamId, datasetId));
      writer.close();
    }

    testCommandOutputContains(cli, String.format("deploy app %s with config %s", appJarFile.getAbsolutePath(),
                                                 configFile.getAbsolutePath()), "Successfully deployed application");
    testCommandOutputContains(cli, "list streams", streamId);
    testCommandOutputContains(cli, "list dataset instances", datasetId);
    testCommandOutputContains(cli, "delete app " + ConfigTestApp.NAME, "Successfully");
    testCommandOutputContains(cli, "delete dataset instance " + datasetId, "Successfully deleted");
    testCommandOutputContains(cli, "delete stream " + streamId, "Successfully deleted stream");
  }

  @Test
  public void testAppOwner() throws Exception {
    // load an artifact
    File appJarFile = createAppJarFile(ConfigTestApp.class);
    String streamId = ConfigTestApp.DEFAULT_STREAM;
    String datasetId = ConfigTestApp.DEFAULT_TABLE;
    testCommandOutputContains(cli, String.format("load artifact %s name %s version %s", appJarFile.getAbsolutePath(),
                                                 "OwnedConfigTestAppArtifact", V1),
                              "Successfully added artifact");

    // create a config file which has principal information
    File configFile = new File(TMP_FOLDER.newFolder(), "testOwnerConfigFile.txt");
    try (BufferedWriter writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      writer.write(String.format("{\n\"%s\":\"%s\"\n}", ArgumentName.PRINCIPAL, "user/host.net@kdc.net"));
      writer.close();
    }

    // create app with the config containing the principla
    testCommandOutputContains(cli, String.format("create app %s %s %s %s %s",
                                                 "OwnedApp", "OwnedConfigTestAppArtifact", V1,
                                                 ArtifactScope.USER, configFile.getAbsolutePath()),
                              "Successfully created application");

    /// ensure that the app details have owner information
    testCommandOutputContains(cli, "list apps", "user/host.net@kdc.net");

    // clean up
    if (!appJarFile.delete()) {
      LOG.warn("Failed to delete temporary app jar file: {}", appJarFile.getAbsolutePath());
    }
    testCommandOutputContains(cli, "delete app " + "OwnedApp", "Successfully");
    testCommandOutputContains(cli, "delete dataset instance " + datasetId, "Successfully deleted");
    testCommandOutputContains(cli, "delete stream " + streamId, "Successfully deleted stream");
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
    String scheduleId = FakeApp.NAME + "." + FakeApp.TIME_SCHEDULE_NAME;
    String workflowId = FakeApp.NAME + "." + FakeWorkflow.NAME;
    testCommandOutputContains(cli, "get schedule status " + scheduleId, "SUSPENDED");
    testCommandOutputContains(cli, "resume schedule " + scheduleId, "Successfully resumed");
    testCommandOutputContains(cli, "get schedule status " + scheduleId, "SCHEDULED");
    testCommandOutputContains(cli, "suspend schedule " + scheduleId, "Successfully suspended");
    testCommandOutputContains(cli, "get schedule status " + scheduleId, "SUSPENDED");
    testCommandOutputContains(cli, "get workflow schedules " + workflowId, FakeApp.TIME_SCHEDULE_NAME);
  }

  @Test
  public void testDataset() throws Exception {
    String datasetName = PREFIX + "sdf123lkj";
    String ownedDatasetName = PREFIX + "owned";

    DatasetTypeClient datasetTypeClient = new DatasetTypeClient(cliConfig.getClientConfig());
    DatasetTypeMeta datasetType = datasetTypeClient.list(NamespaceId.DEFAULT).get(0);
    testCommandOutputContains(cli, "create dataset instance " + datasetType.getName() + " " + datasetName + " \"a=1\"",
                              "Successfully created dataset");
    testCommandOutputContains(cli, "list dataset instances", FakeDataset.class.getSimpleName());
    testCommandOutputContains(cli, "get dataset instance properties " + datasetName, "a,1");

    // test dataset creation with owner
    String commandOutput = getCommandOutput(cli, "create dataset instance " + datasetType.getName() + " " +
      ownedDatasetName + " \"a=1\"" + " " + "someDescription " + ArgumentName.PRINCIPAL +
      " alice/somehost.net@somekdc.net");
    Assert.assertTrue(commandOutput.contains("Successfully created dataset"));
    Assert.assertTrue(commandOutput.contains("alice/somehost.net@somekdc.net"));

    // test describing the table returns the given owner information
    testCommandOutputContains(cli, "describe dataset instance " + ownedDatasetName, "alice/somehost.net@somekdc.net");

    NamespaceClient namespaceClient = new NamespaceClient(cliConfig.getClientConfig());
    NamespaceId barspace = new NamespaceId("bar");
    namespaceClient.create(new NamespaceMeta.Builder().setName(barspace).build());
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
    testCommandOutputContains(cli, "delete dataset instance " + ownedDatasetName, "Successfully deleted");
  }

  @Test
  public void testService() throws Exception {
    ServiceId service = FAKE_APP_ID.service(PrefixedEchoHandler.NAME);
    ServiceId serviceV1 = FAKE_APP_ID_V_1.service(PrefixedEchoHandler.NAME);
    String serviceName = String.format("%s.%s", FakeApp.NAME, PrefixedEchoHandler.NAME);
    String serviceArgument = String.format("%s version %s", serviceName, ApplicationId.DEFAULT_VERSION);
    String serviceV1Argument = String.format("%s version %s", serviceName, V1_SNAPSHOT);
    try {
      // Test service commands with no optional version argument
      testCommandOutputContains(cli, "start service " + serviceName, "Successfully started service");
      assertProgramStatus(programClient, service, ProgramStatus.RUNNING.name());
      testCommandOutputContains(cli, "get endpoints service " + serviceName, "POST");
      testCommandOutputContains(cli, "get endpoints service " + serviceName, "/echo");
      testCommandOutputContains(cli, "check service availability " + serviceName, "Service is available");
      testCommandOutputContains(cli, "call service " + serviceName
        + " POST /echo body \"testBody\"", ":testBody");
      testCommandOutputContains(cli, "stop service " + serviceName, "Successfully stopped service");
      assertProgramStatus(programClient, service, ProgramStatus.STOPPED.name());
      // Test service commands with version argument when two versions of the service are running
      testCommandOutputContains(cli, "start service " + serviceArgument, "Successfully started service");
      testCommandOutputContains(cli, "start service " + serviceV1Argument, "Successfully started service");
      assertProgramStatus(programClient, service, ProgramStatus.RUNNING.name());
      assertProgramStatus(programClient, serviceV1, ProgramStatus.RUNNING.name());
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
      programClient.stopAll(NamespaceId.DEFAULT);
    }
  }

  @Test
  public void testRuntimeArgs() throws Exception {
    String qualifiedServiceId = String.format("%s.%s", FakeApp.NAME, PrefixedEchoHandler.NAME);
    ServiceId service = NamespaceId.DEFAULT.app(FakeApp.NAME).service(PrefixedEchoHandler.NAME);
    testServiceRuntimeArgs(qualifiedServiceId, service);
  }

  @Test
  public void testVersionedRuntimeArgs() throws Exception {
    String versionedServiceId = String.format("%s.%s version %s", FakeApp.NAME, PrefixedEchoHandler.NAME, V1_SNAPSHOT);
    ServiceId service = FAKE_APP_ID_V_1.service(PrefixedEchoHandler.NAME);
    testServiceRuntimeArgs(versionedServiceId, service);
  }

  public void testServiceRuntimeArgs(String qualifiedServiceId, ServiceId service) throws Exception {
    Map<String, String> runtimeArgs = ImmutableMap.of("sdf", "bacon");
    String runtimeArgsKV = SPACE_EQUALS_JOINER.join(runtimeArgs);
    testCommandOutputContains(cli, "start service " + qualifiedServiceId + " '" + runtimeArgsKV + "'",
                              "Successfully started service");
    assertProgramStatus(programClient, service, "RUNNING");
    testCommandOutputContains(cli, "call service " + qualifiedServiceId + " POST /echo body \"testBody\"",
                              "bacon:testBody");
    testCommandOutputContains(cli, "stop service " + qualifiedServiceId, "Successfully stopped service");
    assertProgramStatus(programClient, service, "STOPPED");

    Map<String, String> runtimeArgs2 = ImmutableMap.of("sdf", "chickenz");
    String runtimeArgs2Json = GSON.toJson(runtimeArgs2);
    String runtimeArgs2KV = SPACE_EQUALS_JOINER.join(runtimeArgs2);
    testCommandOutputContains(cli, "set service runtimeargs " + qualifiedServiceId + " '" + runtimeArgs2KV + "'",
                              "Successfully set runtime args");
    testCommandOutputContains(cli, "start service " + qualifiedServiceId, "Successfully started service");
    assertProgramStatus(programClient, service, "RUNNING");
    testCommandOutputContains(cli, "get service runtimeargs " + qualifiedServiceId, runtimeArgs2Json);
    testCommandOutputContains(cli, "call service " + qualifiedServiceId + " POST /echo body \"testBody\"",
                              "chickenz:testBody");
    testCommandOutputContains(cli, "stop service " + qualifiedServiceId, "Successfully stopped service");
    assertProgramStatus(programClient, service, "STOPPED");
  }

  @Test
  public void testRouteConfig() throws Exception {
    ServiceId service = FAKE_APP_ID.service(PrefixedEchoHandler.NAME);
    ServiceId serviceV1 = FAKE_APP_ID_V_1.service(PrefixedEchoHandler.NAME);
    String serviceName = String.format("%s.%s", FakeApp.NAME, PrefixedEchoHandler.NAME);
    String serviceArgument = String.format("%s version %s", serviceName, ApplicationId.DEFAULT_VERSION);
    String serviceV1Argument = String.format("%s version %s", serviceName, V1_SNAPSHOT);

    try {
      // Start service with default version
      Map<String, String> runtimeArgs = ImmutableMap.of("sdf", ApplicationId.DEFAULT_VERSION);
      String runtimeArgsKV = SPACE_EQUALS_JOINER.join(runtimeArgs);
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
      String runtimeArgs1KV = SPACE_EQUALS_JOINER.join(runtimeArgs1);
      testCommandOutputContains(cli, "start service " + serviceV1Argument + " '" + runtimeArgs1KV + "'",
                                "Successfully started service");
      assertProgramStatus(programClient, serviceV1, "RUNNING");

      // Set RouteConfig to route all traffic to service with default version
      Map<String, Integer> routeConfig = ImmutableMap.of(ApplicationId.DEFAULT_VERSION, 100, V1_SNAPSHOT, 0);
      String routeConfigKV = SPACE_EQUALS_JOINER.join(routeConfig);
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
      routeConfigKV = SPACE_EQUALS_JOINER.join(routeConfig);
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
      programClient.stopAll(NamespaceId.DEFAULT);
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
    testPreferencesOutput(cli, "get instance preferences", ImmutableMap.<String, String>of());
    Map<String, String> propMap = Maps.newHashMap();
    propMap.put("key", "newinstance");
    propMap.put("k1", "v1");
    testCommandOutputContains(cli, "delete instance preferences", "successfully");
    testCommandOutputContains(cli, "set instance preferences 'key=newinstance k1=v1'",
                              "successfully");
    testPreferencesOutput(cli, "get instance preferences", propMap);
    testPreferencesOutput(cli, "get resolved instance preferences", propMap);
    testCommandOutputContains(cli, "delete instance preferences", "successfully");
    propMap.clear();
    testPreferencesOutput(cli, "get instance preferences", propMap);
    propMap.put("key", "flow");
    testCommandOutputContains(cli, String.format("set flow preferences %s.%s 'key=flow'",
                                                 FakeApp.NAME, FakeFlow.NAME), "successfully");
    testPreferencesOutput(cli, String.format("get flow preferences %s.%s", FakeApp.NAME, FakeFlow.NAME), propMap);
    testCommandOutputContains(cli, String.format("delete flow preferences %s.%s", FakeApp.NAME, FakeFlow.NAME),
                              "successfully");
    propMap.clear();
    testPreferencesOutput(cli, String.format("get app preferences %s", FakeApp.NAME), propMap);
    testCommandOutputContains(cli, "delete namespace preferences", "successfully");
    testPreferencesOutput(cli, "get namespace preferences", propMap);
    testCommandOutputContains(cli, "get app preferences invalidapp", "not found");

    File file = new File(TMP_FOLDER.newFolder(), "prefFile.txt");
    // If the file not exist or not a file, upload should fails with an error.
    testCommandOutputContains(cli, "load instance preferences " + file.getAbsolutePath() + " json", "Not a file");
    testCommandOutputContains(cli, "load instance preferences " + file.getParentFile().getAbsolutePath() + " json",
                              "Not a file");
    // Generate a file to load
    BufferedWriter writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      writer.write("{'key':'somevalue'}");
    } finally {
      writer.close();
    }
    testCommandOutputContains(cli, "load instance preferences " + file.getAbsolutePath() + " json", "successful");
    propMap.clear();
    propMap.put("key", "somevalue");
    testPreferencesOutput(cli, "get instance preferences", propMap);
    testCommandOutputContains(cli, "delete namespace preferences", "successfully");
    testCommandOutputContains(cli, "delete instance preferences", "successfully");

    //Try invalid Json
    file = new File(TMP_FOLDER.newFolder(), "badPrefFile.txt");
    writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      writer.write("{'key:'somevalue'}");
    } finally {
      writer.close();
    }
    testCommandOutputContains(cli, "load instance preferences " + file.getAbsolutePath() + " json", "invalid");
    testCommandOutputContains(cli, "load instance preferences " + file.getAbsolutePath() + " xml", "Unsupported");

    testCommandOutputContains(cli, "set namespace preferences 'k1=v1'", "successfully");
    testCommandOutputContains(cli, "set namespace preferences 'k1=v1' name",
                              "Error: Expected format: set namespace preferences <preferences>");
    testCommandOutputContains(cli, "set instance preferences 'k1=v1' name",
                              "Error: Expected format: set instance preferences <preferences>");
  }

  @Test
  public void testNamespaces() throws Exception {
    final String name = PREFIX + "testNamespace";
    final String description = "testDescription";
    final String keytab = "keytab";
    final String principal = "principal";
    final String group = "group";
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
    String command = String.format("create namespace %s description %s principal %s group-name %s keytab-URI %s " +
                                     "hbase-namespace %s hive-database %s root-directory %s %s %s %s %s",
                                   name, description, principal, group, keytab, hbaseNamespace,
                                   hiveDatabase, rootDirectory, ArgumentName.NAMESPACE_SCHEDULER_QUEUENAME,
                                   schedulerQueueName, ArgumentName.NAMESPACE_EXPLORE_AS_PRINCIPAL, false);
    testCommandOutputContains(cli, command, String.format("Namespace '%s' created successfully.", name));

    NamespaceMeta expected = new NamespaceMeta.Builder()
      .setName(name).setDescription(description).setPrincipal(principal).setGroupName(group).setKeytabURI(keytab)
      .setHBaseNamespace(hbaseNamespace).setSchedulerQueueName(schedulerQueueName)
      .setHiveDatabase(hiveDatabase).setRootDirectory(rootDirectory).setExploreAsPrincipal(false).build();
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
    String runtimeArgsKV = SPACE_EQUALS_JOINER.join(runtimeArgs);
    testCommandOutputContains(cli, "start workflow " + workflow + " '" + runtimeArgsKV + "'",
                              "Successfully started workflow");

    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return programClient.getProgramRuns(FAKE_WORKFLOW_ID, ProgramRunStatus.COMPLETED.name(), 0, Long.MAX_VALUE,
                                            Integer.MAX_VALUE).size();
      }
    }, 180, TimeUnit.SECONDS);

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
    testCommandOutputContains(cli, String.format("get workflow token %s %s scope user key %s", workflow, runId,
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

    // Test schedule
    testCommandOutputContains(
      cli, String.format("add time schedule %s for workflow %s at \"0 4 * * *\"", FakeWorkflow.SCHEDULE, workflow),
      String.format("Successfully added schedule '%s' in app '%s'", FakeWorkflow.SCHEDULE, FakeApp.NAME));
    testCommandOutputContains(cli, String.format("get workflow schedules %s", workflow), "0 4 * * *");

    testCommandOutputContains(
      cli, String.format("update time schedule %s for workflow %s description \"testdesc\" at \"* * * * *\" " +
                           "concurrency 4 properties \"key=value\"", FakeWorkflow.SCHEDULE, workflow),
      String.format("Successfully updated schedule '%s' in app '%s'", FakeWorkflow.SCHEDULE, FakeApp.NAME));
    testCommandOutputContains(cli, String.format("get workflow schedules %s", workflow), "* * * * *");
    testCommandOutputContains(cli, String.format("get workflow schedules %s", workflow), "testdesc");
    testCommandOutputContains(cli, String.format("get workflow schedules %s", workflow), "{\"key\":\"value\"}");

    testCommandOutputContains(
      cli, String.format("delete schedule %s.%s", FakeApp.NAME, FakeWorkflow.SCHEDULE),
      String.format("Successfully deleted schedule '%s' in app '%s'", FakeWorkflow.SCHEDULE, FakeApp.NAME));
    testCommandOutputNotContains(cli, String.format("get workflow schedules %s", workflow), "* * * * *");
    testCommandOutputNotContains(cli, String.format("get workflow schedules %s", workflow), "testdesc");
    testCommandOutputNotContains(cli, String.format("get workflow schedules %s", workflow), "{\"key\":\"value\"}");

    // stop workflow
    testCommandOutputContains(cli, "stop workflow " + workflow,
                              String.format("400: Program '%s' is not running", FAKE_WORKFLOW_ID));
  }

  @Test
  public void testMetadata() throws Exception {
    testCommandOutputContains(cli, "cli render as csv", "Now rendering as CSV");
    // verify system metadata
    testCommandOutputContains(cli, String.format("get metadata %s scope system", FAKE_APP_ID),
                              FakeApp.class.getSimpleName());
    testCommandOutputContains(cli, String.format("get metadata-tags %s scope system", FAKE_WORKFLOW_ID),
                              FakeWorkflow.FakeAction.class.getSimpleName());
    testCommandOutputContains(cli, String.format("get metadata-tags %s scope system", FAKE_WORKFLOW_ID),
                              FakeWorkflow.FakeAction.ANOTHER_FAKE_NAME);
    testCommandOutputContains(cli, String.format("get metadata-tags %s scope system", FAKE_DS_ID),
                              "batch");
    testCommandOutputContains(cli, String.format("get metadata-tags %s scope system", FAKE_DS_ID),
                              "explore");
    testCommandOutputContains(cli, String.format("get metadata-tags %s scope system", FAKE_STREAM_ID),
                              AbstractSystemMetadataWriter.EXPLORE_TAG);
    testCommandOutputContains(cli, String.format("add metadata-properties %s appKey=appValue", FAKE_APP_ID),
                              "Successfully added metadata properties");
    testCommandOutputContains(cli, String.format("get metadata-properties %s", FAKE_APP_ID), "appKey,appValue");
    testCommandOutputContains(cli, String.format("add metadata-tags %s 'wfTag1 wfTag2'", FAKE_WORKFLOW_ID),
                              "Successfully added metadata tags");
    String output = getCommandOutput(cli, String.format("get metadata-tags %s", FAKE_WORKFLOW_ID));
    List<String> lines = Arrays.asList(output.split("\\r?\\n"));
    Assert.assertTrue(lines.contains("wfTag1") && lines.contains("wfTag2"));
    testCommandOutputContains(cli, String.format("add metadata-properties %s dsKey=dsValue", FAKE_DS_ID),
                              "Successfully added metadata properties");
    testCommandOutputContains(cli, String.format("get metadata-properties %s", FAKE_DS_ID), "dsKey,dsValue");
    testCommandOutputContains(cli, String.format("add metadata-tags %s 'streamTag1 streamTag2'", FAKE_STREAM_ID),
                              "Successfully added metadata tags");
    output = getCommandOutput(cli, String.format("get metadata-tags %s", FAKE_STREAM_ID));
    lines = Arrays.asList(output.split("\\r?\\n"));
    Assert.assertTrue(lines.contains("streamTag1") && lines.contains("streamTag2"));
    // test search
    testCommandOutputContains(cli, String.format("search metadata %s filtered by target-type artifact",
                                                 FakeApp.class.getSimpleName()), FAKE_ARTIFACT_ID.toString());
    testCommandOutputContains(cli, "search metadata appKey:appValue", FAKE_APP_ID.toString());
    testCommandOutputContains(cli, "search metadata fake* filtered by target-type app", FAKE_APP_ID.toString());
    output = getCommandOutput(cli, "search metadata fake* filtered by target-type program");
    lines = Arrays.asList(output.split("\\r?\\n"));
    List<String> expected = ImmutableList.of("Entity", FAKE_WORKFLOW_ID.toString(), FAKE_SPARK_ID.toString(),
                                             FAKE_FLOW_ID.toString());
    Assert.assertTrue(lines.containsAll(expected) && expected.containsAll(lines));
    testCommandOutputContains(cli, "search metadata fake* filtered by target-type dataset", FAKE_DS_ID.toString());
    testCommandOutputContains(cli, "search metadata fake* filtered by target-type stream", FAKE_STREAM_ID.toString());
    testCommandOutputContains(cli, String.format("search metadata %s", FakeApp.TIME_SCHEDULE_NAME),
                              FAKE_APP_ID.toString());
    testCommandOutputContains(cli, String.format("search metadata %s", FakeApp.STREAM_SCHEDULE_NAME),
                              FAKE_APP_ID.toString());
    testCommandOutputContains(cli, String.format("search metadata %s filtered by target-type app", PingService.NAME),
                              FAKE_APP_ID.toString());
    testCommandOutputContains(cli, String.format("search metadata %s filtered by target-type program",
                                                 PrefixedEchoHandler.NAME), PREFIXED_ECHO_HANDLER_ID.toString());
    testCommandOutputContains(cli, "search metadata batch* filtered by target-type dataset", FAKE_DS_ID.toString());
    testCommandOutputNotContains(cli, "search metadata batchwritable filtered by target-type dataset",
                                 FAKE_DS_ID.toString());
    testCommandOutputContains(cli, "search metadata bat* filtered by target-type dataset", FAKE_DS_ID.toString());
    output = getCommandOutput(cli, "search metadata batch filtered by target-type program");
    lines = Arrays.asList(output.split("\\r?\\n"));
    expected = ImmutableList.of("Entity", FAKE_SPARK_ID.toString(), FAKE_WORKFLOW_ID.toString());
    Assert.assertTrue(lines.containsAll(expected) && expected.containsAll(lines));
    output = getCommandOutput(cli, "search metadata realtime filtered by target-type program");
    lines = Arrays.asList(output.split("\\r?\\n"));
    expected = ImmutableList.of("Entity", FAKE_FLOW_ID.toString(), PING_SERVICE_ID.toString(),
                                PREFIXED_ECHO_HANDLER_ID.toString());
    Assert.assertTrue(lines.containsAll(expected) && expected.containsAll(lines));
    output = getCommandOutput(cli, "search metadata fake* filtered by target-type dataset,stream");
    lines = Arrays.asList(output.split("\\r?\\n"));
    expected = ImmutableList.of("Entity", FAKE_DS_ID.toString(), FAKE_STREAM_ID.toString());
    Assert.assertTrue(lines.containsAll(expected) && expected.containsAll(lines));
    output = getCommandOutput(cli, "search metadata fake* filtered by target-type dataset,stream,app");
    lines = Arrays.asList(output.split("\\r?\\n"));
    expected = ImmutableList.of("Entity", FAKE_DS_ID.toString(), FAKE_STREAM_ID.toString(), FAKE_APP_ID.toString());
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

  private void assertProgramStatus(final ProgramClient programClient, final ProgramId programId, String programStatus)
    throws Exception {
    Tasks.waitFor(programStatus, new Callable<String>() {
      @Override
      public String call() throws Exception {
        return programClient.getStatus(programId);
      }
    }, 180, TimeUnit.SECONDS);
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
      queryClient.execute(NamespaceId.DEFAULT, "create database " + hiveDb);
    assertExploreQuerySuccess(future);
    future = queryClient.execute(NamespaceId.DEFAULT, "describe database " + hiveDb);
    assertExploreQuerySuccess(future);
  }

  private static void dropHiveDb(String hiveDb) throws Exception {
    assertExploreQuerySuccess(queryClient.execute(NamespaceId.DEFAULT, "drop database " + hiveDb));
  }

  private static void assertExploreQuerySuccess(
    ListenableFuture<ExploreExecutionResult> dbCreationFuture) throws Exception {
    ExploreExecutionResult exploreExecutionResult = dbCreationFuture.get(10, TimeUnit.SECONDS);
    QueryStatus status = exploreExecutionResult.getStatus();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, status.getStatus());
  }

  private static File createPluginConfig() throws IOException {
    File tmpFolder = TMP_FOLDER.newFolder();
    File pluginConfig = new File(tmpFolder, "pluginConfig.txt");

    List<String> parents = new ArrayList<>();
    parents.add(String.format("%s[0.0.0,9.9.9]", FakeApp.NAME));

    List<Map<String, String>> plugins = new ArrayList<>();
    Map<String, String> plugin = new HashMap<>();
    plugin.put("name", "FakePlugin");
    plugin.put("type", "runnable");
    plugin.put("className", "co.cask.cdap.client.app.FakePlugin");

    PrintWriter writer = new PrintWriter(pluginConfig);
    writer.print(GSON.toJson(new PluginConfig(parents, plugins)));
    writer.close();

    return pluginConfig;
  }

  private static File createPluginJarFile(Class<?> cls) throws IOException {
    File tmpFolder = TMP_FOLDER.newFolder();
    LocationFactory locationFactory = new LocalLocationFactory(tmpFolder);
    Location deploymentJar = AppJarHelper.createDeploymentJar(locationFactory, cls);
    File appJarFile =
      new File(tmpFolder, String.format("%s-1.0.jar", cls.getSimpleName()));
    Files.copy(Locations.newInputSupplier(deploymentJar), appJarFile);
    return appJarFile;
  }
}
