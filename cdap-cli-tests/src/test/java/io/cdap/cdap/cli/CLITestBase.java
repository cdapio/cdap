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

package io.cdap.cdap.cli;

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
import io.cdap.cdap.ConfigTestApp;
import io.cdap.cdap.StandaloneTester;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.cli.command.NamespaceCommandUtils;
import io.cdap.cdap.cli.command.metadata.MetadataCommandHelper;
import io.cdap.cdap.cli.util.InstanceURIParser;
import io.cdap.cdap.cli.util.RowMaker;
import io.cdap.cdap.cli.util.table.CsvTableRenderer;
import io.cdap.cdap.cli.util.table.Table;
import io.cdap.cdap.client.DatasetTypeClient;
import io.cdap.cdap.client.NamespaceClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.client.QueryClient;
import io.cdap.cdap.client.app.FakeApp;
import io.cdap.cdap.client.app.FakeDataset;
import io.cdap.cdap.client.app.FakePlugin;
import io.cdap.cdap.client.app.FakeSpark;
import io.cdap.cdap.client.app.FakeWorkflow;
import io.cdap.cdap.client.app.PingService;
import io.cdap.cdap.client.app.PluginConfig;
import io.cdap.cdap.client.app.PrefixedEchoHandler;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.DatasetTypeNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.explore.client.ExploreExecutionResult;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.QueryStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.WorkflowTokenDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.DatasetTypeId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.common.cli.CLI;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Base class for CLI Tests.
 */
public abstract class CLITestBase {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @ClassRule
  public static final StandaloneTester STANDALONE = new StandaloneTester(
    // Turn off the log event delay to make log event flush on every write to eliminate race condition.
    Constants.Logging.PIPELINE_EVENT_DELAY_MS, 0
  );

  private static final Joiner.MapJoiner SPACE_EQUALS_JOINER = Joiner.on(" ").withKeyValueSeparator("=");
  private static final Gson GSON = new Gson();
  private static final String PREFIX = "123ff1_";
  private static final String V1 = "1.0";
  protected static final String V1_SNAPSHOT = "v1.0.0-SNAPSHOT";

  private static final ArtifactId FAKE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact(FakeApp.NAME, V1);
  private static final ApplicationId FAKE_APP_ID = NamespaceId.DEFAULT.app(FakeApp.NAME);
  private static final ApplicationId FAKE_APP_ID_V_1 = NamespaceId.DEFAULT.app(FakeApp.NAME, V1_SNAPSHOT);
  private static final ArtifactId FAKE_PLUGIN_ID = NamespaceId.DEFAULT.artifact(FakePlugin.NAME, V1);
  private static final ProgramId FAKE_WORKFLOW_ID = FAKE_APP_ID.workflow(FakeWorkflow.NAME);
  private static final ProgramId FAKE_SPARK_ID = FAKE_APP_ID.spark(FakeSpark.NAME);
  private static final ServiceId PREFIXED_ECHO_HANDLER_ID = FAKE_APP_ID.service(PrefixedEchoHandler.NAME);
  private static final DatasetId FAKE_DS_ID = NamespaceId.DEFAULT.dataset(FakeApp.DS_NAME);
  private static final Logger LOG = LoggerFactory.getLogger(CLITestBase.class);

  public static CLIConfig createCLIConfig(URI standaloneUri) throws Exception {
    ConnectionConfig connectionConfig = InstanceURIParser.DEFAULT.parse(standaloneUri.toString());
    ClientConfig clientConfig = new ClientConfig.Builder().setConnectionConfig(connectionConfig).build();
    clientConfig.setAllTimeouts(60000);
    return new CLIConfig(clientConfig, System.out, new CsvTableRenderer());
  }

  public static void testCommandOutputContains(CLI cli, String command,
                                               final String expectedOutput) throws Exception {
    testCommand(cli, command, new Function<String, Void>() {
      @Nullable
      @Override
      public Void apply(@Nullable String output) {
        Assert.assertTrue(String.format("Expected output '%s' to contain '%s'", output, expectedOutput),
                          output != null && output.contains(expectedOutput));
        return null;
      }
    });
  }

  public static void testCommandOutputNotContains(CLI cli, String command,
                                                  final String expectedOutput) throws Exception {
    testCommand(cli, command, new Function<String, Void>() {
      @Nullable
      @Override
      public Void apply(@Nullable String output) {
        Assert.assertTrue(String.format("Expected output '%s' to not contain '%s'", output, expectedOutput),
                          output != null && !output.contains(expectedOutput));
        return null;
      }
    });
  }

  public static void testCommand(CLI cli, String command, Function<String, Void> outputValidator) throws Exception {
    String output = getCommandOutput(cli, command);
    outputValidator.apply(output);
  }

  public static String getCommandOutput(CLI cli, String command) throws Exception {
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
         PrintStream printStream = new PrintStream(outputStream)) {
      cli.execute(command, printStream);
      return outputStream.toString();
    }
  }

  protected static void testSetup(CLI cli, StandaloneTester standaloneTester,
                                  TemporaryFolder temporaryFolder) throws Exception {
    testCommandOutputContains(cli, "connect " + standaloneTester.getBaseURI(), "Successfully connected");
    testCommandOutputNotContains(cli, "list apps", FakeApp.NAME);

    File appJarFile = createAppJarFile(FakeApp.class);

    testCommandOutputContains(cli, String.format("load artifact %s name %s version %s",
                                                 appJarFile.getAbsolutePath(),
                                                 FakeApp.NAME, V1),
                              "Successfully added artifact");
    testCommandOutputContains(cli, "deploy app " + appJarFile.getAbsolutePath(),
                              "Successfully deployed app");
    testCommandOutputContains(cli, String.format("create app %s version %s %s %s %s",
                                                 FakeApp.NAME, V1_SNAPSHOT, FakeApp.NAME, V1,
                                                 ArtifactScope.USER),
                              "Successfully created application");

    File pluginJarFile = createPluginJarFile(FakePlugin.class);
    testCommandOutputContains(cli,
                              String.format("load artifact %s config-file %s",
                                            pluginJarFile.getAbsolutePath(),
                                            createPluginConfig().
                                              getAbsolutePath()),
                              "Successfully");
    if (!appJarFile.delete()) {
      LOG.warn("Failed to delete temporary app jar file: {}", appJarFile.getAbsolutePath());
    }
  }


  abstract CLI getCLI();

  abstract ProgramClient getProgramClient();

  abstract CLIMain getCliMain();

  abstract CLIConfig getCliConfig();

  abstract QueryClient getQueryClient();

  private void testCommandOutputContains(String command,
                                         final String expectedOutput) throws Exception {
    testCommandOutputContains(getCLI(), command, expectedOutput);
  }

  private void testCommandOutputNotContains(String command,
                                            final String expectedOutput) throws Exception {
    testCommandOutputNotContains(getCLI(), command, expectedOutput);
  }

  protected void testCommand(String command, Function<String, Void> outputValidator) throws Exception {
    testCommand(getCLI(), command, outputValidator);
  }

  protected String getCommandOutput(String command) throws Exception {
    return getCommandOutput(getCLI(), command);
  }

  @Test
  public void testConnect() throws Exception {
    testCommandOutputContains("connect fakehost", "could not be reached");
    testCommandOutputContains("connect " + STANDALONE.getBaseURI(), "Successfully connected");
  }

  @Test
  public void testList() throws Exception {
    testCommandOutputContains("list app versions " + FakeApp.NAME, V1_SNAPSHOT);
    testCommandOutputContains("list app versions " + FakeApp.NAME, ApplicationId.DEFAULT_VERSION);
    testCommandOutputContains("list dataset instances", FakeApp.DS_NAME);
  }

  @Test
  public void testPrompt() throws Exception {
    CLIMain cliMain = getCliMain();
    CLIConfig cliConfig = getCliConfig();
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
    ProgramClient programClient = getProgramClient();
    final ProgramId serviceId = FAKE_APP_ID.service(FakeApp.SERVICES.get(0));

    String qualifiedServiceId = FakeApp.NAME + "." + serviceId.getProgram();
    testCommandOutputContains("start service " + qualifiedServiceId, "Successfully started service");
    assertProgramStatus(programClient, serviceId, "RUNNING");
    testCommandOutputContains("stop service " + qualifiedServiceId, "Successfully stopped service");
    assertProgramStatus(programClient, serviceId, "STOPPED");
    testCommandOutputContains("get service status " + qualifiedServiceId, "STOPPED");
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        List<RunRecord> output = programClient.getProgramRuns(serviceId, "KILLED", 0L,
                                                              Long.MAX_VALUE, Integer.MAX_VALUE);
        return output != null && output.size() != 0;
      }
    }, 5, TimeUnit.SECONDS);
    testCommandOutputContains("get service runs " + qualifiedServiceId, "KILLED");
    testCommandOutputContains("get service live " + qualifiedServiceId, serviceId.getProgram());
  }

  @Test
  public void testAppDeploy() throws Exception {
    testDeploy(null);
    testDeploy(new ConfigTestApp.ConfigClass("testTable"));
  }

  private void testDeploy(ConfigTestApp.ConfigClass config) throws Exception {
    String datasetId = ConfigTestApp.DEFAULT_TABLE;
    if (config != null) {
      datasetId = config.getTableName();
    }

    File appJarFile = createAppJarFile(ConfigTestApp.class);
    if (config != null) {
      String appConfig = GSON.toJson(config);
      testCommandOutputContains(String.format("deploy app %s %s", appJarFile.getAbsolutePath(), appConfig),
                                "Successfully deployed app");
    } else {
      testCommandOutputContains(String.format("deploy app %s", appJarFile.getAbsolutePath()), "Successfully");
    }

    if (!appJarFile.delete()) {
      LOG.warn("Failed to delete temporary app jar file: {}", appJarFile.getAbsolutePath());
    }
    testCommandOutputContains("list dataset instances", datasetId);
    testCommandOutputContains("delete app " + ConfigTestApp.NAME, "Successfully");
    testCommandOutputContains("delete dataset instance " + datasetId, "Successfully deleted");
  }

  @Test
  public void testGetParentArtifact() throws Exception {
    testCommandOutputContains(String.format("describe artifact %s %s", FAKE_PLUGIN_ID.getArtifact(),
                                            FAKE_PLUGIN_ID.getVersion()), FakeApp.NAME);
  }

  @Test
  public void testDeployAppWithConfigFile() throws Exception {
    String datasetId = "testTable";

    File appJarFile = createAppJarFile(ConfigTestApp.class);
    File configFile = new File(TMP_FOLDER.newFolder(), "testConfigFile.txt");
    try (BufferedWriter writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      new Gson().toJson(new ConfigTestApp.ConfigClass(datasetId), writer);
    }

    testCommandOutputContains(String.format("deploy app %s with config %s", appJarFile.getAbsolutePath(),
                                            configFile.getAbsolutePath()), "Successfully deployed application");
    testCommandOutputContains("list dataset instances", datasetId);
    testCommandOutputContains("delete app " + ConfigTestApp.NAME, "Successfully");
    testCommandOutputContains("delete dataset instance " + datasetId, "Successfully deleted");
  }

  @Test
  public void testAppOwner() throws Exception {
    // load an artifact
    File appJarFile = createAppJarFile(ConfigTestApp.class);
    String datasetId = ConfigTestApp.DEFAULT_TABLE;
    testCommandOutputContains(String.format("load artifact %s name %s version %s", appJarFile.getAbsolutePath(),
                                            "OwnedConfigTestAppArtifact", V1),
                              "Successfully added artifact");

    // create a config file which has principal information
    File configFile = new File(TMP_FOLDER.newFolder(), "testOwnerConfigFile.txt");
    try (BufferedWriter writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      writer.write(String.format("{\n\"%s\":\"%s\"\n}", ArgumentName.PRINCIPAL, "user/host.net@kdc.net"));
      writer.close();
    }

    // create app with the config containing the principla
    testCommandOutputContains(String.format("create app %s %s %s %s %s",
                                            "OwnedApp", "OwnedConfigTestAppArtifact", V1,
                                            ArtifactScope.USER, configFile.getAbsolutePath()),
                              "Successfully created application");

    /// ensure that the app details have owner information
    testCommandOutputContains("list apps", "user/host.net@kdc.net");

    // clean up
    if (!appJarFile.delete()) {
      LOG.warn("Failed to delete temporary app jar file: {}", appJarFile.getAbsolutePath());
    }
    testCommandOutputContains("delete app " + "OwnedApp", "Successfully");
    testCommandOutputContains("delete dataset instance " + datasetId, "Successfully deleted");
  }

  @Test
  public void testSchedule() throws Exception {
    String scheduleId = FakeApp.NAME + "." + FakeApp.TIME_SCHEDULE_NAME;
    String workflowId = FakeApp.NAME + "." + FakeWorkflow.NAME;
    testCommandOutputContains("get schedule status " + scheduleId, "SUSPENDED");
    testCommandOutputContains("resume schedule " + scheduleId, "Successfully resumed");
    testCommandOutputContains("get schedule status " + scheduleId, "SCHEDULED");
    testCommandOutputContains("suspend schedule " + scheduleId, "Successfully suspended");
    testCommandOutputContains("get schedule status " + scheduleId, "SUSPENDED");
    testCommandOutputContains("get workflow schedules " + workflowId, FakeApp.TIME_SCHEDULE_NAME);
  }

  @Test
  public void testDataset() throws Exception {
    String datasetName = PREFIX + "sdf123lkj";
    String ownedDatasetName = PREFIX + "owned";

    CLIConfig cliConfig = getCliConfig();
    DatasetTypeClient datasetTypeClient = new DatasetTypeClient(cliConfig.getClientConfig());
    DatasetTypeMeta datasetType = datasetTypeClient.list(NamespaceId.DEFAULT).get(0);
    testCommandOutputContains("create dataset instance " + datasetType.getName() + " " + datasetName + " \"a=1\"",
                              "Successfully created dataset");
    testCommandOutputContains("list dataset instances", FakeDataset.class.getSimpleName());
    testCommandOutputContains("get dataset instance properties " + datasetName, "a,1");

    // test dataset creation with owner
    String commandOutput = getCommandOutput("create dataset instance " + datasetType.getName() + " " +
                                              ownedDatasetName + " \"a=1\"" + " " + "someDescription " +
                                              ArgumentName.PRINCIPAL +
                                              " alice/somehost.net@somekdc.net");
    Assert.assertTrue(commandOutput.contains("Successfully created dataset"));
    Assert.assertTrue(commandOutput.contains("alice/somehost.net@somekdc.net"));

    // test describing the table returns the given owner information
    testCommandOutputContains("describe dataset instance " + ownedDatasetName, "alice/somehost.net@somekdc.net");

    NamespaceClient namespaceClient = new NamespaceClient(cliConfig.getClientConfig());
    NamespaceId barspace = new NamespaceId("bar");
    namespaceClient.create(new NamespaceMeta.Builder().setName(barspace).build());
    cliConfig.setNamespace(barspace);
    // list of dataset instances is different in 'foo' namespace
    testCommandOutputNotContains("list dataset instances", FakeDataset.class.getSimpleName());

    // also can not create dataset instances if the type it depends on exists only in a different namespace.
    DatasetTypeId datasetType1 = barspace.datasetType(datasetType.getName());
    testCommandOutputContains("create dataset instance " + datasetType.getName() + " " + datasetName,
                              new DatasetTypeNotFoundException(datasetType1).getMessage());

    testCommandOutputContains("use namespace default", "Now using namespace 'default'");
    try {
      testCommandOutputContains("truncate dataset instance " + datasetName, "Successfully truncated");
    } finally {
      testCommandOutputContains("delete dataset instance " + datasetName, "Successfully deleted");
    }

    String datasetName2 = PREFIX + "asoijm39485";
    String description = "test-description-for-" + datasetName2;
    testCommandOutputContains("create dataset instance " + datasetType.getName() + " " + datasetName2 +
                                " \"a=1\"" + " " + description,
                              "Successfully created dataset");
    testCommandOutputContains("list dataset instances", description);
    testCommandOutputContains("delete dataset instance " + datasetName2, "Successfully deleted");
    testCommandOutputContains("delete dataset instance " + ownedDatasetName, "Successfully deleted");
  }

  @Test
  public void testService() throws Exception {
    ProgramClient programClient = getProgramClient();
    ServiceId service = FAKE_APP_ID.service(PrefixedEchoHandler.NAME);
    ServiceId serviceV1 = FAKE_APP_ID_V_1.service(PrefixedEchoHandler.NAME);
    String serviceName = String.format("%s.%s", FakeApp.NAME, PrefixedEchoHandler.NAME);
    String serviceArgument = String.format("%s version %s", serviceName, ApplicationId.DEFAULT_VERSION);
    String serviceV1Argument = String.format("%s version %s", serviceName, V1_SNAPSHOT);
    try {
      // Test service commands with no optional version argument
      testCommandOutputContains("start service " + serviceName, "Successfully started service");
      assertProgramStatus(programClient, service, ProgramStatus.RUNNING.name());
      testCommandOutputContains("get endpoints service " + serviceName, "POST");
      testCommandOutputContains("get endpoints service " + serviceName, "/echo");
      testCommandOutputContains("check service availability " + serviceName, "Service is available");
      testCommandOutputContains("call service " + serviceName
                                  + " POST /echo body \"testBody\"", ":testBody");
      testCommandOutputContains("stop service " + serviceName, "Successfully stopped service");
      assertProgramStatus(programClient, service, ProgramStatus.STOPPED.name());
      // Test service commands with version argument when two versions of the service are running
      testCommandOutputContains("start service " + serviceArgument, "Successfully started service");
      testCommandOutputContains("start service " + serviceV1Argument, "Successfully started service");
      assertProgramStatus(programClient, service, ProgramStatus.RUNNING.name());
      assertProgramStatus(programClient, serviceV1, ProgramStatus.RUNNING.name());
      testCommandOutputContains("get endpoints service " + serviceArgument, "POST");
      testCommandOutputContains("get endpoints service " + serviceV1Argument, "POST");
      testCommandOutputContains("get endpoints service " + serviceArgument, "/echo");
      testCommandOutputContains("get endpoints service " + serviceV1Argument, "/echo");
      testCommandOutputContains("check service availability " + serviceArgument, "Service is available");
      testCommandOutputContains("check service availability " + serviceV1Argument, "Service is available");
      testCommandOutputContains("call service " + serviceArgument
                                  + " POST /echo body \"testBody\"", ":testBody");
      testCommandOutputContains("call service " + serviceV1Argument
                                  + " POST /echo body \"testBody\"", ":testBody");
      testCommandOutputContains("get service logs " + serviceName, "Starting HTTP server for Service " + service);
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
    ProgramClient programClient = getProgramClient();
    Map<String, String> runtimeArgs = ImmutableMap.of("sdf", "bacon");
    String runtimeArgsKV = SPACE_EQUALS_JOINER.join(runtimeArgs);
    testCommandOutputContains("start service " + qualifiedServiceId + " '" + runtimeArgsKV + "'",
                              "Successfully started service");
    assertProgramStatus(programClient, service, "RUNNING");
    testCommandOutputContains("call service " + qualifiedServiceId + " POST /echo body \"testBody\"",
                              "bacon:testBody");
    testCommandOutputContains("stop service " + qualifiedServiceId, "Successfully stopped service");
    assertProgramStatus(programClient, service, "STOPPED");

    Map<String, String> runtimeArgs2 = ImmutableMap.of("sdf", "chickenz");
    String runtimeArgs2Json = GSON.toJson(runtimeArgs2);
    String runtimeArgs2KV = SPACE_EQUALS_JOINER.join(runtimeArgs2);
    testCommandOutputContains("set service runtimeargs " + qualifiedServiceId + " '" + runtimeArgs2KV + "'",
                              "Successfully set runtime args");
    testCommandOutputContains("start service " + qualifiedServiceId, "Successfully started service");
    assertProgramStatus(programClient, service, "RUNNING");
    testCommandOutputContains("get service runtimeargs " + qualifiedServiceId, runtimeArgs2Json);
    testCommandOutputContains("call service " + qualifiedServiceId + " POST /echo body \"testBody\"",
                              "chickenz:testBody");
    testCommandOutputContains("stop service " + qualifiedServiceId, "Successfully stopped service");
    assertProgramStatus(programClient, service, "STOPPED");
  }

  @Test
  public void testSpark() throws Exception {
    ProgramClient programClient = getProgramClient();
    String sparkId = FakeApp.SPARK.get(0);
    String qualifiedSparkId = FakeApp.NAME + "." + sparkId;
    ProgramId spark = NamespaceId.DEFAULT.app(FakeApp.NAME).spark(sparkId);

    testCommandOutputContains("list spark", sparkId);
    testCommandOutputContains("start spark " + qualifiedSparkId, "Successfully started Spark");
    assertProgramStatus(programClient, spark, "RUNNING");
    assertProgramStatus(programClient, spark, "STOPPED");
    testCommandOutputContains("get spark status " + qualifiedSparkId, "STOPPED");
    testCommandOutputContains("get spark runs " + qualifiedSparkId, "COMPLETED");
    testCommandOutputContains("get spark logs " + qualifiedSparkId, "HelloFakeSpark");
  }

  @Test
  public void testPreferences() throws Exception {
    testPreferencesOutput("get instance preferences", ImmutableMap.<String, String>of());
    Map<String, String> propMap = Maps.newHashMap();
    propMap.put("key", "newinstance");
    propMap.put("k1", "v1");
    testCommandOutputContains("delete instance preferences", "successfully");
    testCommandOutputContains("set instance preferences 'key=newinstance k1=v1'",
                              "successfully");
    testPreferencesOutput("get instance preferences", propMap);
    testPreferencesOutput("get resolved instance preferences", propMap);
    testCommandOutputContains("delete instance preferences", "successfully");
    propMap.clear();
    testPreferencesOutput("get instance preferences", propMap);
    testPreferencesOutput(String.format("get app preferences %s", FakeApp.NAME), propMap);
    testCommandOutputContains("delete namespace preferences", "successfully");
    testPreferencesOutput("get namespace preferences", propMap);
    testCommandOutputContains("get app preferences invalidapp", "not found");

    File file = new File(TMP_FOLDER.newFolder(), "prefFile.txt");
    // If the file not exist or not a file, upload should fails with an error.
    testCommandOutputContains("load instance preferences " + file.getAbsolutePath() + " json", "Not a file");
    testCommandOutputContains("load instance preferences " + file.getParentFile().getAbsolutePath() + " json",
                              "Not a file");
    // Generate a file to load
    BufferedWriter writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      writer.write("{'key':'somevalue'}");
    } finally {
      writer.close();
    }
    testCommandOutputContains("load instance preferences " + file.getAbsolutePath() + " json", "successful");
    propMap.clear();
    propMap.put("key", "somevalue");
    testPreferencesOutput("get instance preferences", propMap);
    testCommandOutputContains("delete namespace preferences", "successfully");
    testCommandOutputContains("delete instance preferences", "successfully");

    //Try invalid Json
    file = new File(TMP_FOLDER.newFolder(), "badPrefFile.txt");
    writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      writer.write("{'key:'somevalue'}");
    } finally {
      writer.close();
    }
    testCommandOutputContains("load instance preferences " + file.getAbsolutePath() + " json", "invalid");
    testCommandOutputContains("load instance preferences " + file.getAbsolutePath() + " xml", "Unsupported");

    testCommandOutputContains("set namespace preferences 'k1=v1'", "successfully");
    testCommandOutputContains("set namespace preferences 'k1=v1' name",
                              "Error: Expected format: set namespace preferences <preferences>");
    testCommandOutputContains("set instance preferences 'k1=v1' name",
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
    NamespaceMeta defaultNs = NamespaceMeta.DEFAULT;
    List<NamespaceMeta> expectedNamespaces = Lists.newArrayList(defaultNs);
    testNamespacesOutput("list namespaces", expectedNamespaces);

    // describe non-existing namespace
    testCommandOutputContains(String.format("describe namespace %s", doesNotExist),
                              String.format("Error: 'namespace:%s' was not found", doesNotExist));
    // delete non-existing namespace
    // TODO: uncomment when fixed - this makes build hang since it requires confirmation from user
//    testCommandOutputContains(String.format("delete namespace %s", doesNotExist),
//                              String.format("Error: namespace '%s' was not found", doesNotExist));

    // create a namespace
    String command = String.format("create namespace %s description %s principal %s group-name %s keytab-URI %s " +
                                     "hbase-namespace %s hive-database %s root-directory %s %s %s %s %s",
                                   name, description, principal, group, keytab, hbaseNamespace,
                                   hiveDatabase, rootDirectory, ArgumentName.NAMESPACE_SCHEDULER_QUEUENAME,
                                   schedulerQueueName, ArgumentName.NAMESPACE_EXPLORE_AS_PRINCIPAL, false);
    testCommandOutputContains(command, String.format("Namespace '%s' created successfully.", name));

    NamespaceMeta expected = new NamespaceMeta.Builder()
      .setName(name).setDescription(description).setPrincipal(principal).setGroupName(group).setKeytabURI(keytab)
      .setHBaseNamespace(hbaseNamespace).setSchedulerQueueName(schedulerQueueName)
      .setHiveDatabase(hiveDatabase).setRootDirectory(rootDirectory).setExploreAsPrincipal(false).build();
    expectedNamespaces = Lists.newArrayList(defaultNs, expected);
    // list namespaces and verify
    testNamespacesOutput("list namespaces", expectedNamespaces);

    // get namespace details and verify
    expectedNamespaces = Lists.newArrayList(expected);
    command = String.format("describe namespace %s", name);
    testNamespacesOutput(command, expectedNamespaces);

    // try creating a namespace with existing id
    command = String.format("create namespace %s", name);
    testCommandOutputContains(command, String.format("Error: 'namespace:%s' already exists\n", name));

    // create a namespace with default name and description
    command = String.format("create namespace %s", defaultFields);
    testCommandOutputContains(command, String.format("Namespace '%s' created successfully.", defaultFields));

    NamespaceMeta namespaceDefaultFields = new NamespaceMeta.Builder()
      .setName(defaultFields).setDescription("").build();
    // test that there are 3 namespaces including default
    expectedNamespaces = Lists.newArrayList(defaultNs, namespaceDefaultFields, expected);
    testNamespacesOutput("list namespaces", expectedNamespaces);
    // describe namespace with default fields
    expectedNamespaces = Lists.newArrayList(namespaceDefaultFields);
    testNamespacesOutput(String.format("describe namespace %s", defaultFields), expectedNamespaces);

    // delete namespace and verify
    // TODO: uncomment when fixed - this makes build hang since it requires confirmation from user
//    command = String.format("delete namespace %s", name);
//    testCommandOutputContains(command, String.format("Namespace '%s' deleted successfully.", name));
    dropHiveDb(hiveDatabase);
  }

  @Test
  public void testWorkflows() throws Exception {
    ProgramClient programClient = getProgramClient();
    String workflow = String.format("%s.%s", FakeApp.NAME, FakeWorkflow.NAME);
    File doneFile = TMP_FOLDER.newFile("fake.done");
    Map<String, String> runtimeArgs = ImmutableMap.of("done.file", doneFile.getAbsolutePath());
    String runtimeArgsKV = SPACE_EQUALS_JOINER.join(runtimeArgs);
    testCommandOutputContains("start workflow " + workflow + " '" + runtimeArgsKV + "'",
                              "Successfully started workflow");

    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return programClient.getProgramRuns(FAKE_WORKFLOW_ID, ProgramRunStatus.COMPLETED.name(), 0, Long.MAX_VALUE,
                                            Integer.MAX_VALUE).size();
      }
    }, 180, TimeUnit.SECONDS);

    testCommandOutputContains("cli render as csv", "Now rendering as CSV");
    String commandOutput = getCommandOutput("get workflow runs " + workflow);
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
    testCommandOutputContains(String.format("get workflow token %s %s", workflow, runId),
                              Joiner.on(",").join(FakeWorkflow.FakeAction.TOKEN_KEY, GSON.toJson(tokenValues)));
    testCommandOutputNotContains(String.format("get workflow token %s %s scope system", workflow, runId),
                                 Joiner.on(",").join(FakeWorkflow.FakeAction.TOKEN_KEY, GSON.toJson(tokenValues)));
    testCommandOutputContains(String.format("get workflow token %s %s scope user key %s", workflow, runId,
                                            FakeWorkflow.FakeAction.TOKEN_KEY),
                              Joiner.on(",").join(FakeWorkflow.FakeAction.TOKEN_KEY, GSON.toJson(tokenValues)));

    // Test with node name
    String fakeNodeValue = Joiner.on(",").join(FakeWorkflow.FakeAction.TOKEN_KEY, FakeWorkflow.FakeAction.TOKEN_VALUE);
    testCommandOutputContains(
      String.format("get workflow token %s %s at node %s", workflow, runId,
                    FakeWorkflow.FakeAction.class.getSimpleName()), fakeNodeValue);
    testCommandOutputNotContains(
      String.format("get workflow token %s %s at node %s scope system", workflow, runId,
                    FakeWorkflow.FakeAction.ANOTHER_FAKE_NAME), fakeNodeValue);
    testCommandOutputContains(
      String.format("get workflow token %s %s at node %s scope user key %s", workflow, runId,
                    FakeWorkflow.FakeAction.ANOTHER_FAKE_NAME, FakeWorkflow.FakeAction.TOKEN_KEY), fakeNodeValue);

    testCommandOutputContains("get workflow logs " + workflow, FakeWorkflow.FAKE_LOG);

    // Test schedule
    testCommandOutputContains(
      String.format("add time schedule %s for workflow %s at \"0 4 * * *\"", FakeWorkflow.SCHEDULE, workflow),
      String.format("Successfully added schedule '%s' in app '%s'", FakeWorkflow.SCHEDULE, FakeApp.NAME));
    testCommandOutputContains(String.format("get workflow schedules %s", workflow), "0 4 * * *");

    testCommandOutputContains(
      String.format("update time schedule %s for workflow %s description \"testdesc\" at \"* * * * *\" " +
                      "concurrency 4 properties \"key=value\"", FakeWorkflow.SCHEDULE, workflow),
      String.format("Successfully updated schedule '%s' in app '%s'", FakeWorkflow.SCHEDULE, FakeApp.NAME));
    testCommandOutputContains(String.format("get workflow schedules %s", workflow), "* * * * *");
    testCommandOutputContains(String.format("get workflow schedules %s", workflow), "testdesc");
    testCommandOutputContains(String.format("get workflow schedules %s", workflow), "{\"key\":\"value\"}");

    testCommandOutputContains(
      String.format("delete schedule %s.%s", FakeApp.NAME, FakeWorkflow.SCHEDULE),
      String.format("Successfully deleted schedule '%s' in app '%s'", FakeWorkflow.SCHEDULE, FakeApp.NAME));
    testCommandOutputNotContains(String.format("get workflow schedules %s", workflow), "* * * * *");
    testCommandOutputNotContains(String.format("get workflow schedules %s", workflow), "testdesc");
    testCommandOutputNotContains(String.format("get workflow schedules %s", workflow), "{\"key\":\"value\"}");

    // stop workflow
    testCommandOutputContains("stop workflow " + workflow,
                              String.format("400: Program '%s' is not running", FAKE_WORKFLOW_ID));
  }

  @Test
  public void testMetadata() throws Exception {
    testCommandOutputContains("cli render as csv", "Now rendering as CSV");

    // Since metadata indexing is asynchronous, and the FakeApp has just been deployed, we need to
    // wait for all entities to be indexed. Knowing that the workflow is the last program to be processed
    // by the metadata message subscriber, we can wait until its tag "Batch" is returned in queries.
    // This is not elegant because it depends on implementation, but seems better than adding a waitFor()
    // for each of the following lines.
    Tasks.waitFor(true, () -> {
      String output = getCommandOutput(String.format("get metadata-tags %s scope system",
                                                     FAKE_WORKFLOW_ID));
      return Arrays.asList(output.split("\\r?\\n")).contains("Batch");
    }, 10, TimeUnit.SECONDS);

    // verify system metadata
    testCommandOutputContains(String.format("get metadata %s scope system", FAKE_APP_ID),
                              FakeApp.class.getSimpleName());
    testCommandOutputContains(String.format("get metadata-tags %s scope system", FAKE_WORKFLOW_ID),
                              FakeWorkflow.FakeAction.class.getSimpleName());
    testCommandOutputContains(String.format("get metadata-tags %s scope system", FAKE_WORKFLOW_ID),
                              FakeWorkflow.FakeAction.ANOTHER_FAKE_NAME);
    testCommandOutputContains(String.format("get metadata-tags %s scope system", FAKE_DS_ID),
                              "batch");
    testCommandOutputContains(String.format("get metadata-tags %s scope system", FAKE_DS_ID),
                              "explore");
    testCommandOutputContains(String.format("add metadata-properties %s appKey=appValue", FAKE_APP_ID),
                              "Successfully added metadata properties");
    testCommandOutputContains(String.format("get metadata-properties %s", FAKE_APP_ID), "appKey,appValue");
    testCommandOutputContains(String.format("add metadata-tags %s 'wfTag1 wfTag2'", FAKE_WORKFLOW_ID),
                              "Successfully added metadata tags");
    String output = getCommandOutput(String.format("get metadata-tags %s", FAKE_WORKFLOW_ID));
    List<String> lines = Arrays.asList(output.split("\\r?\\n"));
    Assert.assertTrue(lines.contains("wfTag1") && lines.contains("wfTag2"));
    testCommandOutputContains(String.format("add metadata-properties %s dsKey=dsValue", FAKE_DS_ID),
                              "Successfully added metadata properties");
    testCommandOutputContains(String.format("get metadata-properties %s", FAKE_DS_ID), "dsKey,dsValue");

    // test search
    testCommandOutputContains(String.format("search metadata %s filtered by target-type artifact",
                                            FakeApp.class.getSimpleName()), FAKE_ARTIFACT_ID.toString());
    testCommandOutputContains("search metadata appKey:appValue", FAKE_APP_ID.toString());
    testCommandOutputContains("search metadata fake* filtered by target-type application", FAKE_APP_ID.toString());
    output = getCommandOutput("search metadata fake* filtered by target-type program");
    lines = Arrays.asList(output.split("\\r?\\n"));
    List<String> expected = ImmutableList.of("Entity", FAKE_WORKFLOW_ID.toString(), FAKE_SPARK_ID.toString());
    Assert.assertTrue(lines.containsAll(expected) && expected.containsAll(lines));
    testCommandOutputContains("search metadata fake* filtered by target-type dataset", FAKE_DS_ID.toString());
    testCommandOutputContains(String.format("search metadata %s", FakeApp.TIME_SCHEDULE_NAME),
                              FAKE_APP_ID.toString());
    testCommandOutputContains(String.format("search metadata %s filtered by target-type application",
                                            PingService.NAME),
                              FAKE_APP_ID.toString());
    testCommandOutputContains(String.format("search metadata %s filtered by target-type program",
                                            PrefixedEchoHandler.NAME), PREFIXED_ECHO_HANDLER_ID.toString());
    testCommandOutputContains("search metadata batch* filtered by target-type dataset", FAKE_DS_ID.toString());
    testCommandOutputNotContains("search metadata batchwritable filtered by target-type dataset",
                                 FAKE_DS_ID.toString());
    testCommandOutputContains("search metadata bat* filtered by target-type dataset", FAKE_DS_ID.toString());
    output = getCommandOutput("search metadata batch filtered by target-type program");
    lines = Arrays.asList(output.split("\\r?\\n"));
    expected = ImmutableList.of("Entity", FAKE_SPARK_ID.toString(), FAKE_WORKFLOW_ID.toString());
    Assert.assertTrue(lines.containsAll(expected) && expected.containsAll(lines));
    output = getCommandOutput("search metadata fake* filtered by target-type dataset,application");
    lines = Arrays.asList(output.split("\\r?\\n"));
    expected = ImmutableList.of("Entity", FAKE_DS_ID.toString(), FAKE_APP_ID.toString());
    Assert.assertTrue(lines.containsAll(expected) && expected.containsAll(lines));
    output = getCommandOutput("search metadata wfTag* filtered by target-type program");
    lines = Arrays.asList(output.split("\\r?\\n"));
    expected = ImmutableList.of("Entity", FAKE_WORKFLOW_ID.toString());
    Assert.assertTrue(lines.containsAll(expected) && expected.containsAll(lines));

    MetadataEntity fieldEntity =
      MetadataEntity.builder(FAKE_DS_ID.toMetadataEntity()).appendAsType("field", "empName").build();

    testCommandOutputContains(String.format("add metadata-tags %s 'fieldTag1 fieldTag2 fieldTag3'",
                                            MetadataCommandHelper.toCliString(fieldEntity)),
                              "Successfully added metadata tags");
    output = getCommandOutput(String.format("get metadata-tags %s",
                                            MetadataCommandHelper.toCliString(fieldEntity)));
    lines = Arrays.asList(output.split("\\r?\\n"));
    Assert.assertTrue(lines.contains("fieldTag1") && lines.contains("fieldTag2") && lines.contains("fieldTag3"));
    testCommandOutputContains(String.format("add metadata-properties %s fieldKey=fieldValue",
                                            MetadataCommandHelper.toCliString(fieldEntity)),
                              "Successfully added metadata properties");
    testCommandOutputContains(String.format("get metadata-properties %s",
                                            MetadataCommandHelper.toCliString(fieldEntity)),
                              "fieldKey,fieldValue");

    // test get metadata for custom entity and verify that return is in new format
    testCommandOutputContains(String.format("get metadata %s", MetadataCommandHelper.toCliString(fieldEntity)),
                              fieldEntity.toString());

    // test remove
    testCommandOutputContains(String.format("remove metadata-tag %s 'fieldTag3'",
                                            MetadataCommandHelper.toCliString(fieldEntity)),
                              "Successfully removed metadata tag");
    output = getCommandOutput(String.format("get metadata-tags %s",
                                            MetadataCommandHelper.toCliString(fieldEntity)));
    lines = Arrays.asList(output.split("\\r?\\n"));
    // should not contain the removed tag
    Assert.assertTrue(lines.contains("fieldTag1") && lines.contains("fieldTag2") && !lines.contains("fieldTag3"));
    testCommandOutputContains(String.format("remove metadata-tags %s",
                                            MetadataCommandHelper.toCliString(fieldEntity)),
                              "Successfully removed metadata tags");
    output = getCommandOutput(String.format("get metadata-tags %s",
                                            MetadataCommandHelper.toCliString(fieldEntity)));
    lines = Arrays.asList(output.split("\\r?\\n"));
    // should not contain any tags except the header added by cli
    Assert.assertTrue(lines.size() == 1 && lines.contains("tags"));

    testCommandOutputContains(String.format("remove metadata-properties %s",
                                            MetadataCommandHelper.toCliString(fieldEntity)),
                              "Successfully removed metadata properties");

    // test remove properties
    output = getCommandOutput(String.format("get metadata-properties %s",
                                            MetadataCommandHelper.toCliString(fieldEntity)));
    lines = Arrays.asList(output.split("\\r?\\n"));
    // should not contain any properties except the header added by cli
    Assert.assertTrue(lines.size() == 1 && lines.contains("key,value"));
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

  private void testNamespacesOutput(String command, final List<NamespaceMeta> expected)
    throws Exception {
    CLIMain cliMain = getCliMain();
    CLIConfig cliConfig = getCliConfig();
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

    testCommand(command, new Function<String, Void>() {
      @Nullable
      @Override
      public Void apply(@Nullable String output) {
        Assert.assertNotNull(output);
        Assert.assertEquals(expectedOutput, output);
        return null;
      }
    });
  }

  private void testPreferencesOutput(String command, final Map<String, String> expected)
    throws Exception {
    testCommand(command, new Function<String, Void>() {
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

  private void createHiveDB(String hiveDb) throws Exception {
    QueryClient queryClient = getQueryClient();
    ListenableFuture<ExploreExecutionResult> future =
      queryClient.execute(NamespaceId.DEFAULT, "create database " + hiveDb);
    assertExploreQuerySuccess(future);
    future = queryClient.execute(NamespaceId.DEFAULT, "describe database " + hiveDb);
    assertExploreQuerySuccess(future);
  }

  private void dropHiveDb(String hiveDb) throws Exception {
    QueryClient queryClient = getQueryClient();
    assertExploreQuerySuccess(queryClient.execute(NamespaceId.DEFAULT, "drop database " + hiveDb));
  }

  private void assertExploreQuerySuccess(
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
    plugin.put("className", "io.cdap.cdap.client.app.FakePlugin");

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
