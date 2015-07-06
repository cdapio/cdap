/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.cli.util.InstanceURIParser;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.CsvTableRenderer;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.DatasetTypeClient;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.app.FakeDataset;
import co.cask.cdap.client.app.FakeFlow;
import co.cask.cdap.client.app.FakeSpark;
import co.cask.cdap.client.app.FakeWorkflow;
import co.cask.cdap.client.app.PrefixedEchoHandler;
import co.cask.cdap.client.config.AuthenticatedConnectionConfig;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.DatasetTypeNotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.XSlowTests;
import co.cask.common.cli.CLI;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.gson.Gson;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Test for {@link CLIMain}.
 */
@Category(XSlowTests.class)
public class CLIMainTest {

  @ClassRule
  public static final StandaloneTester STANDALONE = new StandaloneTester();

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static final Logger LOG = LoggerFactory.getLogger(CLIMainTest.class);
  private static final Gson GSON = new Gson();

  private static final String PREFIX = "123ff1_";

  private static ProgramClient programClient;
  private static ClientConfig clientConfig;
  private static CLIConfig cliConfig;
  private static CLIMain cliMain;
  private static CLI cli;

  @BeforeClass
  public static void setUpClass() throws Exception {

    ConnectionConfig connectionConfig = InstanceURIParser.DEFAULT.parse(STANDALONE.getBaseURI().toString());
    clientConfig = new ClientConfig.Builder().setConnectionConfig(connectionConfig).build();
    clientConfig.setAllTimeouts(60000);
    cliConfig = new CLIConfig(clientConfig, System.out, new CsvTableRenderer());
    LaunchOptions launchOptions = new LaunchOptions(LaunchOptions.DEFAULT.getUri(), true, true, false);
    cliMain = new CLIMain(launchOptions, cliConfig);
    programClient = new ProgramClient(cliConfig.getClientConfig());

    cli = cliMain.getCLI();

    testCommandOutputContains(cli, "connect " + STANDALONE.getBaseURI(), "Successfully connected");
    testCommandOutputNotContains(cli, "list apps", FakeApp.NAME);

    File appJarFile = createAppJarFile(FakeApp.class);
    testCommandOutputContains(cli, "deploy app " + appJarFile.getAbsolutePath(), "Successfully deployed app");
    if (!appJarFile.delete()) {
      LOG.warn("Failed to delete temporary app jar file: {}", appJarFile.getAbsolutePath());
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    testCommandOutputContains(cli, "delete app " + FakeApp.NAME, "Successfully deleted app");
  }

  @Before
  public void setUp() {
    clientConfig.setNamespace(Constants.DEFAULT_NAMESPACE_ID);
  }

  @Test
  public void testConnect() throws Exception {
    testCommandOutputContains(cli, "connect fakehost", "could not be reached");
    testCommandOutputContains(cli, "connect " + STANDALONE.getBaseURI(), "Successfully connected");
  }

  @Test
  public void testList() throws Exception {
    testCommandOutputContains(cli, "list apps", FakeApp.NAME);
    testCommandOutputContains(cli, "list dataset instances", FakeApp.DS_NAME);
    testCommandOutputContains(cli, "list streams", FakeApp.STREAM_NAME);
    testCommandOutputContains(cli, "list flows", FakeApp.FLOWS.get(0));
  }

  @Test
  public void testPrompt() throws Exception {
    String prompt = cliMain.getPrompt(cliConfig.getClientConfig());
    Assert.assertFalse(prompt.contains("@"));
    Assert.assertTrue(prompt.contains(STANDALONE.getBaseURI().getHost()));
    Assert.assertTrue(prompt.contains(cliConfig.getCurrentNamespace().getId()));

    ConnectionConfig oldConnectionConfig = clientConfig.getConnectionConfig();
    ConnectionConfig authConnectionConfig = new AuthenticatedConnectionConfig(oldConnectionConfig, "test-username");
    cliConfig.setConnectionConfig(authConnectionConfig);
    prompt = cliMain.getPrompt(cliConfig.getClientConfig());
    Assert.assertTrue(prompt.contains("test-username@"));
    Assert.assertTrue(prompt.contains(STANDALONE.getBaseURI().getHost()));
    Assert.assertTrue(prompt.contains(cliConfig.getCurrentNamespace().getId()));
    cliConfig.setConnectionConfig(oldConnectionConfig);
  }

  @Test
  public void testProgram() throws Exception {
    String flowId = FakeApp.FLOWS.get(0);
    String qualifiedFlowId = FakeApp.NAME + "." + flowId;
    testCommandOutputContains(cli, "start flow " + qualifiedFlowId, "Successfully started Flow");
    assertProgramStatus(programClient, FakeApp.NAME, ProgramType.FLOW, flowId, "RUNNING");
    testCommandOutputContains(cli, "stop flow " + qualifiedFlowId, "Successfully stopped Flow");
    assertProgramStatus(programClient, FakeApp.NAME, ProgramType.FLOW, flowId, "STOPPED");
    testCommandOutputContains(cli, "get flow status " + qualifiedFlowId, "STOPPED");
    testCommandOutputContains(cli, "get flow runs " + qualifiedFlowId, "KILLED");
    testCommandOutputContains(cli, "get flow live " + qualifiedFlowId, flowId);
  }

  @Test
  public void testStream() throws Exception {
    String streamId = PREFIX + "sdf123";
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
                              "Successfully set notification threshold of Stream");
    testCommandOutputContains(cli, "describe stream " + streamId, "100000");

    File file = new File(TMP_FOLDER.newFolder(), "test.txt");
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
                              "Successfully sent stream event to stream");
    testCommandOutputContains(cli, "get stream " + streamId, "9, Event 9");
    testCommandOutputContains(cli, "get stream-stats " + streamId,
                              String.format("No schema found for Stream '%s'", streamId));
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
    testCommandOutputContains(cli, "get schedule status " + scheduleId, "SCHEDULED");
    testCommandOutputContains(cli, "suspend schedule " + scheduleId, "Successfully suspended");
    testCommandOutputContains(cli, "get schedule status " + scheduleId, "SUSPENDED");
    testCommandOutputContains(cli, "resume schedule " + scheduleId, "Successfully resumed");
    testCommandOutputContains(cli, "get schedule status " + scheduleId, "SCHEDULED");
    testCommandOutputContains(cli, "get workflow schedules " + workflowId, FakeApp.SCHEDULE_NAME);
  }

  @Test
  public void testDataset() throws Exception {
    String datasetName = PREFIX + "sdf123lkj";
    DatasetTypeClient datasetTypeClient = new DatasetTypeClient(cliConfig.getClientConfig());
    DatasetTypeMeta datasetType = datasetTypeClient.list().get(0);
    testCommandOutputContains(cli, "create dataset instance " + datasetType.getName() + " " + datasetName,
                              "Successfully created dataset");
    testCommandOutputContains(cli, "list dataset instances", FakeDataset.class.getSimpleName());

    NamespaceClient namespaceClient = new NamespaceClient(cliConfig.getClientConfig());
    Id.Namespace barspace = Id.Namespace.from("bar");
    namespaceClient.create(new NamespaceMeta.Builder().setName(barspace).build());
    cliConfig.getClientConfig().setNamespace(barspace);
    // list of dataset instances is different in 'foo' namespace
    testCommandOutputNotContains(cli, "list dataset instances", FakeDataset.class.getSimpleName());

    // also can not create dataset instances if the type it depends on exists only in a different namespace.
    Id.DatasetType datasetType1 = Id.DatasetType.from(barspace, datasetType.getName());
    testCommandOutputContains(cli, "create dataset instance " + datasetType.getName() + " " + datasetName,
                              new DatasetTypeNotFoundException(datasetType1).getMessage());

    testCommandOutputContains(cli, "use namespace default", "Now using namespace 'default'");
    try {
      testCommandOutputContains(cli, "truncate dataset instance " + datasetName, "Successfully truncated dataset");
    } finally {
      testCommandOutputContains(cli, "delete dataset instance " + datasetName, "Successfully deleted dataset");
    }
  }

  @Test
  public void testService() throws Exception {
    String qualifiedServiceId = String.format("%s.%s", FakeApp.NAME, PrefixedEchoHandler.NAME);
    testCommandOutputContains(cli, "start service " + qualifiedServiceId, "Successfully started Service");
    assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SERVICE, PrefixedEchoHandler.NAME, "RUNNING");
    try {
      testCommandOutputContains(cli, "get endpoints service " + qualifiedServiceId, "POST");
      testCommandOutputContains(cli, "get endpoints service " + qualifiedServiceId, "/echo");
      testCommandOutputContains(cli, "call service " + qualifiedServiceId
        + " POST /echo body \"testBody\"", ":testBody");
    } finally {
      testCommandOutputContains(cli, "stop service " + qualifiedServiceId, "Successfully stopped Service");
      assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SERVICE, PrefixedEchoHandler.NAME, "STOPPED");
    }
  }

  @Test
  public void testRuntimeArgs() throws Exception {
    String qualifiedServiceId = String.format("%s.%s", FakeApp.NAME, PrefixedEchoHandler.NAME);

    Map<String, String> runtimeArgs = ImmutableMap.of("sdf", "bacon");
    String runtimeArgsKV = Joiner.on(",").withKeyValueSeparator("=").join(runtimeArgs);
    testCommandOutputContains(cli, "start service " + qualifiedServiceId + " '" + runtimeArgsKV + "'",
                              "Successfully started Service");
    try {
      assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SERVICE, PrefixedEchoHandler.NAME, "RUNNING");
      testCommandOutputContains(cli, "call service " + qualifiedServiceId + " POST /echo body \"testBody\"",
                                "bacon:testBody");
      testCommandOutputContains(cli, "stop service " + qualifiedServiceId, "Successfully stopped Service");
      assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SERVICE, PrefixedEchoHandler.NAME, "STOPPED");

      Map<String, String> runtimeArgs2 = ImmutableMap.of("sdf", "chickenz");
      String runtimeArgs2Json = GSON.toJson(runtimeArgs2);
      String runtimeArgs2KV = Joiner.on(",").withKeyValueSeparator("=").join(runtimeArgs2);
      testCommandOutputContains(cli, "set service runtimeargs " + qualifiedServiceId + " '" + runtimeArgs2KV + "'",
                                "Successfully set runtime args");
      testCommandOutputContains(cli, "start service " + qualifiedServiceId, "Successfully started Service");
      testCommandOutputContains(cli, "get service runtimeargs " + qualifiedServiceId, runtimeArgs2Json);
      testCommandOutputContains(cli, "call service " + qualifiedServiceId + " POST /echo body \"testBody\"",
                                "chickenz:testBody");
    } finally {
      testCommandOutputContains(cli, "stop service " + qualifiedServiceId, "Successfully stopped Service");
      assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SERVICE, PrefixedEchoHandler.NAME, "STOPPED");
    }
  }

  @Test
  public void testSpark() throws Exception {
    String sparkId = FakeApp.SPARK.get(0);
    String qualifiedSparkId = FakeApp.NAME + "." + sparkId;
    testCommandOutputContains(cli, "list spark", sparkId);
    testCommandOutputContains(cli, "start spark " + qualifiedSparkId, "Successfully started Spark");
    assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SPARK, FakeSpark.NAME, "RUNNING");
    assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SPARK, FakeSpark.NAME, "STOPPED");
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

    testCommandOutputContains(cli, "set preferences namespace 'k1=v1'",
            "successfully");
    testCommandOutputContains(cli, "set preferences namespace 'k1=v1' name",
            "Error: Expected format: set preferences namespace <runtime-args>");
    testCommandOutputContains(cli, "set preferences instance 'k1=v1' name",
            "Error: Expected format: set preferences instance <runtime-args>");

  }

  @Test
  public void testNamespaces() throws Exception {
    final String name = PREFIX + "testNamespace";
    final String description = "testDescription";
    final String defaultFields = PREFIX + "defaultFields";
    final String doesNotExist = "doesNotExist";

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
    String command = String.format("create namespace %s %s", name, description);
    testCommandOutputContains(cli, command, String.format("Namespace '%s' created successfully.", name));

    NamespaceMeta expected = new NamespaceMeta.Builder()
      .setName(name).setDescription(description).build();
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
  }

  private static File createAppJarFile(Class<?> cls) throws IOException {
    LocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Location deploymentJar = AppJarHelper.createDeploymentJar(locationFactory, cls);
    File appJarFile = TMP_FOLDER.newFile();
    Files.copy(Locations.newInputSupplier(deploymentJar), appJarFile);
    return appJarFile;
  }

  private static void testCommandOutputContains(CLI cli, String command, final String expectedOutput) throws Exception {
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

  private static void testCommandOutputNotContains(CLI cli, String command,
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

  private static void testCommand(CLI cli, String command, Function<String, Void> outputValidator) throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(outputStream);
    cli.execute(command, printStream);
    String output = outputStream.toString();
    outputValidator.apply(output);
  }

  protected void assertProgramStatus(ProgramClient programClient, String appId, ProgramType programType,
                                     String programId, String programStatus, int tries)
    throws IOException, ProgramNotFoundException, UnauthorizedException {

    String status;
    int numTries = 0;
    do {
      status = programClient.getStatus(appId, programType, programId);
      numTries++;
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        // NO-OP
      }
    } while (!status.equals(programStatus) && numTries <= tries);
    Assert.assertEquals(programStatus, status);
  }

  protected void assertProgramStatus(ProgramClient programClient, String appId, ProgramType programType,
                                     String programId, String programStatus)
    throws IOException, ProgramNotFoundException, UnauthorizedException {

    assertProgramStatus(programClient, appId, programType, programId, programStatus, 180);
  }

  private static void testNamespacesOutput(CLI cli, String command, final List<NamespaceMeta> expected)
    throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream output = new PrintStream(outputStream);
    Table table = Table.builder()
      .setHeader("name", "description")
      .setRows(expected, new RowMaker<NamespaceMeta>() {
        @Override
        public List<?> makeRow(NamespaceMeta object) {
          return Lists.newArrayList(object.getName(), object.getDescription());
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
    final String expectedOutput = Joiner.on(String.format("%n")).join(expected.entrySet().iterator());
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
}
