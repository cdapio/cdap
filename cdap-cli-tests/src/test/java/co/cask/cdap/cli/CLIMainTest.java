/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.cli.app.FakeApp;
import co.cask.cdap.cli.app.FakeProcedure;
import co.cask.cdap.cli.app.FakeSpark;
import co.cask.cdap.cli.app.PrefixedEchoHandler;
import co.cask.cdap.client.DatasetTypeClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.exception.ProgramNotFoundException;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.ProgramState;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.internal.AppFabricTestHelper;
import co.cask.cdap.test.standalone.StandaloneTestBase;
import co.cask.common.cli.CLI;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Test for {@link CLIMain}.
 */
@Category(XSlowTests.class)
public class CLIMainTest extends StandaloneTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(CLIMainTest.class);
  private static final Gson GSON = new Gson();

  private static final String PREFIX = "123ff1_";
  private static final boolean START_LOCAL_STANDALONE = true;
  private static final String PROTOCOL = "http";
  private static final String HOSTNAME = "localhost";
  private static final String PORT = "10000";

  private static ProgramClient programClient;
  private static CLIConfig cliConfig;
  private static CLI cli;

  @BeforeClass
  public static void setUpClass() throws Exception {
    if (START_LOCAL_STANDALONE) {
      StandaloneTestBase.setUpClass();
    }

    cliConfig = new CLIConfig(HOSTNAME);
    cliConfig.getClientConfig().setAllTimeouts(60000);

    programClient = new ProgramClient(cliConfig.getClientConfig());

    CLIMain cliMain = new CLIMain(cliConfig);
    cli = cliMain.getCLI();

    testCommandOutputContains(cli, "connect " + PROTOCOL + "://" + HOSTNAME + ":" + PORT, "Successfully connected");
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

    if (START_LOCAL_STANDALONE) {
      StandaloneTestBase.tearDownClass();
    }
  }

  @Test
  public void testConnect() throws Exception {
    testCommandOutputContains(cli, "connect fakehost", "could not be reached");
    testCommandOutputContains(cli, "connect " + HOSTNAME, "Successfully connected");
    testCommandOutputContains(cli, "connect " + PROTOCOL + "://" + HOSTNAME, "Successfully connected");
    testCommandOutputContains(cli, "connect " + PROTOCOL + "://" + HOSTNAME + ":" + PORT, "Successfully connected");
  }

  @Test
  public void testList() throws Exception {
    testCommandOutputContains(cli, "list apps", FakeApp.NAME);
    testCommandOutputContains(cli, "list dataset instances", FakeApp.DS_NAME);
    testCommandOutputContains(cli, "list streams", FakeApp.STREAM_NAME);
    testCommandOutputContains(cli, "list flows", FakeApp.FLOWS.get(0));
  }

  @Test
  public void testProgram() throws Exception {
    String flowId = FakeApp.FLOWS.get(0);
    String qualifiedFlowId = FakeApp.NAME + "." + flowId;
    testCommandOutputContains(cli, "start flow " + qualifiedFlowId, "Successfully started Flow");
    assertProgramStatus(programClient, FakeApp.NAME, ProgramType.FLOW, flowId, ProgramState.RUNNING);
    testCommandOutputContains(cli, "stop flow " + qualifiedFlowId, "Successfully stopped Flow");
    assertProgramStatus(programClient, FakeApp.NAME, ProgramType.FLOW, flowId, ProgramState.STOPPED);
    testCommandOutputContains(cli, "get flow status " + qualifiedFlowId, ProgramState.STOPPED);
    testCommandOutputContains(cli, "get flow runs " + qualifiedFlowId, "COMPLETED");
    testCommandOutputContains(cli, "get flow live " + qualifiedFlowId, flowId);
  }

  @Test
  public void testStream() throws Exception {
    String streamId = PREFIX + "sdf123";
    testCommandOutputContains(cli, "create stream " + streamId, "Successfully created stream");
    testCommandOutputContains(cli, "list streams", streamId);
    testCommandOutputNotContains(cli, "get stream " + streamId, "helloworld");
    testCommandOutputContains(cli, "send stream " + streamId + " helloworld", "Successfully send stream event");
    testCommandOutputContains(cli, "get stream " + streamId, "helloworld");
    testCommandOutputContains(cli, "get stream " + streamId + " -10m -0s 1", "helloworld");
    testCommandOutputContains(cli, "get stream " + streamId + " -10m -0s", "helloworld");
    testCommandOutputContains(cli, "get stream " + streamId + " -10m", "helloworld");
    testCommandOutputContains(cli, "truncate stream " + streamId, "Successfully truncated stream");
    testCommandOutputNotContains(cli, "get stream " + streamId, "helloworld");
    testCommandOutputContains(cli, "set stream ttl " + streamId + " 123", "Successfully set TTL of stream");
    testCommandOutputContains(cli, "describe stream " + streamId, "123");
  }

  @Test
  public void testDataset() throws Exception {
    String datasetName = PREFIX + "sdf123lkj";
    DatasetTypeClient datasetTypeClient = new DatasetTypeClient(cliConfig.getClientConfig());
    DatasetTypeMeta datasetType = datasetTypeClient.list().get(0);
    testCommandOutputContains(cli, "create dataset instance " + datasetType.getName() + " " + datasetName,
                              "Successfully created dataset");
    try {
      testCommandOutputContains(cli, "truncate dataset instance " + datasetName, "Successfully truncated dataset");
    } finally {
      testCommandOutputContains(cli, "delete dataset instance " + datasetName, "Successfully deleted dataset");
    }
  }

  @Test
  public void testProcedure() throws Exception {
    String qualifiedProcedureId = String.format("%s.%s", FakeApp.NAME, FakeProcedure.NAME);
    testCommandOutputContains(cli, "start procedure " + qualifiedProcedureId, "Successfully started Procedure");
    assertProgramStatus(programClient, FakeApp.NAME, ProgramType.PROCEDURE,
                        FakeProcedure.NAME, ProgramState.RUNNING);
    try {
      testCommandOutputContains(cli, "call procedure " + qualifiedProcedureId
        + " " + FakeProcedure.METHOD_NAME + " 'customer bob'", "realbob");
    } finally {
      testCommandOutputContains(cli, "stop procedure " + qualifiedProcedureId, "Successfully stopped Procedure");
      assertProgramStatus(programClient, FakeApp.NAME, ProgramType.PROCEDURE,
                          FakeProcedure.NAME, ProgramState.STOPPED);
    }
  }

  @Test
  public void testService() throws Exception {
    String qualifiedServiceId = String.format("%s.%s", FakeApp.NAME, PrefixedEchoHandler.NAME);
    testCommandOutputContains(cli, "start service " + qualifiedServiceId, "Successfully started Service");
    assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SERVICE,
                        PrefixedEchoHandler.NAME, ProgramState.RUNNING);
    try {
      testCommandOutputContains(cli, "get endpoints service " + qualifiedServiceId, "POST");
      testCommandOutputContains(cli, "get endpoints service " + qualifiedServiceId, "/echo");
      testCommandOutputContains(cli, "call service " + qualifiedServiceId
        + " POST /echo body \"testBody\"", ":testBody");
    } finally {
      testCommandOutputContains(cli, "stop service " + qualifiedServiceId, "Successfully stopped Service");
      assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SERVICE,
                          PrefixedEchoHandler.NAME, ProgramState.STOPPED);
    }
  }

  @Test
  public void testRuntimeArgs() throws Exception {
    String qualifiedServiceId = String.format("%s.%s", FakeApp.NAME, PrefixedEchoHandler.NAME);

    Map<String, String> runtimeArgs = ImmutableMap.of("sdf", "bacon");
    String runtimeArgsKV = Joiner.on(" ").withKeyValueSeparator("=").join(runtimeArgs);
    testCommandOutputContains(cli, "start service " + qualifiedServiceId + " '" + runtimeArgsKV + "'",
                              "Successfully started Service");
    try {
      assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SERVICE,
                          PrefixedEchoHandler.NAME, ProgramState.RUNNING);
      testCommandOutputContains(cli, "call service " + qualifiedServiceId + " POST /echo body \"testBody\"",
                                "bacon:testBody");
      testCommandOutputContains(cli, "stop service " + qualifiedServiceId, "Successfully stopped Service");
      assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SERVICE,
                          PrefixedEchoHandler.NAME, ProgramState.STOPPED);

      Map<String, String> runtimeArgs2 = ImmutableMap.of("sdf", "chickenz");
      String runtimeArgs2Json = GSON.toJson(runtimeArgs2);
      String runtimeArgs2KV = Joiner.on(" ").withKeyValueSeparator("=").join(runtimeArgs2);
      testCommandOutputContains(cli, "set service runtimeargs " + qualifiedServiceId + " '" + runtimeArgs2KV + "'",
                                "Successfully set runtime args");
      testCommandOutputContains(cli, "start service " + qualifiedServiceId, "Successfully started Service");
      testCommandOutputContains(cli, "get service runtimeargs " + qualifiedServiceId, runtimeArgs2Json);
      testCommandOutputContains(cli, "call service " + qualifiedServiceId + " POST /echo body \"testBody\"",
                                "chickenz:testBody");
    } finally {
      testCommandOutputContains(cli, "stop service " + qualifiedServiceId, "Successfully stopped Service");
      assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SERVICE,
                          PrefixedEchoHandler.NAME, ProgramState.STOPPED);
    }
  }

  @Test
  public void testSpark() throws Exception {
    String sparkId = FakeApp.SPARK.get(0);
    String qualifiedSparkId = FakeApp.NAME + "." + sparkId;
    testCommandOutputContains(cli, "list spark", sparkId);
    testCommandOutputContains(cli, "start spark " + qualifiedSparkId, "Successfully started Spark");
    assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SPARK, FakeSpark.NAME, ProgramState.RUNNING);
    assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SPARK, FakeSpark.NAME, ProgramState.STOPPED);
    testCommandOutputContains(cli, "get spark status " + qualifiedSparkId, ProgramState.STOPPED);
    testCommandOutputContains(cli, "get spark runs " + qualifiedSparkId, "COMPLETED");
    testCommandOutputContains(cli, "get spark logs " + qualifiedSparkId, "HelloFakeSpark");
  }

  private static File createAppJarFile(Class<?> cls) {
    return new File(AppFabricTestHelper.createAppJar(cls).toURI());
  }

  private static void testCommandOutputContains(CLI cli, String command,
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

  private static void testCommandOutputContains(CLI cli, String command,
                                                final Object expectedOutput) throws Exception {
    testCommandOutputContains(cli, command, expectedOutput.toString());
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
                                     String programId, ProgramState programStatus, int tries)
    throws IOException, ProgramNotFoundException, UnAuthorizedAccessTokenException {

    ProgramState status;
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
                                     String programId, ProgramState programStatus)
    throws IOException, ProgramNotFoundException, UnAuthorizedAccessTokenException {

    assertProgramStatus(programClient, appId, programType, programId, programStatus, 180);
  }

}
