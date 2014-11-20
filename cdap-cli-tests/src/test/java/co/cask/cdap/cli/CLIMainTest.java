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

import co.cask.cdap.cli.app.EchoHandler;
import co.cask.cdap.cli.app.FakeApp;
import co.cask.cdap.cli.app.FakeProcedure;
import co.cask.cdap.cli.app.FakeSpark;
import co.cask.cdap.client.DatasetTypeClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.exception.ProgramNotFoundException;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.internal.AppFabricTestHelper;
import co.cask.cdap.test.standalone.StandaloneTestBase;
import co.cask.common.cli.CLI;
import com.google.common.base.Function;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Test for {@link CLIMain}.
 */
@Category(XSlowTests.class)
public class CLIMainTest extends StandaloneTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(CLIMainTest.class);

  @Test
  public void testCommands() throws Exception {
    CLIConfig cliConfig = new CLIConfig("localhost");
    CLIMain cliMain = new CLIMain(cliConfig);
    CLI cli = cliMain.getCLI();

    testCommandOutputContains(cli, "connect fakehost", "could not be reached");
    testCommandOutputContains(cli, "connect localhost", "Successfully connected");
    testCommandOutputContains(cli, "connect http://localhost", "Successfully connected");
    testCommandOutputContains(cli, "connect http://localhost:10000", "Successfully connected");

    testCommandOutputNotContains(cli, "list apps", FakeApp.NAME);

    File appJarFile = createAppJarFile(FakeApp.class);
    testCommandOutputContains(cli, "deploy app " + appJarFile.getAbsolutePath(), "Successfully deployed app");
    if (!appJarFile.delete()) {
      LOG.warn("Failed to delete temporary app jar file: {}", appJarFile.getAbsolutePath());
    }

    testCommandOutputContains(cli, "list apps", FakeApp.NAME);
    testCommandOutputContains(cli, "list dataset instances", FakeApp.DS_NAME);
    testCommandOutputContains(cli, "list streams", FakeApp.STREAM_NAME);
    testCommandOutputContains(cli, "list flows", FakeApp.FLOWS.get(0));

    // test program-related commands
    String flowId = FakeApp.FLOWS.get(0);
    String qualifiedFlowId = FakeApp.NAME + "." + flowId;
    testCommandOutputContains(cli, "start flow " + qualifiedFlowId, "Successfully started Flow");
    testCommandOutputContains(cli, "stop flow " + qualifiedFlowId, "Successfully stopped Flow");
    ProgramClient programClient = new ProgramClient(cliConfig.getClientConfig());
    assertProgramStatus(programClient, FakeApp.NAME, ProgramType.FLOW, flowId, "STOPPED");
    testCommandOutputContains(cli, "get flow status " + qualifiedFlowId, "STOPPED");
    testCommandOutputContains(cli, "get flow runs " + qualifiedFlowId, "COMPLETED");
    testCommandOutputContains(cli, "get flow live " + qualifiedFlowId, flowId);

    // test stream commands
    String streamId = "sdf123";
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

    // test dataset commands
    String datasetName = "sdf123lkj";
    DatasetTypeClient datasetTypeClient = new DatasetTypeClient(cliConfig.getClientConfig());
    DatasetTypeMeta datasetType = datasetTypeClient.list().get(0);
    testCommandOutputContains(cli, "create dataset instance " + datasetType.getName() + " " + datasetName,
                              "Successfully created dataset");
    testCommandOutputContains(cli, "truncate dataset instance " + datasetName, "Successfully truncated dataset");
    testCommandOutputContains(cli, "delete dataset instance " + datasetName, "Successfully deleted dataset");

    // test procedure commands
    String qualifiedProcedureId = String.format("%s.%s", FakeApp.NAME, FakeProcedure.NAME);
    testCommandOutputContains(cli, "start procedure " + qualifiedProcedureId, "Successfully started Procedure");
    testCommandOutputContains(cli, "call procedure " + qualifiedProcedureId
      + " " + FakeProcedure.METHOD_NAME + " 'customer bob'", "realbob");
    testCommandOutputContains(cli, "stop procedure " + qualifiedProcedureId, "Successfully stopped Procedure");

    //test service commands
    String qualifiedServiceId = String.format("%s.%s", FakeApp.NAME, EchoHandler.NAME);
    testCommandOutputContains(cli, "start service " + qualifiedServiceId, "Successfully started Service");
    testCommandOutputContains(cli, "get service " + qualifiedServiceId + " endpoints", "POST");
    testCommandOutputContains(cli, "get service " + qualifiedServiceId + " endpoints", "/echo");
    testCommandOutputContains(cli, "call service " + qualifiedServiceId + " POST /echo body \"testBody\"", "testBody");
    testCommandOutputContains(cli, "stop service " + qualifiedServiceId, "Successfully stopped Service");

    // test spark commands
    String sparkId = FakeApp.SPARK.get(0);
    String qualifiedSparkId = FakeApp.NAME + "." + sparkId;
    testCommandOutputContains(cli, "list spark", sparkId);
    testCommandOutputContains(cli, "start spark " + qualifiedSparkId, "Successfully started Spark");
    assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SPARK, FakeSpark.NAME, "STARTING", 30);
    assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SPARK, FakeSpark.NAME, "RUNNING", 180);
    assertProgramStatus(programClient, FakeApp.NAME, ProgramType.SPARK, FakeSpark.NAME, "STOPPED", 180);
    testCommandOutputContains(cli, "get spark status " + qualifiedSparkId, "STOPPED");
    testCommandOutputContains(cli, "get spark runs " + qualifiedSparkId, "COMPLETED");
    testCommandOutputContains(cli, "get spark logs " + qualifiedSparkId, "HelloFakeSpark");

    // cleanup
    testCommandOutputContains(cli, "delete app " + FakeApp.NAME, "Successfully deleted app");
    testCommandOutputContains(cli, "delete dataset instance " + FakeApp.DS_NAME, "Successfully deleted dataset");
  }

  private File createAppJarFile(Class<?> cls) {
    return new File(AppFabricTestHelper.createAppJar(cls).toURI());
  }

  private void testCommandOutputContains(CLI cli, String command, final String expectedOutput) throws Exception {
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

  private void testCommandOutputNotContains(CLI cli, String command, final String expectedOutput) throws Exception {
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

  private void testCommand(CLI cli, String command, Function<String, Void> outputValidator) throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(outputStream);
    cli.execute(command, printStream);
    String output = outputStream.toString();
    outputValidator.apply(output);
  }

  protected void assertProgramStatus(ProgramClient programClient, String appId, ProgramType programType,
                                     String programId, String programStatus, int tries)
    throws IOException, ProgramNotFoundException, UnAuthorizedAccessTokenException {

    String status;
    int numTries = 0;
    int maxTries = 10;
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
    throws IOException, ProgramNotFoundException, UnAuthorizedAccessTokenException {

    assertProgramStatus(programClient, appId, programType, programId, programStatus, 10);
  }

}
