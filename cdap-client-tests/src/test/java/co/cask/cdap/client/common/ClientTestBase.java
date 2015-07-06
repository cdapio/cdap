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

package co.cask.cdap.client.common;

import co.cask.cdap.StandaloneTester;
import co.cask.cdap.cli.util.InstanceURIParser;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.app.DummyWorkerTemplate;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.SingletonExternalResource;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public abstract class ClientTestBase {

  @ClassRule
  public static final SingletonExternalResource STANDALONE =
    new SingletonExternalResource(new StandaloneTester(DummyWorkerTemplate.class));

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  protected ClientConfig clientConfig;

  @Before
  public void setUp() throws Throwable {
    StandaloneTester standalone = STANDALONE.get();
    ConnectionConfig connectionConfig = InstanceURIParser.DEFAULT.parse(standalone.getBaseURI().toString());
    clientConfig = new ClientConfig.Builder().setConnectionConfig(connectionConfig).build();
  }

  protected ClientConfig getClientConfig() {
    return clientConfig;
  }

  protected void verifyProgramNames(List<String> expected, List<ProgramRecord> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (ProgramRecord actualProgram : actual) {
      Assert.assertTrue(expected.contains(actualProgram.getName()));
    }
  }

  protected void verifyProgramRecords(List<String> expected, Map<ProgramType, List<ProgramRecord>> map) {
    verifyProgramNames(expected, Lists.newArrayList(Iterables.concat(map.values())));
  }

  protected void assertFlowletInstances(ProgramClient programClient, String appId, String flowId, String flowletId,
                                        int numInstances)
    throws IOException, NotFoundException, UnauthorizedException {

    int actualInstances;
    int numTries = 0;
    int maxTries = 5;
    do {
      actualInstances = programClient.getFlowletInstances(appId, flowId, flowletId);
      numTries++;
    } while (actualInstances != numInstances && numTries <= maxTries);
    Assert.assertEquals(numInstances, actualInstances);
  }

  protected void assertProgramRunning(ProgramClient programClient, String appId, ProgramType programType,
                                      String programId)
    throws IOException, ProgramNotFoundException, UnauthorizedException, InterruptedException {

    assertProgramStatus(programClient, appId, programType, programId, "RUNNING");
  }

  protected void assertProgramStopped(ProgramClient programClient, String appId, ProgramType programType,
                                      String programId)
    throws IOException, ProgramNotFoundException, UnauthorizedException, InterruptedException {

    assertProgramStatus(programClient, appId, programType, programId, "STOPPED");
  }

  protected void assertProgramStatus(ProgramClient programClient, String appId, ProgramType programType,
                                     String programId, String programStatus)
    throws IOException, ProgramNotFoundException, UnauthorizedException, InterruptedException {

    try {
      programClient.waitForStatus(appId, programType, programId, programStatus, 60, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // NO-OP
    }

    Assert.assertEquals(programStatus, programClient.getStatus(appId, programType, programId));
  }

  protected File createAppJarFile(Class<?> cls) throws IOException {
    LocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Location deploymentJar = AppJarHelper.createDeploymentJar(locationFactory, cls);
    File appJarFile = TMP_FOLDER.newFile();
    Files.copy(Locations.newInputSupplier(deploymentJar), appJarFile);
    return appJarFile;
  }
}
