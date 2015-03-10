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

package co.cask.cdap.client.common;

import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.exception.ProgramNotFoundException;
import co.cask.cdap.common.exception.UnauthorizedException;
import co.cask.cdap.proto.ProgramDetail;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.internal.AppFabricTestHelper;
import co.cask.cdap.test.standalone.StandaloneTestBase;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public abstract class ClientTestBase extends StandaloneTestBase {
  protected static final boolean START_LOCAL_STANDALONE = true;
  protected static final String HOSTNAME = "localhost";
  protected static final int PORT = 10000;

  protected ClientConfig clientConfig;

  @Before
  public void setUp() throws Throwable {
    clientConfig = new ClientConfig.Builder().setHostname(HOSTNAME).setPort(PORT).build();
  }

  @Override
  protected ClientConfig getClientConfig() {
    return clientConfig;
  }

  protected void verifyProgramNames(List<String> expected, List<ProgramDetail> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (ProgramDetail actualProgram : actual) {
      Assert.assertTrue(expected.contains(actualProgram.getName()));
    }
  }

  protected void verifyProgramRecords(List<String> expected, List<ProgramRecord> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (ProgramRecord actualProgram : actual) {
      Assert.assertTrue(expected.contains(actualProgram.getName()));
    }
  }

  protected void verifyProgramRecords(List<String> expected, Map<ProgramType, List<ProgramRecord>> map) {
    verifyProgramRecords(expected, convert(map));
  }

  private List<ProgramRecord> convert(Map<ProgramType, List<ProgramRecord>> map) {
    List<ProgramRecord> result = Lists.newArrayList();
    for (List<ProgramRecord> subList : map.values()) {
      result.addAll(subList);
    }
    return result;
  }

  protected void assertProcedureInstances(ProgramClient programClient, String appId, String procedureId,
                                          int numInstances)
    throws IOException, NotFoundException, UnauthorizedException {

    int actualInstances;
    int numTries = 0;
    int maxTries = 5;
    do {
      actualInstances = programClient.getProcedureInstances(appId, procedureId);
      numTries++;
    } while (actualInstances != numInstances && numTries <= maxTries);
    Assert.assertEquals(numInstances, actualInstances);
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
      programClient.waitForStatus(appId, programType, programId, programStatus, 30, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // NO-OP
    }

    Assert.assertEquals(programStatus, programClient.getStatus(appId, programType, programId));
  }

  protected File createAppJarFile(Class<?> cls) {
    return new File(AppFabricTestHelper.createAppJar(cls).toURI());
  }
}
