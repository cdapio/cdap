/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.reactor.client.common;

import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.exception.NotFoundException;
import co.cask.cdap.client.exception.ProgramNotFoundException;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.security.authentication.client.AuthenticationClient;
import co.cask.cdap.security.authentication.client.basic.BasicAuthenticationClient;
import co.cask.cdap.test.internal.AppFabricTestHelper;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class ClientTestBase extends StandaloneTestBase {
  protected static final String HOSTNAME = "localhost";
  protected static final int PORT = 10000;

  protected ClientConfig clientConfig;

  @Before
  public void setUp() throws Throwable {
    AuthenticationClient authenticationClient = new BasicAuthenticationClient();
    authenticationClient.setConnectionInfo(HOSTNAME, PORT, false);
    clientConfig = new ClientConfig(HOSTNAME, authenticationClient);
  }

  protected void verifyProgramNames(List<String> expected, List<ProgramRecord> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (ProgramRecord actualProgramRecord : actual) {
      Assert.assertTrue(expected.contains(actualProgramRecord.getId()));
    }
  }

  protected void verifyProgramNames(List<String> expected, Map<ProgramType, List<ProgramRecord>> actual) {
    verifyProgramNames(expected, convert(actual));
  }

  private List<ProgramRecord> convert(Map<ProgramType, List<ProgramRecord>> map) {
    List<ProgramRecord> result = Lists.newArrayList();
    for (List<ProgramRecord> subList : map.values()) {
      result.addAll(subList);
    }
    return result;
  }

  protected void assertProcedureInstances(ProgramClient programClient, String appId, String procedureId,
                                          int numInstances) throws IOException, NotFoundException,
    UnAuthorizedAccessTokenException {

    int actualInstances;
    int numTries = 0;
    int maxTries = 5;
    do {
      actualInstances = programClient.getProcedureInstances(appId, procedureId);
      numTries++;
    } while (actualInstances != numInstances && numTries <= maxTries);
    Assert.assertEquals(numInstances, actualInstances);
  }

  protected void assertFlowletInstances(ProgramClient programClient, String appId, String flowId,
                                        String flowletId, int numInstances) throws IOException, NotFoundException,
    UnAuthorizedAccessTokenException {

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
                                      String programId) throws IOException, ProgramNotFoundException,
    UnAuthorizedAccessTokenException {

    assertProgramStatus(programClient, appId, programType, programId, "RUNNING");
  }


  protected void assertProgramStopped(ProgramClient programClient, String appId, ProgramType programType,
                                      String programId) throws IOException, ProgramNotFoundException,
    UnAuthorizedAccessTokenException {

    assertProgramStatus(programClient, appId, programType, programId, "STOPPED");
  }

  protected void assertProgramStatus(ProgramClient programClient, String appId, ProgramType programType,
                                     String programId, String programStatus)
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
    } while (!status.equals(programStatus) && numTries <= maxTries);
    Assert.assertEquals(programStatus, status);
  }

  protected File createAppJarFile(Class<?> cls) {
    return new File(AppFabricTestHelper.createAppJar(cls).toURI());
  }
}
