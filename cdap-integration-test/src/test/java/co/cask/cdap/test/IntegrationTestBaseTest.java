/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.test;

import co.cask.cdap.StandaloneTester;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.proto.Id;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Test for {@link IntegrationTestBase}.
 */
public class IntegrationTestBaseTest extends IntegrationTestBase {

  @ClassRule
  public static final StandaloneTester STANDALONE = new StandaloneTester();

  @Override
  protected String getInstanceURI() {
    return STANDALONE.getBaseURI().toString();
  }

  @Test
  public void testFlowManager() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestApplication.class);
    FlowManager flowManager = applicationManager.getFlowManager(TestFlow.NAME).start();
    flowManager.stop();
  }

  @Test
  public void testDeployApplicationInNamespace() throws Exception {
    Id.Namespace namespace = createNamespace("Test1");
    ClientConfig clientConfig = new ClientConfig.Builder(getClientConfig()).build();
    deployApplication(namespace, TestApplication.class);

    // Check the default namespaces applications to see whether the application wasnt made in the default namespace
    ClientConfig defaultClientConfig = new ClientConfig.Builder(getClientConfig()).build();
    Assert.assertEquals(0, new ApplicationClient(defaultClientConfig).list(Id.Namespace.DEFAULT).size());

    ApplicationClient applicationClient = new ApplicationClient(clientConfig);
    Assert.assertEquals("TestApplication", applicationClient.list(namespace).get(0).getName());
    applicationClient.delete(Id.Application.from(namespace, "TestApplication"));
    Assert.assertEquals(0, new ApplicationClient(clientConfig).list(namespace).size());

  }
}
