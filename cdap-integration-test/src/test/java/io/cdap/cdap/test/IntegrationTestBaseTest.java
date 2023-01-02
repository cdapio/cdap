/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package io.cdap.cdap.test;

import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.StandaloneTester;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
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
  public void testDeployApplicationInNamespace() throws Exception {
    NamespaceId namespace = new NamespaceId("Test1");
    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder().setName(namespace).build();
    getNamespaceClient().create(namespaceMeta);
    ClientConfig clientConfig = new ClientConfig.Builder(getClientConfig()).build();
    deployApplication(namespace, AllProgramsApp.class);

    // Check the default namespaces applications to see whether the application wasn't made in the default namespace
    ClientConfig defaultClientConfig = new ClientConfig.Builder(getClientConfig()).build();
    Assert.assertTrue(new ApplicationClient(defaultClientConfig).list(NamespaceId.DEFAULT).isEmpty());

    ApplicationClient applicationClient = new ApplicationClient(clientConfig);
    Assert.assertEquals(AllProgramsApp.NAME, applicationClient.list(namespace).get(0).getName());
    ApplicationDetail appDetail = applicationClient.get(new ApplicationId(namespace.getNamespace(),
                                                                          AllProgramsApp.NAME));
    applicationClient.delete(namespace.app(AllProgramsApp.NAME, appDetail.getAppVersion()));
    Assert.assertTrue(new ApplicationClient(clientConfig).list(namespace).isEmpty());

  }
}
