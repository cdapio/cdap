/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpMethod;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;

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
    flowManager.waitForStatus(true);
    flowManager.stop();
  }

  @Test
  public void testDeployApplicationInNamespace() throws Exception {
    NamespaceId namespace = new NamespaceId("Test1");
    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder().setName(namespace).build();
    getNamespaceClient().create(namespaceMeta);
    ClientConfig clientConfig = new ClientConfig.Builder(getClientConfig()).build();
    deployApplication(namespace, TestApplication.class);

    // Check the default namespaces applications to see whether the application wasn't made in the default namespace
    ClientConfig defaultClientConfig = new ClientConfig.Builder(getClientConfig()).build();
    Assert.assertEquals(0, new ApplicationClient(defaultClientConfig).list(NamespaceId.DEFAULT).size());

    ApplicationClient applicationClient = new ApplicationClient(clientConfig);
    Assert.assertEquals(TestApplication.NAME, applicationClient.list(namespace).get(0).getName());
    applicationClient.delete(namespace.app(TestApplication.NAME));
    Assert.assertEquals(0, new ApplicationClient(clientConfig).list(namespace).size());

  }

  @Test
  public void testGetApplicationManager() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestApplication.class);
    ApplicationId applicationId = NamespaceId.DEFAULT.app(TestApplication.NAME);
    ApplicationManager testApplicationManager = getApplicationManager(applicationId);
    Assert.assertEquals(applicationManager.getFlowManager(TestFlow.NAME).getFlowletInstances(TestFlowlet.NAME),
                        testApplicationManager.getFlowManager(TestFlow.NAME).getFlowletInstances(TestFlowlet.NAME));

  }

  @Test
  public void testSQLQuery() throws Exception {
    getTestManager().deployDatasetModule(NamespaceId.DEFAULT.datasetModule("my-kv"), AppUsingCustomModule.Module.class);

    DatasetAdmin dsAdmin = getTestManager().addDatasetInstance("myKeyValueTable",
                                                               NamespaceId.DEFAULT.dataset("myTable"));
    Assert.assertTrue(dsAdmin.exists());

    ApplicationManager appManager = deployApplication(NamespaceId.DEFAULT, AppUsingCustomModule.class);
    ServiceManager serviceManager = appManager.getServiceManager("MyService").start();
    serviceManager.waitForStatus(true);

    put(serviceManager, "a", "1");
    put(serviceManager, "b", "2");
    put(serviceManager, "c", "1");

    try (
      Connection connection = getTestManager().getQueryClient(NamespaceId.DEFAULT);
      // the value (character) "1" corresponds to the decimal 49. In hex, that is 31.
      ResultSet results = connection.prepareStatement("select key from dataset_mytable where hex(value) = '31'")
        .executeQuery()
    ) {
      // run a query over the dataset
      Assert.assertTrue(results.next());
      Assert.assertEquals("a", results.getString(1));
      Assert.assertTrue(results.next());
      Assert.assertEquals("c", results.getString(1));
      Assert.assertFalse(results.next());
    }

    dsAdmin.drop();
    Assert.assertFalse(dsAdmin.exists());
  }

  private void put(ServiceManager serviceManager, String key,
                   String value) throws IOException, UnauthenticatedException, UnauthorizedException {
    URL url = new URL(serviceManager.getServiceURL(), key);
    getRestClient().execute(HttpMethod.PUT, url, value,
                            ImmutableMap.of(), getClientConfig().getAccessToken());
  }
}
