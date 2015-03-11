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

package co.cask.cdap.client;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.app.FakeFlow;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.explore.client.FixedAddressExploreClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Test for {@link QueryClient}.
 */
@Category(XSlowTests.class)
public class QueryClientTestRun extends ClientTestBase {
  private ApplicationClient appClient;
  private QueryClient queryClient;
  private QueryClient queryClientOtherNamespace;
  private NamespaceClient namespaceClient;
  private ProgramClient programClient;
  private StreamClient streamClient;
  private ExploreClient exploreClient;

  private Id.Namespace otherNamespace = Id.Namespace.from("otherNamespace");

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    appClient = new ApplicationClient(clientConfig);
    queryClient = new QueryClient(clientConfig);
    programClient = new ProgramClient(clientConfig);
    streamClient = new StreamClient(clientConfig);
    String accessToken = (clientConfig.getAccessToken() == null) ? null : clientConfig.getAccessToken().getValue();
    ConnectionConfig connectionConfig = clientConfig.getConnectionConfig();
    exploreClient = new FixedAddressExploreClient(connectionConfig.getHostname(),
                                                  connectionConfig.getPort(),
                                                  accessToken);
    namespaceClient = new NamespaceClient(clientConfig);
    ClientConfig config = new ClientConfig.Builder().setConnectionConfig(connectionConfig).build();
    config.setNamespace(otherNamespace);
    queryClientOtherNamespace = new QueryClient(config);
  }

  @Test
  public void testAll() throws Exception {
    namespaceClient.create(new NamespaceMeta.Builder().setId(otherNamespace).build());
    appClient.deploy(createAppJarFile(FakeApp.class));

    try {
      programClient.start(FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);
      assertProgramRunning(programClient, FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);
      streamClient.sendEvent(FakeApp.STREAM_NAME, "bob:123");
      streamClient.sendEvent(FakeApp.STREAM_NAME, "joe:321");

      Thread.sleep(3000);

      Id.Namespace namespace = getClientConfig().getNamespace();
      Id.DatasetInstance datasetInstance = Id.DatasetInstance.from(namespace, FakeApp.DS_NAME);

      executeBasicQuery(FakeApp.DS_NAME);

      exploreClient.disableExploreDataset(datasetInstance).get();
      try {
        queryClient.execute("select * from " + FakeApp.DS_NAME).get();
        Assert.fail("Explore Query should have thrown an ExecutionException since explore is disabled");
      } catch (ExecutionException e) {
      }

      exploreClient.enableExploreDataset(datasetInstance).get();
      executeBasicQuery(FakeApp.DS_NAME);

      ExploreExecutionResult executionResult = queryClientOtherNamespace.execute("show tables").get();
      List<QueryResult> otherNamespaceTables = Lists.newArrayList(executionResult);
      Assert.assertEquals(0, otherNamespaceTables.size());

      programClient.stop(FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);
      assertProgramStopped(programClient, FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);
    } finally {
      appClient.delete(FakeApp.NAME);
    }
  }

  private void executeBasicQuery(String instanceName) throws Exception {
    // Hive replaces the periods with underscores
    String query = "select * from dataset_" + instanceName.replace(".", "_");
    ExploreExecutionResult executionResult = queryClient.execute(query).get();
    Assert.assertNotNull(executionResult.getResultSchema());
    List<QueryResult> results = Lists.newArrayList(executionResult);
    Assert.assertNotNull(results);
    Assert.assertEquals(2, results.size());

    Assert.assertEquals("bob", Bytes.toString((byte[]) results.get(0).getColumns().get(0)));
    Assert.assertEquals("123", Bytes.toString((byte[]) results.get(0).getColumns().get(1)));
    Assert.assertEquals("joe", Bytes.toString((byte[]) results.get(1).getColumns().get(0)));
    Assert.assertEquals("321", Bytes.toString((byte[]) results.get(1).getColumns().get(1)));
  }
}
