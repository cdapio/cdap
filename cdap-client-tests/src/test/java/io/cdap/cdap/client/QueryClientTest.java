/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.client;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import io.cdap.cdap.StandaloneTester;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.client.app.DatasetWriterService;
import io.cdap.cdap.client.app.FakeApp;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.explore.client.ExploreExecutionResult;
import io.cdap.cdap.explore.client.FixedAddressExploreClient;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.QueryResult;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.cdap.test.SingletonExternalResource;
import io.cdap.cdap.test.XSlowTests;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Test for {@link QueryClient}.
 */
@Category(XSlowTests.class)
public class QueryClientTest extends AbstractClientTest {

  @ClassRule
  public static final SingletonExternalResource STANDALONE = new SingletonExternalResource(new StandaloneTester());

  private static final Logger LOG = LoggerFactory.getLogger(QueryClientTest.class);

  private ApplicationClient appClient;
  private QueryClient queryClient;
  private NamespaceClient namespaceClient;
  private ProgramClient programClient;
  private ServiceClient serviceClient;
  private ExploreClient exploreClient;

  @Override
  protected StandaloneTester getStandaloneTester() {
    return STANDALONE.get();
  }

  @Before
  public void setUp() throws Throwable {
    super.setUp();

    appClient = new ApplicationClient(clientConfig);
    queryClient = new QueryClient(clientConfig);
    programClient = new ProgramClient(clientConfig);
    serviceClient = new ServiceClient(clientConfig);
    String accessToken = (clientConfig.getAccessToken() == null) ? null : clientConfig.getAccessToken().getValue();
    ConnectionConfig connectionConfig = clientConfig.getConnectionConfig();
    exploreClient = new FixedAddressExploreClient(connectionConfig.getHostname(), connectionConfig.getPort(),
                                                  accessToken, connectionConfig.isSSLEnabled(),
                                                  clientConfig.isVerifySSLCert());
    namespaceClient = new NamespaceClient(clientConfig);
  }

  @Test
  public void testAll() throws Exception {
    NamespaceId namespace = new NamespaceId("queryClientTestNamespace");
    NamespaceId otherNamespace = new NamespaceId("queryClientOtherNamespace");
    namespaceClient.create(new NamespaceMeta.Builder().setName(namespace).build());

    ApplicationId app = namespace.app(FakeApp.NAME);
    ServiceId service = app.service(DatasetWriterService.NAME);
    DatasetId dataset = namespace.dataset(FakeApp.DS_NAME);

    appClient.deploy(namespace, createAppJarFile(FakeApp.class));

    try {
      programClient.start(service);
      assertProgramRunning(programClient, service);

      // Write some data through the service
      Map<String, String> data = new HashMap<>();
      data.put("bob", "123");
      data.put("joe", "321");

      URL writeURL = new URL(serviceClient.getServiceURL(service), "write");
      HttpResponse response = HttpRequests.execute(HttpRequest.post(writeURL)
                                                     .withBody(new Gson().toJson(data)).build(),
                                                   new DefaultHttpRequestConfig(false));
      Assert.assertEquals(200, response.getResponseCode());

      executeBasicQuery(namespace, FakeApp.DS_NAME);

      exploreClient.disableExploreDataset(dataset).get();
      try {
        queryClient.execute(namespace, "select * from " + FakeApp.DS_NAME).get();
        Assert.fail("Explore Query should have thrown an ExecutionException since explore is disabled");
      } catch (ExecutionException e) {
        // ignored
      }

      exploreClient.enableExploreDataset(dataset).get();
      executeBasicQuery(namespace, FakeApp.DS_NAME);

      try {
        queryClient.execute(otherNamespace, "show tables").get();
        Assert.fail("Explore Query should have thrown an ExecutionException since the database should not exist");
      } catch (ExecutionException e) {
        // expected
      }
    } finally {
      programClient.stop(service);
      assertProgramStopped(programClient, service);

      try {
        appClient.delete(app);
      } catch (Exception e) {
        LOG.error("Error deleting app {} during test cleanup.", e);
      }
    }
  }

  private void executeBasicQuery(NamespaceId namespace, String instanceName) throws Exception {
    // Hive replaces the periods with underscores
    String query = "select * from dataset_" + instanceName.replace(".", "_");
    ExploreExecutionResult executionResult = queryClient.execute(namespace, query).get();
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
