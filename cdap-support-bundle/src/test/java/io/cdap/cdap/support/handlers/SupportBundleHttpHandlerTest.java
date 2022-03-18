/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.support.handlers;

import io.cdap.cdap.SupportBundleTestBase;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Monitor handler tests.
 */
public class SupportBundleHttpHandlerTest extends SupportBundleTestBase {

  private static final NamespaceId NAMESPACE = new NamespaceId("test");
  private static final NamespaceId NAMESPACE_DEFAULT_ID = NamespaceId.DEFAULT;
  private static final ApplicationId APP_WORKFLOW = NAMESPACE.app("AppWithWorkflow");
  private static final String RUNNING = "RUNNING";
  private static CConfiguration cConf;
  private static Store store;
  private int sourceId;

  @Before
  public void setup() throws Exception {
    Assert.assertEquals(HttpURLConnection.HTTP_OK, createNamespace(NAMESPACE).getResponseCode());
    store = getInjector().getInstance(DefaultStore.class);
    cConf = getInjector().getInstance(CConfiguration.class);
  }

  @After
  public void cleanup() throws IOException {
    Assert.assertEquals(HttpURLConnection.HTTP_OK, deleteNamespace(NAMESPACE).getResponseCode());
  }

  @Test
  public void testCreateSupportBundleWithValidNamespace() throws Exception {

    String bundleId = requestBundle(Collections.singletonMap("namespace", NAMESPACE.getNamespace()));

    Assert.assertNotNull(bundleId);
    Assert.assertFalse(bundleId.isEmpty());
  }
  
  /**
   * Requests generation of support bundle.
   *
   * @param params a map of query parameters
   * @return the bundle UUID
   * @throws IOException if failed to request bundle generation
   */
  private String requestBundle(Map<String, String> params) throws IOException {
    DiscoveryServiceClient discoveryServiceClient = getInjector().getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable =
      new RandomEndpointStrategy(() -> discoveryServiceClient.discover(Constants.Service.SUPPORT_BUNDLE_SERVICE)).pick(
        5, TimeUnit.SECONDS);

    Assert.assertNotNull("No service for support bundle", discoverable);

    StringBuilder queryBuilder = new StringBuilder();
    String sep = "?";
    for (Map.Entry<String, String> entry : params.entrySet()) {
      queryBuilder.append(sep)
        .append(URLEncoder.encode(entry.getKey(), "UTF-8"))
        .append("=")
        .append(URLEncoder.encode(entry.getValue(), "UTF-8"));
      sep = "&";
    }

    String path = String.format("%s/support/bundle%s", Constants.Gateway.API_VERSION_3, queryBuilder);

    HttpRequest request = HttpRequest.post(URIScheme.createURI(discoverable, path).toURL()).build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));

    Assert.assertEquals(HttpURLConnection.HTTP_CREATED, response.getResponseCode());
    return response.getResponseBodyAsString();
  }
}
