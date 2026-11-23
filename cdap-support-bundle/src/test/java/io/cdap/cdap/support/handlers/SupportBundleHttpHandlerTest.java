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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.cdap.cdap.SupportBundleTestBase;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Monitor handler tests.
 */
public class SupportBundleHttpHandlerTest extends SupportBundleTestBase {

  private static String bundleId = "";
  private static final NamespaceId NAMESPACE = TEST_NAMESPACE_META1.getNamespaceId();

  @Test
  public void testCreateSupportBundleWithValidNamespace() throws Exception {

    Assert.assertNotNull(bundleId);
    Assert.assertFalse(bundleId.isEmpty());
  }

  @Test
  public void testExportSupportBundle() throws Exception {
    HttpResponse response = requestExportBundle(bundleId);

    // Validate the message digest of the content
    String digest = response.getHeaders().get("digest").stream().findFirst().orElse(null);
    Assert.assertNotNull(digest);
    String[] splits = digest.split("=", 2);
    MessageDigest md = MessageDigest.getInstance(splits[0]);
    Assert.assertEquals(splits[1], Base64.getEncoder().encodeToString(md.digest(response.getResponseBody())));

    File bundleFile = TEMP_FOLDER.newFile();
    bundleFile.delete();
    Files.write(response.getResponseBody(), bundleFile);

    File bundleDir = TEMP_FOLDER.newFolder();
    BundleJarUtil.unJar(bundleFile, bundleDir);

    File bundleExport = new File(bundleDir, bundleId);
    Assert.assertTrue(bundleExport.isDirectory());
    Assert.assertTrue(new File(bundleExport, "status.json").isFile());
    Assert.assertTrue(new File(bundleExport, "system-log").isDirectory());
  }

  @Test
  public void testCreateSupportBundleRejectsInvalidParameters() throws Exception {
    // A bundle from setup() may still be in progress, invalid parameters must still be rejected.
    for (Map<String, String> params : Arrays.asList(
        ImmutableMap.of("namespace", NAMESPACE.getNamespace(), "programType", "invalid"),
        ImmutableMap.of("namespace", NAMESPACE.getNamespace(), "run", "../../../preferences"),
        ImmutableMap.of("namespace", NAMESPACE.getNamespace(), "application", "../app"),
        ImmutableMap.of("namespace", NAMESPACE.getNamespace(), "maxRunsPerProgram", "0"))) {
      HttpResponse response = executeRequest(
          HttpRequest.post(getSupportBundleUrl(String.format("/support/bundles%s", toQuery(params)))));
      Assert.assertEquals(params.toString(), HttpURLConnection.HTTP_BAD_REQUEST, response.getResponseCode());
    }
  }

  @Test
  public void testExportSupportBundleRejectsInvalidRequests() throws Exception {
    HttpResponse response = executeRequest(
        HttpRequest.post(getSupportBundleUrl("/support/bundles/not-a-uuid")));
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, response.getResponseCode());

    for (String body : Arrays.asList("{\"files\": [\"../../status.json\"]}", "{\"files\": [\".\"]}",
                                     "{}", "{\"files\": null}", "null")) {
      response = executeRequest(
          HttpRequest.post(getSupportBundleUrl("/support/bundles/" + bundleId)).withBody(body));
      Assert.assertEquals(body, HttpURLConnection.HTTP_BAD_REQUEST, response.getResponseCode());
    }

    // A blank body exports the whole bundle.
    response = executeRequest(HttpRequest.post(getSupportBundleUrl("/support/bundles/" + bundleId)).withBody("  "));
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
  }

  @Before
  public void setup() throws Exception {
    Assert.assertEquals(HttpURLConnection.HTTP_OK, createNamespace(NAMESPACE).getResponseCode());
    bundleId = requestBundle(Collections.singletonMap("namespace", NAMESPACE.getNamespace()));
  }

  @After
  public void cleanup() throws IOException {
    Assert.assertEquals(HttpURLConnection.HTTP_OK, deleteNamespace(NAMESPACE).getResponseCode());
  }

  /**
   * Requests generation of support bundle.
   *
   * @param params a map of query parameters
   * @return the bundle UUID
   * @throws IOException if failed to request bundle generation
   */
  private String requestBundle(Map<String, String> params)
    throws IOException, ExecutionException, InterruptedException, TimeoutException {
    DiscoveryServiceClient discoveryServiceClient = getInjector().getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable =
      new RandomEndpointStrategy(() -> discoveryServiceClient.discover(Constants.Service.SUPPORT_BUNDLE_SERVICE)).pick(
        5, TimeUnit.SECONDS);

    Assert.assertNotNull("No service for support bundle", discoverable);

    String path = String.format("%s/support/bundles%s", Constants.Gateway.API_VERSION_3, toQuery(params));

    HttpRequest request = HttpRequest.post(URIScheme.createURI(discoverable, "%s", path).toURL()).build();
    AtomicReference<HttpResponse> response =
      new AtomicReference<>(HttpRequests.execute(request, new DefaultHttpRequestConfig(false)));

    //Wait for the previous bundle created
    Tasks.waitFor(HttpURLConnection.HTTP_CREATED, () -> {
      response.set(HttpRequests.execute(request, new DefaultHttpRequestConfig(false)));
      return response.get().getResponseCode();
    }, 60, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
    Assert.assertEquals(HttpURLConnection.HTTP_CREATED, response.get().getResponseCode());
    return response.get().getResponseBodyAsString();
  }

  /**
   * Requests zip and download support bundle.
   *
   * @param bundleId String
   * @return HTTPResponse
   * @throws IOException if failed to request bundle download
   */
  private HttpResponse requestExportBundle(String bundleId) throws IOException {
    DiscoveryServiceClient discoveryServiceClient = getInjector().getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable =
      new RandomEndpointStrategy(() -> discoveryServiceClient.discover(Constants.Service.SUPPORT_BUNDLE_SERVICE)).pick(
        5, TimeUnit.SECONDS);

    Assert.assertNotNull("No service for support bundle", discoverable);

    String path = String.format("%s/support/bundles/%s", Constants.Gateway.API_VERSION_3, bundleId);

    HttpRequest request = HttpRequest.post(URIScheme.createURI(discoverable, "%s", path).toURL()).build();
    return HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
  }

  private static String toQuery(Map<String, String> params) throws IOException {
    StringBuilder queryBuilder = new StringBuilder();
    String sep = "?";
    for (Map.Entry<String, String> entry : params.entrySet()) {
      queryBuilder.append(sep)
        .append(URLEncoder.encode(entry.getKey(), "UTF-8"))
        .append("=")
        .append(URLEncoder.encode(entry.getValue(), "UTF-8"));
      sep = "&";
    }
    return queryBuilder.toString();
  }

  private URL getSupportBundleUrl(String path) throws IOException {
    DiscoveryServiceClient discoveryServiceClient = getInjector().getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable =
      new RandomEndpointStrategy(() -> discoveryServiceClient.discover(Constants.Service.SUPPORT_BUNDLE_SERVICE)).pick(
        5, TimeUnit.SECONDS);
    Assert.assertNotNull("No service for support bundle", discoverable);
    return URIScheme.createURI(discoverable, "%s%s", Constants.Gateway.API_VERSION_3, path).toURL();
  }

  private static HttpResponse executeRequest(HttpRequest.Builder request) throws IOException {
    return HttpRequests.execute(request.build(), new DefaultHttpRequestConfig(false));
  }
}
