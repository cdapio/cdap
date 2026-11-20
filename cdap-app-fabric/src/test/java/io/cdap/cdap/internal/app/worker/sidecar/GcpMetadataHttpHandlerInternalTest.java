/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.encryption.NoOpAeadCipher;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.internal.remote.NoOpInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.NoOpRemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.utils.GcpMetadataTaskContextUtil;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.credential.ProvisionedCredential;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.GcpMetadataTaskContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.net.InetAddress;
import java.net.URL;
import java.time.Instant;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link GcpMetadataHttpHandlerInternal}.
 */
public class GcpMetadataHttpHandlerInternalTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final Gson GSON =
      ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  // The token endpoint returns this placeholder while no context is set.
  private static final String NO_CONTEXT_TOKEN = "invalidToken";
  // The fake provision endpoint returns this prefix followed by the namespace of the context.
  private static final String TOKEN_PREFIX = "token-";
  private static NettyHttpService httpService;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    NamespaceAdmin namespaceAdmin = new InMemoryNamespaceAdmin();
    namespaceAdmin.create(NamespaceMeta.SYSTEM);
    namespaceAdmin.create(NamespaceMeta.DEFAULT);

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    RemoteClientFactory remoteClientFactory =
        new RemoteClientFactory(discoveryService, new NoOpInternalAuthenticator());
    httpService = new CommonNettyHttpServiceBuilder(cConf, "test",
        new NoOpMetricsCollectionService(), true, auditLogContexts -> {}, new NoOpAeadCipher())
        // GcpMetadataTaskContextUtil connects to the loopback address.
        .setHost(InetAddress.getLoopbackAddress().getHostName())
        .setHttpHandlers(
            new GcpMetadataHttpHandlerInternal(cConf, remoteClientFactory,
                new NoOpRemoteAuthenticator()),
            new FakeProvisionHandler()
        )
        .setChannelPipelineModifier(new ChannelPipelineModifier() {
          @Override
          public void modify(ChannelPipeline pipeline) {
            pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
          }
        })
        .build();
    httpService.start();
    discoveryService.register(
        URIScheme.createDiscoverable(Constants.Service.APP_FABRIC_HTTP, httpService));
  }

  @AfterClass
  public static void finish() throws Exception {
    httpService.stop();
  }

  @After
  public void clearContext() throws Exception {
    // The handler is shared by all tests, so no test may leave a context behind.
    SecurityRequestContext.reset();
    Assert.assertEquals(200, clearContextWithHeader().getResponseCode());
  }

  @Test
  public void testStatus() throws Exception {
    String endpoint = String.format("http://%s:%s/",
        httpService.getBindAddress().getHostName(), httpService.getBindAddress().getPort());
    URL url = new URL(endpoint);
    HttpRequest httpRequest = HttpRequest.get(url).build();
    HttpResponse httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(403, httpResponse.getResponseCode());

    httpRequest = HttpRequest.get(url).addHeader(
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE).build();
    httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(200, httpResponse.getResponseCode());
  }

  @Test
  public void testSetAndClearContext() throws Exception {
    String endpoint = String.format("http://%s:%s",
        httpService.getBindAddress().getHostName(), httpService.getBindAddress().getPort());
    NamespaceId namespaceId = new NamespaceId("test");
    GcpMetadataTaskContext gcpMetadataTaskContext =
        new GcpMetadataTaskContext(namespaceId.getNamespace(),
            "alice", "0.0.0.0", null);

    // set context
    URL url = new URL(String.format("%s/set-context", endpoint));
    HttpRequest httpRequest = HttpRequest.put(url).withBody(GSON.toJson(gcpMetadataTaskContext,
        GcpMetadataTaskContext.class)).addHeader(
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE).build();
    HttpResponse httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(200, httpResponse.getResponseCode());
    String expectedResponse = String.format("Context was set successfully with namespace '%s'.",
        namespaceId.getNamespace());
    Assert.assertEquals(expectedResponse, httpResponse.getResponseBodyAsString());

    //clear context
    url = new URL(String.format("%s/clear-context", endpoint));
    httpRequest = HttpRequest.delete(url).addHeader(
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE).build();
    httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(200, httpResponse.getResponseCode());
  }

  @Test
  public void testSetContextWithoutMetadataFlavorHeaderIsForbidden() throws Exception {
    HttpResponse response = HttpRequests.execute(
        HttpRequest.put(setContextUrl()).withBody(contextJson("test")).build());
    Assert.assertEquals(403, response.getResponseCode());
    Assert.assertEquals(NO_CONTEXT_TOKEN, fetchAccessToken());
  }

  @Test
  public void testSetContextWithWrongMetadataFlavorValueIsForbidden() throws Exception {
    HttpResponse response = HttpRequests.execute(
        HttpRequest.put(setContextUrl()).withBody(contextJson("test"))
            .addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY, "NotGoogle")
            .build());
    Assert.assertEquals(403, response.getResponseCode());
    Assert.assertEquals(NO_CONTEXT_TOKEN, fetchAccessToken());
  }

  @Test
  public void testSetContextAcceptsLowercaseMetadataFlavorValue() throws Exception {
    HttpResponse response = HttpRequests.execute(
        HttpRequest.put(setContextUrl()).withBody(contextJson("test"))
            .addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY, "google")
            .build());
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(TOKEN_PREFIX + "test", fetchAccessToken());
  }

  @Test
  public void testForbiddenSetContextKeepsExistingContext() throws Exception {
    setContextWithHeader("test");

    HttpResponse response = HttpRequests.execute(
        HttpRequest.put(setContextUrl()).withBody(contextJson("other")).build());
    Assert.assertEquals(403, response.getResponseCode());
    Assert.assertEquals(TOKEN_PREFIX + "test", fetchAccessToken());
  }

  @Test
  public void testClearContextWithoutMetadataFlavorHeaderIsForbidden() throws Exception {
    setContextWithHeader("test");

    HttpResponse response =
        HttpRequests.execute(HttpRequest.delete(clearContextUrl()).build());
    Assert.assertEquals(403, response.getResponseCode());
    Assert.assertEquals(TOKEN_PREFIX + "test", fetchAccessToken());
  }

  @Test
  public void testClearContextWithWrongMetadataFlavorValueIsForbidden() throws Exception {
    setContextWithHeader("test");

    HttpResponse response = HttpRequests.execute(
        HttpRequest.delete(clearContextUrl())
            .addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY, "NotGoogle")
            .build());
    Assert.assertEquals(403, response.getResponseCode());
    Assert.assertEquals(TOKEN_PREFIX + "test", fetchAccessToken());
  }

  @Test
  public void testClearContextWithMetadataFlavorHeaderClearsContext() throws Exception {
    setContextWithHeader("test");

    HttpResponse response = clearContextWithHeader();
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(NO_CONTEXT_TOKEN, fetchAccessToken());
  }

  @Test
  public void testTaskContextUtilSetsContext() throws Exception {
    CConfiguration cConf = createTaskContextUtilConf();
    // The util copies the caller's identity into the context it sends to the sidecar.
    SecurityRequestContext.setUserId("alice");
    SecurityRequestContext.setUserCredential(
        new Credential("credential", Credential.CredentialType.EXTERNAL));

    GcpMetadataTaskContextUtil.setGcpMetadataTaskContext(new NamespaceId("test"), cConf);
    Assert.assertEquals(TOKEN_PREFIX + "test", fetchAccessToken());
  }

  @Test
  public void testTaskContextUtilClearsContext() throws Exception {
    CConfiguration cConf = createTaskContextUtilConf();
    setContextWithHeader("test");

    GcpMetadataTaskContextUtil.clearGcpMetadataTaskContext(cConf);
    Assert.assertEquals(NO_CONTEXT_TOKEN, fetchAccessToken());
  }

  private static CConfiguration createTaskContextUtilConf() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), true);
    cConf.setInt(Constants.ArtifactLocalizer.PORT, httpService.getBindAddress().getPort());
    return cConf;
  }

  private static String baseUrl() {
    return String.format("http://%s:%s",
        httpService.getBindAddress().getHostName(), httpService.getBindAddress().getPort());
  }

  private static URL setContextUrl() throws Exception {
    return new URL(baseUrl() + "/set-context");
  }

  private static URL clearContextUrl() throws Exception {
    return new URL(baseUrl() + "/clear-context");
  }

  private static HttpRequest.Builder withMetadataFlavor(HttpRequest.Builder builder) {
    return builder.addHeader(GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_KEY,
        GcpMetadataHttpHandlerInternal.METADATA_FLAVOR_HEADER_VALUE);
  }

  private static String contextJson(String namespace) {
    return GSON.toJson(new GcpMetadataTaskContext(namespace, "alice", "0.0.0.0",
        new Credential("credential", Credential.CredentialType.EXTERNAL)),
        GcpMetadataTaskContext.class);
  }

  private static void setContextWithHeader(String namespace) throws Exception {
    HttpResponse response = HttpRequests.execute(
        withMetadataFlavor(HttpRequest.put(setContextUrl())).withBody(contextJson(namespace))
            .build());
    Assert.assertEquals(200, response.getResponseCode());
  }

  private static HttpResponse clearContextWithHeader() throws Exception {
    return HttpRequests.execute(
        withMetadataFlavor(HttpRequest.delete(clearContextUrl())).build());
  }

  /**
   * Returns the access token the sidecar serves: {@link #NO_CONTEXT_TOKEN} while no context is
   * set, otherwise {@link #TOKEN_PREFIX} followed by the namespace of the context.
   */
  private static String fetchAccessToken() throws Exception {
    URL url = new URL(baseUrl()
        + "/computeMetadata/v1/instance/service-accounts/default/token");
    HttpResponse response =
        HttpRequests.execute(withMetadataFlavor(HttpRequest.get(url)).build());
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), GcpTokenResponse.class).accessToken;
  }

  /**
   * Stands in for the App Fabric endpoint that provisions a credential for a namespace.
   */
  @Path(Constants.Gateway.INTERNAL_API_VERSION_3)
  public static final class FakeProvisionHandler extends AbstractHttpHandler {

    /**
     * Returns a token that names the requested namespace.
     */
    @GET
    @Path("/namespaces/{namespace}/credentials/workloadIdentity/provision")
    public void provision(io.netty.handler.codec.http.HttpRequest request,
        HttpResponder responder, @PathParam("namespace") String namespace) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(
          new ProvisionedCredential(TOKEN_PREFIX + namespace, Instant.now().plusSeconds(3600))));
    }
  }
}
