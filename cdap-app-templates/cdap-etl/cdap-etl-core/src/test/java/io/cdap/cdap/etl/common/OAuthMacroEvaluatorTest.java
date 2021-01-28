/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.common;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.app.services.AbstractServiceDiscoverer;
import io.cdap.cdap.common.service.ServiceDiscoverable;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class OAuthMacroEvaluatorTest {

  private static final Gson GSON = new Gson();
  private static final String PROVIDER = "test";
  private static final String CREDENTIAL_ID = "testcredential";

  private static NettyHttpService httpService;
  private static ServiceDiscoverer serviceDiscoverer;

  @BeforeClass
  public static void init() throws Exception {
    httpService = NettyHttpService.builder("OAuthTest")
      .setHttpHandlers(new OAuthHandler(
        ImmutableMap.of(
          PROVIDER, ImmutableMap.of(CREDENTIAL_ID, new OAuthInfo("accessToken", "bearer"))
        )))
      .build();

    httpService.start();

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    String discoveryName = ServiceDiscoverable.getName(NamespaceId.SYSTEM.getNamespace(),
                                                       Constants.PIPELINEID, ProgramType.SERVICE,
                                                       Constants.STUDIO_SERVICE_NAME);
    discoveryService.register(new Discoverable(discoveryName, httpService.getBindAddress()));
    serviceDiscoverer = new AbstractServiceDiscoverer(NamespaceId.DEFAULT.app("testapp").spark("testspark")) {

      @Override
      protected DiscoveryServiceClient getDiscoveryServiceClient() {
        return discoveryService;
      }
    };
  }

  @AfterClass
  public static void finish() throws Exception {
    httpService.stop();
  }

  @Test
  public void testOAuthMacro() {
    MacroEvaluator macroEvaluator = new OAuthMacroEvaluator(serviceDiscoverer);
    String value = macroEvaluator.evaluate(OAuthMacroEvaluator.FUNCTION_NAME, PROVIDER, CREDENTIAL_ID);
    OAuthInfo oAuthInfo = GSON.fromJson(value, OAuthInfo.class);

    Assert.assertEquals("accessToken", oAuthInfo.accessToken);
    Assert.assertEquals("bearer", oAuthInfo.tokenType);
  }

  /**
   * A http handler to provide the OAuth endpoint.
   */
  public static final class OAuthHandler extends AbstractHttpHandler {

    private final Map<String, Map<String, OAuthInfo>> credentials;

    public OAuthHandler(Map<String, Map<String, OAuthInfo>> credentials) {
      this.credentials = credentials;
    }

    @Path("/v3/namespaces/system/apps/" + Constants.PIPELINEID + "/services/" +
      Constants.STUDIO_SERVICE_NAME + "/methods/v1/oauth/provider/{provider}/credential/{credentialId}")
    @GET
    public void getOAuth(HttpRequest request, HttpResponder responder,
                         @PathParam("provider") String provider,
                         @PathParam("credentialId") String credentialId) {
      Map<String, OAuthInfo> providerCredentials = credentials.get(provider);
      if (providerCredentials == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }

      OAuthInfo oAuthInfo = providerCredentials.get(credentialId);
      if (oAuthInfo == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }

      responder.sendString(HttpResponseStatus.OK, GSON.toJson(oAuthInfo));
    }
  }

  private static final class OAuthInfo {
    private final String accessToken;
    private final String tokenType;

    private OAuthInfo(String accessToken, String tokenType) {
      this.accessToken = accessToken;
      this.tokenType = tokenType;
    }
  }
}
