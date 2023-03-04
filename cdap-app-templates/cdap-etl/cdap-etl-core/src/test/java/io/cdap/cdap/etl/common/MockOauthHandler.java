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
 *
 */

package io.cdap.cdap.etl.common;

import com.google.gson.Gson;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A http handler to provide the OAuth endpoint.
 */
public class MockOauthHandler extends AbstractHttpHandler {

  private static final Gson GSON = new Gson();
  private final Map<String, Map<String, OAuthInfo>> credentials;

  public MockOauthHandler(Map<String, Map<String, OAuthInfo>> credentials) {
    this.credentials = credentials;
  }

  @Path("/v3/namespaces/system/apps/" + Constants.PIPELINEID + "/services/"
      + Constants.STUDIO_SERVICE_NAME
      + "/methods/v1/oauth/provider/{provider}/credential/{credentialId}")
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

    static final class OAuthInfo {
        final String accessToken;
        final String tokenType;

        OAuthInfo(String accessToken, String tokenType) {
            this.accessToken = accessToken;
            this.tokenType = tokenType;
        }
    }
}
