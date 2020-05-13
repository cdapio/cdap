/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.cdap.common.internal.remote.RemoteAuthenticator;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * A {@link RemoteAuthenticator} that authenticate remote calls using Google Cloud token acquired from GCE metadata.
 *
 * TODO (CDAP-16583): Move it to extension loader
 */
@SuppressWarnings("unused")
public class GceRemoteAuthenticator extends RemoteAuthenticator {

  private static final Gson GSON = new Gson();

  private volatile AccessToken accessToken;

  @Override
  public String getType() throws IOException {
    return getAccessToken().getType();
  }

  @Override
  public String getCredentials() throws IOException {
    return getAccessToken().getToken();
  }

  /**
   * Returns an unexpired access token for authentication.
   */
  private AccessToken getAccessToken() throws IOException {
    AccessToken accessToken = this.accessToken;
    if (accessToken != null && !accessToken.isExpired()) {
      return accessToken;
    }

    URL url = new URL("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token");
    HttpResponse response = HttpRequests.execute(HttpRequest.get(url).addHeader("Metadata-Flavor", "Google").build());
    if (response.getResponseCode() != 200) {
      throw new IOException("Failed to default service account token");
    }
    JsonObject jsonObj = GSON.fromJson(response.getResponseBodyAsString(), JsonObject.class);
    this.accessToken = accessToken = new AccessToken(
      jsonObj.getAsJsonPrimitive("token_type").getAsString(),
      jsonObj.getAsJsonPrimitive("access_token").getAsString(),
      jsonObj.getAsJsonPrimitive("expires_in").getAsLong()
    );
    return accessToken;
  }

  /**
   * Private class to hold the access token that contains the type and the token.
   */
  private static final class AccessToken {

    private final String type;
    private final String token;
    private final long expiryMillis;
    private final long creationTimeMillis;

    AccessToken(String type, String token, long expirySeconds) {
      this.type = type;
      this.token = token;
      // It's safe to reduce 5 seconds since the metadata server will not return token that expires in < 60 seconds.
      this.expiryMillis = TimeUnit.SECONDS.toMillis(expirySeconds - 5);
      this.creationTimeMillis = System.currentTimeMillis();
    }

    String getType() {
      return type;
    }

    String getToken() {
      return token;
    }

    boolean isExpired() {
      return System.currentTimeMillis() - creationTimeMillis >= expiryMillis;
    }
  }
}
