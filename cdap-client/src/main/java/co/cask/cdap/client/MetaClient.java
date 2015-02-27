/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.exception.ResetFailureException;
import co.cask.cdap.common.exception.ResetNotEnabledException;
import co.cask.cdap.common.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.proto.Version;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP.
 */
public class MetaClient {

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public MetaClient(ClientConfig config) {
    this.config = config;
    this.restClient = RESTClient.create(config);
  }

  public void ping() throws IOException, UnAuthorizedAccessTokenException {
    restClient.execute(HttpMethod.GET, config.resolveURL("ping"), config.getAccessToken());
  }

  public Version getVersion() throws IOException, UnAuthorizedAccessTokenException {
    HttpResponse response = restClient.execute(HttpMethod.GET, config.resolveURL("version"), config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, Version.class).getResponseObject();
  }

  public void resetUnrecoverably() throws ResetFailureException, UnAuthorizedAccessTokenException, IOException, UnAuthorizedAccessTokenException, ResetNotEnabledException {

    URL url = config.resolveURL(String.format("unrecoverable/reset"));
    HttpRequest request = HttpRequest.post(url).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken(),
                                               HttpURLConnection.HTTP_UNAUTHORIZED, HttpURLConnection.HTTP_BAD_REQUEST,
                                               HttpURLConnection.HTTP_FORBIDDEN);
    if (response.getResponseCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
      throw new UnAuthorizedAccessTokenException();
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new ResetFailureException(response.getResponseMessage());
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_FORBIDDEN) {
      throw new ResetNotEnabledException();
    }
  }
}
