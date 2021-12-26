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

package io.cdap.cdap.internal.app.preview;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Optional;

/**
 * Fetch preview requests from remote server.
 */
public class ForwardingRemotePreviewRequestFetcher implements PreviewRequestFetcher {
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(ForwardingRemotePreviewRequestFetcher.class);

  private final PreviewRequestPollerInfoProvider pollerInfoProvider;
  private final String sidecarBaseURL;
  private final AuthenticationContext authenticationContext;


  @Inject
  ForwardingRemotePreviewRequestFetcher(CConfiguration cConf,
                                        PreviewRequestPollerInfoProvider pollerInfoProvider,
                                        AuthenticationContext authenticationContext) {
    this.sidecarBaseURL = String
      .format("http://localhost:%d/%s/worker", cConf.getInt(Constants.ArtifactLocalizer.PORT),
              Constants.Gateway.INTERNAL_API_VERSION_3_TOKEN);
    this.pollerInfoProvider = pollerInfoProvider;
    this.authenticationContext = authenticationContext;
  }

  @Override
  public Optional<PreviewRequest> fetch() throws IOException, UnauthorizedException {
    String urlPath = String.format("requests/pull");
    URL url;
    try {
      url = new URI(sidecarBaseURL + urlPath).toURL();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }

    LOG.debug("wyzhang: forwarding remote preview request fetcher sent to {}", url);
    HttpRequest.Builder builder = HttpRequest.builder(HttpMethod.GET, url);
    HttpRequest httpRequest = builder.build();
    HttpResponse httpResponse = HttpRequests.execute(httpRequest);

    if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      LOG.debug("wyzhang: forwarding remote preview request fetcher resp {}", httpResponse.toString());
      throw new IOException(String.format("Received status code:%s and body: %s", httpResponse.getResponseCode(),
                                          httpResponse.getResponseBodyAsString(Charsets.UTF_8)));
    }

    PreviewRequest previewRequest = GSON.fromJson(httpResponse.getResponseBodyAsString(), PreviewRequest.class);
    return Optional.ofNullable(previewRequest);
  }
}
