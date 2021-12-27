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
import io.cdap.cdap.common.options.Option;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tukaani.xz.UnsupportedOptionsException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Fetch preview requests from remote server.
 */
public class RemotePreviewRequestFetcherThroughSideCar implements PreviewRequestFetcher {
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(RemotePreviewRequestFetcherThroughSideCar.class);

  private final PreviewRequestPollerInfoProvider pollerInfoProvider;
  private final String sidecarBaseURL;


  @Inject
  RemotePreviewRequestFetcherThroughSideCar(CConfiguration cConf,
                                            PreviewRequestPollerInfoProvider pollerInfoProvider) {
    this.pollerInfoProvider = pollerInfoProvider;
    this.sidecarBaseURL = String
      .format("http://localhost:%d/%s/worker", cConf.getInt(Constants.ArtifactLocalizer.PORT),
              Constants.Gateway.INTERNAL_API_VERSION_3_TOKEN);
  }

  @Override
  public Optional<PreviewRequest> fetch() throws IOException, UnauthorizedException {
    String urlPath = String.format("/requests/pull");
    URL url;
    try {
      url = new URI(sidecarBaseURL + urlPath).toURL();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }

    LOG.debug("wyzhang: forwarding remote preview request fetcher sent to {}", url);
    HttpRequest httpRequest = HttpRequest.builder(HttpMethod.POST, url)
      .withBody(ByteBuffer.wrap(pollerInfoProvider.get()))
      .build();
    HttpResponse httpResponse = HttpRequests.execute(httpRequest);
    LOG.debug("wyzhang: forwarding remote preview request fetcher got back to {}", httpResponse);

    if (httpResponse.getResponseCode() != 200) {
      throw new IOException(String.format("Received status code:%s and body: %s", httpResponse.getResponseCode(),
                                          httpResponse.getResponseBodyAsString(Charsets.UTF_8)));
    }

    Optional<PreviewRequest> previewRequest =
      Optional.ofNullable(GSON.fromJson(httpResponse.getResponseBodyAsString(), PreviewRequest.class));

    if (previewRequest.isPresent()) {
      updatePrinciple(previewRequest.get().getPrincipal());
    }

    return previewRequest;
  }

  @Override
  public Optional<PreviewRequest> fetch(PreviewRequestPollerInfoProvider pollerInfoProvider)
    throws IOException, UnauthorizedException {
    throw new UnsupportedOptionsException();
  }

  private void updatePrinciple(Principal principal) throws UnauthorizedException {
    if (principal.getType() != Principal.PrincipalType.USER) {
      throw new UnauthorizedException("Principle type is not USER");
    }
    SecurityRequestContext.reset();
    SecurityRequestContext.setUserId(principal.getName());
    SecurityRequestContext.setUserCredential(new Credential(Credential.CredentialType.EXTERNAL,
                                                            principal.getCredential()));
  }
}
