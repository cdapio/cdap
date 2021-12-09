/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.service.worker.RemoteExecutionException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * ArtifactLocalizerClient is used by tasks that extend {@link io.cdap.cdap.api.service.worker.RunnableTask} to fetch,
 * cache and unpack artifacts locally.
 */
public class ArtifactLocalizerClient {

  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizerClient.class);
  private final String sidecarBaseURL;
  private final AuthenticationContext authenticationContext;

  @Inject
  ArtifactLocalizerClient(CConfiguration cConf, AuthenticationContext authenticationContext) {
    this.sidecarBaseURL = String
      .format("http://localhost:%d/%s/worker", cConf.getInt(Constants.ArtifactLocalizer.PORT),
              Constants.Gateway.INTERNAL_API_VERSION_3_TOKEN);
    this.authenticationContext = authenticationContext;
  }

  /**
   * Gets the location on the local filesystem for the directory that contains the unpacked artifact. This method
   * handles fetching, caching and unpacking the artifact.
   *
   * @param artifactId The ArtifactId of the artifact to fetch and unpack
   * @return The Local Location of the directory that contains the unpacked artifact files
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException               if there was an exception while fetching, caching or unpacking the artifact
   */
  public File getUnpackedArtifactLocation(ArtifactId artifactId) throws IOException, ArtifactNotFoundException {
    String urlPath = String.format("/artifact/namespaces/%s/artifacts/%s/versions/%s",
                                   artifactId.getNamespace(), artifactId.getArtifact(), artifactId.getVersion());
    URL url;
    try {
      url = new URI(sidecarBaseURL + urlPath).toURL();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }

    LOG.debug("wyzhang: ArtifactLocalizerClient getUnpackedArtifactLocation Sending request to {}", url);
    HttpRequest.Builder builder = HttpRequest.builder(HttpMethod.GET, url);
    {
      Principal principal = authenticationContext.getPrincipal();
      String userID = null;
      Credential internalCredentials = null;
      if (principal != null) {
        userID = principal.getName();
        internalCredentials = principal.getFullCredential();
      }
      if (internalCredentials != null) {
        LOG.debug("wyzhang: ArtifactLocalizerClient getUnpackedArtifactLocation add credential header {}",
                  String.format("%s %s", internalCredentials.getType().getQualifiedName(),
                                internalCredentials.getValue()));
        builder.addHeader(Constants.Security.Headers.RUNTIME_TOKEN,
                          String.format("%s %s", internalCredentials.getType().getQualifiedName(),
                                        internalCredentials.getValue()));
      }
      if (userID != null) {
        LOG.debug("wyzhang: ArtifactLocalizerClient getUnpackedArtifactLocation add user header {}",
                  userID);
        builder.addHeader(Constants.Security.Headers.USER_ID, userID);
      }
    }
    HttpRequest httpRequest = builder.build();
    HttpResponse httpResponse = HttpRequests.execute(httpRequest);

    if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      LOG.info("wyzhang: ArtifactLocalizerClient getUnpackedArtifactLocation failed {}", httpResponse.toString());
      if (httpResponse.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
        throw new ArtifactNotFoundException(artifactId);
      }
      BasicThrowable basicThrowable = GSON
        .fromJson(httpResponse.getResponseBodyAsString(), BasicThrowable.class);
      throw new IOException(RemoteExecutionException.fromBasicThrowable(basicThrowable));
    }

    String path = httpResponse.getResponseBodyAsString();
    LOG.debug("ArtifactLocalizer request returned path {}", path);

    return new File(path);
  }
}
