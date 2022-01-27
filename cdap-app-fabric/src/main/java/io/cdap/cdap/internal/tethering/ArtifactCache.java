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

package io.cdap.cdap.internal.tethering;

import com.google.common.net.HttpHeaders;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteAuthenticator;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerBase;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequestConfig;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;

/**
 * ArtifactCache is responsible for fetching and caching artifacts from tethered peers. The HTTP
 * endpoints are defined in {@link ArtifactCacheHttpHandlerInternal}.
 *
 * Artifacts will be cached using the following file structure:
 * /DATA_DIRECTORY/peers/<peer-name>/artifacts/<namespace>/<artifact-name>/<artifact-version>/<last-modified-timestamp>.jar
 *
 */
public class ArtifactCache extends ArtifactLocalizerBase {
  private final TetheringStore store;
  private final HttpRequestConfig httpRequestConfig;
  private RemoteAuthenticator authenticator;

  @Inject
  public ArtifactCache(CConfiguration cConf, TetheringStore store)
    throws InstantiationException, IllegalAccessException {
    super(cConf.get(Constants.ArtifactCache.LOCAL_DATA_DIR),
          RetryStrategies.fromConfiguration(cConf, Constants.Service.ARTIFACT_CACHE + "."));
    this.store = store;
    httpRequestConfig = new DefaultHttpRequestConfig(true);
    Class<? extends RemoteAuthenticator> authClass = cConf.getClass(Constants.Tethering.CLIENT_AUTHENTICATOR_CLASS,
                                                                    null,
                                                                    RemoteAuthenticator.class);
    if (authClass != null) {
      authenticator = authClass.newInstance();
      RemoteAuthenticator.setDefaultAuthenticator(authenticator);
    }
  }

  @Override
  public HttpURLConnection openConnection(ArtifactId artifactId, @Nullable String peer)
    throws PeerNotFoundException, IOException, URISyntaxException {
    String namespaceId = artifactId.getNamespace();
    ArtifactScope scope = ArtifactScope.USER;
    // Cant use 'system' as the namespace in the request because that generates an error, the namespace doesnt matter
    // as long as it exists. Using default because it will always be there
    if (ArtifactScope.SYSTEM.toString().equalsIgnoreCase(namespaceId)) {
      namespaceId = NamespaceId.DEFAULT.getEntityName();
      scope = ArtifactScope.SYSTEM;
    }

    String url = String.format("/v3Internal/namespaces/%s/artifacts/%s/versions/%s/download?scope=%s",
                               namespaceId,
                               artifactId.getArtifact(),
                               artifactId.getVersion(),
                               scope);

    URI uri = new URI(store.getPeer(peer).getEndpoint()).resolve(url);
    HttpURLConnection urlConn = (HttpURLConnection) uri.toURL().openConnection();
    urlConn.setRequestMethod(HttpMethod.GET.name());
    if (urlConn instanceof HttpsURLConnection && !httpRequestConfig.isVerifySSLCert()) {
      new HttpsEnabler().setTrustAll(true).enable((HttpsURLConnection) urlConn);
    }
    urlConn.setConnectTimeout(httpRequestConfig.getConnectTimeout());
    urlConn.setReadTimeout(httpRequestConfig.getReadTimeout());
    if (authenticator != null) {
      setAuthHeader(urlConn::setRequestProperty, HttpHeaders.AUTHORIZATION, authenticator.getType(),
                    authenticator.getCredentials());
    }
    return urlConn;
  }

  private void setAuthHeader(BiConsumer<String, String> headerSetter, String header, String credentialType,
                             String credentialValue) {
    headerSetter.accept(header, String.format("%s %s", credentialType, credentialValue));
  }
}
