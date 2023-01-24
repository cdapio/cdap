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

import com.google.common.base.Strings;
import com.google.common.net.HttpHeaders;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Tethering client.
 */
public final class TetheringClient {
  private static final String CONNECT_CONTROL_CHANNEL = "v3/tethering/channels/";
  private static final String CREATE_TETHER = "v3/tethering/connections/";
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .create();

  private final RemoteAuthenticator remoteAuthenticator;
  private final String instanceName;
  private final int connectionTimeout;
  private final int readTimeout;

  public TetheringClient(RemoteAuthenticator remoteAuthenticator, CConfiguration cConf) {
    this(remoteAuthenticator, cConf.get(Constants.INSTANCE_NAME),
         cConf.getInt(Constants.Tethering.CLIENT_CONNECTION_TIMEOUT_MS),
         cConf.getInt(Constants.Tethering.CLIENT_READ_TIMEOUT_MS));
  }

  public TetheringClient(RemoteAuthenticator remoteAuthenticator, String instanceName,
                         int connectionTimeout, int readTimeout) {
    this.remoteAuthenticator = remoteAuthenticator;
    this.instanceName = instanceName;
    this.connectionTimeout = connectionTimeout;
    this.readTimeout = readTimeout;
  }

  /**
   * Creates tethering connection with the server.
   * @param peerInfo peer information
   * @throws IOException if there was an exception while connecting to the server, or the server returned an error
   * @throws URISyntaxException if the peer endpoint is invalid
   */
  public void createTethering(PeerInfo peerInfo) throws URISyntaxException, IOException {
    TetheringConnectionRequest tetherRequest = new TetheringConnectionRequest(
      peerInfo.getMetadata().getNamespaceAllocations(), peerInfo.getRequestTime(),
      peerInfo.getMetadata().getDescription());
    String path = appendPath(peerInfo.getEndpoint(), CREATE_TETHER + instanceName);
    URI endpoint = new URI(peerInfo.getEndpoint()).resolve(path);
    HttpResponse response = sendHttpRequest(remoteAuthenticator, HttpMethod.PUT,
                                            endpoint, GSON.toJson(tetherRequest));
    if (response.getResponseCode() != 200) {
      throw new IOException(String.format("Failed to initiate tethering with peer %s, error code %s body %s",
                                          peerInfo.getName(),
                                          response.getResponseCode(),
                                          response.getResponseBodyAsString(StandardCharsets.UTF_8)));
    }
  }

  /**
   * Polls the tethering server for control commands.
   * @param peerInfo peer information
   * @param channelRequest control message sent to the server
   * @return control response
   * @throws IOException if there was an exception while connecting to the server, or the server returned an error
   * @throws URISyntaxException if the peer endpoint is invalid
   * @throws NotFoundException if peer is not found
   */
  public TetheringControlResponseV2 pollControlChannel(PeerInfo peerInfo,
                                                       TetheringControlChannelRequest channelRequest)
    throws IOException, URISyntaxException, NotFoundException {
    String content = GSON.toJson(channelRequest);
    String path = appendPath(Objects.requireNonNull(peerInfo.getEndpoint()),
                             CONNECT_CONTROL_CHANNEL + instanceName);
    URI peerUri  = new URI(peerInfo.getEndpoint()).resolve(path);
    HttpResponse resp = sendHttpRequest(remoteAuthenticator, HttpMethod.POST,
                                        peerUri, content);
    if (resp.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      String body = resp.getResponseBodyAsString(StandardCharsets.UTF_8);
      if (Strings.isNullOrEmpty(body)) {
        throw new NotFoundException(String.format("Peer %s was not found", peerInfo.getName()));
      }
      try {
        return GSON.fromJson(body, TetheringControlResponseV2.class);
      } catch (Exception e) {
        // 404 was not returned by the tethering server. Return the response body as the error message
        throw new NotFoundException(body);
      }
    } else if (resp.getResponseCode() != HttpURLConnection.HTTP_OK) {
      String message = String.format("Peer %s returned unexpected error code %d body %s",
                                     peerInfo.getName(), resp.getResponseCode(),
                                     resp.getResponseBodyAsString(StandardCharsets.UTF_8));
      throw new IOException(message);
    }
    try {
      return GSON.fromJson(resp.getResponseBodyAsString(StandardCharsets.UTF_8), TetheringControlResponseV2.class);
    } catch (Exception e) {
      String message = String.format("Failed to parse response from peer %s: %s",
                                     peerInfo.getName(),
                                     resp.getResponseBodyAsString(StandardCharsets.UTF_8));
      throw new InvalidObjectException(message);
    }
  }

  private HttpResponse sendHttpRequest(RemoteAuthenticator remoteAuthenticator, HttpMethod method,
                                       URI endpoint, @Nullable String content) throws IOException {
    HttpRequest.Builder builder;
    switch (method) {
      case GET:
        builder = HttpRequest.get(endpoint.toURL());
        break;
      case PUT:
        builder = HttpRequest.put(endpoint.toURL());
        break;
      case POST:
        builder = HttpRequest.post(endpoint.toURL());
        break;
      case DELETE:
        builder = HttpRequest.delete(endpoint.toURL());
        break;
      default:
        throw new RuntimeException("Unexpected HTTP method: " + method);
    }
    if (content != null && !content.isEmpty()) {
      builder.withBody(content);
    }

    // Add Authorization header.
    if (remoteAuthenticator != null) {
      Credential credential = remoteAuthenticator.getCredentials();
      if (credential != null) {
        builder.addHeader(HttpHeaders.AUTHORIZATION,
                          String.format("%s %s", credential.getType().getQualifiedName(), credential.getValue()));
      }
    }
    return HttpRequests.execute(builder.build(), new HttpRequestConfig(connectionTimeout, readTimeout));
  }

  private String appendPath(String base, String path) {
    return base.endsWith("/") ? base + path : base + '/' + path;
  }
}
