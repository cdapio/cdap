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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.http.LocationBodyProducer;
import io.cdap.cdap.common.internal.remote.NoOpInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDescriptor;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDetail;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.twill.filesystem.Location;

@Path(Constants.Gateway.INTERNAL_API_VERSION_3)
public class ArtifactCacheHttpHandlerInternal extends AbstractHttpHandler {

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(BasicThrowable.class, new BasicThrowableCodec())
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .create();
  private final ArtifactCache cache;
  private final TetheringStore tetheringStore;
  private final RemoteAuthenticator remoteAuthenticator;

  public ArtifactCacheHttpHandlerInternal(ArtifactCache cache, TetheringStore tetheringStore,
      RemoteAuthenticator remoteAuthenticator) {
    this.cache = cache;
    this.tetheringStore = tetheringStore;
    this.remoteAuthenticator = remoteAuthenticator;
  }

  @GET
  @Path("peers/{peer}/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/download")
  public void fetchArtifact(HttpRequest request, HttpResponder responder,
      @PathParam("peer") String peer,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("artifact-name") String artifactName,
      @PathParam("artifact-version") String artifactVersion,
      @QueryParam("scope") @DefaultValue("user") String scope) throws Exception {
    ArtifactScope artifactScope = validateScope(scope);
    if (artifactScope == ArtifactScope.SYSTEM) {
      namespaceId = NamespaceId.SYSTEM.getNamespace();
    }
    ArtifactId artifactId = new ArtifactId(namespaceId, artifactName, artifactVersion);
    try {
      RemoteClient remoteClient = getRemoteClient(peer);
      File artifactPath = cache.getArtifact(artifactId, peer, remoteClient);
      Location artifactLocation = Locations.toLocation(artifactPath);
      responder.sendContent(HttpResponseStatus.OK, new LocationBodyProducer(artifactLocation),
          new DefaultHttpHeaders().add(HttpHeaders.CONTENT_TYPE,
              MediaType.APPLICATION_OCTET_STREAM));
    } catch (Exception ex) {
      if (ex instanceof HttpErrorStatusProvider) {
        HttpResponseStatus status = HttpResponseStatus.valueOf(
            ((HttpErrorStatusProvider) ex).getStatusCode());
        responder.sendString(status, exceptionToJson(ex));
      } else {
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(ex));
      }
    }
  }

  @GET
  @Path("peers/{peer}/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}")
  public void getArtifactDetail(HttpRequest request, HttpResponder responder,
      @PathParam("peer") String peer,
      @PathParam("namespace-id") String namespace,
      @PathParam("artifact-name") String artifactName,
      @PathParam("artifact-version") String artifactVersion,
      @QueryParam("scope") @DefaultValue("user") String scope) throws Exception {
    RemoteClient remoteClient = getRemoteClient(peer);
    String url = String.format("namespaces/%s/artifacts/%s/versions/%s",
        namespace, artifactName, artifactVersion);
    io.cdap.common.http.HttpRequest req = remoteClient.requestBuilder(HttpMethod.GET, url).build();
    HttpResponse response = remoteClient.execute(req);
    String responseBody = response.getResponseBodyAsString(StandardCharsets.UTF_8);
    int responseCode = response.getResponseCode();
    // this is a hack used when we know the Location will not actually be used,
    // but we need to avoid issues due to the scheme being incompatible.
    // For example, the incoming location is of the form gs://bucket/path/to/artifact whereas the client
    // is configured with a LocationFactory that expects an 'hdfs' scheme.
    // TODO: (CDAP-19150) remove this once Location is removed from ArtifactDescriptor
    if (responseCode / 100 == 2) {
      ArtifactDetail originalDetail = GSON.fromJson(responseBody, ArtifactDetail.class);
      ArtifactDescriptor originalDescriptor = originalDetail.getDescriptor();
      URI uri = originalDescriptor.getLocationURI();
      if (uri.getScheme() != null) {
        uri = URI.create(uri.getPath());
      }
      ArtifactDescriptor modifiedDescriptor = new ArtifactDescriptor(
          originalDescriptor.getNamespace(),
          originalDescriptor.getArtifactId(),
          uri);
      ArtifactDetail modifiedDetail = new ArtifactDetail(modifiedDescriptor,
          originalDetail.getMeta());
      responseBody = GSON.toJson(modifiedDetail);
    }
    responder.sendString(HttpResponseStatus.valueOf(responseCode), responseBody);
  }

  /**
   * Return a remote client that can connect to appfabric on a tethered peer.
   */
  private RemoteClient getRemoteClient(String peer)
      throws PeerNotFoundException, IOException, URISyntaxException {
    String endpoint = tetheringStore.getPeer(peer).getEndpoint();
    RemoteClientFactory factory = new RemoteClientFactory(new NoOpDiscoveryServiceClient(endpoint),
        new NoOpInternalAuthenticator(),
        remoteAuthenticator);
    HttpRequestConfig config = new DefaultHttpRequestConfig(true);
    URI peerUri = new URI(endpoint);
    // Add peer endpoint's path to the beginning of the remote client's base path
    // ex : if peerUri is https://my.host.com/api, then the base path will be /api/v3Internal
    // So some/path would be resolved by the remote client to https://my.host.com/api/v3Internal/some/path
    String basePath =
        peerUri.getPath() != null ? peerUri.getPath() + Constants.Gateway.INTERNAL_API_VERSION_3 :
            Constants.Gateway.INTERNAL_API_VERSION_3;
    return factory.createRemoteClient("", config,
        basePath);
  }

  /**
   * Return json representation of an exception. Used to propagate exception across network for
   * better surfacing errors and debuggability.
   */
  private String exceptionToJson(Exception ex) {
    BasicThrowable basicThrowable = new BasicThrowable(ex);
    return GSON.toJson(basicThrowable);
  }

  private ArtifactScope validateScope(String scope) throws BadRequestException {
    try {
      return ArtifactScope.valueOf(scope.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("Invalid scope " + scope);
    }
  }
}
