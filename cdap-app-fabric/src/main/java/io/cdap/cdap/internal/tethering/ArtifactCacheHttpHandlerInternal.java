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
import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.LocationBodyProducer;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.filesystem.Location;

import java.io.File;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

/**
 * Internal {@link HttpHandler} for returning cached artifacts belonging to tethered peers.
 */
@Path(Constants.Gateway.INTERNAL_API_VERSION_3)
public class ArtifactCacheHttpHandlerInternal extends AbstractHttpHandler {
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(BasicThrowable.class,
                                                                         new BasicThrowableCodec()).create();
  private final ArtifactCache cache;
  public ArtifactCacheHttpHandlerInternal(ArtifactCache cache) {
    this.cache = cache;
  }

  @GET
  @Path("peers/{peer}/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}")
  public void fetchArtifact(HttpRequest request, HttpResponder responder,
                            @PathParam("peer") String peer,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("artifact-name") String artifactName,
                            @PathParam("artifact-version") String artifactVersion) throws Exception {
    ArtifactId artifactId = new ArtifactId(namespaceId, artifactName, artifactVersion);
    try {
      File artifactPath = cache.getArtifact(artifactId, peer);
      Location artifactLocation = Locations.toLocation(artifactPath);
        responder.sendContent(HttpResponseStatus.OK, new LocationBodyProducer(artifactLocation),
                            new DefaultHttpHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM));
    } catch (Exception ex) {
      if (ex instanceof HttpErrorStatusProvider) {
        HttpResponseStatus status = HttpResponseStatus.valueOf(((HttpErrorStatusProvider) ex).getStatusCode());
        responder.sendString(status, exceptionToJson(ex));
      } else {
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(ex));
      }
    }
  }

  /**
   * Return json representation of an exception.
   * Used to propagate exception across network for better surfacing errors and debuggability.
   */
  private String exceptionToJson(Exception ex) {
    BasicThrowable basicThrowable = new BasicThrowable(ex);
    return GSON.toJson(basicThrowable);
  }
}
