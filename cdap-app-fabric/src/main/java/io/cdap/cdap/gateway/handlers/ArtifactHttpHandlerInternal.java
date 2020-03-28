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

package io.cdap.cdap.gateway.handlers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.artifact.ArtifactVersionRange;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Internal {@link io.cdap.http.HttpHandler} for managing artifacts.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3)
public class ArtifactHttpHandlerInternal extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactHttpHandlerInternal.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private static final Type ARTIFACT_INFO_LIST_TYPE = new TypeToken<List<ArtifactInfo>>() { }.getType();
  private static final Type ARTIFACT_DETAIL_LIST_TYPE = new TypeToken<List<ArtifactDetail>>() { }.getType();

  private final ArtifactRepository artifactRepository;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  ArtifactHttpHandlerInternal(ArtifactRepository artifactRepository,
                              NamespaceQueryAdmin namespaceQueryAdmin) {
    this.artifactRepository = artifactRepository;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts")
  public void listArtifacts(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) {
    try {
      List<ArtifactInfo> artifactInfoList = new ArrayList<>();
      artifactInfoList.addAll(artifactRepository.getArtifactsInfo(new NamespaceId(namespaceId)));
      artifactInfoList.addAll(artifactRepository.getArtifactsInfo(NamespaceId.SYSTEM));
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(artifactInfoList, ARTIFACT_INFO_LIST_TYPE));
    } catch (Exception e) {
      LOG.warn("Exception reading artifact metadata for namespace {} from the store.", namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error reading artifact metadata from the store.");
    }
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions")
  public void getArtifactDetailForVersions(HttpRequest request, HttpResponder responder,
                                           @PathParam("namespace-id") String namespace,
                                           @PathParam("artifact-name") String artifactName,
                                           @QueryParam("lower") String lower,
                                           @QueryParam("upper") String upper,
                                           @QueryParam("limit") @DefaultValue("1") int limit,
                                           @QueryParam("order") String order,
                                           @QueryParam("scope") @DefaultValue("user") String scope) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!namespaceId.equals(NamespaceId.SYSTEM)) {
      if (!namespaceQueryAdmin.exists(namespaceId)) {
        throw new NamespaceNotFoundException(namespaceId);
      }
    }
    ArtifactRange range =
      new ArtifactRange(namespaceId.getNamespace(), artifactName,
                        new ArtifactVersionRange(new ArtifactVersion(lower), true,
                                                 new ArtifactVersion(upper),  true));
    ArtifactSortOrder sortOrder = ArtifactSortOrder.valueOf(order);
    List<ArtifactDetail> artifactDetailList = artifactRepository.getArtifactDetails(range, limit, sortOrder);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(artifactDetailList, ARTIFACT_DETAIL_LIST_TYPE));
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}")
  public void getArtifactDetail(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespace,
                                @PathParam("artifact-name") String artifactName,
                                @PathParam("artifact-version") String artifactVersion,
                                @QueryParam("scope") @DefaultValue("user") String scope) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!namespaceId.equals(NamespaceId.SYSTEM)) {
      if (!namespaceQueryAdmin.exists(namespaceId)) {
        throw new NamespaceNotFoundException(namespaceId);
      }
    }
    ArtifactId artifactId = new ArtifactId(namespace, artifactName, artifactVersion);
    ArtifactDetail artifactDetail = artifactRepository.getArtifact(Id.Artifact.fromEntityId(artifactId));
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(artifactDetail));
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/location")
  public void getArtifactLocationPath(HttpRequest request, HttpResponder responder,
                                      @PathParam("namespace-id") String namespaceId,
                                      @PathParam("artifact-name") String artifactName,
                                      @PathParam("artifact-version") String artifactVersion) {
    try {
      ArtifactDetail artifactDetail = artifactRepository.getArtifact(
        Id.Artifact.from(Id.Namespace.from(namespaceId), artifactName, artifactVersion));
      responder.sendString(HttpResponseStatus.OK, artifactDetail.getDescriptor().getLocation().toURI().getPath());
    } catch (Exception e) {
      LOG.warn("Exception reading artifact metadata for namespace {} from the store.", namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Error reading artifact metadata from the store.");
    }
  }
}
