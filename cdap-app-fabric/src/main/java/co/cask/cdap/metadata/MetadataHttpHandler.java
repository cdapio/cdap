/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.security.AuditDetail;
import co.cask.cdap.common.security.AuditPolicy;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.codec.NamespacedEntityIdCodec;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * HttpHandler for Metadata
 */
@Path(Constants.Gateway.API_VERSION_3)
public class MetadataHttpHandler extends AbstractHttpHandler {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
    .create();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type LIST_STRING_TYPE = new TypeToken<List<String>>() { }.getType();
  private static final Type SET_METADATA_RECORD_TYPE = new TypeToken<Set<MetadataRecord>>() { }.getType();

  private static final Function<String, EntityTypeSimpleName> STRING_TO_TARGET_TYPE =
    new Function<String, EntityTypeSimpleName>() {
      @Override
      public EntityTypeSimpleName apply(String input) {
        return EntityTypeSimpleName.valueOf(input.toUpperCase());
      }
    };

  private final MetadataAdmin metadataAdmin;

  @Inject
  MetadataHttpHandler(MetadataAdmin metadataAdmin) {
    this.metadataAdmin = metadataAdmin;
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata")
  public void getAppMetadata(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-id") String appId,
                             @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    ApplicationId app = new ApplicationId(namespaceId, appId);
    responder.sendJson(HttpResponseStatus.OK, getMetadata(app, scope), SET_METADATA_RECORD_TYPE, GSON);
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata")
  public void getProgramMetadata(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("app-id") String appId,
                                 @PathParam("program-type") String programType,
                                 @PathParam("program-id") String programId,
                                 @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    ProgramId program = new ProgramId(namespaceId, appId, ProgramType.valueOfCategoryName(programType), programId);
    responder.sendJson(HttpResponseStatus.OK, getMetadata(program, scope), SET_METADATA_RECORD_TYPE, GSON);
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/metadata")
  public void getArtifactMetadata(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("artifact-name") String artifactName,
                                  @PathParam("artifact-version") String artifactVersionStr,
                                  @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    ArtifactId artifactId = new ArtifactId(namespaceId, artifactName, artifactVersionStr);
    responder.sendJson(HttpResponseStatus.OK, getMetadata(artifactId, scope), SET_METADATA_RECORD_TYPE, GSON);
  }

  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata")
  public void getDatasetMetadata(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("dataset-id") String datasetId,
                                 @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    DatasetId datasetInstance = new DatasetId(namespaceId, datasetId);
    responder.sendJson(HttpResponseStatus.OK, getMetadata(datasetInstance, scope), SET_METADATA_RECORD_TYPE, GSON);
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata")
  public void getStreamMetadata(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("stream-id") String streamId,
                                @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    StreamId stream = new StreamId(namespaceId, streamId);
    responder.sendJson(HttpResponseStatus.OK, getMetadata(stream, scope), SET_METADATA_RECORD_TYPE, GSON);
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/views/{view-id}/metadata")
  public void getViewMetadata(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId,
                              @PathParam("stream-id") String streamId,
                              @PathParam("view-id") String viewId,
                              @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    StreamViewId view = new StreamViewId(namespaceId, streamId, viewId);
    responder.sendJson(HttpResponseStatus.OK, getMetadata(view, scope), SET_METADATA_RECORD_TYPE, GSON);
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata/properties")
  public void getAppProperties(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("app-id") String appId,
                               @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    ApplicationId app = new ApplicationId(namespaceId, appId);
    responder.sendJson(HttpResponseStatus.OK, getProperties(app, scope));
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/metadata/properties")
  public void getArtifactProperties(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("artifact-name") String artifactName,
                                    @PathParam("artifact-version") String artifactVersionStr,
                                    @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    ArtifactId artifactId = new ArtifactId(namespaceId, artifactName, artifactVersionStr);
    responder.sendJson(HttpResponseStatus.OK, getProperties(artifactId, scope));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata/properties")
  public void getProgramProperties(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("app-id") String appId,
                                   @PathParam("program-type") String programType,
                                   @PathParam("program-id") String programId,
                                   @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    ProgramId program = new ProgramId(namespaceId, appId, ProgramType.valueOfCategoryName(programType), programId);
    responder.sendJson(HttpResponseStatus.OK, getProperties(program, scope));
  }

  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata/properties")
  public void getDatasetProperties(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("dataset-id") String datasetId,
                                   @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    DatasetId datasetInstance = new DatasetId(namespaceId, datasetId);
    responder.sendJson(HttpResponseStatus.OK, getProperties(datasetInstance, scope));
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata/properties")
  public void getStreamProperties(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("stream-id") String streamId,
                                  @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    StreamId stream = new StreamId(namespaceId, streamId);
    responder.sendJson(HttpResponseStatus.OK, getProperties(stream, scope));
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/views/{view-id}/metadata/properties")
  public void getViewProperties(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("stream-id") String streamId,
                                @PathParam("view-id") String viewId,
                                @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    StreamViewId view = new StreamViewId(namespaceId, streamId, viewId);
    responder.sendJson(HttpResponseStatus.OK, getProperties(view, scope));
  }

  @POST
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata/properties")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addAppProperties(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("app-id") String appId) throws BadRequestException, NotFoundException {
    ApplicationId app = new ApplicationId(namespaceId, appId);
    metadataAdmin.addProperties(app, readMetadata(request));
    responder.sendString(HttpResponseStatus.OK, "Metadata added successfully to " + app);
  }

  @POST
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/metadata/properties")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addArtifactProperties(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("artifact-name") String artifactName,
                                    @PathParam("artifact-version") String artifactVersionStr)
    throws BadRequestException, NotFoundException {
    ArtifactId artifactId = new ArtifactId(namespaceId, artifactName, artifactVersionStr);
    metadataAdmin.addProperties(artifactId, readMetadata(request));
    responder.sendString(HttpResponseStatus.OK, "Metadata added successfully to " + artifactId);
  }

  @POST
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata/properties")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addProgramProperties(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("app-id") String appId,
                                   @PathParam("program-type") String programType,
                                   @PathParam("program-id") String programId)
    throws BadRequestException, NotFoundException {
    ProgramId program = new ProgramId(namespaceId, appId, ProgramType.valueOfCategoryName(programType), programId);
    metadataAdmin.addProperties(program, readMetadata(request));
    responder.sendString(HttpResponseStatus.OK, "Metadata added successfully to " + program);
  }

  @POST
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata/properties")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addDatasetProperties(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("dataset-id") String datasetId)
    throws BadRequestException, NotFoundException {
    DatasetId dataset = new DatasetId(namespaceId, datasetId);
    metadataAdmin.addProperties(dataset, readMetadata(request));
    responder.sendString(HttpResponseStatus.OK, "Metadata added successfully to " + dataset);
  }

  @POST
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata/properties")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addStreamProperties(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("stream-id") String streamId)
    throws BadRequestException, NotFoundException {
    StreamId stream = new StreamId(namespaceId, streamId);
    metadataAdmin.addProperties(stream, readMetadata(request));
    responder.sendString(HttpResponseStatus.OK, "Metadata added successfully to " + stream);
  }

  @POST
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/views/{view-id}/metadata/properties")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addViewProperties(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("stream-id") String streamId,
                                  @PathParam("view-id") String viewId) throws NotFoundException, BadRequestException {
    StreamViewId view = new StreamViewId(namespaceId, streamId, viewId);
    metadataAdmin.addProperties(view, readMetadata(request));
    responder.sendString(HttpResponseStatus.OK, "Metadata added successfully to " + view);
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata")
  public void removeAppMetadata(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId) throws NotFoundException {
    ApplicationId app = new ApplicationId(namespaceId, appId);
    metadataAdmin.removeMetadata(app);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata for app %s deleted successfully.", app));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/metadata")
  public void removeArtifactMetadata(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("artifact-name") String artifactName,
                                    @PathParam("artifact-version") String artifactVersionStr) throws NotFoundException {
    ArtifactId artifactId = new ArtifactId(namespaceId, artifactName, artifactVersionStr);
    metadataAdmin.removeMetadata(artifactId);
    responder.sendJson(HttpResponseStatus.OK,
                       String.format("Metadata for artifact %s deleted successfully.", artifactId));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata")
  public void removeProgramMetadata(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-id") String appId,
                                    @PathParam("program-type") String programType,
                                    @PathParam("program-id") String programId) throws NotFoundException {
    ProgramId program = new ProgramId(namespaceId, appId, ProgramType.valueOfCategoryName(programType), programId);
    metadataAdmin.removeMetadata(program);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata for program %s deleted successfully.", program));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata")
  public void removeDatasetMetadata(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("dataset-id") String datasetId) throws NotFoundException {
    DatasetId dataset = new DatasetId(namespaceId, datasetId);
    metadataAdmin.removeMetadata(dataset);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata for dataset %s deleted successfully.", dataset));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata")
  public void removeStreamMetadata(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("stream-id") String streamId) throws NotFoundException {
    StreamId stream = new StreamId(namespaceId, streamId);
    metadataAdmin.removeMetadata(stream);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata for stream %s deleted successfully.", stream));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/views/{view-id}/metadata")
  public void removeViewMetadata(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("stream-id") String streamId,
                                   @PathParam("view-id") String viewId) throws NotFoundException {
    StreamViewId view = new StreamViewId(namespaceId, streamId, viewId);
    metadataAdmin.removeMetadata(view);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata for view %s deleted successfully.", view));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata/properties")
  public void removeAppProperties(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId) throws NotFoundException {
    ApplicationId app = new ApplicationId(namespaceId, appId);
    metadataAdmin.removeProperties(app);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata properties for app %s deleted successfully.", app));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata/properties/{property}")
  public void removeAppProperty(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId,
                                @PathParam("property") String property) throws NotFoundException {
    ApplicationId app = new ApplicationId(namespaceId, appId);
    metadataAdmin.removeProperties(app, property);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata property %s for app %s deleted successfully.", property, app));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/metadata/properties")
  public void removeArtifactProperties(HttpRequest request, HttpResponder responder,
                                       @PathParam("namespace-id") String namespaceId,
                                       @PathParam("artifact-name") String artifactName,
                                       @PathParam("artifact-version") String artifactVersionStr)
    throws NotFoundException {
    ArtifactId artifactId = new ArtifactId(namespaceId, artifactName, artifactVersionStr);
    metadataAdmin.removeProperties(artifactId);
    responder.sendJson(HttpResponseStatus.OK,
                       String.format("Metadata properties for artifact %s deleted successfully.", artifactId));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/" +
    "metadata/properties/{property}")
  public void removeArtifactProperty(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("artifact-name") String artifactName,
                                     @PathParam("artifact-version") String artifactVersionStr,
                                     @PathParam("property") String property) throws NotFoundException {
    ArtifactId artifactId = new ArtifactId(namespaceId, artifactName, artifactVersionStr);
    metadataAdmin.removeProperties(artifactId, property);
    responder.sendJson(HttpResponseStatus.OK,
                       String.format("Metadata property %s for  artifact %s deleted successfully.",
                                     property, artifactId));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata/properties")
  public void removeProgramProperties(HttpRequest request, HttpResponder responder,
                                      @PathParam("namespace-id") String namespaceId,
                                      @PathParam("app-id") String appId,
                                      @PathParam("program-type") String programType,
                                      @PathParam("program-id") String programId) throws NotFoundException {
    ProgramId program = new ProgramId(namespaceId, appId, ProgramType.valueOfCategoryName(programType), programId);
    metadataAdmin.removeProperties(program);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata properties for program %s deleted successfully.", program));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata/properties/{property}")
  public void removeProgramProperty(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-id") String appId,
                                    @PathParam("program-type") String programType,
                                    @PathParam("program-id") String programId,
                                    @PathParam("property") String property) throws NotFoundException {
    ProgramId program = new ProgramId(namespaceId, appId, ProgramType.valueOfCategoryName(programType), programId);
    metadataAdmin.removeProperties(program, property);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata property %s for program %s deleted successfully.", property, program));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata/properties")
  public void removeDatasetProperties(HttpRequest request, HttpResponder responder,
                                      @PathParam("namespace-id") String namespaceId,
                                      @PathParam("dataset-id") String datasetId) throws NotFoundException {
    DatasetId dataset = new DatasetId(namespaceId, datasetId);
    metadataAdmin.removeProperties(dataset);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata properties for dataset %s deleted successfully.", dataset));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata/properties/{property}")
  public void removeDatasetProperty(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("dataset-id") String datasetId,
                                    @PathParam("property") String property) throws NotFoundException {
    DatasetId dataset = new DatasetId(namespaceId, datasetId);
    metadataAdmin.removeProperties(dataset, property);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata property %s for dataset %s deleted successfully.", property, dataset));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata/properties")
  public void removeStreamProperties(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("stream-id") String streamId) throws NotFoundException {
    StreamId stream = new StreamId(namespaceId, streamId);
    metadataAdmin.removeProperties(stream);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata properties for stream %s deleted successfully.", stream));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/views/{view-id}/metadata/properties")
  public void removeViewProperties(HttpRequest request, HttpResponder responder,
                                      @PathParam("namespace-id") String namespaceId,
                                      @PathParam("stream-id") String streamId,
                                      @PathParam("view-id") String viewId) throws NotFoundException {
    StreamViewId view = new StreamViewId(namespaceId, streamId, viewId);
    metadataAdmin.removeProperties(view);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata properties for view %s deleted successfully.", view));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata/properties/{property}")
  public void removeStreamProperty(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("stream-id") String streamId,
                                   @PathParam("property") String property) throws NotFoundException {
    StreamId stream = new StreamId(namespaceId, streamId);
    metadataAdmin.removeProperties(stream, property);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata property %s for stream %s deleted successfully.", property, stream));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/views/{view-id}/metadata/properties/{property}")
  public void removeViewProperty(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("stream-id") String streamId,
                                 @PathParam("view-id") String viewId,
                                 @PathParam("property") String property) throws NotFoundException {
    StreamViewId view = new StreamViewId(namespaceId, streamId, viewId);
    metadataAdmin.removeProperties(view, property);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata property %s for view %s deleted successfully.", property, view));
  }

  @POST
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata/tags")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addAppTags(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("app-id") String appId) throws BadRequestException, NotFoundException {
    ApplicationId app = new ApplicationId(namespaceId, appId);
    metadataAdmin.addTags(app, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Added tags to application %s successfully.", app));
  }

  @POST
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/metadata/tags")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addArtifactTags(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId,
                              @PathParam("artifact-name") String artifactName,
                              @PathParam("artifact-version") String artifactVersionStr)
    throws BadRequestException, NotFoundException {
    ArtifactId artifactId = new ArtifactId(namespaceId, artifactName, artifactVersionStr);
    metadataAdmin.addTags(artifactId, readArray(request));
    responder.sendJson(HttpResponseStatus.OK,
                       String.format("Added tags to artifact %s successfully.", artifactId));
  }

  @POST
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata/tags")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addProgramTags(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-id") String appId,
                             @PathParam("program-type") String programType,
                             @PathParam("program-id") String programId) throws BadRequestException, NotFoundException {
    ProgramId program = new ProgramId(namespaceId, appId, ProgramType.valueOfCategoryName(programType), programId);
    metadataAdmin.addTags(program, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Added tags to program %s successfully.", program));
  }

  @POST
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata/tags")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addDatasetTags(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("dataset-id") String datasetId) throws BadRequestException, NotFoundException {
    DatasetId dataset = new DatasetId(namespaceId, datasetId);
    metadataAdmin.addTags(dataset, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Added tags to dataset %s successfully.", dataset));
  }

  @POST
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata/tags")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addStreamTags(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("stream-id") String streamId) throws BadRequestException, NotFoundException {
    StreamId stream = new StreamId(namespaceId, streamId);
    metadataAdmin.addTags(stream, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Added tags to stream %s successfully.", stream));
  }

  @POST
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/views/{view-id}/metadata/tags")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addViewTags(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("stream-id") String streamId,
                            @PathParam("view-id") String viewId) throws NotFoundException, BadRequestException {
    StreamViewId view = new StreamViewId(namespaceId, streamId, viewId);
    metadataAdmin.addTags(view, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Added tags to view %s successfully", view));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata/tags")
  public void getAppTags(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("app-id") String appId,
                         @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    ApplicationId app = new ApplicationId(namespaceId, appId);
    responder.sendJson(HttpResponseStatus.OK, getTags(app, scope));
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/metadata/tags")
  public void getArtifactTags(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId,
                              @PathParam("artifact-name") String artifactName,
                              @PathParam("artifact-version") String artifactVersionStr,
                              @QueryParam("scope") String scope)
    throws BadRequestException, NotFoundException {
    ArtifactId artifactId = new ArtifactId(namespaceId, artifactName, artifactVersionStr);
    responder.sendJson(HttpResponseStatus.OK, getTags(artifactId, scope));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata/tags")
  public void getProgramTags(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-id") String appId,
                             @PathParam("program-type") String programType,
                             @PathParam("program-id") String programId,
                             @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    ProgramId program = new ProgramId(namespaceId, appId, ProgramType.valueOfCategoryName(programType), programId);
    responder.sendJson(HttpResponseStatus.OK, getTags(program, scope));
  }

  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata/tags")
  public void getDatasetTags(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("dataset-id") String datasetId,
                             @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    DatasetId dataset = new DatasetId(namespaceId, datasetId);
    responder.sendJson(HttpResponseStatus.OK, getTags(dataset, scope));
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata/tags")
  public void getStreamTags(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("stream-id") String streamId,
                            @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    StreamId stream = new StreamId(namespaceId, streamId);
    responder.sendJson(HttpResponseStatus.OK, getTags(stream, scope));
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/views/{view-id}/metadata/tags")
  public void getViewTags(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId,
                          @PathParam("stream-id") String streamId,
                          @PathParam("view-id") String viewId,
                          @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    StreamViewId view = new StreamViewId(namespaceId, streamId, viewId);
    responder.sendJson(HttpResponseStatus.OK, getTags(view, scope));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata/tags")
  public void removeAppTags(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("app-id") String appId) throws NotFoundException {
    ApplicationId app = new ApplicationId(namespaceId, appId);
    metadataAdmin.removeTags(app);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tags for app %s deleted successfully.", app));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata/tags/{tag}")
  public void removeAppTag(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId,
                           @PathParam("app-id") String appId,
                           @PathParam("tag") String tag) throws NotFoundException {
    ApplicationId app = new ApplicationId(namespaceId, appId);
    metadataAdmin.removeTags(app, tag);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tag %s for app %s deleted successfully.", tag, app));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/metadata/tags")
  public void removeArtifactTags(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("artifact-name") String artifactName,
                                 @PathParam("artifact-version") String artifactVersionStr) throws NotFoundException {
    ArtifactId artifactId = new ArtifactId(namespaceId, artifactName, artifactVersionStr);
    metadataAdmin.removeTags(artifactId);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tags for artifact %s deleted successfully.", artifactId));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/metadata/tags/{tag}")
  public void removeArtifactTag(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("artifact-name") String artifactName,
                                @PathParam("artifact-version") String artifactVersionStr,
                                @PathParam("tag") String tag) throws NotFoundException {
    ArtifactId artifactId = new ArtifactId(namespaceId, artifactName, artifactVersionStr);
    metadataAdmin.removeTags(artifactId, tag);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tags %s for artifact %s deleted successfully.", tag, artifactId));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata/tags")
  public void removeProgramTags(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId,
                                @PathParam("program-type") String programType,
                                @PathParam("program-id") String programId) throws NotFoundException {
    ProgramId program = new ProgramId(namespaceId, appId, ProgramType.valueOfCategoryName(programType), programId);
    metadataAdmin.removeTags(program);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tags for program %s deleted successfully.", program));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata/tags/{tag}")
  public void removeProgramTag(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("app-id") String appId,
                               @PathParam("program-type") String programType,
                               @PathParam("program-id") String programId,
                               @PathParam("tag") String tag) throws NotFoundException {
    ProgramId program = new ProgramId(namespaceId, appId, ProgramType.valueOfCategoryName(programType), programId);
    metadataAdmin.removeTags(program, tag);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tag %s for program %s deleted successfully.", tag, program));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata/tags")
  public void removeDatasetTags(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("dataset-id") String datasetId) throws NotFoundException {
    DatasetId dataset = new DatasetId(namespaceId, datasetId);
    
    metadataAdmin.removeTags(dataset);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tags for dataset %s deleted successfully.", dataset));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata/tags/{tag}")
  public void removeDatasetTag(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("dataset-id") String datasetId,
                               @PathParam("tag") String tag) throws NotFoundException {
    DatasetId dataset = new DatasetId(namespaceId, datasetId);
    metadataAdmin.removeTags(dataset, tag);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tag %s for dataset %s deleted successfully.", tag, dataset));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata/tags")
  public void removeStreamTags(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("stream-id") String streamId) throws NotFoundException {
    StreamId stream = new StreamId(namespaceId, streamId);
    metadataAdmin.removeTags(stream);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tags for stream %s deleted successfully.", stream));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/views/{view-id}/metadata/tags")
  public void removeViewTags(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("stream-id") String streamId,
                               @PathParam("view-id") String viewId) throws NotFoundException {
    StreamViewId view = new StreamViewId(namespaceId, streamId, viewId);
    metadataAdmin.removeTags(view);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tags for view %s deleted successfully.", view));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata/tags/{tag}")
  public void removeStreamTag(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId,
                              @PathParam("stream-id") String streamId,
                              @PathParam("tag") String tag) throws NotFoundException {
    StreamId stream = new StreamId(namespaceId, streamId);
    metadataAdmin.removeTags(stream, tag);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tag %s for stream %s deleted successfully.", tag, stream));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/views/{view-id}/metadata/tags/{tag}")
  public void removeViewTag(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId,
                              @PathParam("stream-id") String streamId,
                              @PathParam("view-id") String viewId,
                              @PathParam("tag") String tag) throws NotFoundException {
    StreamViewId view = new StreamViewId(namespaceId, streamId, viewId);
    metadataAdmin.removeTags(view, tag);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tag %s for view %s deleted successfully.", tag, view));
  }

  private Map<String, String> readMetadata(HttpRequest request) throws BadRequestException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      throw new BadRequestException("Unable to read metadata properties from the request.");
    }

    Map<String, String> metadata;
    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8)) {
      metadata = GSON.fromJson(reader, MAP_STRING_STRING_TYPE);
    } catch (IOException e) {
      throw new BadRequestException("Unable to read metadata properties from the request.", e);
    }

    if (metadata == null) {
      throw new BadRequestException("Null metadata was read from the request");
    }
    return metadata;
  }

  private String[] readArray(HttpRequest request) throws BadRequestException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      throw new BadRequestException("Unable to read a list of tags from the request.");
    }

    List<String> toReturn;
    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8)) {
      toReturn = GSON.fromJson(reader, LIST_STRING_TYPE);
    } catch (IOException e) {
      throw new BadRequestException("Unable to read a list of tags from the request.", e);
    }

    if (toReturn == null) {
      throw new BadRequestException("Null tags were read from the request.");
    }
    return toReturn.toArray(new String[toReturn.size()]);
  }

  @GET
  @Path("/namespaces/{namespace-id}/metadata/search")
  public void searchMetadata(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @QueryParam("query") @DefaultValue("") String searchQuery,
                             @QueryParam("target") List<String> targets,
                             @QueryParam("sort") @DefaultValue("") String sort,
                             @QueryParam("offset") @DefaultValue("0") int offset,
                             // 2147483647 is Integer.MAX_VALUE
                             @QueryParam("limit") @DefaultValue("2147483647") int limit,
                             @QueryParam("numCursors") @DefaultValue("0") int numCursors,
                             @QueryParam("cursor") @DefaultValue("") String cursor,
                             @QueryParam("showHidden") @DefaultValue("false") boolean showHidden) throws Exception {
    Set<EntityTypeSimpleName> types = Collections.emptySet();
    if (targets != null) {
      types = ImmutableSet.copyOf(Iterables.transform(targets, STRING_TO_TARGET_TYPE));
    }
    SortInfo sortInfo = SortInfo.of(URLDecoder.decode(sort, "UTF-8"));
    if (SortInfo.DEFAULT.equals(sortInfo)) {
      if (!(cursor.isEmpty()) || 0 != numCursors) {
        throw new BadRequestException("Cursors are not supported when sort info is not specified.");
      }
    }
    try {
      MetadataSearchResponse response =
        metadataAdmin.search(namespaceId, URLDecoder.decode(searchQuery, "UTF-8"), types,
                             sortInfo, offset, limit, numCursors, cursor, showHidden);
      responder.sendJson(HttpResponseStatus.OK, response, MetadataSearchResponse.class, GSON);
    } catch (Exception e) {
      // if MetadataDataset throws an exception, it gets wrapped
      if (Throwables.getRootCause(e) instanceof IllegalArgumentException) {
        throw new BadRequestException(e.getMessage(), e);
      }
      throw e;
    }
  }

  private Set<MetadataRecord> getMetadata(NamespacedEntityId namespacedEntityId,
                                          @Nullable String scope) throws NotFoundException, BadRequestException {
    return  (scope == null) ?
      metadataAdmin.getMetadata(namespacedEntityId) :
      metadataAdmin.getMetadata(validateScope(scope), namespacedEntityId);
  }

  private Map<String, String> getProperties(NamespacedEntityId namespacedEntityId,
                                            @Nullable String scope) throws NotFoundException, BadRequestException {
    return  (scope == null) ?
      metadataAdmin.getProperties(namespacedEntityId) :
      metadataAdmin.getProperties(validateScope(scope), namespacedEntityId);
  }

  private Set<String> getTags(NamespacedEntityId namespacedEntityId,
                              @Nullable String scope) throws NotFoundException, BadRequestException {
    return  (scope == null) ?
      metadataAdmin.getTags(namespacedEntityId) :
      metadataAdmin.getTags(validateScope(scope), namespacedEntityId);
  }

  private MetadataScope validateScope(String scope) throws BadRequestException {
    try {
      return MetadataScope.valueOf(scope.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(String.format("Invalid metadata scope '%s'. Expected '%s' or '%s'",
                                                  scope, MetadataScope.USER, MetadataScope.SYSTEM));
    }
  }
}
