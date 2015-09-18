/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.DELETE;
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
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec())
    .create();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type LIST_STRING_TYPE = new TypeToken<List<String>>() { }.getType();
  private static final Type SET_METADATA_RECORD_TYPE = new TypeToken<Set<MetadataRecord>>() { }.getType();
  private static final Type SET_METADATA_SEARCH_RESULT_TYPE =
    new TypeToken<Set<MetadataSearchResultRecord>>() { }.getType();
  private final MetadataAdmin metadataAdmin;

  @Inject
  public MetadataHttpHandler(MetadataAdmin metadataAdmin) {
    this.metadataAdmin = metadataAdmin;
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata")
  public void getAppMetadata(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, metadataAdmin.getMetadata(Id.Application.from(namespaceId, appId)),
                       SET_METADATA_RECORD_TYPE, GSON);
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata")
  public void getProgramMetadata(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-id") String appId,
                                    @PathParam("program-type") String programType,
                                    @PathParam("program-id") String programId) throws Exception {
    Id.Program program = Id.Program.from(Id.Application.from(namespaceId, appId),
                                         ProgramType.valueOfCategoryName(programType), programId);
    responder.sendJson(HttpResponseStatus.OK, metadataAdmin.getMetadata(program),
                       SET_METADATA_RECORD_TYPE, GSON);
  }

  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata")
  public void getDatasetMetadata(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("dataset-id") String datasetId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
                       metadataAdmin.getMetadata(Id.DatasetInstance.from(namespaceId, datasetId)),
                       SET_METADATA_RECORD_TYPE, GSON);
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata")
  public void getStreamMetadata(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("stream-id") String streamId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, metadataAdmin.getMetadata(Id.Stream.from(namespaceId, streamId)),
                       SET_METADATA_RECORD_TYPE, GSON);
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata/properties")
  public void getAppProperties(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("app-id") String appId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, metadataAdmin.getProperties(Id.Application.from(namespaceId, appId)));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata/properties")
  public void getProgramProperties(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("app-id") String appId,
                                   @PathParam("program-type") String programType,
                                   @PathParam("program-id") String programId) throws Exception {
    Id.Program program = Id.Program.from(Id.Application.from(namespaceId, appId),
                                         ProgramType.valueOfCategoryName(programType), programId);
    responder.sendJson(HttpResponseStatus.OK, metadataAdmin.getProperties(program));
  }

  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata/properties")
  public void getDatasetProperties(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("dataset-id") String datasetId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
                       metadataAdmin.getProperties(Id.DatasetInstance.from(namespaceId, datasetId)));
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata/properties")
  public void getStreamProperties(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("stream-id") String streamId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, metadataAdmin.getProperties(Id.Stream.from(namespaceId, streamId)));
  }

  @POST
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata/properties")
  public void addAppProperties(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("app-id") String appId) throws Exception {
    Id.Application app = Id.Application.from(namespaceId, appId);
    metadataAdmin.addProperties(app, readMetadata(request));
    responder.sendString(HttpResponseStatus.OK, "Metadata added successfully to " + app);
  }

  @POST
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata/properties")
  public void addProgramProperties(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("app-id") String appId,
                                   @PathParam("program-type") String programType,
                                   @PathParam("program-id") String programId) throws Exception {
    Id.Program program = Id.Program.from(Id.Application.from(namespaceId, appId),
                                         ProgramType.valueOfCategoryName(programType), programId);
    metadataAdmin.addProperties(program, readMetadata(request));
    responder.sendString(HttpResponseStatus.OK, "Metadata added successfully to " + program);
  }

  @POST
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata/properties")
  public void addDatasetProperties(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("dataset-id") String datasetId) throws Exception {
    Id.DatasetInstance dataset = Id.DatasetInstance.from(namespaceId, datasetId);
    metadataAdmin.addProperties(dataset, readMetadata(request));
    responder.sendString(HttpResponseStatus.OK, "Metadata added successfully to " + dataset);
  }

  @POST
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata/properties")
  public void addStreamProperties(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("stream-id") String streamId) throws Exception {
    Id.Stream stream = Id.Stream.from(namespaceId, streamId);
    metadataAdmin.addProperties(stream, readMetadata(request));
    responder.sendString(HttpResponseStatus.OK, "Metadata added successfully to " + stream);
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata")
  public void removeAppMetadata(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId) throws Exception {
    Id.Application app = Id.Application.from(namespaceId, appId);
    metadataAdmin.removeMetadata(app);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata for app %s deleted successfully.", app));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata")
  public void removeProgramMetadata(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-id") String appId,
                                    @PathParam("program-type") String programType,
                                    @PathParam("program-id") String programId) throws Exception {
    Id.Program program = Id.Program.from(Id.Application.from(namespaceId, appId),
                                         ProgramType.valueOfCategoryName(programType), programId);
    metadataAdmin.removeMetadata(program);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata for program %s deleted successfully.", program));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata")
  public void removeDatasetMetadata(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("dataset-id") String datasetId) throws Exception {
    Id.DatasetInstance dataset = Id.DatasetInstance.from(namespaceId, datasetId);
    metadataAdmin.removeMetadata(dataset);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata for dataset %s deleted successfully.", dataset));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata")
  public void removeStreamMetadata(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("stream-id") String streamId) throws Exception {
    Id.Stream stream = Id.Stream.from(namespaceId, streamId);
    metadataAdmin.removeMetadata(stream);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata for stream %s deleted successfully.", stream));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata/properties")
  public void removeAppProperties(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId) throws Exception {
    Id.Application app = Id.Application.from(namespaceId, appId);
    metadataAdmin.removeProperties(app);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata properties for app %s deleted successfully.", app));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata/properties/{property}")
  public void removeAppProperty(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId,
                                @PathParam("property") String property) throws Exception {
    Id.Application app = Id.Application.from(namespaceId, appId);
    metadataAdmin.removeProperties(app, property);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata property %s for app %s deleted successfully.", property, app));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata/properties")
  public void removeProgramProperties(HttpRequest request, HttpResponder responder,
                                      @PathParam("namespace-id") String namespaceId,
                                      @PathParam("app-id") String appId,
                                      @PathParam("program-type") String programType,
                                      @PathParam("program-id") String programId) throws Exception {
    Id.Program program = Id.Program.from(Id.Application.from(namespaceId, appId),
                                         ProgramType.valueOfCategoryName(programType), programId);
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
                                    @PathParam("property") String property) throws Exception {
    Id.Program program = Id.Program.from(Id.Application.from(namespaceId, appId),
                                         ProgramType.valueOfCategoryName(programType), programId);
    metadataAdmin.removeProperties(program, property);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata property %s for program %s deleted successfully.", property, program));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata/properties")
  public void removeDatasetProperties(HttpRequest request, HttpResponder responder,
                                      @PathParam("namespace-id") String namespaceId,
                                      @PathParam("dataset-id") String datasetId) throws Exception {
    Id.DatasetInstance dataset = Id.DatasetInstance.from(namespaceId, datasetId);
    metadataAdmin.removeProperties(dataset);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata properties for dataset %s deleted successfully.", dataset));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata/properties/{property}")
  public void removeDatasetProperty(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("dataset-id") String datasetId,
                                    @PathParam("property") String property) throws Exception {
    Id.DatasetInstance dataset = Id.DatasetInstance.from(namespaceId, datasetId);
    metadataAdmin.removeProperties(dataset, property);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata property %s for dataset %s deleted successfully.", property, dataset));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata/properties")
  public void removeStreamProperties(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("stream-id") String streamId) throws Exception {
    Id.Stream stream = Id.Stream.from(namespaceId, streamId);
    metadataAdmin.removeProperties(stream);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata properties for stream %s deleted successfully.", stream));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata/properties/{property}")
  public void removeStreamProperty(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("stream-id") String streamId,
                                   @PathParam("property") String property) throws Exception {
    Id.Stream stream = Id.Stream.from(namespaceId, streamId);
    metadataAdmin.removeProperties(stream, property);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata property %s for stream %s deleted successfully.", property, stream));
  }

  @POST
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata/tags")
  public void addAppTags(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("app-id") String appId) throws Exception {
    Id.Application app = Id.Application.from(namespaceId, appId);
    metadataAdmin.addTags(app, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Added tags to application %s successfully", app));
  }

  @POST
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata/tags")
  public void addProgramTags(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-id") String appId,
                             @PathParam("program-type") String programType,
                             @PathParam("program-id") String programId) throws Exception {
    Id.Program program = Id.Program.from(Id.Application.from(namespaceId, appId),
                                         ProgramType.valueOfCategoryName(programType), programId);
    metadataAdmin.addTags(program, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Added tags to program %s successfully", program));
  }

  @POST
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata/tags")
  public void addDatasetTags(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("dataset-id") String datasetId) throws Exception {
    Id.DatasetInstance dataset = Id.DatasetInstance.from(namespaceId, datasetId);
    metadataAdmin.addTags(dataset, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Added tags to dataset %s successfully", dataset));
  }

  @POST
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata/tags")
  public void addStreamTags(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("stream-id") String streamId) throws Exception {
    Id.Stream stream = Id.Stream.from(namespaceId, streamId);
    metadataAdmin.addTags(stream, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Added tags to stream %s successfully", stream));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata/tags")
  public void getAppTags(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("app-id") String appId) throws Exception {
    Id.Application app = Id.Application.from(namespaceId, appId);
    responder.sendJson(HttpResponseStatus.OK, metadataAdmin.getTags(app));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata/tags")
  public void getProgramTags(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-id") String appId,
                             @PathParam("program-type") String programType,
                             @PathParam("program-id") String programId) throws Exception {
    Id.Program program = Id.Program.from(Id.Application.from(namespaceId, appId),
                                         ProgramType.valueOfCategoryName(programType), programId);
    responder.sendJson(HttpResponseStatus.OK, metadataAdmin.getTags(program));
  }

  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata/tags")
  public void getDatasetTags(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("dataset-id") String datasetId) throws Exception {
    Id.DatasetInstance dataset = Id.DatasetInstance.from(namespaceId, datasetId);
    responder.sendJson(HttpResponseStatus.OK, metadataAdmin.getTags(dataset));
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata/tags")
  public void getStreamTags(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("stream-id") String streamId) throws Exception {
    Id.Stream stream = Id.Stream.from(namespaceId, streamId);
    responder.sendJson(HttpResponseStatus.OK, metadataAdmin.getTags(stream));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata/tags")
  public void removeAppTags(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("app-id") String appId) throws Exception {
    Id.Application app = Id.Application.from(namespaceId, appId);
    metadataAdmin.removeTags(app);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tags for app %s deleted successfully.", app));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata/tags/{tag}")
  public void removeAppTags(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("app-id") String appId,
                            @PathParam("tag") String tag) throws Exception {
    Id.Application app = Id.Application.from(namespaceId, appId);
    metadataAdmin.removeTags(app, tag);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tag %s for app %s deleted successfully.", tag, app));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata/tags")
  public void removeProgramTags(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId,
                                @PathParam("program-type") String programType,
                                @PathParam("program-id") String programId) throws Exception {
    Id.Program program = Id.Program.from(Id.Application.from(namespaceId, appId),
                                         ProgramType.valueOfCategoryName(programType), programId);
    metadataAdmin.removeTags(program);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tags for program %s deleted successfully.", program));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata/tags/{tag}")
  public void removeProgramTags(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId,
                                @PathParam("program-type") String programType,
                                @PathParam("program-id") String programId,
                                @PathParam("tag") String tag) throws Exception {
    Id.Program program = Id.Program.from(Id.Application.from(namespaceId, appId),
                                         ProgramType.valueOfCategoryName(programType), programId);
    metadataAdmin.removeTags(program, tag);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tag %s for program %s deleted successfully.", tag, program));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata/tags")
  public void removeDatasetTags(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("dataset-id") String datasetId) throws Exception {
    Id.DatasetInstance dataset = Id.DatasetInstance.from(namespaceId, datasetId);
    metadataAdmin.removeTags(dataset);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tags for dataset %s deleted successfully.", dataset));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata/tags/{tag}")
  public void removeDatasetTags(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("dataset-id") String datasetId,
                                @PathParam("tag") String tag) throws Exception {
    Id.DatasetInstance dataset = Id.DatasetInstance.from(namespaceId, datasetId);
    metadataAdmin.removeTags(dataset, tag);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tag %s for dataset %s deleted successfully.", tag, dataset));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata/tags")
  public void removeStreamTags(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("stream-id") String streamId) throws Exception {
    Id.Stream stream = Id.Stream.from(namespaceId, streamId);
    metadataAdmin.removeTags(stream);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tags for stream %s deleted successfully.", stream));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata/tags/{tag}")
  public void removeStreamTags(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("stream-id") String streamId,
                               @PathParam("tag") String tag) throws Exception {
    Id.Stream stream = Id.Stream.from(namespaceId, streamId);
    metadataAdmin.removeTags(stream, tag);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tag %s for stream %s deleted successfully.", tag, stream));
  }

  private Map<String, String> readMetadata(HttpRequest request) throws BadRequestException, IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      throw new BadRequestException("Unable to read business metadata from request.");
    }
    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8)) {
      return GSON.fromJson(reader, MAP_STRING_STRING_TYPE);
    }
  }

  private String[] readArray(HttpRequest request) throws BadRequestException, IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      throw new BadRequestException("Unable to read a list of keys from the request.");
    }
    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8)) {
      List<String> toReturn = GSON.fromJson(reader, LIST_STRING_TYPE);
      return toReturn.toArray(new String[toReturn.size()]);
    }
  }

  // *** Search endpoints ***

  @GET
  @Path("/namespaces/{namespace-id}/metadata/search")
  public void searchMetadata(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @QueryParam("query") String searchQuery,
                             @QueryParam("target") String target) throws Exception {
    MetadataSearchTargetType metadataSearchTargetType;
    if (target != null) {
      metadataSearchTargetType = MetadataSearchTargetType.valueOf(target.toUpperCase());
    } else {
      metadataSearchTargetType = null;
    }
    Set<MetadataSearchResultRecord> results = metadataAdmin.searchMetadata(searchQuery, metadataSearchTargetType);

    responder.sendJson(HttpResponseStatus.OK, results, SET_METADATA_SEARCH_RESULT_TYPE, GSON);
  }
}
