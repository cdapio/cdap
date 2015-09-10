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
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
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
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * HttpHandler for Metadata
 */
@Path(Constants.Gateway.API_VERSION_3)
public class MetadataHttpHandler extends AbstractHttpHandler {
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type LIST_STRING_TYPE = new TypeToken<List<String>>() { }.getType();
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
    responder.sendJson(HttpResponseStatus.OK, metadataAdmin.get(Id.Application.from(namespaceId, appId)));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata")
  public void getProgramMetadata(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-id") String appId,
                             @PathParam("program-type") String programType,
                             @PathParam("program-id") String programId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
                       metadataAdmin.get(Id.Program.from(Id.Application.from(namespaceId, appId),
                                                         ProgramType.valueOfCategoryName(programType), programId)));
  }

  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata")
  public void getDatasetMetadata(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("dataset-id") String datasetId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, metadataAdmin.get(Id.DatasetInstance.from(namespaceId, datasetId)));
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata")
  public void getStreamMetadata(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("stream-id") String streamId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, metadataAdmin.get(Id.Stream.from(namespaceId, streamId)));
  }

  @POST
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata")
  public void addAppMetadata(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-id") String appId) throws Exception {
    Id.Application app = Id.Application.from(namespaceId, appId);
    metadataAdmin.add(app, readMetadata(request));
    responder.sendString(HttpResponseStatus.OK, "Metadata added successfully to " + app);
  }

  @POST
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/metadata")
  public void addProgramMetadata(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("app-id") String appId,
                                 @PathParam("program-type") String programType,
                                 @PathParam("program-id") String programId) throws Exception {
    Id.Program program = Id.Program.from(Id.Application.from(namespaceId, appId),
                                         ProgramType.valueOfCategoryName(programType), programId);
    metadataAdmin.add(program, readMetadata(request));
    responder.sendString(HttpResponseStatus.OK, "Metadata added successfully to " + program);
  }

  @POST
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata")
  public void addDatasetMetadata(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("dataset-id") String datasetId) throws Exception {
    Id.DatasetInstance dataset = Id.DatasetInstance.from(namespaceId, datasetId);
    metadataAdmin.add(dataset, readMetadata(request));
    responder.sendString(HttpResponseStatus.OK, "Metadata added successfully to " + dataset);
  }

  @POST
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata")
  public void addStreamMetadata(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("stream-id") String streamId) throws Exception {
    Id.Stream stream = Id.Stream.from(namespaceId, streamId);
    metadataAdmin.add(stream, readMetadata(request));
    responder.sendString(HttpResponseStatus.OK, "Metadata added successfully to " + stream);
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata")
  public void removeAppMetadata(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId) throws Exception {
    Id.Application app = Id.Application.from(namespaceId, appId);
    metadataAdmin.remove(app, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata keys for app %s deleted successfully.", app));
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
    metadataAdmin.remove(program, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata keys for program %s deleted successfully.", program));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata")
  public void removeDatasetMetadata(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("dataset-id") String datasetId) throws Exception {
    Id.DatasetInstance dataset = Id.DatasetInstance.from(namespaceId, datasetId);
    metadataAdmin.remove(dataset, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata keys for dataset %s deleted successfully.", dataset));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata")
  public void removeStreamMetadata(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("stream-id") String streamId) throws Exception {
    Id.Stream stream = Id.Stream.from(namespaceId, streamId);
    metadataAdmin.remove(stream, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata keys for stream %s deleted successfully.", stream));
  }

  @POST
  @Path("/namespaces/{namespace-id}/apps/{app-id}/tags")
  public void addAppTags(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("app-id") String appId) throws Exception {
    Id.Application app = Id.Application.from(namespaceId, appId);
    metadataAdmin.addTags(app, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Added tags to application %s successfully", app));
  }

  @POST
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/tags")
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
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/tags")
  public void addDatasetTags(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("dataset-id") String datasetId) throws Exception {
    Id.DatasetInstance dataset = Id.DatasetInstance.from(namespaceId, datasetId);
    metadataAdmin.addTags(dataset, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Added tags to dataset %s successfully", dataset));
  }

  @POST
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/tags")
  public void addStreamTags(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("stream-id") String streamId) throws Exception {
    Id.Stream stream = Id.Stream.from(namespaceId, streamId);
    metadataAdmin.addTags(stream, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Added tags to stream %s successfully", stream));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/tags")
  public void getAppTags(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("app-id") String appId) throws Exception {
    Id.Application app = Id.Application.from(namespaceId, appId);
    responder.sendJson(HttpResponseStatus.OK, Sets.newHashSet(metadataAdmin.getTags(app)));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/tags")
  public void getProgramTags(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-id") String appId,
                             @PathParam("program-type") String programType,
                             @PathParam("program-id") String programId) throws Exception {
    Id.Program program = Id.Program.from(Id.Application.from(namespaceId, appId),
                                         ProgramType.valueOfCategoryName(programType), programId);
    responder.sendJson(HttpResponseStatus.OK, Sets.newHashSet(metadataAdmin.getTags(program)));
  }

  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/tags")
  public void getDatasetTags(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("dataset-id") String datasetId) throws Exception {
    Id.DatasetInstance dataset = Id.DatasetInstance.from(namespaceId, datasetId);
    responder.sendJson(HttpResponseStatus.OK, Sets.newHashSet(metadataAdmin.getTags(dataset)));
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/tags")
  public void getStreamTags(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("stream-id") String streamId) throws Exception {
    Id.Stream stream = Id.Stream.from(namespaceId, streamId);
    responder.sendJson(HttpResponseStatus.OK, Sets.newHashSet(metadataAdmin.getTags(stream)));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/tags")
  public void removeAppTags(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("app-id") String appId) throws Exception {
    Id.Application app = Id.Application.from(namespaceId, appId);
    metadataAdmin.removeTags(app, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tags for app %s deleted successfully.", app));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/tags")
  public void removeProgramTags(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId,
                                @PathParam("program-type") String programType,
                                @PathParam("program-id") String programId) throws Exception {
    Id.Program program = Id.Program.from(Id.Application.from(namespaceId, appId),
                                         ProgramType.valueOfCategoryName(programType), programId);
    metadataAdmin.removeTags(program, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tags for program %s deleted successfully.", program));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/tags")
  public void removeDatasetTags(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("dataset-id") String datasetId) throws Exception {
    Id.DatasetInstance dataset = Id.DatasetInstance.from(namespaceId, datasetId);
    metadataAdmin.removeTags(dataset, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tags for dataset %s deleted successfully.", dataset));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/tags")
  public void removeStreamTags(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("stream-id") String streamId) throws Exception {
    Id.Stream stream = Id.Stream.from(namespaceId, streamId);
    metadataAdmin.removeTags(stream, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Tags for stream %s deleted successfully.", stream));
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

  @POST
  @Path("/metadata/history")
  public void recordRun(HttpRequest request, HttpResponder responder) {
    responder.sendString(HttpResponseStatus.OK, "Metadata recorded successfully");
  }
}
