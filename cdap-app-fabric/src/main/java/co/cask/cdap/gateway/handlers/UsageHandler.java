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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link co.cask.http.HttpHandler} for handling REST calls to the usage registry.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class UsageHandler extends AbstractHttpHandler {

  private static final Gson GSON = new Gson();
  private final UsageRegistry registry;

  @Inject
  public UsageHandler(UsageRegistry registry) {
    this.registry = registry;
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/datasets")
  public void getAppDatasetUsage(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("app-id") String appId) {
    final ApplicationId id = new ApplicationId(namespaceId, appId);
    Set<DatasetId> ids = registry.getDatasets(id);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(ids));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/streams")
  public void getAppStreamUsage(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId) {
    final ApplicationId id = new ApplicationId(namespaceId, appId);
    Set<StreamId> ids = registry.getStreams(id);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(ids));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/datasets")
  public void getProgramDatasetUsage(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("app-id") String appId,
                                     @PathParam("program-type") String programType,
                                     @PathParam("program-id") String programId) {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    final ProgramId id = new ProgramId(namespaceId, appId, type, programId);
    Set<DatasetId> ids = registry.getDatasets(id);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(ids));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/streams")
  public void getProgramStreamUsage(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-id") String appId,
                                    @PathParam("program-type") String programType,
                                    @PathParam("program-id") String programId) {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    final ProgramId id = new ProgramId(namespaceId, appId, type, programId);
    Set<StreamId> ids = registry.getStreams(id);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(ids));
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/programs")
  public void getStreamProgramUsage(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("stream-id") String streamId) {
    final StreamId id = new StreamId(namespaceId, streamId);
    Set<ProgramId> ids = registry.getPrograms(id);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(ids));
  }

  @GET
  @Path("/namespaces/{namespace-id}/data/datasets/{dataset-id}/programs")
  public void getDatasetAppUsage(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("dataset-id") String datasetId) {
    final DatasetId id = new DatasetId(namespaceId, datasetId);
    Set<ProgramId> ids = registry.getPrograms(id);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(ids));
  }
}
