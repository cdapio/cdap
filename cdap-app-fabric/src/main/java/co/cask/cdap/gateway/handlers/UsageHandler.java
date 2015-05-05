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
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link co.cask.http.HttpHandler} for handling REST calls to the usage registry.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class UsageHandler extends AbstractHttpHandler {

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
    final Id.Application id = Id.Application.from(namespaceId, appId);
    Set<? extends Id> ids = registry.getDatasets(id);
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/streams")
  public void getAppStreamUsage(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId) {
    final Id.Application id = Id.Application.from(namespaceId, appId);
    Set<? extends Id> ids = registry.getStreams(id);
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/datasets")
  public void getProgramDatasetUsage(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("app-id") String appId,
                                     @PathParam("program-type") String programType,
                                     @PathParam("program-id") String programId) {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    final Id.Program id = Id.Program.from(namespaceId, appId, type, programId);
    Set<? extends Id> ids = registry.getDatasets(id);
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/streams")
  public void getProgramStreamUsage(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-id") String appId,
                                    @PathParam("program-type") String programType,
                                    @PathParam("program-id") String programId) {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    final Id.Program id = Id.Program.from(namespaceId, appId, type, programId);
    Set<? extends Id> ids = registry.getStreams(id);
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/adapters/{adapter-id}/datasets")
  public void getAdapterDatasetUsage(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("adapter-id") String adapterId) {
    final Id.Adapter id = Id.Adapter.from(namespaceId, adapterId);
    Set<? extends Id> ids = registry.getDatasets(id);
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/adapters/{adapter-id}/streams")
  public void getAdapterStreamUsage(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("adapter-id") String adapterId) {
    final Id.Adapter id = Id.Adapter.from(namespaceId, adapterId);
    Set<? extends Id> ids = registry.getStreams(id);
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/programs")
  public void getStreamProgramUsage(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("stream-id") String streamId) {
    final Id.Stream id = Id.Stream.from(namespaceId, streamId);
    Set<? extends Id> ids = registry.getPrograms(id);
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/adapters")
  public void getStreamAdapterUsage(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("stream-id") String streamId) {
    final Id.Stream id = Id.Stream.from(namespaceId, streamId);
    Set<? extends Id> ids = registry.getAdapters(id);
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/data/datasets/{dataset-id}/programs")
  public void getDatasetAppUsage(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("dataset-id") String datasetId) {
    final Id.DatasetInstance id = Id.DatasetInstance.from(namespaceId, datasetId);
    Set<? extends Id> ids = registry.getPrograms(id);
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/data/datasets/{dataset-id}/adapters")
  public void getDatasetAdapterUsage(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("dataset-id") String datasetId) {
    final Id.DatasetInstance id = Id.DatasetInstance.from(namespaceId, datasetId);
    Set<? extends Id> ids = registry.getAdapters(id);
    responder.sendJson(HttpResponseStatus.OK, ids);
  }
}
