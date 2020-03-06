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
import com.google.inject.Inject;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link HttpHandler} for serving internal program lifecycle REST API requests
 */
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/namespaces/{namespace-id}")
public class ProgramLifecycleHttpHandlerInternal extends AbstractAppFabricHttpHandler {
  private static final Gson GSON = new Gson();

  private final ProgramLifecycleService programLifecycleService;

  @Inject
  ProgramLifecycleHttpHandlerInternal(ProgramLifecycleService programLifecycleService) {
    this.programLifecycleService = programLifecycleService;
  }

  /**
   * Return {@link RunRecordDetail} for the given program run id
   *
   * @param request {@link HttpRequest}
   * @param responder {@link HttpResponse}
   * @param namespaceId namespace of the program
   * @param appName name of the application
   * @param appVersion version of the application
   * @param type type of the program
   * @param programName name of the program
   * @param runid for which {@link RunRecordDetail} will be returned
   * @throws Exception if failed to to get {@link RunRecordDetail}
   */
  @GET
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runs/{run-id}")
  public void getProgramRunRecordMeta(HttpRequest request, HttpResponder responder,
                                      @PathParam("namespace-id") String namespaceId,
                                      @PathParam("app-name") String appName,
                                      @PathParam("app-version") String appVersion,
                                      @PathParam("program-type") String type,
                                      @PathParam("program-name") String programName,
                                      @PathParam("run-id") String runid) throws Exception {
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);
    ProgramId programId = new ApplicationId(namespaceId, appName, appVersion).program(programType, programName);
    RunRecordDetail runRecordMeta = programLifecycleService.getRunRecordMeta(programId.run(runid));
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(runRecordMeta));
  }
}
