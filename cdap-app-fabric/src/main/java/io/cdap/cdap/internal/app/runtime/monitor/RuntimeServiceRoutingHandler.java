/*
 * Copyright © 2020-2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.inject.Inject;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.GoneException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * The http handler for routing CDAP service requests from program runtime.
 */
@Path(Constants.Gateway.INTERNAL_API_VERSION_3
      + "/runtime/namespaces/{namespace}/apps/{app}/versions/{version}/{program-type}/{program}/runs/{run}")
public class RuntimeServiceRoutingHandler extends
    AbstractServiceRoutingHandler {

  private final RuntimeRequestValidator requestValidator;

  @Inject
  RuntimeServiceRoutingHandler(DiscoveryServiceClient discoveryServiceClient,
      RuntimeRequestValidator requestValidator) {
    super(discoveryServiceClient);
    this.requestValidator = requestValidator;
  }

  /**
   * Handles GET and DELETE calls from program runtime to access CDAP services.
   * It simply verifies the request and forward the call to internal CDAP
   * service.
   */
  @Path("/services/{service}/**")
  @GET
  @DELETE
  public void routeService(HttpRequest request, HttpResponder responder,
      @PathParam("namespace") String namespace, @PathParam("app") String app,
      @PathParam("version") String version,
      @PathParam("program-type") String programType,
      @PathParam("program") String program, @PathParam("run") String run,
      @PathParam("service") String service) throws Exception {
    validateRequest(request, namespace, app, version, programType, program, run
    );
    String servicePath = getServicePath(request, namespace, app, version,
        programType, program, run, service);
    routeService(request, responder, service, servicePath);
  }

  /**
   * Handles PUT and POST calls from program runtime to access CDAP services. It
   * simply verifies the request and forwards the call to internal CDAP
   * services.
   */
  @Path("/services/{service}/**")
  @PUT
  @POST
  public BodyConsumer routeServiceWithBody(HttpRequest request,
      HttpResponder responder, @PathParam("namespace") String namespace,
      @PathParam("app") String app, @PathParam("version") String version,
      @PathParam("program-type") String programType,
      @PathParam("program") String program, @PathParam("run") String run,
      @PathParam("service") String service) throws Exception {
    validateRequest(request, namespace, app, version, programType, program,
        run);
    String servicePath = getServicePath(request, namespace, app, version,
        programType, program, run, service);
    return routeServiceWithBody(request, responder, service,
        servicePath);
  }

  /**
   * Validates the request.
   *
   * @throws GoneException if the run already finished
   */
  private void validateRequest(HttpRequest request, String namespace,
      String app, String version, String programType, String program,
      String run) throws BadRequestException, GoneException {
    ApplicationId appId = new NamespaceId(namespace).app(app, version);
    ProgramRunId programRunId = new ProgramRunId(appId,
        ProgramType.valueOfCategoryName(programType, BadRequestException::new),
        program, run);
    requestValidator.getProgramRunStatus(programRunId, request);
  }

  private String getServicePath(HttpRequest request, String namespace,
      String app, String version, String programType, String program,
      String run, String service) {
    String prefix = String.format(
        "%s/runtime/namespaces/%s/apps/%s/versions/%s/%s/%s/runs/%s/services/%s",
        Constants.Gateway.INTERNAL_API_VERSION_3, namespace, app, version,
        programType, program, run, service);
    return request.uri().substring(prefix.length());
  }
}
