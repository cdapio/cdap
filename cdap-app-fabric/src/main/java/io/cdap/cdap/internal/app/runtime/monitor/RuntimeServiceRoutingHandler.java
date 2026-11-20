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
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.GoneException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.authorization.AuthorizationUtil;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
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
public class RuntimeServiceRoutingHandler extends AbstractServiceRoutingHandler {

  private final RuntimeRequestValidator requestValidator;
  private final InternalAuthenticator internalAuthenticator;

  @Inject
  RuntimeServiceRoutingHandler(DiscoveryServiceClient discoveryServiceClient,
      RuntimeRequestValidator requestValidator,
      InternalAuthenticator internalAuthenticator) {
    super(discoveryServiceClient);
    this.requestValidator = requestValidator;
    this.internalAuthenticator = internalAuthenticator;
  }

  /**
   * Handles GET and DELETE calls from program runtime to access CDAP services.
   * It verifies the request and forwards the call to the internal CDAP service.
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
    ValidatedRuntimeRequest validated = validateRequest(request, namespace, app, version,
        programType, program, run);
    String servicePath = getServicePath(request, namespace, app, version,
        programType, program, run, service);
    HttpRequest forwardRequest = validated.isRuntimeToken()
        ? buildExchangedRequest(request) : request;
    routeService(forwardRequest, responder, service, servicePath);
  }

  /**
   * Handles PUT and POST calls from program runtime to access CDAP services.
   * It verifies the request and forwards the call to internal CDAP services.
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
    ValidatedRuntimeRequest validated = validateRequest(request, namespace, app, version,
        programType, program, run);
    String servicePath = getServicePath(request, namespace, app, version,
        programType, program, run, service);
    HttpRequest forwardRequest = validated.isRuntimeToken()
        ? buildExchangedRequest(request) : request;
    return routeServiceWithBody(forwardRequest, responder, service, servicePath);
  }

  /**
   * Validates the request.
   *
   * @throws GoneException if the run already finished
   */
  private ValidatedRuntimeRequest validateRequest(HttpRequest request, String namespace,
      String app, String version, String programType, String program,
      String run) throws BadRequestException, AccessException, GoneException {
    ApplicationId appId = new NamespaceId(namespace).app(app, version);
    ProgramRunId programRunId = new ProgramRunId(appId,
        ProgramType.valueOfCategoryName(programType, BadRequestException::new),
        program, run);
    return requestValidator.validate(programRunId, request);
  }

  /**
   * Builds a fresh downstream request for a {@code RUNTIME}-token caller containing only
   * {@code Content-Type}, {@code If-Modified-Since}, system-minted internal authentication headers,
   * and {@code CDAP-UserId: runtime}.
   */
  private HttpRequest buildExchangedRequest(HttpRequest request) {
    HttpHeaders headers = new DefaultHttpHeaders();
    String contentType = request.headers().get(HttpHeaderNames.CONTENT_TYPE);
    if (contentType != null) {
      headers.set(HttpHeaderNames.CONTENT_TYPE, contentType);
    }
    String ifModifiedSince = request.headers().get(HttpHeaderNames.IF_MODIFIED_SINCE);
    if (ifModifiedSince != null) {
      headers.set(HttpHeaderNames.IF_MODIFIED_SINCE, ifModifiedSince);
    }
    AuthorizationUtil.callAsSystem(() -> {
      internalAuthenticator.applyInternalAuthenticationHeaders(headers::set);
      return null;
    });
    // Overwrite the system USER_ID set by applyInternalAuthenticationHeaders so downstream
    // services authorize the forwarded call as the runtime identity.
    headers.set(Constants.Security.Headers.USER_ID,
        Constants.Security.Authentication.RUNTIME_IDENTITY);
    return new DefaultHttpRequest(request.protocolVersion(), request.method(), request.uri(),
        headers);
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
