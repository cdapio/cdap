/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Singleton;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ForbiddenException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.security.GcpMetadataTaskContext;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.net.URL;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import joptsimple.internal.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal {@link HttpHandler} for Artifact Localizer.
 */
@Singleton
@Path("/")
public class GcpMetadataHttpHandlerInternal extends AbstractAppFabricHttpHandler {

  protected static final String METADATA_FLAVOR_HEADER_KEY = "Metadata-Flavor";
  protected static final String METADATA_FLAVOR_HEADER_VALUE = "Google";
  private static final Logger LOG = LoggerFactory.getLogger(GcpMetadataHttpHandlerInternal.class);
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(BasicThrowable.class,
      new BasicThrowableCodec()).create();
  private final CConfiguration cConf;
  private final String metadataServiceEndpoint;
  private GcpMetadataTaskContext gcpMetadataTaskContext;

  /**
   * Constructs the {@link GcpMetadataHttpHandlerInternal}.
   *
   * @param cConf CConfiguration
   */
  public GcpMetadataHttpHandlerInternal(CConfiguration cConf) {
    this.cConf = cConf;
    this.metadataServiceEndpoint = cConf.get(
        Constants.TaskWorker.METADATA_SERVICE_END_POINT);
  }

  /**
   * Returns the status of metadata server.
   *
   * @param request The {@link HttpRequest}.
   * @param responder a {@link HttpResponder} for sending response.
   * @throws Exception if there is any error.
   */
  @GET
  @Path("/")
  public void status(HttpRequest request, HttpResponder responder) throws Exception {

    // check that metadata header is present in the request.
    if (!request.headers().contains(METADATA_FLAVOR_HEADER_KEY,
        METADATA_FLAVOR_HEADER_VALUE, true)) {
      throw new ForbiddenException(
          String.format("Request is missing required %s header. To access the metadata server, "
              + "you must add the %s: %s header to your request.", METADATA_FLAVOR_HEADER_KEY,
              METADATA_FLAVOR_HEADER_KEY, METADATA_FLAVOR_HEADER_VALUE));
    }
    responder.sendStatus(HttpResponseStatus.OK,
        new DefaultHttpHeaders().add(METADATA_FLAVOR_HEADER_KEY, METADATA_FLAVOR_HEADER_VALUE));
  }

  /**
   * Returns the token of metadata server.
   *
   * @param request The {@link HttpRequest}.
   * @param responder a {@link HttpResponder} for sending response.
   * @throws Exception if there is any error.
   */
  @GET
  @Path("/computeMetadata/v1/instance/service-accounts/default/token")
  public void token(HttpRequest request, HttpResponder responder,
      @QueryParam("scopes") String scopes) throws Exception {

    if (gcpMetadataTaskContext != null && gcpMetadataTaskContext.getNamespace() != null) {
      LOG.trace("Token requested for namespace: {}", gcpMetadataTaskContext.getNamespace());
    } else {
      LOG.trace("Token requested but namespace not set");
    }
    // check that metadata header is present in the request.
    if (!request.headers().contains(METADATA_FLAVOR_HEADER_KEY,
        METADATA_FLAVOR_HEADER_VALUE, true)) {
      throw new ForbiddenException(
          String.format("Request is missing required %s header. To access the metadata server, "
                  + "you must add the %s: %s header to your request.", METADATA_FLAVOR_HEADER_KEY,
              METADATA_FLAVOR_HEADER_KEY, METADATA_FLAVOR_HEADER_VALUE));
    }

    // TODO: CDAP-20750
    if (metadataServiceEndpoint == null) {
      responder.sendString(HttpResponseStatus.NOT_IMPLEMENTED,
          String.format("%s has not been set",
              Constants.TaskWorker.METADATA_SERVICE_END_POINT));
      return;
    }

    try {
      URL url = new URL(metadataServiceEndpoint);
      if (!Strings.isNullOrEmpty(scopes)) {
        url = new URL(String.format("%s?scopes=%s", metadataServiceEndpoint, scopes));
      }
      io.cdap.common.http.HttpRequest tokenRequest = io.cdap.common.http.HttpRequest.get(url)
          .addHeader(METADATA_FLAVOR_HEADER_KEY, METADATA_FLAVOR_HEADER_VALUE)
          .build();
      HttpResponse tokenResponse = HttpRequests.execute(tokenRequest);
      responder.sendJson(HttpResponseStatus.OK, tokenResponse.getResponseBodyAsString());
    } catch (Exception ex) {
      LOG.warn("Failed to fetch token from metadata service", ex);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(ex),
          new DefaultHttpHeaders().set(HttpHeaders.CONTENT_TYPE, "application/json"));
    }
  }

  /**
   * Sets the CDAP Namespace information.
   *
   * @param request The {@link HttpRequest}.
   * @param responder a {@link HttpResponder} for sending response.
   */
  @PUT
  @Path("/set-context")
  public void setContext(FullHttpRequest request, HttpResponder responder)
      throws BadRequestException {
    this.gcpMetadataTaskContext = getGcpMetadataTaskContext(request);
    responder.sendJson(HttpResponseStatus.OK,
        String.format("Context was set successfully with namespace '%s'.",
            gcpMetadataTaskContext.getNamespace()));
  }

  /**
   * Clears the CDAP Namespace information.
   *
   * @param request The {@link HttpRequest}.
   * @param responder a {@link HttpResponder} for sending response.
   */
  @DELETE
  @Path("/clear-context")
  public void clearContext(HttpRequest request, HttpResponder responder) {
    this.gcpMetadataTaskContext = null;
    LOG.debug("Context cleared.");
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Return json representation of an exception. Used to propagate exception across network for
   * better surfacing errors and debuggability.
   */
  private String exceptionToJson(Exception ex) {
    BasicThrowable basicThrowable = new BasicThrowable(ex);
    return GSON.toJson(basicThrowable);
  }

  private GcpMetadataTaskContext getGcpMetadataTaskContext(FullHttpRequest httpRequest)
      throws BadRequestException {
    try {
      return parseBody(httpRequest, GcpMetadataTaskContext.class);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Invalid json object provided in request body.");
    }
  }
}
