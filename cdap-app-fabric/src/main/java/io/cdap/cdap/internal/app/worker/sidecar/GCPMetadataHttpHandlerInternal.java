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
import com.google.inject.Singleton;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal {@link HttpHandler} for Artifact Localizer.
 */
@Singleton
@Path("/")
public class GCPMetadataHttpHandlerInternal extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(GCPMetadataHttpHandlerInternal.class);
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(BasicThrowable.class,
      new BasicThrowableCodec()).create();
  private final CConfiguration cConf;
  private String namespace;

  public GCPMetadataHttpHandlerInternal(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @GET
  @Path("/")
  public void status(HttpRequest request, HttpResponder responder) throws Exception {
    LOG.info("Status requested");
    responder.sendStatus(HttpResponseStatus.OK,
        new DefaultHttpHeaders().add("Metadata-Flavor", "Google"));
  }

  @GET
  @Path("/computeMetadata/v1/instance/service-accounts/default/token")
  public void token(HttpRequest request, HttpResponder responder) throws Exception {
    String token = cConf.get("metadata.server.token.value", "");
    LOG.info("Token requested {}", token);
    responder.sendString(
        HttpResponseStatus.OK,
        GSON.toJson(new GCPTokenResponse(token, 3599, "Bearer")),
        new DefaultHttpHeaders().add("Metadata-Flavor", "Google"));
  }

  @PUT
  @Path("/set-namespace/{namespace-id}")
  public void setNamespace(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId) {
    LOG.info("Set namespace {}", namespaceId);
    this.namespace = namespaceId;
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @DELETE
  @Path("/clear-namespace")
  public void clearNamespace(HttpRequest request, HttpResponder responder) {
    LOG.info("Clear namespace");
    this.namespace = null;
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
}
