/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.common.security.AuditDetail;
import co.cask.cdap.common.security.AuditPolicy;
import co.cask.cdap.proto.codec.NamespacedEntityIdCodec;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A HTTP Handler for handling REST calls for owner information
 */
@Path("/v1/owner")
public class OwnerHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ImpersonationHandler.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
    .create();

  private final OwnerAdmin ownerAdmin;

  @Inject
  public OwnerHandler(OwnerAdmin ownerAdmin) {
    this.ownerAdmin = ownerAdmin;
  }

  @POST
  @Path("/namespaces/{namespace-id}/apps/{app-id}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addAppOwner(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId,
                          @PathParam("app-id") String appId)
    throws BadRequestException, IOException, AlreadyExistsException {
    ApplicationId app = new ApplicationId(namespaceId, appId);
    KerberosPrincipalId kerberosPrincipalId = readKerberosPrincipal(request);
    ownerAdmin.add(app, kerberosPrincipalId);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Owner principal %s stored for entity %s", app, kerberosPrincipalId));
  }

  @POST
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addDatasetProperties(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("dataset-id") String datasetId)
    throws BadRequestException, IOException, AlreadyExistsException {
    DatasetId dataset = new DatasetId(namespaceId, datasetId);
    KerberosPrincipalId kerberosPrincipalId = readKerberosPrincipal(request);
    ownerAdmin.add(dataset, kerberosPrincipalId);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Owner principal %s stored for entity %s", dataset, kerberosPrincipalId));
  }

  @POST
  @Path("/namespaces/{namespace-id}/streams/{stream-id}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addStreamProperties(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("stream-id") String streamId)
    throws BadRequestException, IOException, AlreadyExistsException {
    StreamId stream = new StreamId(namespaceId, streamId);
    KerberosPrincipalId kerberosPrincipalId = readKerberosPrincipal(request);
    ownerAdmin.add(stream, kerberosPrincipalId);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Owner principal %s stored for entity %s", stream, kerberosPrincipalId));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}")
  public void getAppOwner(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId,
                          @PathParam("app-id") String appId) throws IOException {
    ApplicationId app = new ApplicationId(namespaceId, appId);
    responder.sendJson(HttpResponseStatus.OK, ownerAdmin.getOwner(app));
  }

  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}")
  public void getDatasetOwner(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId,
                              @PathParam("dataset-id") String datasetId) throws IOException {
    DatasetId dataset = new DatasetId(namespaceId, datasetId);
    responder.sendJson(HttpResponseStatus.OK, ownerAdmin.getOwner(dataset));
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}")
  public void getStreamOwner(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("stream-id") String streamId) throws IOException {
    StreamId stream = new StreamId(namespaceId, streamId);
    responder.sendJson(HttpResponseStatus.OK, ownerAdmin.getOwner(stream));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/impinfo")
  public void getAppImpersonationInfo(HttpRequest request, HttpResponder responder,
                                      @PathParam("namespace-id") String namespaceId,
                                      @PathParam("app-id") String appId) throws IOException {
    ApplicationId app = new ApplicationId(namespaceId, appId);
    responder.sendJson(HttpResponseStatus.OK, ownerAdmin.getOwner(app));
  }

  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/impinfo")
  public void getDatasetImpersonationInfo(HttpRequest request, HttpResponder responder,
                                          @PathParam("namespace-id") String namespaceId,
                                          @PathParam("dataset-id") String datasetId) throws IOException {
    DatasetId dataset = new DatasetId(namespaceId, datasetId);
    responder.sendJson(HttpResponseStatus.OK, ownerAdmin.getOwner(dataset));
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/impinfo")
  public void getStreamImpersonationInfo(HttpRequest request, HttpResponder responder,
                                         @PathParam("namespace-id") String namespaceId,
                                         @PathParam("stream-id") String streamId) throws IOException {
    StreamId stream = new StreamId(namespaceId, streamId);
    responder.sendJson(HttpResponseStatus.OK, ownerAdmin.getOwner(stream));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/apps/{app-id}")
  public void removeAppOwner(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-id") String appId) throws IOException {
    ApplicationId app = new ApplicationId(namespaceId, appId);
    ownerAdmin.delete(app);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Owner principal deleted for entity %s", app));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}")
  public void removeDatasetOwner(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("dataset-id") String datasetId) throws IOException {
    DatasetId dataset = new DatasetId(namespaceId, datasetId);

    ownerAdmin.delete(dataset);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Owner principal deleted for entity %s", dataset));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/streams/{stream-id}")
  public void removeStreamOwner(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("stream-id") String streamId) throws IOException {
    StreamId stream = new StreamId(namespaceId, streamId);
    ownerAdmin.delete(stream);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Owner principal deleted for entity %s", stream));
  }

  private KerberosPrincipalId readKerberosPrincipal(HttpRequest request) throws BadRequestException {
    String requestContent = request.getContent().toString(Charsets.UTF_8);
    if (Strings.isNullOrEmpty(requestContent)) {
      throw new BadRequestException("Request body is empty. Expecting a kerberos principal.");
    }
    return new KerberosPrincipalId(requestContent);
  }
}
