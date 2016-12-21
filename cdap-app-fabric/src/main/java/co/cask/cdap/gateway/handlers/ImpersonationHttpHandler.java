/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.security.ImpersonationInfo;
import co.cask.cdap.internal.app.runtime.impersonation.ImpersonationStore;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * {@link co.cask.http.HttpHandler} for managing impersonation info.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class ImpersonationHttpHandler extends AbstractHttpHandler {

  private static final Gson GSON = new Gson();

  private final ImpersonationStore impersonationStore;

  @Inject
  ImpersonationHttpHandler(ImpersonationStore impersonationStore) {
    this.impersonationStore = impersonationStore;
  }

  @PUT
  @Path("/impersonation/principals")
  public void put(HttpRequest request, HttpResponder responder) throws Exception {
    // TODO: validate the principal/keytab pair is valid
    String requestBody = request.getContent().toString(Charsets.UTF_8);
    if (requestBody.isEmpty()) {
      throw new BadRequestException("Expected ImpersonationInfo in body of request.");
    }
    ImpersonationInfo impersonationInfo;
    try {
      impersonationInfo = GSON.fromJson(requestBody, ImpersonationInfo.class);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException(e);
    }
    impersonationStore.addImpersonationInfo(impersonationInfo);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/impersonation/principals")
  public void get(HttpRequest request, HttpResponder responder,
                  @QueryParam("principal") String principal) throws Exception {
    if (principal == null) {
      responder.sendJson(HttpResponseStatus.OK, impersonationStore.listImpersonationInfos());
    } else {
      responder.sendJson(HttpResponseStatus.OK, impersonationStore.getImpersonationInfo(principal));
    }
  }

  @DELETE
  @Path("/impersonation/principals")
  public void delete(HttpRequest request, HttpResponder responder,
                  @QueryParam("principal") String principal) throws Exception {
    if (principal == null) {
      throw new BadRequestException("Must specify a principal to delete.");
    }
    impersonationStore.delete(principal);
    responder.sendStatus(HttpResponseStatus.OK);
  }
}
