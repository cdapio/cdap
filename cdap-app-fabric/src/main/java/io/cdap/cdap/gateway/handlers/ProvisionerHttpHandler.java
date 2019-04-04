/*
 * Copyright © 2018-2019 Cask Data, Inc.
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
import com.google.gson.GsonBuilder;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.proto.provisioner.ProvisionerDetail;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link io.cdap.http.HttpHandler} for managing provisioners.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class ProvisionerHttpHandler extends AbstractHttpHandler {
  private static final Gson GSON = new GsonBuilder().create();
  private final ProvisioningService provisioningService;

  @Inject
  public ProvisionerHttpHandler(ProvisioningService provisioningService) {
    this.provisioningService = provisioningService;
  }

  @GET
  @Path("/provisioners")
  public void getProvisioners(HttpRequest request, HttpResponder responder) {
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(provisioningService.getProvisionerDetails()));
  }

  @GET
  @Path("/provisioners/{provisioner-name}")
  public void getProvisioner(HttpRequest request, HttpResponder responder,
                             @PathParam("provisioner-name") String provisionerName) throws NotFoundException {
    ProvisionerDetail provisionerDetail = provisioningService.getProvisionerDetail(provisionerName);
    if (provisionerDetail == null) {
      throw new NotFoundException(String.format("Provisioner %s not found", provisionerName));
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(provisionerDetail));
  }
}
