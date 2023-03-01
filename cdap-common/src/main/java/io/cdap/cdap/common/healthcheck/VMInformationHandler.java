/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.common.healthcheck;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.security.InstancePermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Health Check HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class VMInformationHandler extends AbstractHttpHandler {

  private static final Gson GSON = new Gson();

  private final ContextAccessEnforcer contextAccessEnforcer;

  @Inject
  VMInformationHandler(ContextAccessEnforcer contextAccessEnforcer) {
    this.contextAccessEnforcer = contextAccessEnforcer;
  }

  @GET
  @Path("/vminfo")
  public void call(HttpRequest request, HttpResponder responder) {
    // ensure the user has authentication to get health check
    contextAccessEnforcer.enforce(InstanceId.SELF, InstancePermission.HEALTH_CHECK);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(VMInformation.collect()));
  }
}
