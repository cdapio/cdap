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
 *
 */

package io.cdap.cdap.gateway.handlers;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.bootstrap.BootstrapService;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Bootstrap HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class BootstrapHttpHandler extends AbstractHttpHandler {
  private final BootstrapService bootstrapService;

  @Inject
  BootstrapHttpHandler(BootstrapService bootstrapService) {
    this.bootstrapService = bootstrapService;
  }

  @POST
  @Path("/bootstrap")
  public void bootstrap(HttpRequest request, HttpResponder responder) throws InterruptedException {
    bootstrapService.reload();
    bootstrapService.bootstrap();
    responder.sendStatus(HttpResponseStatus.OK);
  }
}
