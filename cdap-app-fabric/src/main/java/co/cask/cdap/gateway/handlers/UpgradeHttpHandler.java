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

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.services.AppVersionUpgradeService;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * {@link HttpHandler} for getting info about upgrade progress, if applicable.
 */
@Singleton
@Beta
@Path(Constants.Gateway.API_VERSION_3 + "/system/upgrade")
public class UpgradeHttpHandler extends AbstractHttpHandler {
  private final AppVersionUpgradeService appVersionUpgradeService;

  @Inject
  UpgradeHttpHandler(AppVersionUpgradeService appVersionUpgradeService) {
    this.appVersionUpgradeService = appVersionUpgradeService;
  }

  @GET
  @Path("/status")
  public void getUpgradeStatus(HttpRequest request, HttpResponder responder) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, appVersionUpgradeService.getUpgradeStatus());
  }
}
