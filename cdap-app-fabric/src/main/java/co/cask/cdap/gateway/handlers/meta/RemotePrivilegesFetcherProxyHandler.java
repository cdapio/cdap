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

package co.cask.cdap.gateway.handlers.meta;

import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.spi.authorization.PrivilegesFetcher;
import co.cask.http.HttpResponder;
import org.jboss.netty.handler.codec.http.HttpRequest;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * An {@link AbstractRemotePrivilegesFetcherHandler} that runs in the {@link RemoteSystemOperationsService}
 * and proxies list privileges requests to the master.
 */
@Path(AbstractRemoteSystemOpsHandler.VERSION + "/execute")
public class RemotePrivilegesFetcherProxyHandler extends AbstractRemotePrivilegesFetcherHandler {

  @Inject
  RemotePrivilegesFetcherProxyHandler(
    @Named(AuthorizationEnforcementModule.PRIVILEGES_FETCHER_PROXY_CACHE) PrivilegesFetcher privilegesFetcher) {
    super(privilegesFetcher);
  }

  @POST
  @Path("/listPrivileges")
  public void listPrivileges(HttpRequest request, HttpResponder responder) throws Exception {
    doListPrivileges(request, responder);
  }
}
