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

import co.cask.cdap.common.internal.remote.MethodArgument;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.http.HttpResponder;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * An {@link AbstractRemoteSystemOpsHandler} for serving HTTP requests to list privileges of a user.
 */
@Path(AbstractRemoteSystemOpsHandler.VERSION + "/execute")
public class RemotePrivilegeFetcherHandler extends AbstractRemoteSystemOpsHandler {
  private static final Logger LOG = LoggerFactory.getLogger(RemotePrivilegeFetcherHandler.class);

  private final AuthorizerInstantiator authorizerInstantiator;

  @Inject
  RemotePrivilegeFetcherHandler(AuthorizerInstantiator authorizerInstantiator) {
    this.authorizerInstantiator = authorizerInstantiator;
  }

  @POST
  @Path("/listPrivileges")
  public void listPrivileges(HttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);
    Principal principal = deserializeNext(arguments);
    LOG.trace("Listing privileges for principal {}", principal);
    Set<Privilege> privileges = authorizerInstantiator.get().listPrivileges(principal);
    LOG.debug("Returning privileges for principal {} as {}", principal, privileges);
    responder.sendJson(HttpResponseStatus.OK, privileges);
  }
}
