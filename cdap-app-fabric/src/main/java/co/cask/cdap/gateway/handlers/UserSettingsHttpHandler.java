/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * User Settings HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/configuration/usersettings")
public class UserSettingsHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(UserSettingsHttpHandler.class);
  private static final Gson GSON = new Gson();

  private final Map<String, String> inMemoryConfigStore;

  @Inject
  public UserSettingsHttpHandler(Authenticator authenticator) {
    super(authenticator);
    this.inMemoryConfigStore = Maps.newHashMap();
  }

  @Path("/properties")
  @PUT
  public synchronized void setProperties(final HttpRequest request, final HttpResponder responder) throws Exception {
    inMemoryConfigStore.put(getAuthenticatedAccountId(request), GSON.toJson(decodeArguments(request)));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/properties")
  @GET
  public synchronized void getProperties(final HttpRequest request, final HttpResponder responder) throws Exception {
    String userId = getAuthenticatedAccountId(request);
    String properties = inMemoryConfigStore.containsKey(userId) ? inMemoryConfigStore.get(userId) :
      GSON.toJson(ImmutableMap.of());
    responder.sendString(HttpResponseStatus.OK, properties);
  }

  @Path("/properties")
  @DELETE
  public synchronized void deleteProperties(final HttpRequest request, final HttpResponder responder) throws Exception {
    inMemoryConfigStore.remove(getAuthenticatedAccountId(request));
    responder.sendStatus(HttpResponseStatus.OK);
  }
}
