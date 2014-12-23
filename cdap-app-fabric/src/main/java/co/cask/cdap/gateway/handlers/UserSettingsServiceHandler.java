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

import co.cask.cdap.app.config.ConfigService;
import co.cask.cdap.app.config.ConfigType;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Config Service HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/configuration/usersettings")
public class UserSettingsServiceHandler extends ConfigServiceHandler {
  private static final String PREFIX = "default";
  private static final Logger LOG = LoggerFactory.getLogger(UserSettingsServiceHandler.class);
  private static final Gson GSON = new Gson();

  @Inject
  public UserSettingsServiceHandler(Authenticator authenticator,
                                    @Named(Constants.ConfigService.USERSETTING) ConfigService configService) {
    super(authenticator, configService);
  }

  @Path("/properties/{property-name}")
  @GET
  public void getUserProperty(final HttpRequest request, final HttpResponder responder,
                            @PathParam("property-name") String property) throws Exception {
    getProperty(PREFIX, ConfigType.USER, getAuthenticatedAccountId(request), property, responder);
  }

  @Path("/properties/{property-name}")
  @DELETE
  public void deleteUserProperty(final HttpRequest request, final HttpResponder responder,
                               @PathParam("property-name") String property) throws Exception {
    deleteProperty(PREFIX, ConfigType.USER, getAuthenticatedAccountId(request), property, responder);
  }

  @Path("/properties/{property-name}")
  @PUT
  public void putUserProperty(final HttpRequest request, final HttpResponder responder,
                            @PathParam("property-name") String property) throws Exception {
    setProperty(PREFIX, ConfigType.USER, getAuthenticatedAccountId(request), property, request, responder);
  }

  @Path("/properties")
  @POST
  public void postUserProperty(final HttpRequest request, final HttpResponder responder) throws Exception {
    setProperties(PREFIX, ConfigType.USER, getAuthenticatedAccountId(request), request, responder);
  }

  @Path("/properties")
  @GET
  public void getUserProperties(final HttpRequest request, final HttpResponder responder) throws Exception {
    getProperties(PREFIX, ConfigType.USER, getAuthenticatedAccountId(request), responder);
  }

  @Path("/properties")
  @DELETE
  public void deleteUserProperties(final HttpRequest request, final HttpResponder responder) throws Exception {
    deleteProperties(PREFIX, ConfigType.USER, getAuthenticatedAccountId(request), responder);
  }
}
