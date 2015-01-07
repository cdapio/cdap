/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.config.Config;
import co.cask.cdap.config.ConfigNotFoundException;
import co.cask.cdap.config.ConfigStore;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
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
public class UserSettingsHttpHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(UserSettingsHttpHandler.class);
  private static final String CONFIG_TYPE = "usersettings";
  private static final String PROPERTY_NAME = "property";
  private static final String NAMESPACE = "default";
  private static final String ID = "id";

  private final ConfigStore configStore;

  @Inject
  public UserSettingsHttpHandler(Authenticator authenticator, ConfigStore configStore) {
    super(authenticator);
    this.configStore = configStore;
  }

  @Path("/")
  @GET
  public void get(final HttpRequest request, final HttpResponder responder) throws Exception {
    String userId = getAuthenticatedAccountId(request);
    Config userConfig;
    try {
      userConfig = configStore.get(NAMESPACE, CONFIG_TYPE, userId);
    } catch (ConfigNotFoundException e) {
      Map<String, String> propMap = ImmutableMap.of(PROPERTY_NAME, "{}");
      userConfig = new Config(userId, propMap);
    }

    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty(ID, userConfig.getId());
    jsonObject.add(PROPERTY_NAME, new JsonParser().parse(userConfig.getProperties().get(PROPERTY_NAME)));
    responder.sendJson(HttpResponseStatus.OK, jsonObject);
  }

  @Path("/")
  @DELETE
  public void delete(final HttpRequest request, final HttpResponder responder) throws Exception {
    String userId = getAuthenticatedAccountId(request);
    try {
      configStore.delete(NAMESPACE, CONFIG_TYPE, userId);
    } catch (ConfigNotFoundException e) {
      // no-op if configuration does not exist - possible if nothing was 'put'
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/")
  @PUT
  public void set(final HttpRequest request, final HttpResponder responder) throws Exception {
    String data = request.getContent().toString(Charsets.UTF_8);
    if (!isValidJSON(data)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
      return;
    }

    Map<String, String> propMap = ImmutableMap.of(PROPERTY_NAME, data);
    String userId = getAuthenticatedAccountId(request);
    Config userConfig = new Config(userId, propMap);
    try {
      configStore.update(NAMESPACE, CONFIG_TYPE, userConfig);
    } catch (ConfigNotFoundException e) {
      configStore.create(NAMESPACE, CONFIG_TYPE, userConfig);
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private boolean isValidJSON(String json) {
    try {
      new JsonParser().parse(json);
    } catch (JsonSyntaxException ex) {
      return false;
    }
    return true;
  }
}
