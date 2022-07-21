/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.config.Config;
import io.cdap.cdap.config.ConfigNotFoundException;
import io.cdap.cdap.config.ConsoleSettingsStore;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * Console Settings HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/configuration/user")
public class ConsoleSettingsHttpHandler extends AbstractHttpHandler {

  private static final JsonParser JSON_PARSER = new JsonParser();

  private static final String CONFIG_PROPERTY = "property";
  private static final String ID = "id";

  private final ConsoleSettingsStore store;

  @Inject
  public ConsoleSettingsHttpHandler(ConsoleSettingsStore store) {
    this.store = store;
  }

  @Path("/")
  @GET
  public void get(HttpRequest request, HttpResponder responder) throws Exception {
    // The current behavior that only the creator should see the corresponding plugin template when RBAC is enabled is
    // correct. But the customers has been used to the old behavior that plugin template is exposed to all users in the
    // same namespace when RBAC is enabled. Revert the behavior to meet customers' requests.
    String userId = "";
    Config userConfig;
    try {
      userConfig = store.get(userId);
    } catch (ConfigNotFoundException e) {
      Map<String, String> propMap = ImmutableMap.of(CONFIG_PROPERTY, "{}");
      userConfig = new Config(userId, propMap);
    }

    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty(ID, userConfig.getName());

    //We store the serialized JSON string of the properties in ConfigStore and we return a JsonObject back
    jsonObject.add(CONFIG_PROPERTY, JSON_PARSER.parse(userConfig.getProperties().get(CONFIG_PROPERTY)));
    responder.sendJson(HttpResponseStatus.OK, jsonObject.toString());
  }

  @Path("/")
  @DELETE
  public void delete(HttpRequest request, HttpResponder responder) throws Exception {
    String userId = "";
    try {
      store.delete(userId);
    } catch (ConfigNotFoundException e) {
      // no-op if configuration does not exist - possible if nothing was 'put'
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/")
  @PUT
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void set(FullHttpRequest request, HttpResponder responder) throws Exception {
    String data = request.content().toString(StandardCharsets.UTF_8);
    if (!isValidJSON(data)) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
      return;
    }

    //Configuration Layout for UserSettings:
    //Config ID : userId
    //Config Properties : Map (Key = CONFIG_PROPERTY, Value = Serialized JSON string of properties)
    //User Settings configurations are stored under empty NAMESPACE.
    Map<String, String> propMap = ImmutableMap.of(CONFIG_PROPERTY, data);
    String userId = "";
    Config userConfig = new Config(userId, propMap);
    store.put(userConfig);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private boolean isValidJSON(String json) {
    try {
      JSON_PARSER.parse(json);
    } catch (JsonSyntaxException ex) {
      return false;
    }
    return true;
  }
}
