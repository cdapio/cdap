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
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;

/**
 * Helper Methods for handlers which use {@link ConfigService}.
 */
public abstract class ConfigServiceHandler extends AbstractAppFabricHttpHandler {
  private static final Gson GSON = new Gson();
  private final ConfigService configService;

  public ConfigServiceHandler(Authenticator authenticator, ConfigService configService) {
    super(authenticator);
    this.configService = configService;
  }

  protected void getProperty(String prefix, ConfigType type, String name, String property, HttpResponder responder)
    throws Exception {
    String value = configService.readSetting(prefix, type, name, property);
    if (value == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      responder.sendString(HttpResponseStatus.OK, value);
    }
  }

  protected void setProperty(String prefix, ConfigType type, String name, String key, HttpRequest request,
                             HttpResponder responder) throws Exception {
    String value = parseBody(request, String.class);
    if (!isConfigPresent(prefix, type, name)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      configService.writeSetting(prefix, type, name, key, value);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  protected void deleteProperty(String prefix, ConfigType type, String name, String key, HttpResponder responder)
    throws Exception {
    String value = configService.readSetting(prefix, type, name, key);
    if (value != null) {
      configService.deleteSetting(prefix, type, name, key);
      responder.sendStatus(HttpResponseStatus.OK);
    } else {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  protected void getProperties(String prefix, ConfigType type, String name, HttpResponder responder)
    throws Exception {
    if (!isConfigPresent(prefix, type, name)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      Map<String, String> settings = configService.readSetting(prefix, type, name);
      responder.sendString(HttpResponseStatus.OK, GSON.toJson(settings));
    }
  }

  protected void setProperties(String prefix, ConfigType type, String name, HttpRequest request,
                               HttpResponder responder) throws Exception {
    Map<String, String> settings = decodeArguments(request);
    if (!isConfigPresent(prefix, type, name)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      configService.writeSetting(prefix, type, name, settings);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  protected void deleteProperties(String prefix, ConfigType type, String name, HttpResponder responder)
    throws Exception {
    if (!isConfigPresent(prefix, type, name)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      configService.deleteSetting(prefix, type, name);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  protected boolean isConfigPresent(String prefix, ConfigType type, String name) throws Exception {
    return configService.checkConfig(prefix, type, name);
  }
}
