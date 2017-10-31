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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Handles requests to view {@link CConfiguration}.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class ConfigHandler extends AbstractHttpHandler {

  private static final Gson GSON = new Gson();
  private final ConfigService configService;

  @Inject
  public ConfigHandler(ConfigService configService) {
    this.configService = configService;
  }

  @Path("/config/cdap")
  @GET
  public void configCDAP(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                         @DefaultValue("json") @QueryParam("format") String format) throws IOException {
    if ("json".equals(format)) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(configService.getCConf()));
    } else if ("xml".equals(format)) {
      responder.sendString(HttpResponseStatus.OK, configService.getCConfXMLString(),
                           new DefaultHttpHeaders().set(HttpHeaderNames.CONTENT_TYPE, "application/xml"));
    } else {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid format: " + format + ". Valid formats: json, xml");
    }
  }

  @Path("/config/hadoop")
  @GET
  public void configHadoop(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                          @DefaultValue("json") @QueryParam("format") String format) throws IOException {
    if ("json".equals(format)) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(configService.getHConf()));
    } else if ("xml".equals(format)) {
      responder.sendString(HttpResponseStatus.OK, configService.getHConfXMLString(),
                           new DefaultHttpHeaders().set(HttpHeaderNames.CONTENT_TYPE, "application/xml"));
    } else {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid format: " + format + ". Valid formats: json, xml");
    }
  }
}
