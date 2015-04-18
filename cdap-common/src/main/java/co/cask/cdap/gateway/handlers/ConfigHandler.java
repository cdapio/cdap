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
import com.google.common.base.Charsets;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteOrder;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Handles requests to view {@link CConfiguration}.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class ConfigHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigHandler.class);
  private final ConfigService configService;

  @Inject
  public ConfigHandler(ConfigService configService) {
    this.configService = configService;
  }

  @Path("/config/cdap")
  @GET
  public void configCDAP(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                         @DefaultValue("json") @QueryParam("format") String format) {
    if ("json".equals(format)) {
      responder.sendJson(HttpResponseStatus.OK, configService.getCConf());
    } else if ("xml".equals(format)) {
      try {
        String xmlString = configService.getCConfXMLString();
        responder.sendContent(HttpResponseStatus.OK, newChannelBuffer(xmlString), "application/xml", null);
      } catch (IOException e) {
        LOG.info("Failed to write cConf to XML", e);
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    } else {

      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid format: " + format + ". Valid formats: json, xml");
    }
  }

  @Path("/config/hadoop")
  @GET
  public void configHadoop(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                          @DefaultValue("json") @QueryParam("format") String format) {
    if ("json".equals(format)) {
      responder.sendJson(HttpResponseStatus.OK, configService.getHConf());
    } else if ("xml".equals(format)) {
      try {
        String xmlString = configService.getHConfXMLString();
        responder.sendContent(HttpResponseStatus.OK, newChannelBuffer(xmlString), "application/xml", null);
      } catch (IOException e) {
        LOG.info("Failed to write hConf to XML", e);
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    } else {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid format: " + format + ". Valid formats: json, xml");
    }
  }

  private ChannelBuffer newChannelBuffer(String string) {
    return ChannelBuffers.copiedBuffer(ByteOrder.BIG_ENDIAN, string, Charsets.UTF_8);
  }
}
