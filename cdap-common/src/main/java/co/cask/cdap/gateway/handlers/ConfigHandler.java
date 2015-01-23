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
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteOrder;
import java.util.Map;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Handles requests to view {@link CConfiguration}.
 */
public class ConfigHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigHandler.class);

  private final CConfiguration cConf;
  private final Configuration hConf;

  @Inject
  public ConfigHandler(CConfiguration cConf, Configuration hConf) {
    this.cConf = cConf;
    this.hConf = hConf;
  }

  @Path("/config/cdap")
  @GET
  public void configCDAP(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                        @DefaultValue("json") @QueryParam("format") String format) {
    if ("json".equals(format)) {
      responder.sendJson(HttpResponseStatus.OK, toMap(cConf));
    } else if ("xml".equals(format)) {
      try {
        StringWriter stringWriter = new StringWriter();
        cConf.writeXml(stringWriter);
        responder.sendContent(HttpResponseStatus.OK, stringWriter2ChannelBuffer(stringWriter), "application/xml", null);
      } catch (IOException e) {
        LOG.info("Failed to write cConf to XML", e);
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    } else {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid format: " + format + ". Valid formats: json, xml");
    }
  }

  @Path("/config/hbase")
  @GET
  public void configHBase(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                          @DefaultValue("json") @QueryParam("format") String format) {
    if ("json".equals(format)) {
      responder.sendJson(HttpResponseStatus.OK, toMap(cConf));
    } else if ("xml".equals(format)) {
      try {
        StringWriter stringWriter = new StringWriter();
        hConf.writeXml(stringWriter);
        responder.sendContent(HttpResponseStatus.OK, stringWriter2ChannelBuffer(stringWriter), "application/xml", null);
      } catch (IOException e) {
        LOG.info("Failed to write hConf to XML", e);
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    } else {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid format: " + format + ". Valid formats: json, xml");
    }
  }

  private ChannelBuffer stringWriter2ChannelBuffer(StringWriter stringWriter) {
    return ChannelBuffers.copiedBuffer(ByteOrder.BIG_ENDIAN, stringWriter.toString(), Charsets.UTF_8);
  }

  private Map<String, String> toMap(Iterable<Map.Entry<String, String>> configuration) {
    ImmutableMap.Builder<String, String> result = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : configuration) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result.build();
  }
}
