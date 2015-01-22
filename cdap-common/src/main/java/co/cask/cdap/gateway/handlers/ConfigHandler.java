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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.Iterator;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Handles requests to view {@link CConfiguration}.
 */
public class ConfigHandler extends AbstractHttpHandler {

  private final CConfiguration configuration;

  @Inject
  public ConfigHandler(CConfiguration configuration) {
    this.configuration = configuration;
  }

  @Path("/config")
  @GET
  public void ping(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder) {
    StringBuilder result = new StringBuilder();
    result.append("<configuration>\n");
    for (Map.Entry<String, String> entry : configuration) {
      result.append("  <property>\n");
      result.append("    <name>").append(entry.getKey()).append("</name>\n");
      result.append("    <value>").append(entry.getValue()).append("</value>\n");
      result.append("  </property>\n");
    }
    result.append("</configuration>\n");
    responder.sendString(HttpResponseStatus.OK, result.toString() + "\n");
  }
}
