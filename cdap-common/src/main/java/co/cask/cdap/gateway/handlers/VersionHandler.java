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
import co.cask.cdap.proto.Version;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Handles version requests.
 */
@Path(Constants.Gateway.API_VERSION_2)
public class VersionHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(VersionHandler.class);

  private final String version;

  public VersionHandler() {
    this.version = determineVersion();
  }

  @Path("/version")
  @GET
  public void version(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder) {
    responder.sendJson(HttpResponseStatus.OK, new Version(version));
  }

  private String determineVersion() {
    try {
      String version = Resources.toString(Resources.getResource("VERSION"), Charsets.UTF_8);
      if (!version.equals("${project.version}")) {
        return version.trim();
      }
    } catch (IOException e) {
      LOG.warn("Failed to determine current version", e);
    }
    return "unknown";
  }
}
