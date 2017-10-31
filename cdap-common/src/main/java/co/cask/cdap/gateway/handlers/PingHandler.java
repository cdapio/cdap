/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.gson.JsonObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Handles ping requests.
 */
public class PingHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PingHandler.class);
  protected static final JsonObject OK_JSON;
  static {
    OK_JSON = new JsonObject();
    OK_JSON.addProperty("status", Constants.Monitor.STATUS_OK);
  }

  @Path("/ping")
  @GET
  public void ping(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder) {
    LOG.trace("Ping request received");
    responder.sendString(HttpResponseStatus.OK, "OK.\n");
  }

  @Path(Constants.Gateway.API_VERSION_3 + "/system/services/{service-name}/status")
  @GET
  public void status(HttpRequest request, HttpResponder responder) {
    // ignore the service-name, since we don't need it. its only used for routing
    responder.sendJson(HttpResponseStatus.OK, OK_JSON.toString());
  }
}
