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

import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Handles ping requests.
 */
public class PingHandler extends AbstractHttpHandler {

  @Path("/ping")
  @GET
  public void ping(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder) {
    responder.sendString(HttpResponseStatus.OK, "OK.\n");
  }


  @Path("/status")
  @GET
  public void status(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder) {
    responder.sendString(HttpResponseStatus.OK, "OK.\n");
  }
}
