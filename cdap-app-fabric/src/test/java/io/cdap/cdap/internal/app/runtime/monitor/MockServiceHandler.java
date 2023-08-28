/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A mock handler for testing service routing. It provides service endpoints for
 * testing.
 */
@Path("/mock")
public final class MockServiceHandler extends AbstractHttpHandler {

  @Path("/get/{status}")
  @GET
  public void get(HttpRequest request, HttpResponder responder,
      @PathParam("status") int statusCode) {
    responder.sendString(HttpResponseStatus.valueOf(statusCode),
        "Status is " + statusCode);
  }

  @Path("/delete/{status}")
  @DELETE
  public void delete(HttpRequest request, HttpResponder responder,
      @PathParam("status") int statusCode) {
    responder.sendString(HttpResponseStatus.valueOf(statusCode),
        "Status is " + statusCode);
  }

  @Path("/put/{status}")
  @PUT
  public void put(FullHttpRequest request, HttpResponder responder,
      @PathParam("status") int statusCode) {
    responder.sendString(HttpResponseStatus.valueOf(statusCode),
        request.content().toString(
            StandardCharsets.UTF_8));
  }

  @Path("/post/{status}")
  @POST
  public void post(FullHttpRequest request, HttpResponder responder,
      @PathParam("status") int statusCode) {
    responder.sendString(HttpResponseStatus.valueOf(statusCode),
        request.content().toString(StandardCharsets.UTF_8));
  }
}
