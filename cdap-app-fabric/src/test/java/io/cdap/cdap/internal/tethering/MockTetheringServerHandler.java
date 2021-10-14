/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering;

import com.google.gson.Gson;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;

import java.nio.charset.StandardCharsets;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Mock tethering server handler used in unit tests.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class MockTetheringServerHandler extends AbstractHttpHandler {
  private static final Gson GSON = new Gson();
  private HttpResponseStatus responseStatus = HttpResponseStatus.OK;
  private boolean tetheringCreated = false;

  @GET
  @Path("/tethering/controlchannels/{peer}")
  public void getControlChannels(HttpRequest request, HttpResponder responder,
                                 @PathParam("peer") String peer,
                                 @QueryParam("messageId") String messageId) {
    Assert.assertEquals(TetheringClientHandlerTest.CLIENT_INSTANCE, peer);
    if (responseStatus != HttpResponseStatus.OK) {
      responder.sendStatus(responseStatus);
      return;
    }
    TetheringControlMessage keepalive = new TetheringControlMessage(TetheringControlMessage.Type.KEEPALIVE);
    TetheringControlResponse[] responses =  { new TetheringControlResponse(messageId, keepalive) };
    responder.sendJson(responseStatus, GSON.toJson(responses, TetheringControlResponse[].class));
  }

  @PUT
  @Path("/tethering/connections/{peer}")
  public void createTether(FullHttpRequest request, HttpResponder responder, @PathParam("peer") String peer) {
    String content = request.content().toString(StandardCharsets.UTF_8);
    TetheringConnectionRequest tetherRequest = GSON.fromJson(content, TetheringConnectionRequest.class);
    Assert.assertEquals(TetheringClientHandlerTest.CLIENT_INSTANCE, peer);
    Assert.assertEquals(TetheringClientHandlerTest.NAMESPACES, tetherRequest.getNamespaceAllocations());
    tetheringCreated = true;
    responder.sendStatus(HttpResponseStatus.OK);
  }

  public void setResponseStatus(HttpResponseStatus responseStatus) {
    this.responseStatus = responseStatus;
  }

  public boolean isTetheringCreated() {
    return tetheringCreated;
  }

  public void setTetheringCreated(boolean tetheringCreated) {
    this.tetheringCreated = tetheringCreated;
  }
}
