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
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Objects;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Mock tethering server handler used in unit tests.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class MockTetheringServerHandler extends AbstractHttpHandler {
  private static final Gson GSON = new Gson();
  private HttpResponseStatus responseStatus = HttpResponseStatus.OK;
  private TetheringStatus tetheringStatus = TetheringStatus.PENDING;
  private String programStatus;

  @POST
  @Path("/tethering/channels/{peer}")
  public void getControlChannels(FullHttpRequest request, HttpResponder responder,
                                 @PathParam("peer") String peer) {
    Assert.assertEquals(TetheringClientHandlerTest.CLIENT_INSTANCE, peer);
    if (responseStatus != HttpResponseStatus.OK) {
      responder.sendStatus(responseStatus);
      return;
    }
    if (tetheringStatus != TetheringStatus.ACCEPTED) {
      TetheringControlResponseV2 controlResponse = new TetheringControlResponseV2(
        Collections.emptyList(), tetheringStatus);
      HttpResponseStatus respStatus = responseStatus;
      if (tetheringStatus == TetheringStatus.NOT_FOUND) {
        // Server returns 404 if tethering status is NOT_FOUND
        respStatus = HttpResponseStatus.NOT_FOUND;
      }
      responder.sendJson(respStatus, GSON.toJson(controlResponse, TetheringControlResponseV2.class));
      return;
    }

    TetheringControlMessage controlMessage = new TetheringControlMessage(TetheringControlMessage.Type.KEEPALIVE,
                                                                         new byte[0]);
    TetheringControlMessageWithId controlMessageWithId = new TetheringControlMessageWithId(controlMessage,
                                                                                           "1");
    String content = request.content().toString(StandardCharsets.UTF_8);
    TetheringControlChannelRequest controlChannelRequest = GSON.fromJson(content, TetheringControlChannelRequest.class);
    programStatus = controlChannelRequest.getNotificationList().stream()
      .map(n -> n.getProperties().get(ProgramOptionConstants.PROGRAM_STATUS))
      .filter(Objects::nonNull)
      .findFirst().orElse(null);

    TetheringControlResponseV2 controlResponse = new TetheringControlResponseV2(
      Collections.singletonList(controlMessageWithId), tetheringStatus);
    responder.sendJson(responseStatus, GSON.toJson(controlResponse, TetheringControlResponseV2.class));
  }

  @PUT
  @Path("/tethering/connections/{peer}")
  public void createTether(FullHttpRequest request, HttpResponder responder, @PathParam("peer") String peer) {
    String content = request.content().toString(StandardCharsets.UTF_8);
    TetheringConnectionRequest tetherRequest = GSON.fromJson(content, TetheringConnectionRequest.class);
    Assert.assertEquals(TetheringClientHandlerTest.CLIENT_INSTANCE, peer);
    Assert.assertEquals(TetheringClientHandlerTest.NAMESPACES, tetherRequest.getNamespaceAllocations());
    responder.sendStatus(responseStatus);
  }

  public void setResponseStatus(HttpResponseStatus responseStatus) {
    this.responseStatus = responseStatus;
  }

  public void setTetheringStatus(TetheringStatus tetheringStatus) {
    this.tetheringStatus = tetheringStatus;
  }

  public String getProgramStatus() {
    return programStatus;
  }
}
