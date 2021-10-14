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

package io.cdap.cdap.internal.tether;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Mock tethering server handler used in unit tests.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class MockTetherServerHandler extends AbstractHttpHandler {
  private static final Gson GSON = new Gson();
  private HttpResponseStatus responseStatus = HttpResponseStatus.OK;
  private boolean tetherCreated = false;

  @GET
  @Path("/tethering/controlchannels/{peer}")
  public void getControlChannels(HttpRequest request, HttpResponder responder,
                                 @PathParam("peer") String peer) {
    Assert.assertEquals(TetherClientHandlerTest.CLIENT_INSTANCE, peer);
    Type type = new TypeToken<List<TetherControlMessage>>() { }.getType();
    if (responseStatus != HttpResponseStatus.OK) {
      responder.sendStatus(responseStatus);
      return;
    }
    List<TetherControlMessage> controlMessages = Collections.singletonList(
      new TetherControlMessage(TetherControlMessage.Type.KEEPALIVE, null));
    responder.sendJson(responseStatus, GSON.toJson(controlMessages, type));
  }

  @POST
  @Path("/tethering/connect")
  public void createTether(FullHttpRequest request, HttpResponder responder) {
    String content = request.content().toString(StandardCharsets.UTF_8);
    TetherConnectionRequest tetherRequest = GSON.fromJson(content, TetherConnectionRequest.class);
    Assert.assertEquals(TetherClientHandlerTest.CLIENT_INSTANCE, tetherRequest.getInstance());
    Assert.assertEquals(TetherClientHandlerTest.NAMESPACES, tetherRequest.getNamespaces());
    tetherCreated = true;
    responder.sendStatus(HttpResponseStatus.OK);
  }

  public void setResponseStatus(HttpResponseStatus responseStatus) {
    this.responseStatus = responseStatus;
  }

  public boolean isTetherCreated() {
    return tetherCreated;
  }

  public void setTetherCreated(boolean tetherCreated) {
    this.tetherCreated = tetherCreated;
  }
}
