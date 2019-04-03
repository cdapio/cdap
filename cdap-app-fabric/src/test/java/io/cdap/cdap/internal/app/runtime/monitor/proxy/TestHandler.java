/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor.proxy;

import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * A {@link HttpHandler} for testing.
 */
public final class TestHandler extends AbstractHttpHandler {

  @GET
  @Path("/ping")
  public void ping(HttpRequest request, HttpResponder responder) {
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/echo")
  public void echo(FullHttpRequest request, HttpResponder responder) {
    responder.sendString(HttpResponseStatus.OK, request.content().toString(StandardCharsets.UTF_8));
  }

  @POST
  @Path("/chunk")
  public void chunk(FullHttpRequest request, HttpResponder responder) {
    ByteBuf content = request.content().copy();

    responder.sendContent(HttpResponseStatus.OK, new BodyProducer() {

      int count = 0;

      @Override
      public ByteBuf nextChunk() {
        if (count++ < 10) {
          return content.copy();
        }
        return Unpooled.EMPTY_BUFFER;
      }

      @Override
      public void finished() {
        // no-op
      }

      @Override
      public void handleError(@Nullable Throwable cause) {
        // no-op
      }
    }, new DefaultHttpHeaders());
  }
}
