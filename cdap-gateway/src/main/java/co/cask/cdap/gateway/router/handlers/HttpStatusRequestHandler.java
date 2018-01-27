/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package co.cask.cdap.gateway.router.handlers;

import co.cask.cdap.common.conf.Constants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.StandardCharsets;

/**
 * Handler for the status requests
 */
public class HttpStatusRequestHandler extends ChannelInboundHandlerAdapter {

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (!(msg instanceof HttpRequest)) {
      ctx.fireChannelRead(msg);
      return;
    }

    try {
      HttpRequest request = (HttpRequest) msg;
      // To have an independent health check of the router status, the command should
      // be served by the router itself without it talking to any downstream services
      if (request.uri().equals(Constants.EndPoints.STATUS)) {
        ByteBuf content = Unpooled.copiedBuffer(Constants.Monitor.STATUS_OK, StandardCharsets.UTF_8);

        HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
        HttpUtil.setContentLength(response, content.readableBytes());
        HttpUtil.setKeepAlive(response, false);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=utf-8");

        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
      } else {
        ReferenceCountUtil.retain(msg);
        ctx.fireChannelRead(msg);
      }
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }
}
