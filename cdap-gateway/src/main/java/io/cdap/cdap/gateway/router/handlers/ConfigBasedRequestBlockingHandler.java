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

package io.cdap.cdap.gateway.router.handlers;

import io.cdap.cdap.common.conf.Configuration;
import io.cdap.cdap.common.conf.Constants;
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
 * Handler to block all the inbound requests if config-declared error is enabled in {@link #cConf}
 */
public class ConfigBasedRequestBlockingHandler extends ChannelInboundHandlerAdapter {

  private final Configuration cConf;

  public ConfigBasedRequestBlockingHandler(Configuration cConf) {
    this.cConf = cConf;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (!(msg instanceof HttpRequest)
        || !cConf.getBoolean(Constants.Router.BLOCK_REQUEST_ENABLED)) {
      ctx.fireChannelRead(msg);
      return;
    }

    try {
      int statusCode = cConf.getInt(Constants.Router.BLOCK_REQUEST_STATUS_CODE);
      String responseMsg = cConf.get(Constants.Router.BLOCK_REQUEST_MESSAGE, "");

      ByteBuf content = Unpooled.copiedBuffer(responseMsg, StandardCharsets.UTF_8);
      HttpResponse response = new DefaultFullHttpResponse(
          HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(statusCode), content
      );
      HttpUtil.setContentLength(response, content.readableBytes());
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json;charset=UTF-8");
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }
}
