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

package io.cdap.cdap.internal.app.runtime.monitor;

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Networks;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;

/**
 * A {@link ChannelInboundHandler} for properly propagating runtime identity to calls to other system services.
 */
public class RuntimeIdentityHandler extends ChannelInboundHandlerAdapter {
  private static final String RUNTIME_IDENTITY = "runtime";

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (!(msg instanceof HttpRequest)) {
      ctx.fireChannelRead(msg);
      return;
    }

    HttpRequest request = (HttpRequest) msg;
    // TODO(CDAP-17754): This is a placeholder for proper runtime identity propagation.
    // For now, only support propagating the system-level root principal.
    request.headers().remove(HttpHeaderNames.AUTHORIZATION);
    request.headers().set(Constants.Security.Headers.USER_ID, RUNTIME_IDENTITY);
    String clientIP = Networks.getIP(ctx.channel().remoteAddress());
    if (clientIP != null) {
      request.headers().set(Constants.Security.Headers.USER_IP, clientIP);
    }
    ctx.fireChannelRead(msg);
  }
}
