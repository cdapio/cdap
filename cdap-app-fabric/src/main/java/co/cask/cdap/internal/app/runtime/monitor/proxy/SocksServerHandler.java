/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.monitor.proxy;

import co.cask.cdap.internal.app.runtime.monitor.SSHSessionProvider;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.socksx.SocksMessage;
import io.netty.handler.codec.socksx.v5.DefaultSocks5InitialResponse;
import io.netty.handler.codec.socksx.v5.Socks5AuthMethod;
import io.netty.handler.codec.socksx.v5.Socks5CommandRequestDecoder;
import io.netty.handler.codec.socksx.v5.Socks5InitialRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotSupportedException;

/**
 * A {@link ChannelHandler} for handling SOCKS handshake requests.
 */
final class SocksServerHandler extends SimpleChannelInboundHandler<SocksMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(SocksServerHandler.class);

  private final SSHSessionProvider sshSessionProvider;

  SocksServerHandler(SSHSessionProvider sshSessionProvider) {
    this.sshSessionProvider = sshSessionProvider;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, SocksMessage msg) throws Exception {
    switch (msg.version()) {
      case SOCKS4a:
        ctx.pipeline().remove(this);
        ctx.pipeline().addLast(new SocksServerConnectHandler(sshSessionProvider));
        ctx.fireChannelRead(msg);
        break;
      case SOCKS5:
        // For SOCKS5 protocol, needs to handle the initial authentication handshake
        if (msg instanceof Socks5InitialRequest) {
          ctx.pipeline().remove(this);
          ctx.pipeline().addFirst(new Socks5CommandRequestDecoder());
          ctx.pipeline().addLast(new SocksServerConnectHandler(sshSessionProvider));
          ctx.write(new DefaultSocks5InitialResponse(Socks5AuthMethod.NO_AUTH));
        } else {
          // We don't expect other type of Socks5 message
          throw new NotSupportedException("Unsupported SOCKS message " + msg);
        }
        break;
      default:
        throw new NotSupportedException("Unsupported SOCKS version '" + msg.version() + "'");
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Exception raised while handling socks request for {}", ctx.channel(), cause);
    } else {
      LOG.debug("Exception raised while handling socks request for {} due to {}", ctx.channel(), cause);
    }

    // Just close the channel if there is exception
    ctx.close();
  }
}
