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

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.socksx.SocksMessage;
import io.netty.handler.codec.socksx.v5.DefaultSocks5InitialResponse;
import io.netty.handler.codec.socksx.v5.DefaultSocks5PasswordAuthResponse;
import io.netty.handler.codec.socksx.v5.Socks5AuthMethod;
import io.netty.handler.codec.socksx.v5.Socks5CommandRequestDecoder;
import io.netty.handler.codec.socksx.v5.Socks5InitialRequest;
import io.netty.handler.codec.socksx.v5.Socks5PasswordAuthRequest;
import io.netty.handler.codec.socksx.v5.Socks5PasswordAuthRequestDecoder;
import io.netty.handler.codec.socksx.v5.Socks5PasswordAuthStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.NotSupportedException;

/**
 * Abstract base class for implementing SOCKS proxy handler.
 */
abstract class AbstractSocksServerHandler extends SimpleChannelInboundHandler<SocksMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSocksServerHandler.class);

  @Nullable
  protected ChannelHandler createSocks4ConnectHandler() {
    return null;
  }

  @Nullable
  protected abstract ChannelHandler createSocks5ConnectHandler();

  /**
   * Returns {@code true} if password authentication is needed for Socks5 connection.
   */
  protected boolean requireAuthentication() {
    return false;
  }

  /**
   * Authenticates the given username and password.
   *
   * @param username the username
   * @param password the password
   * @return {@code true} if authenticated, {@code false} otherwise
   */
  protected boolean authenticate(String username, String password) {
    return true;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, SocksMessage msg) {
    switch (msg.version()) {
      case SOCKS4a: {
        ChannelHandler connectHandler = createSocks4ConnectHandler();
        if (connectHandler == null) {
          throw new NotSupportedException("SOCKS4 is not supported");
        }
        ctx.pipeline().remove(this);
        ctx.pipeline().addLast(connectHandler);
        ctx.fireChannelRead(msg);
        break;
      }
      case SOCKS5: {
        ChannelHandler connectHandler = createSocks5ConnectHandler();
        if (connectHandler == null) {
          throw new NotSupportedException("SOCKS5 is not supported");
        }

        // For SOCKS5 protocol, needs to handle the initial authentication handshake
        if (msg instanceof Socks5InitialRequest) {
          if (requireAuthentication()) {
            ctx.pipeline().addFirst("auth", new Socks5PasswordAuthRequestDecoder());
            ctx.write(new DefaultSocks5InitialResponse(Socks5AuthMethod.PASSWORD));
          } else {
            ctx.pipeline().remove(this);
            ctx.pipeline().addFirst(new Socks5CommandRequestDecoder());
            ctx.pipeline().addLast(connectHandler);
            ctx.write(new DefaultSocks5InitialResponse(Socks5AuthMethod.NO_AUTH));
          }
        } else if (msg instanceof Socks5PasswordAuthRequest) {
          Socks5PasswordAuthRequest authRequest = (Socks5PasswordAuthRequest) msg;
          if (!authenticate(authRequest.username(), authRequest.password())) {
            ctx.write(new DefaultSocks5PasswordAuthResponse(Socks5PasswordAuthStatus.FAILURE))
              .addListener(ChannelFutureListener.CLOSE);
          } else {
            ctx.pipeline().remove("auth");
            ctx.pipeline().remove(this);
            ctx.pipeline().addFirst(new Socks5CommandRequestDecoder());
            ctx.pipeline().addLast(connectHandler);
            ctx.write(new DefaultSocks5PasswordAuthResponse(Socks5PasswordAuthStatus.SUCCESS));
          }
        } else {
          // We don't expect other type of Socks5 message
          throw new NotSupportedException("Unsupported SOCKS message " + msg);
        }
        break;
      }
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
