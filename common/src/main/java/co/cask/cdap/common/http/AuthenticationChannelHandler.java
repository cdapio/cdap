/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.common.http;


import co.cask.cdap.common.conf.Constants;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

/**
 * An UpstreamHandler that verifies the userId in a request header and updates the {@code SecurityRequestContext}.
 */
public class AuthenticationChannelHandler extends SimpleChannelUpstreamHandler {
  public static final String HANDLER_NAME = "authenticator";
  private String currentUserId;

  /**
   * Decode the AccessTokenIdentifier passed as a header and set it in a ThreadLocal.
   * Returns a 401 if the identifier is malformed. 
   * @param ctx
   * @param e
   * @throws Exception
   */
  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    Object message = e.getMessage();
    if (!(message instanceof HttpRequest)) {
      if (currentUserId == null) {
        // TODO: throw correct exception
        throw new RuntimeException("No userId was found in request.");
      }
      SecurityRequestContext.setUserId(currentUserId);
      if (((HttpChunk) message).isLast()) {
        currentUserId = null;
      }
      super.messageReceived(ctx, e);
      return;
    }

    // TODO: authenticate the user using user id
    HttpRequest request = (HttpRequest) message;
    String userIdHeader = request.getHeader(Constants.Security.Headers.USER_ID);
    try {
      currentUserId = userIdHeader;
      SecurityRequestContext.setUserId(userIdHeader);
      super.messageReceived(ctx, e);
    } catch (Exception ex) {
      throw ex;
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    ChannelFuture future = Channels.future(ctx.getChannel());
    future.addListener(ChannelFutureListener.CLOSE);
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
    Channels.write(ctx, future, response);
    Channels.close(ctx, future);
  }
}
