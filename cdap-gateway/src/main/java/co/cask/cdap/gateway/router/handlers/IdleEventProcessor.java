/*
 * Copyright © 2015 Cask Data, Inc.
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

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.timeout.IdleState;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles states when a channel has been idle for a configured time interval, by closing the channel if an
 * HTTP Request is not in progress.
 */
public class IdleEventProcessor extends IdleStateAwareChannelHandler {
  private static final Logger LOG = LoggerFactory.getLogger(IdleEventProcessor.class);
  private boolean requestInProgress;

  @Override
  public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e) throws Exception {
    if (IdleState.ALL_IDLE == e.getState()) {
      if (requestInProgress) {
        LOG.trace("Request is in progress, so not closing channel.");
      } else {
        // No data has been sent or received for a while. Close channel.
        Channel channel = ctx.getChannel();
        channel.close();
        LOG.trace("No data has been sent or received for channel '{}' for more than the configured idle timeout. " +
                    "Closing the channel. Local Address: {}, Remote Address: {}",
                  channel, channel.getLocalAddress(), channel.getRemoteAddress());
      }
    }
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
    throws Exception {
    Object message = e.getMessage();
    if (message instanceof HttpResponse) {
      HttpResponse response = (HttpResponse) message;
      if (!response.isChunked()) {
        requestInProgress = false;
      }
    } else if (message instanceof HttpChunk) {
      HttpChunk chunk = (HttpChunk) message;

      if (chunk.isLast()) {
        requestInProgress = false;
      }
    }

    ctx.sendUpstream(e);
  }

  @Override
  public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    Object message = e.getMessage();
    if (message instanceof HttpRequest || message instanceof HttpChunk) {
      requestInProgress = true;
    }
    ctx.sendDownstream(e);
  }
}
