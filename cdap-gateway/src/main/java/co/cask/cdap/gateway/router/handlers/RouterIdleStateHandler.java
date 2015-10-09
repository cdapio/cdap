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

import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.timeout.IdleState;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles states when the router has been idle for a configured time interval.
 */
public class RouterIdleStateHandler extends IdleStateAwareChannelHandler {
  private static final Logger LOG = LoggerFactory.getLogger(RouterIdleStateHandler.class);

  private final AtomicInteger numConnectionsClosed = new AtomicInteger(0);

  @Override
  public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e) throws Exception {
    LOG.debug("Received idle event '{}' for channel '{}'.", e.getState(), ctx.getName());
    if (IdleState.ALL_IDLE == e.getState()) {
      // No data has been sent or received for a while. Close channel.
      ctx.getChannel().close().addListener(ChannelFutureListener.CLOSE);
      numConnectionsClosed.incrementAndGet();
      LOG.trace("No data has been sent or received for channel '{}' for more than the configured idle timeout. " +
                  "Closing the channel. Number of connections closed so far = {}.",
                ctx.getChannel(), numConnectionsClosed.get());
    }
    super.channelIdle(ctx, e);
  }
}
