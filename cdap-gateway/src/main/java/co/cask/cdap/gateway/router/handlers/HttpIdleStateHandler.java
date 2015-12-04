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

import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.LifeCycleAwareChannelHandler;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.util.ExternalResourceReleasable;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;

import java.util.concurrent.TimeUnit;

import static org.jboss.netty.channel.Channels.fireExceptionCaught;

/**
 * Closes the channel when an HTTP request is not in session or has been in session for the configured timeout.
 *
 * The {@link Timer} which was specified when the {@link HttpIdleStateHandler} is
 * created should be stopped manually by calling {@link #releaseExternalResources()}
 * or {@link Timer#stop()} when your application shuts down.
 *
 */
@ChannelHandler.Sharable
public class HttpIdleStateHandler extends SimpleChannelHandler
  implements LifeCycleAwareChannelHandler, ExternalResourceReleasable {

  private final Timer timer;
  private final long requestIdleTimeMillis;

  /**
   * Creates a new instance.
   *
   * @param timer the {@link Timer} that is used to trigger the scheduled channel close.
   *        The recommended {@link Timer} implementation is {@link HashedWheelTimer}.
   * @param requestIdleTimeSeconds the associated channel will be closed request was performed for the specified
   *        period of time.  Specify {@code 0} to disable.
   */
  public HttpIdleStateHandler(Timer timer, int requestIdleTimeSeconds) {
    this(timer, requestIdleTimeSeconds, TimeUnit.SECONDS);
  }

  /**
   * Creates a new instance.
   *
   * @param timer the {@link Timer} that is used to trigger the scheduled event.
   *        The recommended {@link Timer} implementation is {@link HashedWheelTimer}.
   * @param requestIdleTime the associated channel will be closed request was performed for the specified
   *        period of time.  Specify {@code 0} to disable.
   * @param unit the {@link TimeUnit} of {@code requestIdleTime}, {@code writeIdleTime}, and {@code allIdleTime}
   */
  public HttpIdleStateHandler(Timer timer, long requestIdleTime, TimeUnit unit) {
    if (timer == null) {
      throw new NullPointerException("timer");
    }
    if (unit == null) {
      throw new NullPointerException("unit");
    }

    this.timer = timer;
    if (requestIdleTime <= 0) {
      requestIdleTimeMillis = 0;
    } else {
      requestIdleTimeMillis = Math.max(unit.toMillis(requestIdleTime), 1);
    }
  }

  /**
   * Return the configured requestIdleTime, in milliseconds.
   *
   */
  public long getRequestIdleTimeInMillis() {
    return requestIdleTimeMillis;
  }

  /**
   * Stops the {@link Timer} which was specified in the constructor of this
   * handler.  You should not call this method if the {@link Timer} is in use
   * by other objects.
   */
  public void releaseExternalResources() {
    timer.stop();
  }

  public void beforeAdd(ChannelHandlerContext ctx) throws Exception {
    if (ctx.getPipeline().isAttached()) {
      // channelOpen event has been fired already, which means
      // this.channelOpen() will not be invoked.
      // We have to initialize here instead.
      initialize(ctx);
    } else {
      // channelOpen event has not been fired yet.
      // this.channelOpen() will be invoked and initialization will occur there.
    }
  }

  public void afterAdd(ChannelHandlerContext ctx) throws Exception {
    // NOOP
  }

  public void beforeRemove(ChannelHandlerContext ctx) throws Exception {
    destroy(ctx);
  }

  public void afterRemove(ChannelHandlerContext ctx) throws Exception {
    // NOOP
  }

  @Override
  public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
    throws Exception {
    // This method will be invoked only if this handler was added
    // before channelOpen event is fired.  If a user adds this handler
    // after the channelOpen event, initialize() will be called by beforeAdd().
    initialize(ctx);
    ctx.sendUpstream(e);
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
    throws Exception {
    destroy(ctx);
    ctx.sendUpstream(e);
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
    throws Exception {
    Object message = e.getMessage();
    if (message instanceof HttpResponse) {
      HttpResponse response = (HttpResponse) message;
      if (!response.isChunked()) {
        markRequestEnd(ctx);
      }
    } else if (message instanceof HttpChunk) {
      HttpChunk chunk = (HttpChunk) message;

      if (chunk.isLast()) {
        markRequestEnd(ctx);
      }
    }

    ctx.sendUpstream(e);
  }

  private void markRequestEnd(ChannelHandlerContext ctx) {
    State state = (State) ctx.getAttachment();
    state.lastMessageTime = System.currentTimeMillis();
    state.requestInProgress = false;
  }

  private void markRequestInProgress(ChannelHandlerContext ctx) {
    // TODO: check if already in progress. if so, return.
    // this helps avoid the call to System#currentTimeMillis,
    // at the cost of `lastMessageTime` being inaccurate when requestInProgress is true

    State state = (State) ctx.getAttachment();
    state.lastMessageTime = System.currentTimeMillis();
    state.requestInProgress = true;
  }

  @Override
  public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    Object message = e.getMessage();
    if (message instanceof HttpRequest || message instanceof HttpChunk) {
      markRequestInProgress(ctx);
    }
    ctx.sendDownstream(e);
  }

  private void initialize(ChannelHandlerContext ctx) {
    State state = state(ctx);

    // Avoid the case where destroy() is called before scheduling timeouts.
    // See: https://github.com/netty/netty/issues/143
    synchronized (state) {
      switch (state.state) {
        case 1:
        case 2:
          return;
      }
      state.state = 1;
    }

    state.lastMessageTime = System.currentTimeMillis();
    if (requestIdleTimeMillis > 0) {
      state.requestIdleTimeout = timer.newTimeout(new RequestIdleTimeoutTask(ctx),
                                                  requestIdleTimeMillis, TimeUnit.MILLISECONDS);
    }
  }

  private static void destroy(ChannelHandlerContext ctx) {
    State state = state(ctx);
    synchronized (state) {
      if (state.state != 1) {
        return;
      }
      state.state = 2;
    }

    if (state.requestIdleTimeout != null) {
      state.requestIdleTimeout.cancel();
      state.requestIdleTimeout = null;
    }
  }

  private static State state(ChannelHandlerContext ctx) {
    synchronized (ctx) {
      State state = (State) ctx.getAttachment();
      if (state != null) {
        return state;
      }
      state = new State();
      ctx.setAttachment(state);
      return state;
    }
  }

  private void fireChannelIdle(final ChannelHandlerContext ctx) {
    ctx.getPipeline().execute(new Runnable() {

      public void run() {
        try {
          ctx.getChannel().close();
        } catch (Throwable t) {
          fireExceptionCaught(ctx, t);
        }
      }
    });
  }

  private final class RequestIdleTimeoutTask implements TimerTask {

    private final ChannelHandlerContext ctx;

    RequestIdleTimeoutTask(ChannelHandlerContext ctx) {
      this.ctx = ctx;
    }

    public void run(Timeout timeout) throws Exception {
      if (timeout.isCancelled() || !ctx.getChannel().isOpen()) {
        return;
      }

      State state = (State) ctx.getAttachment();

      long nextDelay = computeNextDelay(state);
      if (!state.requestInProgress && nextDelay <= 0) {
        // request is idle - set a new timeout and notify the callback.
        state.requestIdleTimeout = timer.newTimeout(this, requestIdleTimeMillis, TimeUnit.MILLISECONDS);
        fireChannelIdle(ctx);
      } else {
        // either request is in progress or was in progress within the past requestIdleTimeMillis
        state.requestIdleTimeout = timer.newTimeout(this, nextDelay, TimeUnit.MILLISECONDS);
      }
    }

    private long computeNextDelay(State state) {
      if (state.requestInProgress) {
        return requestIdleTimeMillis;
      } else {
        long currentTime = System.currentTimeMillis();
        long lastMessageTime = state.lastMessageTime;
        return requestIdleTimeMillis - (currentTime - lastMessageTime);
      }
    }
  }

  private static final class State {
    // 0 - none, 1 - initialized, 2 - destroyed
    int state;

    volatile Timeout requestIdleTimeout;
    volatile long lastMessageTime;
    volatile boolean requestInProgress;

    State() {
    }
  }
}
