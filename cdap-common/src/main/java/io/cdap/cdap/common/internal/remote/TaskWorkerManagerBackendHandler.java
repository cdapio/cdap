/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import io.cdap.cdap.common.conf.Constants;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Relays task worker responses back to AppFabric and updates the worker's lease from each
 * response.
 */
class TaskWorkerManagerBackendHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerManagerBackendHandler.class);

  /** The connection back to AppFabric; this handler sits on the worker pipeline. */
  private final Channel clientChannel;
  private final TaskWorkerLeaser leaser;
  private final String targetWorkerAddress;
  /** The namespace of the request sent to the worker on this connection. */
  private final String requestNamespace;

  private boolean decremented = false;
  /** Set once the frontend owns the reply (a 503) and the caller connection's close. */
  private boolean answeredByFrontend = false;

  TaskWorkerManagerBackendHandler(Channel clientChannel, TaskWorkerLeaser leaser,
      String targetWorkerAddress, String requestNamespace) {
    this.clientChannel = clientChannel;
    this.leaser = leaser;
    this.targetWorkerAddress = targetWorkerAddress;
    this.requestNamespace = requestNamespace;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (msg instanceof HttpResponse) {
      // STEP 1: Update pod availability, rejection cooldown, or activity from the response.
      HttpResponse resp = (HttpResponse) msg;

      if (isTerminating(resp)) {
        // The worker restarts after this response, whatever its status code: back off instead of
        // routing to it again or adopting its lease. This releases the slot now.
        leaser.markUnavailable(targetWorkerAddress, "a TERMINATING status");
        decremented = true;
      } else if (resp.status().code() == HttpResponseStatus.TOO_MANY_REQUESTS.code()) {
        String leasedNamespace =
            resp.headers().get(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE);
        leaser.markRejected(targetWorkerAddress, leasedNamespace, requestNamespace);
        decremented = true;
      } else {
        leaser.recordActivity(targetWorkerAddress);
      }
    } else if (msg instanceof LastHttpContent) {
      // STEP 2: Release the slot once the response completes.
      releaseSlot();
    }

    // STEP 3: Relay to AppFabric, then read the next chunk from the worker.
    clientChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
      if (future.isSuccess()) {
        ctx.channel().read();
      } else {
        LOG.warn("Failed to relay the response from task worker {} back to the caller. "
            + "Closing the caller connection.", targetWorkerAddress, future.cause());
        future.channel().close();
      }
    });
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) {
    // Request-direction backpressure: stop reading from the client while the worker can't keep up.
    Channel workerChannel = ctx.channel();
    if (clientChannel != null && clientChannel.isActive()) {
      clientChannel.config().setAutoRead(workerChannel.isWritable());
    }
    ctx.fireChannelWritabilityChanged();
  }

  /**
   * Returns whether the worker reports it is terminating. A missing or unknown status counts as
   * running, so the status code alone decides, as for a worker that sends no status.
   */
  private static boolean isTerminating(HttpResponse response) {
    return TaskWorkerStatus.TERMINATING.name().equals(
        response.headers().get(Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS));
  }

  private void releaseSlot() {
    if (!decremented) {
      leaser.releaseSlot(targetWorkerAddress);
      decremented = true;
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    releaseSlot();
    // If backend worker disconnects or crashes, flush and close the client socket, unless the
    // frontend is answering 503 and closes it once the request is drained.
    if (!answeredByFrontend) {
      TaskWorkerManagerFrontendHandler.closeOnFlush(clientChannel);
    }
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Error on the connection to task worker {}", targetWorkerAddress, cause);
    releaseSlot();
    TaskWorkerManagerFrontendHandler.closeOnFlush(ctx.channel());
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
    if (evt instanceof SslHandshakeCompletionEvent) {
      onHandshakeComplete((SslHandshakeCompletionEvent) evt);
    }
    ctx.fireUserEventTriggered(evt);
  }

  private void onHandshakeComplete(SslHandshakeCompletionEvent evt) {
    if (evt.isSuccess()) {
      // Cleared here, not on TCP connect, so a pod that fails TLS keeps escalating its backoff.
      leaser.markReachable(targetWorkerAddress);
      return;
    }
    // The caller already left, so we closed this mid-handshake; that says nothing about the pod.
    if (!clientChannel.isActive() || decremented) {
      return;
    }
    // Fires before exceptionCaught, so this request's slot is still held and released here.
    leaser.markUnavailable(targetWorkerAddress, "a failed TLS handshake");
    decremented = true;
    // No request byte got past TLS, so the task didn't start: let the frontend answer 503.
    // Both channels share one event loop, so this runs inline.
    answeredByFrontend = true;
    clientChannel.pipeline().fireUserEventTriggered(
        TaskWorkerManagerFrontendHandler.WorkerUnreachableEvent.INSTANCE);
  }
}
