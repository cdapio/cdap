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

package io.cdap.cdap.internal.app.worker.manager;

import io.cdap.cdap.common.conf.Constants;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.ReferenceCountUtil;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Queue;
import javax.net.ssl.SSLException;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes AppFabric task requests to a task worker leased to the request's namespace and
 * streams the request to it.
 */
class TaskWorkerManagerFrontendHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerManagerFrontendHandler.class);

  /** Built once, since creating an SslContext per connection is expensive. */
  private static final SslContext CLIENT_SSL_CONTEXT = createClientSslContext();

  /** Fail fast on unreachable workers instead of waiting out Netty's 30s default. */
  private static final int WORKER_CONNECT_TIMEOUT_MS = 5000;

  /**
   * Sent by {@link TaskWorkerManagerBackendHandler} to the caller's pipeline when the worker's TLS
   * fails.
   */
  enum WorkerUnreachableEvent {
    INSTANCE
  }

  private final TaskWorkerLeaseManager leaseManager;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final boolean internalSslEnabled;
  /** The connection to the task worker; this handler sits on the client pipeline. */
  private Channel workerChannel;
  private boolean connecting = false;
  private boolean rejecting = false;
  private boolean requestComplete = false;
  private final Queue<Object> pendingMessages = new LinkedList<>();

  public TaskWorkerManagerFrontendHandler(TaskWorkerLeaseManager leaseManager,
      DiscoveryServiceClient discoveryServiceClient, boolean internalSslEnabled) {
    this.leaseManager = leaseManager;
    this.discoveryServiceClient = discoveryServiceClient;
    this.internalSslEnabled = internalSslEnabled;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof LastHttpContent) {
      requestComplete = true;
    }
    if (msg instanceof HttpObject && ((HttpObject) msg).decoderResult().isFailure()) {
      LOG.warn("Malformed inbound HTTP message; closing connection.",
          ((HttpObject) msg).decoderResult().cause());
      if (workerChannel == null && !connecting && !rejecting) {
        respondAndClose(ctx, msg, HttpResponseStatus.BAD_REQUEST);
      } else {
        ReferenceCountUtil.release(msg);
        // Close the caller channel first so worker close handlers see an inactive caller
        // and do not mark a healthy worker unavailable.
        ctx.close();
        if (workerChannel != null) {
          workerChannel.close();
        }
      }
      return;
    }
    if (msg instanceof HttpRequest) {
      HttpRequest req = (HttpRequest) msg;

      // One request per connection: reject pipelined requests before touching leases or state.
      if (connecting || rejecting || workerChannel != null) {
        LOG.warn("Rejecting pipelined HTTP request {} {} on an active manager connection.",
            req.method(), req.uri());
        respondAndClose(ctx, msg, HttpResponseStatus.BAD_REQUEST);
        closeOnFlush(ctx.channel());
        return;
      }

      // STEP 0: Readiness probe. Checked before the namespace, since the probe sends none.
      if (HttpMethod.GET.equals(req.method())
          && Constants.TaskWorkerManager.PING_PATH.equals(
              new QueryStringDecoder(req.uri()).path())) {
        respondAndClose(ctx, msg, HttpResponseStatus.OK);
        return;
      }

      // STEP 1: Resolve the Target Namespace
      // The namespace is the lease key, so reject requests without one before leasing.
      String targetNamespace = req.headers().get(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE);
      if (targetNamespace == null || targetNamespace.trim().isEmpty()) {
        LOG.warn("Rejecting task request {} {} without the {} header.",
            req.method(), req.uri(), Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE);
        respondAndClose(ctx, msg, HttpResponseStatus.BAD_REQUEST);
        return;
      }

      // STEP 2: Discover Live Task Workers and Acquire a Lease
      // Discovery is served from an in-memory cache fed by the discovery watch.
      Iterable<Discoverable> discoverables =
          discoveryServiceClient.discover(Constants.Service.TASK_WORKER);
      leaseManager.syncDiscovery(discoverables);
      String targetWorkerAddress = leaseManager.acquireSlot(targetNamespace);

      // STEP 3: Saturation Rejection (HTTP 429)
      // Every worker is full or leased to another namespace, so let AppFabric retry.
      if (targetWorkerAddress == null) {
        LOG.warn("All task workers are saturated or leased to other namespaces. "
            + "Rejecting request for namespace '{}'", targetNamespace);
        respondAndClose(ctx, msg, HttpResponseStatus.TOO_MANY_REQUESTS);
        return;
      }

      final String chosenWorker = targetWorkerAddress;

      // Split on the last colon so IPv6 hosts keep their colons.
      int portSeparator = chosenWorker.lastIndexOf(':');
      int workerPort =
          portSeparator < 0 ? -1 : parsePort(chosenWorker.substring(portSeparator + 1));
      if (workerPort < 0) {
        // Malformed discovery payload: release the slot and fail only this request.
        LOG.error("Task worker address '{}' is not a valid host:port. Rejecting the request.",
            chosenWorker);
        leaseManager.releaseSlot(chosenWorker);
        respondAndClose(ctx, msg, HttpResponseStatus.BAD_GATEWAY);
        return;
      }
      final String workerHost = chosenWorker.substring(0, portSeparator);

      LOG.debug("Opening connection to task worker {} for namespace {}",
          targetWorkerAddress, targetNamespace);

      // STEP 4: Connect to the worker, pausing client reads until the connection is up.
      ctx.channel().config().setAutoRead(false);
      connecting = true;

      // Share the client's event loop so both channels are handled on one thread.
      Bootstrap b = new Bootstrap();
      b.group(ctx.channel().eventLoop())
          .channel(NioSocketChannel.class)
          .option(ChannelOption.SO_KEEPALIVE, true)
          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, WORKER_CONNECT_TIMEOUT_MS)
          .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              ChannelPipeline p = ch.pipeline();
              if (internalSslEnabled) {
                p.addLast(CLIENT_SSL_CONTEXT.newHandler(ch.alloc(), workerHost, workerPort));
              }
              p.addLast(new HttpClientCodec());
              p.addLast(new TaskWorkerManagerBackendHandler(
                  ctx.channel(), leaseManager, chosenWorker, targetNamespace));
            }
          });

      // One request per worker connection: the worker closes it after responding, which closes the
      // caller's connection too, so a keep-alive request can't reuse a stale workerChannel.
      HttpUtil.setKeepAlive(req, false);
      // Queue the header before connecting, since the listener can run inline and drain the queue.
      pendingMessages.add(msg);

      ChannelFuture f = b.connect(workerHost, workerPort);
      workerChannel = f.channel();

      f.addListener((ChannelFutureListener) future -> {
        connecting = false;
        if (future.isSuccess()) {
          // With TLS enabled, TaskWorkerManagerBackendHandler clears the backoff once the handshake
          // succeeds; over plain HTTP, TCP connect is the readiness signal.
          if (!internalSslEnabled) {
            leaseManager.markReachable(chosenWorker);
          }
          if (!ctx.channel().isActive()) {
            // The caller left while we were connecting; closing the worker releases the slot.
            releasePendingMessages();
            workerChannel.close();
            return;
          }
          // Flush everything queued while connecting.
          LOG.debug("Connected to task worker {}", chosenWorker);
          Object pendingMsg = pendingMessages.poll();
          while (pendingMsg != null) {
            workerChannel.write(pendingMsg);
            pendingMsg = pendingMessages.poll();
          }
          workerChannel.flush();
          // Resume reading the body unless the flush already filled the worker's buffer.
          ctx.channel().config().setAutoRead(workerChannel.isWritable());
        } else {
          if (!ctx.channel().isActive()) {
            releasePendingMessages();
            leaseManager.releaseSlot(chosenWorker);
            return;
          }
          // Usually a restarting worker. Discovery keeps listing it while the container
          // boots, so back off rather than letting least-loaded selection pick it again.
          LOG.warn("Failed to connect to task worker {}.", chosenWorker, future.cause());
          leaseManager.markUnavailable(chosenWorker, "a failed connect");
          respondWorkerUnreachable(ctx);
        }
      });

    } else if (msg instanceof HttpContent) {
      // STEP 5: Stream Inbound HTTP Request Body Chunks
      if (rejecting) {
        // Drain the rejected request's body before closing, so the client isn't reset mid-upload.
        boolean isLast = msg instanceof LastHttpContent;
        ReferenceCountUtil.release(msg);
        if (isLast) {
          // Close after the rejection is written; a plain close() can drop it.
          closeOnFlush(ctx.channel());
        }
        return;
      }
      if (connecting || !pendingMessages.isEmpty()) {
        // Socket still connecting or queue not yet drained: preserve ordering
        pendingMessages.add(msg);
      } else if (workerChannel != null && workerChannel.isActive()) {
        workerChannel.writeAndFlush(msg);
      } else {
        ReferenceCountUtil.release(msg);
      }
    } else {
      // Unreachable with HttpServerCodec; release anyway to avoid leaks.
      LOG.debug("Discarding unexpected inbound message type {}", msg.getClass().getName());
      ReferenceCountUtil.release(msg);
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    // Flush any buffered outbound data to the worker socket
    if (workerChannel != null && workerChannel.isActive() && !connecting) {
      workerChannel.flush();
    }
    ctx.fireChannelReadComplete();
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) {
    // Response-direction backpressure: stop reading from the worker while the client can't keep up.
    Channel clientChannel = ctx.channel();
    if (workerChannel != null && workerChannel.isActive()) {
      workerChannel.config().setAutoRead(clientChannel.isWritable());
    }
    ctx.fireChannelWritabilityChanged();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    // Release anything queued if the client leaves before the connect completes.
    releasePendingMessages();

    // Abort the outbound worker socket immediately so mid-handshake or stalled writes don't hang.
    if (workerChannel != null) {
      workerChannel.close();
    }
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Error on the inbound manager connection", cause);
    closeOnFlush(ctx.channel());
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt == WorkerUnreachableEvent.INSTANCE) {
      respondWorkerUnreachable(ctx);
      return;
    }
    ctx.fireUserEventTriggered(evt);
  }

  /**
   * Releases every buffer still sitting in the pending queue and empties it.
   */
  private void releasePendingMessages() {
    Object pendingMsg = pendingMessages.poll();
    while (pendingMsg != null) {
      ReferenceCountUtil.release(pendingMsg);
      pendingMsg = pendingMessages.poll();
    }
  }

  /**
   * Responds with {@code status} and closes the connection once the request body is drained, so
   * the client isn't reset mid-upload.
   */
  private void respondAndClose(ChannelHandlerContext ctx, Object msg, HttpResponseStatus status) {
    rejecting = true;
    ChannelFuture writeFuture = ctx.writeAndFlush(closingResponse(status, Unpooled.EMPTY_BUFFER));
    if (requestComplete) {
      writeFuture.addListener(ChannelFutureListener.CLOSE);
    }
    ReferenceCountUtil.release(msg);
  }

  /**
   * Answers 503 for a request that never reached the worker (connect or TLS failure), so the
   * caller knows the task didn't start and can retry it on another worker.
   */
  private void respondWorkerUnreachable(ChannelHandlerContext ctx) {
    releasePendingMessages();
    if (rejecting || !ctx.channel().isActive()) {
      return;
    }
    rejecting = true;
    ChannelFuture written = ctx.writeAndFlush(closingResponse(
        HttpResponseStatus.SERVICE_UNAVAILABLE,
        Unpooled.copiedBuffer(Constants.TaskWorkerManager.WORKER_UNREACHABLE_BODY, StandardCharsets.UTF_8)));
    if (requestComplete) {
      written.addListener(ChannelFutureListener.CLOSE);
      return;
    }
    // Reads were paused for the connect; resume so the body drains and the last chunk closes.
    ctx.channel().config().setAutoRead(true);
  }

  private static FullHttpResponse closingResponse(HttpResponseStatus status, ByteBuf body) {
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, body);
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.readableBytes());
    response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
    return response;
  }

  /**
   * Parses a TCP port, returning -1 rather than throwing when the text is not a valid port.
   */
  private static int parsePort(String port) {
    try {
      int parsed = Integer.parseInt(port);
      return parsed >= 0 && parsed <= 65535 ? parsed : -1;
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private static SslContext createClientSslContext() {
    try {
      return SslContextBuilder.forClient()
          .trustManager(InsecureTrustManagerFactory.INSTANCE)
          .build();
    } catch (SSLException e) {
      // Fail at startup: the manager can't forward anything when TLS is enabled without an SSL context.
      throw new IllegalStateException(
          "Failed to build the outbound SSL context for the task worker manager", e);
    }
  }

  /**
   * Closes the channel gracefully after flushing any remaining in-flight buffers.
   */
  static void closeOnFlush(Channel ch) {
    if (ch.isActive()) {
      ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }
  }
}
