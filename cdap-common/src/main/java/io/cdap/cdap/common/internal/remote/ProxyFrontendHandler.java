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

import io.netty.bootstrap.Bootstrap;
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
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;

import java.util.LinkedList;
import java.util.Queue;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import javax.net.ssl.SSLException;
import io.cdap.cdap.common.conf.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes AppFabric task requests to a task worker pod leased to the request's namespace and
 * streams the request to it.
 */
class ProxyFrontendHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyFrontendHandler.class);

    /** Built once, since creating an SslContext per connection is expensive. */
    private static final SslContext CLIENT_SSL_CONTEXT = createClientSslContext();

    /** Fail fast on unreachable pods instead of waiting out Netty's 30s default. */
    private static final int WORKER_CONNECT_TIMEOUT_MS = 5000;

    private final PodLeaseManager podLeaseManager;
    private final DiscoveryServiceClient discoveryServiceClient;
    /** The connection to the task worker; this handler sits on the client pipeline. */
    private Channel workerChannel;
    private boolean connecting = false;
    private boolean rejecting = false;
    private final Queue<Object> pendingMessages = new LinkedList<>();

    ProxyFrontendHandler(PodLeaseManager podLeaseManager, DiscoveryServiceClient discoveryServiceClient) {
        this.podLeaseManager = podLeaseManager;
        this.discoveryServiceClient = discoveryServiceClient;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) msg;

            // STEP 1: Resolve the Target Namespace
            // The namespace is the lease key, so reject requests without one before leasing.
            String targetNamespace = req.headers().get(Constants.Gateway.HEADER_CDAP_NAMESPACE);
            if (targetNamespace == null || targetNamespace.trim().isEmpty()) {
                LOG.warn("Rejecting task request {} {} without the {} header.",
                         req.method(), req.uri(), Constants.Gateway.HEADER_CDAP_NAMESPACE);
                reject(ctx, msg, HttpResponseStatus.BAD_REQUEST);
                return;
            }

            // STEP 2: Discover Live Task Worker Pods and Acquire a Lease
            // Discovery is served from an in-memory cache fed by the Kubernetes Endpoints watch.
            Iterable<Discoverable> discoverables = discoveryServiceClient.discover(Constants.Service.TASK_WORKER);
            podLeaseManager.syncDiscovery(discoverables);
            String targetWorkerAddress = podLeaseManager.acquireLease(targetNamespace);

            // STEP 3: Saturation Rejection (HTTP 429)
            // Every pod is at its limit or leased to another namespace, so fail fast and let AppFabric retry.
            if (targetWorkerAddress == null) {
                LOG.warn("All task worker pods are saturated or leased to other namespaces. "
                         + "Rejecting request for namespace '{}'", targetNamespace);
                reject(ctx, msg, HttpResponseStatus.TOO_MANY_REQUESTS);
                return;
            }

            final String chosenWorker = targetWorkerAddress;

            // Split on the last colon so IPv6 hosts keep their colons.
            int portSeparator = chosenWorker.lastIndexOf(':');
            int workerPort = portSeparator < 0 ? -1 : parsePort(chosenWorker.substring(portSeparator + 1));
            if (workerPort < 0) {
                // Malformed discovery payload: release the slot and fail only this request.
                LOG.error("Task worker address '{}' is not a valid host:port. Rejecting the request.",
                          chosenWorker);
                podLeaseManager.releaseLease(chosenWorker);
                reject(ctx, msg, HttpResponseStatus.BAD_GATEWAY);
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
                     p.addLast(CLIENT_SSL_CONTEXT.newHandler(ch.alloc(), workerHost, workerPort));
                     p.addLast(new HttpClientCodec());
                     p.addLast(new ProxyBackendHandler(ctx.channel(), podLeaseManager, chosenWorker));
                 }
             });

            // Queue the header before connecting, since the listener can run inline and drain the queue.
            pendingMessages.add(msg);

            ChannelFuture f = b.connect(workerHost, workerPort);
            workerChannel = f.channel();

            f.addListener((ChannelFutureListener) future -> {
                connecting = false;
                if (future.isSuccess()) {
                    // Flush everything queued while connecting.
                    LOG.debug("Connected to task worker {}", chosenWorker);
                    Object pendingMsg = pendingMessages.poll();
                    while (pendingMsg != null) {
                        workerChannel.write(pendingMsg);
                        pendingMsg = pendingMessages.poll();
                    }
                    workerChannel.flush();
                    // Resume reading remaining body chunks from the client
                    ctx.channel().config().setAutoRead(true);
                } else {
                    // Worker unreachable: release the slot and buffers; discovery evicts the pod.
                    LOG.warn("Failed to connect to task worker {}.", chosenWorker, future.cause());
                    podLeaseManager.releaseLease(chosenWorker);
                    releasePendingMessages();
                    ctx.channel().close();
                }
            });

        } else if (msg instanceof HttpContent) {
            // STEP 5: Stream Inbound HTTP Request Body Chunks
            if (rejecting) {
                // Drain the rejected request's body before closing, so the client isn't reset mid-upload.
                boolean isLast = msg instanceof io.netty.handler.codec.http.LastHttpContent;
                ReferenceCountUtil.release(msg);
                if (isLast) {
                    ctx.channel().close();
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

        // When client closes connection, cleanly close the outbound worker socket
        if (workerChannel != null) {
            closeOnFlush(workerChannel);
        }
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("Error on the inbound proxy connection", cause);
        closeOnFlush(ctx.channel());
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
     * Responds with {@code status} and closes the connection once the request body is drained,
     * so the client isn't reset mid-upload.
     */
    private void reject(ChannelHandlerContext ctx, Object msg, HttpResponseStatus status) {
        rejecting = true;
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);
        response.headers().set("Content-Length", "0");
        response.headers().set("Connection", "close");
        ctx.writeAndFlush(response);
        ReferenceCountUtil.release(msg);
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
            // Fail at startup: the proxy can't forward anything without TLS.
            throw new IllegalStateException(
                "Failed to build the outbound SSL context for the task worker proxy", e);
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
