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
 * ProxyFrontendHandler intercepts inbound HTTP requests from AppFabric, discovers available
 * Task Worker pods, selects a warm or idle worker pod based on the target namespace,
 * and streams the request payload across an outbound Netty TCP socket to the chosen worker.
 *
 * <p>Key Responsibilities:
 * <ul>
 *   <li>Namespace-Aware Routing: Matches requests to pods already warm for the target namespace
 *       or claims an idle pod.</li>
 *   <li>Outbound Socket Connection: Lazily opens an asynchronous Netty TCP socket to the chosen
 *       Task Worker pod and sets up the outbound SSL/HTTP pipeline.</li>
 *   <li>Bidirectional Backpressure: Pauses inbound reads while connecting or when outbound socket
 *       buffers are full, preventing out-of-memory errors under high traffic spikes.</li>
 *   <li>Zero-Copy Streaming: Forwards raw {@link io.netty.buffer.ByteBuf} chunks without JVM heap
 *       copies, managing explicit reference counting ({@code retain()}/{@code release()}).</li>
 * </ul>
 */
class ProxyFrontendHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyFrontendHandler.class);

    /**
     * Outbound TLS context shared by every proxied connection.
     *
     * <p>An {@link SslContext} is immutable and thread safe once built, and building one allocates
     * a full JSSE (or OpenSSL) context plus its session cache. Doing that per connection puts one
     * of the most expensive operations in Netty directly on the request hot path, so it is built
     * exactly once here and reused for the lifetime of the process.
     */
    private static final SslContext CLIENT_SSL_CONTEXT = createClientSslContext();

    /** Fail fast on unreachable pods instead of waiting out Netty's 30s default. */
    private static final int WORKER_CONNECT_TIMEOUT_MS = 5000;

    private final PodLeaseManager podLeaseManager;
    private final DiscoveryServiceClient discoveryServiceClient;
    /**
     * The connection to the task worker pod. This handler is installed on the CLIENT pipeline, so
     * this is the peer channel: events arrive here from the client, writes go out to the worker.
     */
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
            // Twill's DiscoveryServiceClient evaluates an in-memory discoverables cache backed by
            // Kubernetes Endpoints watch events, giving sub-millisecond pod discovery without DNS lag.
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

            // Split on the LAST colon, not the first. Addresses are built in
            // PodLeaseManager#syncDiscovery as hostString + ":" + port, and for an IPv6 pod
            // hostString is itself colon separated (for example 2001:db8::1:11015). Splitting on
            // the first colon would hand a hex group to Integer.parseInt instead of the port.
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

            // STEP 4: Establish Outbound TCP Socket to Chosen Task Worker Pod
            // 1. Temporarily pause reading from the client (AppFabric) socket so data does not pile up in RAM
            //    while the TCP handshake to the worker is completing.
            ctx.channel().config().setAutoRead(false);
            connecting = true;

            // 2. Initialize the outbound Netty client Bootstrap.
            //    Sharing ctx.channel().eventLoop() ensures that both inbound and outbound channels run on the same
            //    event loop thread, guaranteeing thread safety without thread context-switching overhead.

            Bootstrap b = new Bootstrap();
            b.group(ctx.channel().eventLoop())
             .channel(NioSocketChannel.class)
             .option(ChannelOption.SO_KEEPALIVE, true)
             .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, WORKER_CONNECT_TIMEOUT_MS)
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) {
                     ChannelPipeline p = ch.pipeline();
                     // Attach SSL handler for internal TLS encrypted communication with the worker pod.
                     // The context is process wide, see CLIENT_SSL_CONTEXT.
                     p.addLast(CLIENT_SSL_CONTEXT.newHandler(ch.alloc(), workerHost, workerPort));
                     // HTTP codec for encoding requests to worker and decoding responses from worker
                     p.addLast(new HttpClientCodec());
                     // Attach backend handler to stream worker responses back to AppFabric
                     p.addLast(new ProxyBackendHandler(ctx.channel(), podLeaseManager, chosenWorker));
                 }
             });

            // 3. Queue the request header BEFORE initiating the connect.
            //    b.connect() can complete inline (an immediate resolver failure, or a connect that
            //    finishes on this event loop before addListener returns), in which case the
            //    listener below runs synchronously and drains the queue. If the header were
            //    enqueued afterwards it would land in a queue nobody will ever poll again, and the
            //    request would hang with its body streaming into a socket that never got a header.
            pendingMessages.add(msg);

            // 4. Initiate non-blocking asynchronous TCP connect to the Task Worker IP and Port
            ChannelFuture f = b.connect(workerHost, workerPort);
            workerChannel = f.channel();

            // 5. Register listener to handle connection success or failure
            f.addListener((ChannelFutureListener) future -> {
                connecting = false;
                if (future.isSuccess()) {
                    // Flush any request headers/chunks that arrived while TCP connection was being negotiated
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
                // If this request was rejected, drain and release remaining body chunks to prevent TCP reset
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
                // Outbound socket active: stream raw ByteBuf directly to worker without copying to Java Heap!
                workerChannel.writeAndFlush(msg);
            } else {
                ReferenceCountUtil.release(msg);
            }
        } else {
            // HttpServerCodec only emits HttpRequest and HttpContent, so this is unreachable today.
            // Releasing anyway means a future pipeline change cannot turn into a silent buffer leak.
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
        // Backpressure on the response direction.
        //
        // This handler is installed on the client pipeline, so this fires when the CLIENT's own
        // write buffer crosses a watermark. That buffer holds response bytes we are relaying back
        // to AppFabric, and the only thing feeding it is our reads from the worker. So when the
        // client stops keeping up, stop pulling from the worker.
        //
        // The request direction is the mirror of this and lives in
        // ProxyBackendHandler#channelWritabilityChanged. Each handler reacts to its own channel's
        // writability, because Netty raises this event only on the pipeline of the channel whose
        // buffer actually moved.
        Channel clientChannel = ctx.channel();
        if (workerChannel != null && workerChannel.isActive()) {
            workerChannel.config().setAutoRead(clientChannel.isWritable());
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // If the client goes away while the outbound connect is still in flight, the connect
        // listener may never run, so anything already queued would never be written and never be
        // released. Drain it here; the queue is empty on the normal path so this is a no-op.
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
     * Fails the current request with the given status without tearing the connection down
     * immediately.
     *
     * <p>The response is written now, but the channel is closed only once
     * {@link io.netty.handler.codec.http.LastHttpContent} arrives (see the {@code rejecting} branch
     * in {@link #channelRead}). Closing right away would leave the client mid-upload of a body that
     * can run to many megabytes, and the resulting TCP reset would frequently destroy the response
     * before AppFabric managed to read it, turning a clean retryable status into an opaque
     * connection error.
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
            // The proxy cannot forward a single request without TLS to the workers, so there is no
            // degraded mode worth limping along in. Failing at class initialization surfaces the
            // real cause instead of a stream of confusing handshake errors at request time.
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
