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
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.HashSet;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
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
 *       or claims an idle pod (up to 10 concurrent requests per pod).</li>
 *   <li>Outbound Socket Connection: Lazily opens an asynchronous Netty TCP socket to the chosen
 *       Task Worker pod and sets up the outbound SSL/HTTP pipeline.</li>
 *   <li>Bidirectional Backpressure: Pauses inbound reads while connecting or when outbound socket
 *       buffers are full, preventing out-of-memory errors under high traffic spikes.</li>
 *   <li>Zero-Copy Streaming: Forwards raw {@link io.netty.buffer.ByteBuf} chunks without JVM heap
 *       copies, managing explicit reference counting ({@code retain()}/{@code release()}).</li>
 * </ul>
 */
public class ProxyFrontendHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyFrontendHandler.class);

    private final Map<String, PodState> podRegistry;
    private final DiscoveryServiceClient discoveryServiceClient;
    private Channel outboundChannel;
    private boolean connecting = false;
    private boolean rejecting = false;
    private final Queue<Object> pendingMessages = new LinkedList<>();

    public ProxyFrontendHandler(Map<String, PodState> podRegistry, DiscoveryServiceClient discoveryServiceClient) {
        this.podRegistry = podRegistry;
        this.discoveryServiceClient = discoveryServiceClient;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpRequest) {
            LOG.info("shruzard - ProxyFrontendHandler Received request!");
            HttpRequest req = (HttpRequest) msg;

            // STEP 0: Discover Live Task Worker Pods (Zero-Stale State)
            // Twill's DiscoveryServiceClient evaluates an in-memory discoverables cache backed by
            // Kubernetes Endpoints watch events, giving sub-millisecond pod discovery without DNS lag.
            Iterable<Discoverable> discoverables = discoveryServiceClient.discover(Constants.Service.TASK_WORKER);
            Set<String> activePods = new HashSet<>();
            for (Discoverable d : discoverables) {
                activePods.add(d.getSocketAddress().getHostString() + ":" + d.getSocketAddress().getPort());
            }

            // Sync the active discovery set with our local routing registry:
            // Register newly discovered pods and prune terminated pods.
            for (String podIp : activePods) {
                podRegistry.putIfAbsent(podIp, new PodState(null, 0));
            }
            podRegistry.keySet().removeIf(existingPod -> !activePods.contains(existingPod));

            LOG.info("shruzard - ProxyFrontendHandler leases: [{}]",
                podRegistry.entrySet().stream()
                    .map(e -> e.getKey() + "="
                        + (e.getValue().getLeasedNamespace() == null
                        ? "null" : e.getValue().getLeasedNamespace() + "_" + e.getValue().getInflightRequests()))
                    .collect(java.util.stream.Collectors.joining(", ")));

            // Extract target namespace from the request header (defaults to "default" if omitted)
            String targetNamespace = req.headers().get("X-CDF-Namespace");
            if (targetNamespace == null) targetNamespace = "default";

            String targetWorkerAddress = null;

            // We deliberately evaluate sequentially (no randomization/shuffling) to maximize pod density. 
            // This ensures a namespace fills a single pod to its maximum capacity (10 tasks) before 
            // spilling over and leasing an entirely new empty pod, preserving cluster availability 
            // for other namespaces.

            // STEP 1: Warm Match Selection
            // Perform lock-free Compare-And-Swap evaluation using the AtomicReference loop
            for (Map.Entry<String, PodState> entry : podRegistry.entrySet()) {
                PodState state = entry.getValue();
                
                if (state.tryAcquireWarmLease(targetNamespace, 10)) {
                    targetWorkerAddress = entry.getKey();
                    LOG.info("shruzard - ProxyFrontendHandler: Found warm match "
                             + "for '{}' at {}. Occupancy: {}", 
                             targetNamespace, targetWorkerAddress, state.getInflightRequests());
                    break;
                }
            }

            // STEP 2: Idle Pod Claiming
            if (targetWorkerAddress == null) {
                // Determine our starvation/eviction threshold (35 seconds in NanoTime)
                long idleTimeoutNanos = java.util.concurrent.TimeUnit.SECONDS.toNanos(35);
                
                for (Map.Entry<String, PodState> entry : podRegistry.entrySet()) {
                    PodState state = entry.getValue();
                    
                    if (state.tryClaimIdleLease(targetNamespace, idleTimeoutNanos)) {
                        targetWorkerAddress = entry.getKey();
                        LOG.info("shruzard - ProxyFrontendHandler: Claimed idle pod "
                                 + "for new namespace '{}' at {}",
                                 targetNamespace, targetWorkerAddress);
                        break;
                    }
                }
            }

            // STEP 3: Saturation Rejection (HTTP 429)
            // If all worker pods are 100% occupied (10/10 tasks each), fail fast with HTTP 429 Too Many Requests.
            if (targetWorkerAddress == null) {
                LOG.warn("shruzard - ProxyFrontendHandler: All pods saturated or leased "
                         + "incorrectly. Rejecting request for namespace '{}'", targetNamespace);
                rejecting = true;
                FullHttpResponse response = new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, HttpResponseStatus.TOO_MANY_REQUESTS);
                response.headers().set("Content-Length", "0");
                response.headers().set("Connection", "close");
                ctx.writeAndFlush(response);
                ReferenceCountUtil.release(msg);
                return;
            }

            final String chosenWorker = targetWorkerAddress;
            String[] hostPort = targetWorkerAddress.split(":");

            LOG.info("shruzard - ProxyFrontendHandler: Setting up TCP connection to task worker IP - {} namespace {}",
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
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) {
                     ChannelPipeline p = ch.pipeline();
                     try {
                         // Attach SSL handler for internal TLS encrypted communication with the worker pod
                         SslContext sslCtx = SslContextBuilder.forClient()
                             .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
                         p.addLast(sslCtx.newHandler(ch.alloc(), hostPort[0], Integer.parseInt(hostPort[1])));
                     } catch (Exception e) {
                         LOG.error("shruzard - Failed to initialize SSL for outbound proxy", e);
                     }
                     // HTTP codec for encoding requests to worker and decoding responses from worker
                     p.addLast(new HttpClientCodec());
                     // Attach backend handler to stream worker responses back to AppFabric
                     p.addLast(new ProxyBackendHandler(ctx.channel(), podRegistry, chosenWorker));
                 }
             });

            // 3. Initiate non-blocking asynchronous TCP connect to the Task Worker IP and Port
            ChannelFuture f = b.connect(hostPort[0], Integer.parseInt(hostPort[1]));
            outboundChannel = f.channel();

            // 4. Register listener to handle connection success or failure
            f.addListener((ChannelFutureListener) future -> {
                connecting = false;
                if (future.isSuccess()) {
                    // Flush any request headers/chunks that arrived while TCP connection was being negotiated
                    LOG.info("shruzard - ProxyFrontendHandler: Connected with task worker successfully! ");
                    Object pendingMsg = pendingMessages.poll();
                    while (pendingMsg != null) {
                        outboundChannel.write(pendingMsg);
                        pendingMsg = pendingMessages.poll();
                    }
                    outboundChannel.flush();
                    // Resume reading remaining body chunks from the client
                    ctx.channel().config().setAutoRead(true);
                } else {
                    // If connection failed (worker crashed/terminated), evict from registry and release buffers
                    LOG.warn("shruzard - ProxyFrontendHandler: Failed to connect to backend worker {}. "
                             + "Evicting from registry.", chosenWorker);
                    podRegistry.remove(chosenWorker);
                    Object pendingMsg = pendingMessages.poll();
                    while (pendingMsg != null) {
                        ReferenceCountUtil.release(pendingMsg);
                        pendingMsg = pendingMessages.poll();
                    }
                    // Decrement inflight count on failed connection
                    PodState fallbackState = podRegistry.get(chosenWorker);
                    if (fallbackState != null) {
                        fallbackState.decrementInflightRequests();
                    }
                    ctx.channel().close();
                }
            });

            // Retain the HttpRequest header message in pending queue until outbound socket connection completes
            pendingMessages.add(ReferenceCountUtil.retain(msg));

        } else if (msg instanceof HttpContent) {
            // STEP 5: Stream Inbound HTTP Request Body Chunks
            if (rejecting) {
                // If previously rejected with 429, drain and release remaining body chunks to prevent TCP reset
                boolean isLast = msg instanceof io.netty.handler.codec.http.LastHttpContent;
                ReferenceCountUtil.release(msg);
                if (isLast) {
                    ctx.channel().close();
                }
                return;
            }
            if (connecting) {
                // Socket still connecting: queue body chunk with retained reference count
                pendingMessages.add(ReferenceCountUtil.retain(msg));
            } else if (outboundChannel != null && outboundChannel.isActive()) {
                // Outbound socket active: stream raw ByteBuf directly to worker without copying to Java Heap!
                outboundChannel.writeAndFlush(ReferenceCountUtil.retain(msg));
            } else {
                ReferenceCountUtil.release(msg);
            }
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        // Flush any buffered outbound data to the worker socket
        if (outboundChannel != null && outboundChannel.isActive() && !connecting) {
            outboundChannel.flush();
        }
        ctx.fireChannelReadComplete();
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        // Forward backpressure: If the client socket write buffer is full,
        // stop reading from the backend worker socket to avoid buffer bloat.
        if (outboundChannel != null && outboundChannel.isActive()) {
            outboundChannel.config().setAutoRead(ctx.channel().isWritable());
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // When client closes connection, cleanly close the outbound worker socket
        if (outboundChannel != null) {
            closeOnFlush(outboundChannel);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        closeOnFlush(ctx.channel());
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
