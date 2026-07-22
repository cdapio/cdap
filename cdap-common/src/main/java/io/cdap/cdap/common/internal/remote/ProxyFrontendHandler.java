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
import io.cdap.cdap.common.conf.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyFrontendHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyFrontendHandler.class);


    private final Map<String, PodState> podRegistry;
    private final DiscoveryServiceClient discoveryServiceClient;
    private final Iterable<Discoverable> discoverables;
    private Channel outboundChannel;
    private boolean connecting = false;
    private final Queue<Object> pendingMessages = new LinkedList<>();

    public ProxyFrontendHandler(Map<String, PodState> podRegistry, DiscoveryServiceClient discoveryServiceClient) {
        this.podRegistry = podRegistry;
        this.discoveryServiceClient = discoveryServiceClient;
        // Pre-warm the Discovery client so its WatcherThread spawns immediately on Proxy startup
        this.discoverables = discoveryServiceClient.discover(Constants.Service.TASK_WORKER);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) msg;

            // 0. Synchronous K8s Discovery (Zero-Stale State)
            // Completely non-blocking on the EventLoop: Twill's DiscoveryServiceClient
            // evaluates a local memory cache backed by a push-based ZooKeeper watch.
            // (Iterates the pre-warmed discoverables cache)
            Set<String> activePods = new HashSet<>();
            for (Discoverable d : discoverables) {
                activePods.add(d.getSocketAddress().getHostString() + ":" + d.getSocketAddress().getPort());
            }

            for (String podIp : activePods) {
                podRegistry.putIfAbsent(podIp, new PodState(null, 0));
            }
            podRegistry.keySet().removeIf(existingPod -> !activePods.contains(existingPod));

            String targetNamespace = req.headers().get("X-CDF-Namespace");
            if (targetNamespace == null) targetNamespace = "default";

            String targetWorkerAddress = null;
            // 1. Warm Match: Thread-safe scan specifically locking evaluation
            for (Map.Entry<String, PodState> entry : podRegistry.entrySet()) {
                String workerAddr = entry.getKey();
                PodState state = entry.getValue();
                boolean isHostnameFallback = workerAddr.matches(".*[a-zA-Z].*"); // True if hostname instead of IP

                synchronized (state) {
                    if (isHostnameFallback || (targetNamespace.equals(state.getLeasedNamespace()) && state.getInflightRequests() < 10)) {
                        targetWorkerAddress = workerAddr;
                        state.setInflightRequests(state.getInflightRequests() + 1);
                        if (!isHostnameFallback) {
                            LOG.info("shruzard - ProxyFrontendHandler: Found warm match "
                                     + "for '{}' at {}. Occupancy: {}", 
                                     targetNamespace, targetWorkerAddress, state.getInflightRequests());
                        }
                        break;
                    }
                }
            }

            // 2. Idle Choice: Thread-safe claim of an unleased pod, 
            // OR an expired pod (35s predicted timeout avoiding clock drift)
            if (targetWorkerAddress == null) {
                for (Map.Entry<String, PodState> entry : podRegistry.entrySet()) {
                    String workerAddr = entry.getKey();
                    PodState state = entry.getValue();
                    boolean isHostnameFallback = workerAddr.matches(".*[a-zA-Z].*");

                    synchronized (state) {
                        boolean isUnleased = (state.getLeasedNamespace() == null 
                            || state.getLeasedNamespace().isEmpty());
                        boolean isExpiredIdle = (state.getInflightRequests() == 0 
                            && (System.currentTimeMillis() - state.getLastActivityTime() > 35000));
                        
                        if (isHostnameFallback || (state.getInflightRequests() == 0 && (isUnleased || isExpiredIdle))) {
                            targetWorkerAddress = workerAddr;
                            state.setLeasedNamespace(targetNamespace); // Doesn't matter much for hostname
                            state.setInflightRequests(state.getInflightRequests() + 1);
                            if (!isHostnameFallback) {
                                LOG.info("shruzard - ProxyFrontendHandler: Claimed idle pod "
                                         + "(Unleased: {}, ExpiredIdle: {}) at {} for namespace '{}'.", 
                                    isUnleased, isExpiredIdle, targetWorkerAddress, targetNamespace);
                            }
                            break;
                        }
                    }
                }
            }

            // 3. Busy Rejection: All pods saturatedd
            if (targetWorkerAddress == null) {
                LOG.warn("shruzard - ProxyFrontendHandler: All pods saturated or leased "
                         + "incorrectly. Rejecting request for namespace '{}'", targetNamespace);
                FullHttpResponse response = new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, HttpResponseStatus.TOO_MANY_REQUESTS);
                ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                ReferenceCountUtil.release(msg);
                return;
            }

            final String chosenWorker = targetWorkerAddress;
            String[] hostPort = targetWorkerAddress.split(":");

            // Apply backpressure on client until connection established
            ctx.channel().config().setAutoRead(false);
            connecting = true;

            Bootstrap b = new Bootstrap();
            b.group(ctx.channel().eventLoop())
             .channel(NioSocketChannel.class)
             .option(ChannelOption.SO_KEEPALIVE, true)
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) {
                     ChannelPipeline p = ch.pipeline();
                     p.addLast(new HttpClientCodec());
                     p.addLast(new ProxyBackendHandler(ctx.channel(), podRegistry, chosenWorker));
                 }
             });

            ChannelFuture f = b.connect(hostPort[0], Integer.parseInt(hostPort[1]));
            outboundChannel = f.channel();

            f.addListener((ChannelFutureListener) future -> {
                connecting = false;
                if (future.isSuccess()) {
                    Object pendingMsg = pendingMessages.poll();
                    while (pendingMsg != null) {
                        outboundChannel.write(pendingMsg);
                        pendingMsg = pendingMessages.poll();
                    }
                    outboundChannel.flush();
                    ctx.channel().config().setAutoRead(true);
                } else {
                    Object pendingMsg = pendingMessages.poll();
                    while (pendingMsg != null) {
                        ReferenceCountUtil.release(pendingMsg);
                        pendingMsg = pendingMessages.poll();
                    }
                    // Thread-safe decrement on fallback
                    PodState fallbackState = podRegistry.get(chosenWorker);
                    if (fallbackState != null) {
                        synchronized (fallbackState) {
                            fallbackState.setInflightRequests(Math.max(0, fallbackState.getInflightRequests() - 1));
                        }
                    }
                    ctx.channel().close();
                }
            });

            pendingMessages.add(ReferenceCountUtil.retain(msg));

        } else if (msg instanceof HttpContent) {
            if (connecting) {
                pendingMessages.add(ReferenceCountUtil.retain(msg));
            } else if (outboundChannel != null && outboundChannel.isActive()) {
                outboundChannel.writeAndFlush(ReferenceCountUtil.retain(msg));
            } else {
                ReferenceCountUtil.release(msg);
            }
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        if (outboundChannel != null && outboundChannel.isActive() && !connecting) {
            outboundChannel.flush();
        }
        ctx.fireChannelReadComplete();
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (outboundChannel != null && outboundChannel.isActive()) {
            outboundChannel.config().setAutoRead(ctx.channel().isWritable());
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (outboundChannel != null) {
            closeOnFlush(outboundChannel);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        closeOnFlush(ctx.channel());
    }

    static void closeOnFlush(Channel ch) {
        if (ch.isActive()) {
            ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
