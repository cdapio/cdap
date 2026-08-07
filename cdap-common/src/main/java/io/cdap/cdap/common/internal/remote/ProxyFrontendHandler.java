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
import java.util.stream.Collectors;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
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
    private boolean rejecting = false;
    private final Queue<Object> pendingMessages = new LinkedList<>();

    public ProxyFrontendHandler(Map<String, PodState> podRegistry, DiscoveryServiceClient discoveryServiceClient) {
        this.podRegistry = podRegistry;
        this.discoveryServiceClient = discoveryServiceClient;

        // Enable live Kubernetes Endpoints streaming if supported by the discovery client
        try {
            java.lang.reflect.Method method = discoveryServiceClient.getClass().getMethod("enableEndpointsWatcher");
            method.invoke(discoveryServiceClient);
            LOG.info("shruzard - Enabled Kubernetes Endpoints watcher on discovery service {}",
                     discoveryServiceClient.getClass().getSimpleName());
        } catch (NoSuchMethodException ignored) {
            // Normal for discovery services without K8s Endpoints support (e.g. In-memory / ZK)
        } catch (Exception e) {
            LOG.warn("shruzard - Failed to invoke enableEndpointsWatcher on discovery service", e);
        }

        // Pre-warm the Discovery client so its WatcherThreads spawn immediately on Proxy startup
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
                String host = d.getSocketAddress().getHostString();
                int port = d.getSocketAddress().getPort();
                activePods.add(host + ":" + port);
            }

            for (String podIp : activePods) {
                podRegistry.putIfAbsent(podIp, new PodState(null, 0));
            }
            podRegistry.keySet().removeIf(existingPod -> !activePods.contains(existingPod));

            LOG.info("shruzard - ProxyFrontendHandler leases: [{}]",
                podRegistry.entrySet().stream()
                    .map(e -> e.getKey() + "="
                        + (e.getValue().getLeasedNamespace() == null
                        ? "null" : e.getValue().getLeasedNamespace() + "_" + e.getValue().getInflightRequests()))
                    .collect(Collectors.joining(", ")));

            String targetNamespace = req.headers().get("X-CDF-Namespace");
            if (targetNamespace == null) targetNamespace = "default";

            String targetWorkerAddress = null;
            // 1. Warm Match: Thread-safe scan specifically locking evaluation
            for (Map.Entry<String, PodState> entry : podRegistry.entrySet()) {
                String workerAddr = entry.getKey();
                PodState state = entry.getValue();
                synchronized (state) {
                    if (targetNamespace.equals(state.getLeasedNamespace()) && state.getInflightRequests() < 10) {
                        targetWorkerAddress = workerAddr;
                        state.setInflightRequests(state.getInflightRequests() + 1);
                        LOG.info("shruzard - ProxyFrontendHandler: Found warm match "
                                 + "for '{}' at {}. Occupancy: {}", 
                                 targetNamespace, targetWorkerAddress, state.getInflightRequests());
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

                    synchronized (state) {
                        boolean isUnleased = (state.getLeasedNamespace() == null 
                            || state.getLeasedNamespace().isEmpty());
                        boolean isExpiredIdle = (state.getInflightRequests() == 0 
                            && (System.nanoTime() - state.getLastActivityTime() 
                                > java.util.concurrent.TimeUnit.SECONDS.toNanos(35)));
                        
                        if (state.getInflightRequests() == 0 && (isUnleased || isExpiredIdle)) {
                            targetWorkerAddress = workerAddr;
                            state.setLeasedNamespace(targetNamespace);
                            state.setInflightRequests(state.getInflightRequests() + 1);
                            LOG.info("shruzard - ProxyFrontendHandler: Claimed idle pod "
                                     + "(Unleased: {}, ExpiredIdle: {}) at {} for namespace '{}'.", 
                                isUnleased, isExpiredIdle, targetWorkerAddress, targetNamespace);
                            break;
                        }
                    }
                }
            }

            // 3. Busy Rejection: All pods saturated
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
                     try {
                         SslContext sslCtx = SslContextBuilder.forClient()
                             .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
                         p.addLast(sslCtx.newHandler(ch.alloc(), hostPort[0], Integer.parseInt(hostPort[1])));
                     } catch (Exception e) {
                         LOG.error("shruzard - Failed to initialize SSL for outbound proxy", e);
                     }
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
                    LOG.warn("shruzard - ProxyFrontendHandler: Failed to connect to backend worker {}. "
                             + "Evicting from registry.", chosenWorker);
                    podRegistry.remove(chosenWorker);
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
            if (rejecting) {
                boolean isLast = msg instanceof io.netty.handler.codec.http.LastHttpContent;
                ReferenceCountUtil.release(msg);
                if (isLast) {
                    ctx.channel().close();
                }
                return;
            }
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
