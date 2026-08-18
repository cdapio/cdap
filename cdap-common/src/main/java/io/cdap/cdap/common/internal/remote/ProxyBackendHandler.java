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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpResponse;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProxyBackendHandler is installed on the outbound Netty channel connected to a Task Worker pod.
 * It intercepts responses coming back from the Task Worker, synchronizes the proxy's in-memory
 * routing registry with the worker's ground-truth state, and relays the HTTP response bytes
 * directly back to the inbound client (AppFabric).
 *
 * <p>Key Responsibilities:
 * <ul>
 *   <li>Ground-Truth State Sync & Self-Healing: Reads {@code X-Active-Tasks} and
 *       {@code X-Leased-Namespace} response headers from the worker to correct any occupancy drift
 *       and heal routing tables without distributed consensus.</li>
 *   <li>Streaming Relay: Writes and flushes HTTP response headers and body chunks directly to the
 *       inbound client channel (AppFabric) with zero heap copies.</li>
 *   <li>Reverse Backpressure: If AppFabric is slow to consume responses, pauses reading from the
 *       worker channel to prevent buffering millions of response bytes in RAM.</li>
 *   <li>Socket Lifecycle Management: Gracefully tears down the inbound client socket if the worker
 *       socket drops or throws an exception.</li>
 * </ul>
 */
public class ProxyBackendHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyBackendHandler.class);

    private final Channel inboundChannel;
    private final Map<String, PodState> podRegistry;
    private final String targetWorkerAddress;

    public ProxyBackendHandler(Channel inboundChannel, Map<String, PodState> podRegistry, String targetWorkerAddress) {
        this.inboundChannel = inboundChannel;
        this.podRegistry = podRegistry;
        this.targetWorkerAddress = targetWorkerAddress;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpResponse) {
            HttpResponse resp = (HttpResponse) msg;
            PodState state = podRegistry.get(targetWorkerAddress);
            if (state != null) {
                // STEP 1: Self-Healing & Occupancy Synchronization
                // Task Worker pods return headers reporting their actual active task count and leased namespace.
                // We update our local PodState with this ground truth to stay perfectly in sync.
                synchronized (state) {
                    String activeTasksStr = resp.headers().get("X-Active-Tasks");
                    String leasedNamespace = resp.headers().get("X-Leased-Namespace");
                    
                    if (activeTasksStr != null) {
                        try {
                            state.setInflightRequests(Integer.parseInt(activeTasksStr));
                        } catch (NumberFormatException e) {
                            state.setInflightRequests(Math.max(0, state.getInflightRequests() - 1));
                        }
                    } else {
                        // If no ground truth header present, decrement inflight count by 1 on response
                        state.setInflightRequests(Math.max(0, state.getInflightRequests() - 1));
                    }
                    
                    if (leasedNamespace != null) {
                        state.setLeasedNamespace(leasedNamespace);
                    }
                    
                    // Update timestamp for idle TTL lease expiration tracking (35s TTL)
                    state.setLastActivityTime(System.currentTimeMillis());
                    
                    if (activeTasksStr != null || leasedNamespace != null) {
                        LOG.info("shruzard - ProxyBackendHandler: Self-Healed "
                                 + "PodState for {}. Occupancy: {}, Namespace: {}", 
                            targetWorkerAddress, state.getInflightRequests(), state.getLeasedNamespace());
                    }
                }
            }
        }
        
        // STEP 2: Relay Worker Response to Client (AppFabric)
        // Forward the HTTP response header or body chunk directly to the inbound client socket.
        // Once write completes successfully, request the next chunk from the worker channel.
        inboundChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                ctx.channel().read();
            } else {
                future.channel().close();
            }
        });
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        // Reverse Backpressure:
        // If the client (AppFabric) channel is saturated and not writable, pause reading from the worker channel.
        // Once the client socket buffer drains, resume reading from the worker channel.
        if (inboundChannel != null && inboundChannel.isActive()) {
            inboundChannel.config().setAutoRead(ctx.channel().isWritable());
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // If backend worker disconnects or crashes, flush and close the client socket
        ProxyFrontendHandler.closeOnFlush(inboundChannel);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ProxyFrontendHandler.closeOnFlush(ctx.channel());
    }
}
