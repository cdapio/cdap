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
                // Thread-safe update from Worker Ground Truth headers
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
                        state.setInflightRequests(Math.max(0, state.getInflightRequests() - 1));
                    }
                    
                    
                    if (leasedNamespace != null) {
                        state.setLeasedNamespace(leasedNamespace);
                    }
                    
                    state.setLastActivityTime(System.currentTimeMillis());
                    
                    if (activeTasksStr != null || leasedNamespace != null) {
                        LOG.info("shruzard - ProxyBackendHandler: Self-Healed "
                                 + "PodState for {}. Occupancy: {}, Namespace: {}", 
                            targetWorkerAddress, state.getInflightRequests(), state.getLeasedNamespace());
                    }
                }
            }
        }
        
        // Forward worker responses directly back to the client
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
        // Backend Worker channel is saturated; pause reading from App Fabric client
        if (inboundChannel != null && inboundChannel.isActive()) {
            inboundChannel.config().setAutoRead(ctx.channel().isWritable());
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        ProxyFrontendHandler.closeOnFlush(inboundChannel);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ProxyFrontendHandler.closeOnFlush(ctx.channel());
    }
}
