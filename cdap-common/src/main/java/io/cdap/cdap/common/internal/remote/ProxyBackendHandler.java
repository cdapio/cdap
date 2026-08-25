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
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProxyBackendHandler is installed on the outbound Netty channel connected to a Task Worker pod.
 * It intercepts responses coming back from the Task Worker, synchronizes the proxy's in-memory
 * routing registry with the worker's ground-truth state upon rejections, and relays the HTTP response bytes
 * directly back to the inbound client (AppFabric).
 *
 * <p>Key Responsibilities:
 * <ul>
 *   <li>Selective Self-Healing on Rejection: Upon {@code 409 Conflict} or {@code 429 Too Many Requests},
 *       reads {@code X-Active-Tasks} and {@code X-Leased-Namespace} response headers from the worker to correct
 *       any occupancy drift and heal routing tables immediately without distributed consensus.</li>
 *   <li>Occupancy Release: Decrements the local {@code inflightRequests} counter when {@link LastHttpContent}
 *       is received for a completed response stream.</li>
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

    private boolean decremented = false;

    public ProxyBackendHandler(Channel inboundChannel, Map<String, PodState> podRegistry, String targetWorkerAddress) {
        this.inboundChannel = inboundChannel;
        this.podRegistry = podRegistry;
        this.targetWorkerAddress = targetWorkerAddress;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpResponse) {
            HttpResponse resp = (HttpResponse) msg;
            int statusCode = resp.status().code();
            PodState state = podRegistry.get(targetWorkerAddress);

            if (state != null) {
                // STEP 1: Selective Self-Healing & Occupancy Synchronization
                // ONLY synchronize ground truth from headers when the worker explicitly rejects the request
                if (statusCode == HttpResponseStatus.CONFLICT.code()
                        || statusCode == HttpResponseStatus.TOO_MANY_REQUESTS.code()) {
                    
                    String activeTasksStr = resp.headers().get("X-Active-Tasks");
                    String leasedNamespace = resp.headers().get("X-Leased-Namespace");
                    
                    state.updateFromHeader(activeTasksStr, leasedNamespace);
                    decremented = true;
                    
                    LOG.info("shruzard - ProxyBackendHandler: Self-Healed PodState after status {} for {}. "
                             + "Occupancy: {}, Namespace: {}",
                        statusCode, targetWorkerAddress, state.getInflightRequests(), state.getLeasedNamespace());
                } else {
                    // For normal responses (e.g. 200 OK), preserve local occupancy count and update activity timestamp
                    state.recordActivity();
                }
            }
        } else if (msg instanceof LastHttpContent) {
            // STEP 2: Release Occupancy on Stream Completion
            // When the entire HTTP response payload finishes streaming, decrement the in-flight concurrency count.
            releaseOccupancy();
        }

        // STEP 3: Relay Worker Response to Client (AppFabric)
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

    private void releaseOccupancy() {
        if (!decremented) {
            PodState state = podRegistry.get(targetWorkerAddress);
            if (state != null) {
                state.decrementInflightRequests();
            }
            decremented = true;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        releaseOccupancy();
        // If backend worker disconnects or crashes, flush and close the client socket
        ProxyFrontendHandler.closeOnFlush(inboundChannel);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        releaseOccupancy();
        ProxyFrontendHandler.closeOnFlush(ctx.channel());
    }
}
