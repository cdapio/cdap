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

    /**
     * The connection back to AppFabric. This handler is installed on the WORKER pipeline, so this
     * is the peer channel: events arrive here from the worker, writes go out to the client.
     */
    private final Channel clientChannel;
    private final PodLeaseManager podLeaseManager;
    private final String targetWorkerAddress;

    private boolean decremented = false;

    public ProxyBackendHandler(Channel clientChannel, PodLeaseManager podLeaseManager, String targetWorkerAddress) {
        this.clientChannel = clientChannel;
        this.podLeaseManager = podLeaseManager;
        this.targetWorkerAddress = targetWorkerAddress;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpResponse) {
            HttpResponse resp = (HttpResponse) msg;
            int statusCode = resp.status().code();

            PodState state = podLeaseManager.getRegistry().get(targetWorkerAddress);

            if (state != null) {
                // STEP 1: Selective self-healing.
                //
                // Lease ownership is only adopted from the worker when the worker explicitly
                // rejects the request. A rejection is proof that this proxy's view of the pod was
                // wrong, which makes it the one point where overriding local state is justified.
                if (statusCode == HttpResponseStatus.CONFLICT.code()
                        || statusCode == HttpResponseStatus.TOO_MANY_REQUESTS.code()) {

                    // Only X-Leased-Namespace is adopted. X-Active-Tasks is intentionally not read
                    // here: it counts work owned by connections this proxy does not hold, so it
                    // can never be decremented and would strand the pod. See
                    // PodState#adoptRejectedLease for the full reasoning.
                    String leasedNamespace = resp.headers().get("X-Leased-Namespace");

                    state.adoptRejectedLease(leasedNamespace);
                    // adoptRejectedLease already released the slot taken for this request, so the
                    // decrement that would normally happen on LastHttpContent must be suppressed.
                    decremented = true;

                    LOG.debug("Self-healed pod state for {} after status {}. Lease now attributed "
                              + "to namespace {}, occupancy {}.",
                        targetWorkerAddress, statusCode, state.getLeasedNamespace(),
                        state.getInflightRequests());
                } else {
                    // For successful responses the locally tracked occupancy is authoritative: it
                    // counts exactly the requests this proxy dispatched and will see complete.
                    //
                    // The worker sends X-Active-Tasks on success too, but adopting it would hurt.
                    // The worker samples that value before it decrements for the finishing task,
                    // so any request dispatched while this response was in flight is missing from
                    // it, and adopting it would under-count. Under-counting over-subscribes the
                    // pod and earns a saturation rejection, which AppFabric retries with
                    // exponential backoff - a latency cost paid on the hot path. Over-counting
                    // merely leaves a slot unused. The conservative direction is cheaper, so the
                    // local count wins.
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
        clientChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                ctx.channel().read();
            } else {
                LOG.warn("Failed to relay the task worker response back to the caller. "
                         + "Closing the channel.");
                future.channel().close();
            }
        });
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        // Backpressure on the request direction.
        //
        // This handler is installed on the worker pipeline, so this fires when the WORKER's own
        // write buffer crosses a watermark. That buffer holds request body we are streaming to the
        // worker, and the only thing feeding it is our reads from the client. So when the worker
        // stops keeping up, stop pulling from the client.
        //
        // The response direction is the mirror of this and lives in
        // ProxyFrontendHandler#channelWritabilityChanged. Each handler reacts to its own channel's
        // writability, because Netty raises this event only on the pipeline of the channel whose
        // buffer actually moved.
        Channel workerChannel = ctx.channel();
        if (clientChannel != null && clientChannel.isActive()) {
            clientChannel.config().setAutoRead(workerChannel.isWritable());
        }
        ctx.fireChannelWritabilityChanged();
    }

    private void releaseOccupancy() {
        if (!decremented) {
            podLeaseManager.releaseLease(targetWorkerAddress);
            decremented = true;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        releaseOccupancy();
        // If backend worker disconnects or crashes, flush and close the client socket
        ProxyFrontendHandler.closeOnFlush(clientChannel);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("Error on the connection to task worker {}", targetWorkerAddress, cause);
        releaseOccupancy();
        ProxyFrontendHandler.closeOnFlush(ctx.channel());
    }
}
