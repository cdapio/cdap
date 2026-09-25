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

import io.cdap.cdap.common.conf.Constants;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Relays task worker responses back to AppFabric, releasing the pod lease slot when the response
 * completes and adopting the worker's namespace when it rejects the request.
 */
class ProxyBackendHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyBackendHandler.class);

    /** The connection back to AppFabric; this handler sits on the worker pipeline. */
    private final Channel clientChannel;
    private final PodLeaseManager podLeaseManager;
    private final String targetWorkerAddress;

    private boolean decremented = false;

    ProxyBackendHandler(Channel clientChannel, PodLeaseManager podLeaseManager, String targetWorkerAddress) {
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
                // STEP 1: On a worker rejection, adopt the namespace it reports.
                if (statusCode == HttpResponseStatus.CONFLICT.code()
                        || statusCode == HttpResponseStatus.TOO_MANY_REQUESTS.code()) {
                    String leasedNamespace = resp.headers().get(Constants.Gateway.HEADER_LEASED_NAMESPACE);

                    state.adoptRejectedLease(leasedNamespace);
                    // adoptRejectedLease already released this request's slot.
                    decremented = true;

                    LOG.debug("Self-healed pod state for {} after status {}. Lease now attributed "
                              + "to namespace {}, occupancy {}.",
                        targetWorkerAddress, statusCode, state.getLeasedNamespace(),
                        state.getInflightRequests());
                } else {
                    state.recordActivity();
                }
            }
        } else if (msg instanceof LastHttpContent) {
            // STEP 2: Release the slot once the response completes.
            releaseOccupancy();
        }

        // STEP 3: Relay to AppFabric, then read the next chunk from the worker.
        clientChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                ctx.channel().read();
            } else {
                LOG.warn("Failed to relay the response from task worker {} back to the caller. "
                         + "Closing the caller connection.", targetWorkerAddress, future.cause());
                future.channel().close();
            }
        });
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        // Request-direction backpressure: stop reading from the client while the worker can't keep up.
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
