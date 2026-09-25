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

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ResourceLeakDetector;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProxyHandlersTest {

    @BeforeClass
    public static void setup() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @Test
    public void testSaturationRejection() {
        DiscoveryServiceClient mockDiscovery = mock(DiscoveryServiceClient.class);
        Discoverable discoverable = mock(Discoverable.class);
        when(discoverable.getSocketAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 8080));
        ServiceDiscovered serviceDiscovered = mock(ServiceDiscovered.class);
        when(serviceDiscovered.iterator()).thenReturn(Collections.singletonList(discoverable).iterator());
        when(mockDiscovery.discover(Mockito.anyString())).thenReturn(serviceDiscovered);

        io.cdap.cdap.common.conf.CConfiguration cConf = io.cdap.cdap.common.conf.CConfiguration.create();
        PodLeaseManager podLeaseManager = new PodLeaseManager(cConf);
        PodState saturatedPod = new PodState("namespace-A", 10);
        podLeaseManager.getRegistry().put("127.0.0.1:8080", saturatedPod);

        ProxyFrontendHandler frontendHandler = new ProxyFrontendHandler(podLeaseManager, mockDiscovery);
        EmbeddedChannel channel = new EmbeddedChannel(frontendHandler);

        HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api");
        req.headers().set(Constants.Gateway.HEADER_CDAP_NAMESPACE, "namespace-A");

        channel.writeInbound(req);
        
        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, response.status());
        response.release();

        channel.writeInbound(new DefaultLastHttpContent());
        assertFalse(channel.isActive());
    }

    @Test
    public void testConcurrencyCappingAndNamespaceIsolation() {
        Map<String, PodState> registry = new HashMap<>();
        // Two pods, both starting idle, registered up front so lease claiming does not hit the
        // starvation threshold.
        PodState pod1 = new PodState(null, 0);
        PodState pod2 = new PodState(null, 0);

        // Force the last activity time back to mark it as completely idle
        registry.put("127.0.0.1:8081", pod1);
        registry.put("127.0.0.1:8082", pod2);

        // PodState is asserted directly rather than through ProxyFrontendHandler: the handler routes
        // via Bootstrap.connect, which is asynchronous and not readable synchronously from an
        // EmbeddedChannel without mocking Bootstrap. PodState is what actually dictates isolation.
        
        boolean acquired = pod1.tryClaimFreshLease("namespace-A");
        assertTrue(acquired);
        assertEquals("namespace-A", pod1.getLeasedNamespace());
        assertEquals(1, pod1.getInflightRequests());

        // Cap to 10
        for (int i = 2; i <= 10; i++) {
            assertTrue(pod1.tryAcquireWarmLease("namespace-A", 10));
        }
        assertEquals(10, pod1.getInflightRequests());

        // 11th request for namespace-A must fail on Pod 1, spill to Pod 2
        assertFalse(pod1.tryAcquireWarmLease("namespace-A", 10));
        assertTrue(pod2.tryClaimFreshLease("namespace-A"));
        assertEquals("namespace-A", pod2.getLeasedNamespace());
        
        // Namespace Isolation: namespace-B must go to an entirely new pod, but we don't have one!
        // It will fail because pod1 and pod2 are both leased by namespace-A.
        assertFalse(pod1.tryAcquireWarmLease("namespace-B", 10));
        assertFalse(pod2.tryAcquireWarmLease("namespace-B", 10));
        assertFalse(pod1.tryStealIdleLease("namespace-B"));
        assertFalse(pod2.tryStealIdleLease("namespace-B"));
    }

    @Test
    public void testSelfHealingAdoptsNamespaceButNotTaskCount() {
        CConfiguration cConf = CConfiguration.create();
        cConf.setInt(Constants.TaskWorker.REQUEST_LIMIT, 10);
        PodLeaseManager podLeaseManager = new PodLeaseManager(cConf);
        // The proxy speculatively claimed this pod for namespace-A and has one request in flight.
        PodState pod1 = new PodState("namespace-A", 1);
        podLeaseManager.getRegistry().put("worker1:8080", pod1);

        EmbeddedChannel inboundClientChannel = new EmbeddedChannel();
        ProxyBackendHandler backendHandler =
            new ProxyBackendHandler(inboundClientChannel, podLeaseManager, "worker1:8080");
        EmbeddedChannel workerChannel = new EmbeddedChannel(backendHandler);

        // The worker rejects the request: it is actually leased to namespace-B and reports seven
        // active tasks belonging to connections this proxy does not own.
        DefaultFullHttpResponse conflictResponse =
            new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONFLICT);
        conflictResponse.headers().set("X-Leased-Namespace", "namespace-B");
        conflictResponse.headers().set("X-Active-Tasks", "7");

        workerChannel.writeInbound(conflictResponse);

        // Ownership is adopted, so retries stop treating this pod as a warm match for namespace-A.
        assertEquals("namespace-B", pod1.getLeasedNamespace());
        // The reported count is discarded and the speculative slot is released. Adopting 7 here
        // would be un-decrementable, since no completion for those tasks will ever reach us.
        assertEquals(0, pod1.getInflightRequests());

        FullHttpResponse relayedClientResponse = inboundClientChannel.readOutbound();
        assertNotNull(relayedClientResponse);
        relayedClientResponse.release();
    }

    @Test
    public void testSelfHealedPodIsNotPinnedToRejectedNamespace() {
        PodState pod = new PodState("namespace-A", 1);

        pod.adoptRejectedLease("namespace-B");

        // The rejected namespace must stop matching, otherwise the warm-match tier would send every
        // retry straight back to the worker that just rejected it.
        assertFalse(pod.tryAcquireWarmLease("namespace-A", 10));
        // Nor is it claimable as an unleased pod, since it now has a known owner.
        assertFalse(pod.tryClaimFreshLease("namespace-A"));

        // It does stay a last-resort steal candidate, which is what allows progress once every pod
        // in the cluster is leased.
        assertTrue(pod.tryStealIdleLease("namespace-A"));
    }

    @Test
    public void testSelfHealedPodKeepsFullCapacityForReportedOwner() {
        PodState pod = new PodState("namespace-A", 1);

        pod.adoptRejectedLease("namespace-B");

        // No phantom occupancy was recorded, so the reported owner can still use all ten slots.
        for (int i = 1; i <= 10; i++) {
            assertTrue(pod.tryAcquireWarmLease("namespace-B", 10));
        }
        assertEquals(10, pod.getInflightRequests());
        assertFalse(pod.tryAcquireWarmLease("namespace-B", 10));
    }

    @Test
    public void testSyncDiscoveryEvictsDroppedPodsImmediately() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());

        PodState busyPod = new PodState("namespace-A", 2);
        podLeaseManager.getRegistry().put("10.0.0.1:11015", busyPod);

        podLeaseManager.syncDiscovery(Collections.singletonList(discoverableAt("10.0.0.2", 11015)));

        // Evicted even though requests are in flight.
        assertNull(podLeaseManager.getRegistry().get("10.0.0.1:11015"));
        assertNotNull(podLeaseManager.getRegistry().get("10.0.0.2:11015"));
        assertEquals("10.0.0.2:11015", podLeaseManager.acquireLease("namespace-A"));

        // Late releases are no-ops.
        podLeaseManager.releaseLease("10.0.0.1:11015");
        podLeaseManager.releaseLease("10.0.0.1:11015");
        assertNull(podLeaseManager.getRegistry().get("10.0.0.1:11015"));
    }

    @Test
    public void testRediscoveredPodStartsFresh() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());
        PodState oldState = new PodState("namespace-A", 2);
        podLeaseManager.getRegistry().put("10.0.0.1:11015", oldState);

        podLeaseManager.syncDiscovery(Collections.emptyList());
        podLeaseManager.syncDiscovery(Collections.singletonList(discoverableAt("10.0.0.1", 11015)));

        // Old counts are not carried over.
        PodState newState = podLeaseManager.getRegistry().get("10.0.0.1:11015");
        assertNotNull(newState);
        assertNotSame(oldState, newState);
        assertNull(newState.getLeasedNamespace());
        assertEquals(0, newState.getInflightRequests());

        // A late release for the old entry is floored at zero.
        podLeaseManager.releaseLease("10.0.0.1:11015");
        assertEquals(0, newState.getInflightRequests());
    }

    @Test
    public void testReleaseAfterFailedConnectKeepsPodAndOtherCounts() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());

        // Three connections are routed to the same pod; one of them fails to connect.
        PodState pod = new PodState("namespace-A", 3);
        podLeaseManager.getRegistry().put("10.0.0.1:11015", pod);

        podLeaseManager.releaseLease("10.0.0.1:11015");

        // Only the failed connection's slot is released; the pod stays registered.
        assertSame(pod, podLeaseManager.getRegistry().get("10.0.0.1:11015"));
        assertEquals(2, pod.getInflightRequests());
        assertEquals("namespace-A", pod.getLeasedNamespace());
    }

    @Test
    public void testMissingNamespaceHeaderIsRejectedBeforeLeasing() {
        DiscoveryServiceClient mockDiscovery = mock(DiscoveryServiceClient.class);
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());
        PodState freshPod = new PodState(null, 0);
        podLeaseManager.getRegistry().put("10.0.0.1:11015", freshPod);

        EmbeddedChannel channel = new EmbeddedChannel(
            new ProxyFrontendHandler(podLeaseManager, mockDiscovery));
        channel.writeInbound(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api"));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        response.release();

        // Nothing leased and discovery never consulted.
        assertNull(freshPod.getLeasedNamespace());
        assertEquals(0, freshPod.getInflightRequests());
        Mockito.verify(mockDiscovery, Mockito.never()).discover(Mockito.anyString());

        // Closed only after the request body is drained.
        assertTrue(channel.isActive());
        channel.writeInbound(new DefaultLastHttpContent());
        assertFalse(channel.isActive());
    }

    private static Discoverable discoverableAt(String host, int port) {
        Discoverable discoverable = mock(Discoverable.class);
        when(discoverable.getSocketAddress()).thenReturn(new InetSocketAddress(host, port));
        return discoverable;
    }
}
