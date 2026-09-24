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
    public void testSyncDiscoveryKeepsPodsWithInflightRequests() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());

        // A pod that is mid-request when it drops out of the discovery payload.
        PodState busyPod = new PodState("namespace-A", 2);
        podLeaseManager.getRegistry().put("10.0.0.1:11015", busyPod);

        // Discovery comes back reporting a completely different set of pods.
        podLeaseManager.syncDiscovery(Collections.singletonList(
            discoverableAt("10.0.0.2", 11015)));

        // The busy pod must survive with its accounting intact. Dropping it here would make the
        // eventual releaseLease a no-op and let the pod return with a zeroed count.
        PodState retained = podLeaseManager.getRegistry().get("10.0.0.1:11015");
        assertNotNull(retained);
        assertEquals(2, retained.getInflightRequests());
        assertEquals("namespace-A", retained.getLeasedNamespace());
    }

    @Test
    public void testSyncDiscoveryPrunesPodsOnceDrained() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());

        PodState busyPod = new PodState("namespace-A", 1);
        podLeaseManager.getRegistry().put("10.0.0.1:11015", busyPod);

        Iterable<Discoverable> elsewhere = Collections.singletonList(
            discoverableAt("10.0.0.2", 11015));

        // Still busy: retained.
        podLeaseManager.syncDiscovery(elsewhere);
        assertNotNull(podLeaseManager.getRegistry().get("10.0.0.1:11015"));

        // Last response lands, so the next sync is free to evict it.
        podLeaseManager.releaseLease("10.0.0.1:11015");
        podLeaseManager.syncDiscovery(elsewhere);
        assertNull(podLeaseManager.getRegistry().get("10.0.0.1:11015"));
    }

    @Test
    public void testInvalidateLeaseOnlyReleasesTheCallersSlot() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());

        // Three connections are routed to the same pod; one of them fails to connect.
        PodState pod = new PodState("namespace-A", 3);
        podLeaseManager.getRegistry().put("10.0.0.1:11015", pod);

        podLeaseManager.invalidateLease("10.0.0.1:11015");

        // The other two are still streaming, so the entry must stay and keep counting them.
        // Removing it outright would strand their releaseLease calls and over-subscribe the pod
        // the moment discovery re-registered it.
        PodState retained = podLeaseManager.getRegistry().get("10.0.0.1:11015");
        assertNotNull(retained);
        assertEquals(2, retained.getInflightRequests());
    }

    @Test
    public void testInvalidateLeaseEvictsPodOnceDrained() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());

        PodState pod = new PodState("namespace-A", 1);
        podLeaseManager.getRegistry().put("10.0.0.1:11015", pod);

        // Sole in-flight request fails to connect, so nothing is left on the pod and a dead worker
        // should not linger in the rotation.
        podLeaseManager.invalidateLease("10.0.0.1:11015");

        assertNull(podLeaseManager.getRegistry().get("10.0.0.1:11015"));

        // Invalidating an address that is already gone is a no-op rather than an error.
        podLeaseManager.invalidateLease("10.0.0.1:11015");
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

        // Nothing was leased on behalf of the unnamed request, and discovery was never consulted.
        assertNull(freshPod.getLeasedNamespace());
        assertEquals(0, freshPod.getInflightRequests());
        Mockito.verify(mockDiscovery, Mockito.never()).discover(Mockito.anyString());

        // The connection stays open until the request body is drained, as with every rejection.
        assertTrue(channel.isActive());
        channel.writeInbound(new DefaultLastHttpContent());
        assertFalse(channel.isActive());
    }

    @Test
    public void testInvalidatedPodTakesNoNewLeaseUntilRediscovered() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());
        PodState pod = new PodState("namespace-A", 2);
        podLeaseManager.getRegistry().put("10.0.0.1:11015", pod);

        // One of the two connections fails to connect. The other is still streaming, so the entry
        // stays, but nothing new may be leased onto it: that is what keeps the drained check and
        // the removal from racing with a concurrent acquire.
        podLeaseManager.invalidateLease("10.0.0.1:11015");
        assertSame(pod, podLeaseManager.getRegistry().get("10.0.0.1:11015"));
        assertFalse(pod.isListed());
        assertEquals(1, pod.getInflightRequests());
        assertNull(podLeaseManager.acquireLease("namespace-A"));

        // Discovery still reports the pod, so the next sync puts it back into rotation.
        podLeaseManager.syncDiscovery(Collections.singletonList(discoverableAt("10.0.0.1", 11015)));
        assertTrue(pod.isListed());
        assertEquals("10.0.0.1:11015", podLeaseManager.acquireLease("namespace-A"));
        assertEquals(2, pod.getInflightRequests());
    }

    @Test
    public void testUnlistedPodRefusesEveryLeasePath() {
        PodState warm = new PodState("namespace-A", 1);
        warm.setListed(false);
        assertFalse(warm.tryAcquireWarmLease("namespace-A", 10));

        PodState idle = new PodState("namespace-A", 0);
        idle.setListed(false);
        assertFalse(idle.tryStealIdleLease("namespace-B"));

        PodState fresh = new PodState(null, 0);
        fresh.setListed(false);
        assertFalse(fresh.tryClaimFreshLease("namespace-B"));

        // Relisting makes it leasable again.
        fresh.setListed(true);
        assertTrue(fresh.tryClaimFreshLease("namespace-B"));
    }

    @Test
    public void testPodDroppedFromDiscoveryDrainsWithoutTakingNewLeases() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());
        podLeaseManager.getRegistry().put("10.0.0.1:11015", new PodState("namespace-A", 2));

        // The pod's node disappears: discovery stops reporting it while two requests are still
        // outstanding on it.
        podLeaseManager.syncDiscovery(Collections.emptyList());

        PodState dropped = podLeaseManager.getRegistry().get("10.0.0.1:11015");
        assertNotNull(dropped);
        assertFalse(dropped.isListed());
        assertEquals(2, dropped.getInflightRequests());

        // It has spare capacity for namespace-A, but must not be offered: nothing would answer.
        assertNull(podLeaseManager.acquireLease("namespace-A"));
        assertEquals(2, dropped.getInflightRequests());

        // The outstanding connections close, their slots come back, and the next sync prunes it.
        podLeaseManager.releaseLease("10.0.0.1:11015");
        podLeaseManager.releaseLease("10.0.0.1:11015");
        podLeaseManager.syncDiscovery(Collections.emptyList());
        assertNull(podLeaseManager.getRegistry().get("10.0.0.1:11015"));
    }

    @Test
    public void testPodRelistedAfterNodeRecoveryKeepsItsCounts() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());
        PodState pod = new PodState("namespace-A", 2);
        podLeaseManager.getRegistry().put("10.0.0.1:11015", pod);

        podLeaseManager.syncDiscovery(Collections.emptyList());
        assertFalse(pod.isListed());

        // The node recovers and the same address comes back while its two requests are still running.
        podLeaseManager.syncDiscovery(Collections.singletonList(discoverableAt("10.0.0.1", 11015)));

        assertSame(pod, podLeaseManager.getRegistry().get("10.0.0.1:11015"));
        assertTrue(pod.isListed());
        assertEquals("10.0.0.1:11015", podLeaseManager.acquireLease("namespace-A"));
        assertEquals(3, pod.getInflightRequests());
    }

    private static Discoverable discoverableAt(String host, int port) {
        Discoverable discoverable = mock(Discoverable.class);
        when(discoverable.getSocketAddress()).thenReturn(new InetSocketAddress(host, port));
        return discoverable;
    }
}
