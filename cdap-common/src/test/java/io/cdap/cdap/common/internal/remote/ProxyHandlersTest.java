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
import io.netty.handler.codec.http.DefaultHttpResponse;
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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

        registry.put("127.0.0.1:8081", pod1);
        registry.put("127.0.0.1:8082", pod2);

        // Tested on PodState directly, since the handler's async connect can't run on an EmbeddedChannel.
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
    public void testRejectionAdoptsNamespaceAndReleasesSlot() {
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

        // The worker rejects the request because it is leased to namespace-B.
        DefaultFullHttpResponse conflictResponse =
            new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONFLICT);
        conflictResponse.headers().set(Constants.Gateway.HEADER_LEASED_NAMESPACE, "namespace-B");

        workerChannel.writeInbound(conflictResponse);

        // Ownership is adopted, so retries stop treating this pod as a warm match for namespace-A.
        assertEquals("namespace-B", pod1.getLeasedNamespace());
        // The slot taken for the rejected request is released.
        assertEquals(0, pod1.getInflightRequests());
        // A lease rejection means the worker is up, so the pod is not backed off.
        assertTrue(pod1.isAvailable(System.nanoTime()));

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
        assertEquals("10.0.0.2:11015", podLeaseManager.acquireSlot("namespace-A"));

        // Late releases are no-ops.
        podLeaseManager.releaseSlot("10.0.0.1:11015");
        podLeaseManager.releaseSlot("10.0.0.1:11015");
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
        podLeaseManager.releaseSlot("10.0.0.1:11015");
        assertEquals(0, newState.getInflightRequests());
    }

    @Test
    public void testReleaseSlotKeepsLeaseAndOtherCounts() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());

        // Three requests are routed to the same pod; one of them finishes.
        PodState pod = new PodState("namespace-A", 3);
        podLeaseManager.getRegistry().put("10.0.0.1:11015", pod);

        podLeaseManager.releaseSlot("10.0.0.1:11015");

        // Only that request's slot is released; the pod stays registered and leased.
        assertSame(pod, podLeaseManager.getRegistry().get("10.0.0.1:11015"));
        assertEquals(2, pod.getInflightRequests());
        assertEquals("namespace-A", pod.getLeasedNamespace());
    }

    @Test
    public void testWarmMatchPicksLeastLoadedPod() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());
        PodState busy = new PodState("namespace-A", 7);
        PodState light = new PodState("namespace-A", 2);
        PodState other = new PodState("namespace-B", 0);
        podLeaseManager.getRegistry().put("10.0.0.1:11015", busy);
        podLeaseManager.getRegistry().put("10.0.0.2:11015", light);
        podLeaseManager.getRegistry().put("10.0.0.3:11015", other);

        assertEquals("10.0.0.2:11015", podLeaseManager.acquireSlot("namespace-A"));
        assertEquals(3, light.getInflightRequests());
        assertEquals(7, busy.getInflightRequests());
        assertEquals(0, other.getInflightRequests());
    }

    @Test
    public void testWarmMatchFillsEvenlyWithoutClaimingFreshPods() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());
        PodState first = new PodState("namespace-A", 0);
        PodState second = new PodState("namespace-A", 0);
        PodState fresh = new PodState(null, 0);
        podLeaseManager.getRegistry().put("10.0.0.1:11015", first);
        podLeaseManager.getRegistry().put("10.0.0.2:11015", second);
        podLeaseManager.getRegistry().put("10.0.0.3:11015", fresh);

        for (int i = 0; i < 10; i++) {
            assertNotNull(podLeaseManager.acquireSlot("namespace-A"));
        }

        // Spread over the two warm pods; the fresh pod stays free for other namespaces.
        assertEquals(5, first.getInflightRequests());
        assertEquals(5, second.getInflightRequests());
        assertNull(fresh.getLeasedNamespace());
        assertEquals(0, fresh.getInflightRequests());
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

    @Test
    public void testSyncDiscoveryRecordsNodeName() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());

        podLeaseManager.syncDiscovery(Arrays.asList(
            discoverableAt("10.0.0.1", 11015, "node-a"),
            discoverableAt("10.0.0.2", 11015)));

        assertEquals("node-a", podLeaseManager.getRegistry().get("10.0.0.1:11015").getNodeName());
        assertNull(podLeaseManager.getRegistry().get("10.0.0.2:11015").getNodeName());
    }

    @Test
    public void testHotNamespaceSpreadsBusyPodsAcrossNodes() {
        CConfiguration cConf = CConfiguration.create();
        cConf.setInt(Constants.TaskWorker.REQUEST_LIMIT, 10);
        PodLeaseManager podLeaseManager = new PodLeaseManager(cConf);
        // 10 / 10 / 5 workers, as on the benchmark cluster.
        Map<String, String> nodeByPod = new HashMap<>();
        for (int i = 0; i < 25; i++) {
            String node = i < 10 ? "node-a" : i < 20 ? "node-b" : "node-c";
            String address = "10.0.0." + i + ":11015";
            nodeByPod.put(address, node);
            podLeaseManager.getRegistry().put(address, new PodState(null, 0, node));
        }

        for (int i = 0; i < 50; i++) {
            assertNotNull(podLeaseManager.acquireSlot("namespace-A"));
        }

        Map<String, Integer> busyPodsPerNode = new HashMap<>();
        for (Map.Entry<String, PodState> entry : podLeaseManager.getRegistry().entrySet()) {
            if (entry.getValue().getInflightRequests() > 0) {
                busyPodsPerNode.merge(nodeByPod.get(entry.getKey()), 1, Integer::sum);
            }
        }
        // Five full pods spread 2 / 2 / 1, never 4 / 0 / 1.
        Integer[] counts = busyPodsPerNode.values().toArray(new Integer[0]);
        Arrays.sort(counts);
        assertEquals(Arrays.asList(1, 2, 2), Arrays.asList(counts));
    }

    @Test
    public void testFreshClaimAvoidsBusyNode() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());
        podLeaseManager.getRegistry().put("10.0.0.1:11015", new PodState("namespace-B", 8, "node-a"));
        PodState freshOnBusyNode = new PodState(null, 0, "node-a");
        PodState freshOnIdleNode = new PodState(null, 0, "node-b");
        podLeaseManager.getRegistry().put("10.0.0.2:11015", freshOnBusyNode);
        podLeaseManager.getRegistry().put("10.0.0.3:11015", freshOnIdleNode);

        assertEquals("10.0.0.3:11015", podLeaseManager.acquireSlot("namespace-A"));
        assertNull(freshOnBusyNode.getLeasedNamespace());
    }

    @Test
    public void testWarmTieBreaksOnNodeLoad() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());
        podLeaseManager.getRegistry().put("10.0.0.1:11015", new PodState("namespace-B", 8, "node-a"));
        PodState warmOnBusyNode = new PodState("namespace-A", 3, "node-a");
        PodState warmOnIdleNode = new PodState("namespace-A", 3, "node-b");
        podLeaseManager.getRegistry().put("10.0.0.2:11015", warmOnBusyNode);
        podLeaseManager.getRegistry().put("10.0.0.3:11015", warmOnIdleNode);

        assertEquals("10.0.0.3:11015", podLeaseManager.acquireSlot("namespace-A"));
        assertEquals(3, warmOnBusyNode.getInflightRequests());
        assertEquals(4, warmOnIdleNode.getInflightRequests());
    }

    @Test
    public void testIdleStealPrefersLeastLoadedNodeOverLru() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());
        podLeaseManager.getRegistry().put("10.0.0.1:11015", new PodState("namespace-B", 5, "node-a"));
        // Older, so LRU alone would pick it.
        PodState idleOnBusyNode = new PodState("namespace-C", 0, "node-a");
        PodState idleOnIdleNode = new PodState("namespace-D", 0, "node-b");
        podLeaseManager.getRegistry().put("10.0.0.2:11015", idleOnBusyNode);
        podLeaseManager.getRegistry().put("10.0.0.3:11015", idleOnIdleNode);

        assertEquals("10.0.0.3:11015", podLeaseManager.acquireSlot("namespace-A"));
        assertEquals("namespace-C", idleOnBusyNode.getLeasedNamespace());
    }

    @Test
    public void testBackoffDoublesThenCaps() {
        assertEquals(TimeUnit.SECONDS.toNanos(10), PodState.backoffNanos(1));
        assertEquals(TimeUnit.SECONDS.toNanos(20), PodState.backoffNanos(2));
        assertEquals(TimeUnit.SECONDS.toNanos(30), PodState.backoffNanos(3));
        assertEquals(TimeUnit.SECONDS.toNanos(30), PodState.backoffNanos(1000));
    }

    @Test
    public void testUnavailablePodIsSkippedUntilBackoffExpires() {
        AtomicLong clock = new AtomicLong();
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create(), clock::get);
        // The restarting pod is idle, so least-loaded selection would otherwise pick it first.
        PodState restarting = new PodState("namespace-A", 1);
        podLeaseManager.getRegistry().put("10.0.0.1:11015", restarting);
        podLeaseManager.getRegistry().put("10.0.0.2:11015", new PodState("namespace-B", 10));

        podLeaseManager.markUnavailable("10.0.0.1:11015", "a failed connect");

        // The slot is released and the lease dropped, since the pod restarts without one.
        assertEquals(0, restarting.getInflightRequests());
        assertNull(restarting.getLeasedNamespace());
        // Neither a fresh claim nor a steal may pick it while it backs off.
        assertNull(podLeaseManager.acquireSlot("namespace-A"));
        assertNull(podLeaseManager.acquireSlot("namespace-C"));

        // Once the backoff expires, the next request probes it.
        clock.set(TimeUnit.SECONDS.toNanos(10));
        assertEquals("10.0.0.1:11015", podLeaseManager.acquireSlot("namespace-A"));
    }

    @Test
    public void testBackoffGrowsUntilPodAnswers() {
        AtomicLong clock = new AtomicLong();
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create(), clock::get);
        PodState pod = new PodState(null, 0);
        podLeaseManager.getRegistry().put("worker1:8080", pod);

        podLeaseManager.markUnavailable("worker1:8080", "a failed connect");
        // The probe at 10s fails too, so the next backoff is 20s.
        clock.set(TimeUnit.SECONDS.toNanos(10));
        podLeaseManager.markUnavailable("worker1:8080", "a failed connect");
        assertFalse(pod.isAvailable(TimeUnit.SECONDS.toNanos(29)));
        assertTrue(pod.isAvailable(TimeUnit.SECONDS.toNanos(30)));

        // The probe at 30s gets an answer, which resets the backoff.
        clock.set(TimeUnit.SECONDS.toNanos(30));
        EmbeddedChannel inboundClientChannel = new EmbeddedChannel();
        EmbeddedChannel workerChannel = new EmbeddedChannel(
            new ProxyBackendHandler(inboundClientChannel, podLeaseManager, "worker1:8080"));
        workerChannel.writeInbound(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
        assertNotNull(inboundClientChannel.readOutbound());

        podLeaseManager.markUnavailable("worker1:8080", "a failed connect");
        assertFalse(pod.isAvailable(TimeUnit.SECONDS.toNanos(39)));
        assertTrue(pod.isAvailable(TimeUnit.SECONDS.toNanos(40)));
    }

    @Test
    public void testConcurrentFailuresStartOneBackoff() {
        AtomicLong clock = new AtomicLong();
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create(), clock::get);
        PodState pod = new PodState("namespace-A", 4);
        podLeaseManager.getRegistry().put("worker1:8080", pod);

        // Four requests were already connecting when the pod went down.
        for (int i = 0; i < 4; i++) {
            podLeaseManager.markUnavailable("worker1:8080", "a failed connect");
        }

        // Every slot is released, but the backoff is still the first one.
        assertEquals(0, pod.getInflightRequests());
        assertFalse(pod.isAvailable(TimeUnit.SECONDS.toNanos(9)));
        assertTrue(pod.isAvailable(TimeUnit.SECONDS.toNanos(10)));

        // The failed probe at 10s is the second failure, not the fifth.
        clock.set(TimeUnit.SECONDS.toNanos(10));
        podLeaseManager.markUnavailable("worker1:8080", "a failed connect");
        assertFalse(pod.isAvailable(TimeUnit.SECONDS.toNanos(29)));
        assertTrue(pod.isAvailable(TimeUnit.SECONDS.toNanos(30)));
    }

    @Test
    public void testBackingOffPodStillCountsTowardNodeLoad() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create(), () -> 0L);
        podLeaseManager.getRegistry().put("10.0.0.1:11015", new PodState("namespace-B", 4, "node-a"));
        podLeaseManager.getRegistry().put("10.0.0.2:11015", new PodState(null, 0, "node-a"));
        podLeaseManager.getRegistry().put("10.0.0.3:11015", new PodState(null, 0, "node-b"));
        podLeaseManager.getRegistry().put("10.0.0.4:11015", new PodState("namespace-C", 2, "node-b"));

        // Leaves three tasks still running on node-a, against two on node-b.
        podLeaseManager.markUnavailable("10.0.0.1:11015", "a drain rejection");

        assertEquals("10.0.0.3:11015", podLeaseManager.acquireSlot("namespace-A"));
    }

    @Test
    public void testDrainRejectionBacksOffPod() {
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());
        PodState pod = new PodState("namespace-A", 1);
        podLeaseManager.getRegistry().put("worker1:8080", pod);

        EmbeddedChannel inboundClientChannel = new EmbeddedChannel();
        EmbeddedChannel workerChannel = new EmbeddedChannel(
            new ProxyBackendHandler(inboundClientChannel, podLeaseManager, "worker1:8080"));
        DefaultFullHttpResponse drainResponse =
            new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.TOO_MANY_REQUESTS);
        drainResponse.headers().set(Constants.Gateway.HEADER_WORKER_DRAINING, "true");

        workerChannel.writeInbound(drainResponse);

        assertFalse(pod.isAvailable(System.nanoTime()));
        assertEquals(0, pod.getInflightRequests());
        assertNull(pod.getLeasedNamespace());
        // The 429 still reaches AppFabric, which retries.
        FullHttpResponse relayedClientResponse = inboundClientChannel.readOutbound();
        assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, relayedClientResponse.status());
        relayedClientResponse.release();
    }

    @Test
    public void testFailedConnectBacksOffPod() {
        DiscoveryServiceClient mockDiscovery = mock(DiscoveryServiceClient.class);
        Discoverable worker = discoverableAt("127.0.0.1", 11015);
        ServiceDiscovered serviceDiscovered = mock(ServiceDiscovered.class);
        when(serviceDiscovered.iterator()).thenReturn(Collections.singletonList(worker).iterator());
        when(mockDiscovery.discover(Mockito.anyString())).thenReturn(serviceDiscovered);
        PodLeaseManager podLeaseManager = new PodLeaseManager(CConfiguration.create());

        EmbeddedChannel channel = new EmbeddedChannel(new ProxyFrontendHandler(podLeaseManager, mockDiscovery));
        HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api");
        req.headers().set(Constants.Gateway.HEADER_CDAP_NAMESPACE, "namespace-A");
        // An EmbeddedChannel's event loop can't register the NIO worker channel, so the connect
        // fails at once and runs the failure path.
        channel.writeInbound(req);

        PodState pod = podLeaseManager.getRegistry().get("127.0.0.1:11015");
        assertNotNull(pod);
        assertFalse(pod.isAvailable(System.nanoTime()));
        assertEquals(0, pod.getInflightRequests());
        assertFalse(channel.isActive());
    }

    private static Discoverable discoverableAt(String host, int port) {
        Discoverable discoverable = mock(Discoverable.class);
        when(discoverable.getSocketAddress()).thenReturn(new InetSocketAddress(host, port));
        return discoverable;
    }

    private static Discoverable discoverableAt(String host, int port, String nodeName) {
        Discoverable discoverable = discoverableAt(host, port);
        when(discoverable.getPayload()).thenReturn(nodeName.getBytes(StandardCharsets.UTF_8));
        return discoverable;
    }
}
