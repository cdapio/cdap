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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ResourceLeakDetector;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class TaskWorkerManagerHandlersTest {

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
    when(serviceDiscovered.iterator())
        .thenReturn(Collections.singletonList(discoverable).iterator());
    when(mockDiscovery.discover(Mockito.anyString())).thenReturn(serviceDiscovered);

    CConfiguration cConf = CConfiguration.create();
    TaskWorkerLeaser leaser = new TaskWorkerLeaser(cConf, System::nanoTime);
    TaskWorkerState saturatedPod = new TaskWorkerState("namespace-A", 10, null, 0L);
    leaser.getRegistry().put("127.0.0.1:8080", saturatedPod);

    TaskWorkerManagerDrainState drainState = new TaskWorkerManagerDrainState();
    TaskWorkerManagerFrontendHandler frontendHandler =
        new TaskWorkerManagerFrontendHandler(leaser, mockDiscovery, drainState, true);
    EmbeddedChannel channel = new EmbeddedChannel(frontendHandler);

    HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api");
    req.headers().set(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, "namespace-A");

    channel.writeInbound(req);

    FullHttpResponse response = channel.readOutbound();
    assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, response.status());
    response.release();
    assertEquals(0, drainState.getInFlight());

    channel.writeInbound(new DefaultLastHttpContent());
    assertFalse(channel.isActive());
  }

  @Test
  public void testConcurrencyCappingAndNamespaceIsolation() {
    Map<String, TaskWorkerState> registry = new HashMap<>();
    // Two pods, both starting idle, registered up front so lease claiming does not hit the
    // starvation threshold.
    TaskWorkerState pod1 = new TaskWorkerState(null, 0, null, 0L);
    TaskWorkerState pod2 = new TaskWorkerState(null, 0, null, 0L);

    registry.put("127.0.0.1:8081", pod1);
    registry.put("127.0.0.1:8082", pod2);

    // Tested on TaskWorkerState directly, since the handler's async connect can't run on an
    // EmbeddedChannel.
    boolean acquired = pod1.tryClaimFreshLease("namespace-A", 10, 1L);
    assertTrue(acquired);
    assertEquals("namespace-A", pod1.getLeasedNamespace());
    assertEquals(1, pod1.getInflightRequests());

    // Cap to 10
    for (int i = 2; i <= 10; i++) {
      assertTrue(pod1.tryAcquireWarmLease("namespace-A", 10, i));
    }
    assertEquals(10, pod1.getInflightRequests());

    // 11th request for namespace-A must fail on Pod 1, spill to Pod 2
    assertFalse(pod1.tryAcquireWarmLease("namespace-A", 10, 11L));
    assertTrue(pod2.tryClaimFreshLease("namespace-A", 10, 11L));
    assertEquals("namespace-A", pod2.getLeasedNamespace());

    // Namespace Isolation: namespace-B must go to an entirely new pod, but we don't have one!
    // It will fail because pod1 and pod2 are both leased by namespace-A.
    assertFalse(pod1.tryAcquireWarmLease("namespace-B", 10, 12L));
    assertFalse(pod2.tryAcquireWarmLease("namespace-B", 10, 12L));
    assertFalse(pod1.tryStealIdleLease("namespace-B", 10, 12L));
    assertFalse(pod2.tryStealIdleLease("namespace-B", 10, 12L));
  }

  @Test
  public void testRejectionAdoptsNamespaceAndReleasesSlot() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.TaskWorker.REQUEST_LIMIT, 10);
    AtomicLong clock = new AtomicLong(100L);
    TaskWorkerLeaser leaser = new TaskWorkerLeaser(cConf, clock::get);
    // The manager speculatively claimed this pod for namespace-A and has one request in flight.
    TaskWorkerState pod1 = new TaskWorkerState("namespace-A", 1, null, 50L);
    leaser.getRegistry().put("worker1:8080", pod1);

    EmbeddedChannel inboundClientChannel = new EmbeddedChannel();
    TaskWorkerManagerBackendHandler backendHandler =
        backendHandler(inboundClientChannel, leaser, "worker1:8080");
    EmbeddedChannel workerChannel = new EmbeddedChannel(backendHandler);

    // The worker rejects the request because it is leased to namespace-B.
    DefaultFullHttpResponse rejection =
        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.TOO_MANY_REQUESTS);
    rejection.headers().set(
        Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE, "namespace-B");

    workerChannel.writeInbound(rejection);

    // Ownership is adopted, so retries stop treating this pod as a warm match for namespace-A.
    assertEquals("namespace-B", pod1.getLeasedNamespace());
    // The slot taken for the rejected request is released and activity uses the injected clock.
    assertEquals(0, pod1.getInflightRequests());
    assertEquals(100L, pod1.getLastActivityTime());
    // Rejected pods cool down for 5s (not the 10s unreachable backoff), then re-open.
    assertFalse(pod1.isAvailable(clock.get()));
    assertTrue(pod1.isAvailable(clock.get() + TaskWorkerState.REJECTION_SKIP_NANOS));

    FullHttpResponse relayedClientResponse = inboundClientChannel.readOutbound();
    assertNotNull(relayedClientResponse);
    relayedClientResponse.release();
  }

  @Test
  public void testSuccessResponseRelaysBodyAndReleasesSlotOnLastContent() {
    AtomicLong clock = new AtomicLong(200L);
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 1, null, 100L);
    leaser.getRegistry().put("worker1:8080", pod);

    EmbeddedChannel inboundClientChannel = new EmbeddedChannel();
    EmbeddedChannel workerChannel = new EmbeddedChannel(
        backendHandler(inboundClientChannel, leaser, "worker1:8080"));

    workerChannel.writeInbound(
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
    // Slot stays held until LastHttpContent arrives; response header stamps activity via clock.
    assertEquals(1, pod.getInflightRequests());
    assertEquals(200L, pod.getLastActivityTime());

    clock.set(300L);
    workerChannel.writeInbound(new DefaultHttpContent(
        Unpooled.copiedBuffer("hello ", StandardCharsets.UTF_8)));
    assertEquals(1, pod.getInflightRequests());

    workerChannel.writeInbound(new DefaultLastHttpContent(
        Unpooled.copiedBuffer("world", StandardCharsets.UTF_8)));
    assertEquals(0, pod.getInflightRequests());
    assertEquals(300L, pod.getLastActivityTime());
    assertEquals("namespace-A", pod.getLeasedNamespace());

    HttpResponse relayedHeader = inboundClientChannel.readOutbound();
    assertEquals(HttpResponseStatus.OK, relayedHeader.status());
    HttpContent chunk1 = inboundClientChannel.readOutbound();
    assertEquals("hello ", chunk1.content().toString(StandardCharsets.UTF_8));
    chunk1.release();
    LastHttpContent chunk2 = inboundClientChannel.readOutbound();
    assertEquals("world", chunk2.content().toString(StandardCharsets.UTF_8));
    chunk2.release();
  }

  @Test
  public void testFailedRelayToClientClosesCallerConnection() {
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 1, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);

    EmbeddedChannel inboundClientChannel = new EmbeddedChannel();
    // Closing the caller channel makes writeAndFlush fail, exercising the failure listener.
    inboundClientChannel.close();
    EmbeddedChannel workerChannel = new EmbeddedChannel(
        backendHandler(inboundClientChannel, leaser, "worker1:8080"));

    workerChannel.writeInbound(
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
    workerChannel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);

    assertEquals(0, pod.getInflightRequests());
    assertFalse(inboundClientChannel.isActive());
  }

  @Test
  public void testSelfHealedPodIsNotPinnedToRejectedNamespace() {
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 1, null, 0L);

    pod.markRejected("namespace-B", "namespace-A", 10L);

    // Direct CAS attempts during the 5s cooldown must also skip the pod, except for its holder.
    assertFalse(pod.tryStealIdleLease("namespace-A", 10, 20L));
    assertFalse(pod.tryStealIdleLease("namespace-C", 10, 20L));
    assertTrue(pod.snapshot().isAvailableFor("namespace-B", 20L));
    // A pod that turned its own holder away is full, so the holder is skipped as well.
    TaskWorkerState fullPod = new TaskWorkerState("namespace-B", 1, null, 0L);
    fullPod.markRejected("namespace-B", "namespace-B", 10L);
    assertFalse(fullPod.tryAcquireWarmLease("namespace-B", 10, 20L));
    assertFalse(fullPod.tryClaimFreshLease("namespace-B", 10, 20L));
    assertFalse(fullPod.tryStealIdleLease("namespace-B", 10, 20L));
    TaskWorkerState coolingFresh = new TaskWorkerState("namespace-A", 1, null, 0L);
    coolingFresh.markRejected(null, "namespace-A", 10L);
    assertFalse(coolingFresh.tryClaimFreshLease("namespace-B", 10, 20L));

    long afterCooldown = 10L + TaskWorkerState.REJECTION_SKIP_NANOS;
    // The rejected namespace must stop matching, otherwise the warm-match tier would send every
    // retry straight back to the worker that just rejected it.
    assertFalse(pod.tryAcquireWarmLease("namespace-A", 10, afterCooldown));
    // Nor is it claimable as an unleased pod, since it now has a known owner.
    assertFalse(pod.tryClaimFreshLease("namespace-A", 10, afterCooldown));

    // Once the cooldown expires, it stays a last-resort steal candidate for progress.
    assertTrue(pod.tryStealIdleLease("namespace-A", 10, afterCooldown));
  }

  @Test
  public void testSelfHealedPodKeepsFullCapacityForReportedOwner() {
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 1, null, 0L);

    pod.markRejected("namespace-B", "namespace-A", 10L);

    long afterCooldown = 10L + TaskWorkerState.REJECTION_SKIP_NANOS;
    // No phantom occupancy was recorded, so the reported owner can use all ten slots after 5s.
    for (int i = 1; i <= 10; i++) {
      assertTrue(pod.tryAcquireWarmLease("namespace-B", 10, afterCooldown + i));
    }
    assertEquals(10, pod.getInflightRequests());
    assertFalse(pod.tryAcquireWarmLease("namespace-B", 10, afterCooldown + 20L));
  }

  @Test
  public void testSyncDiscoveryEvictsDroppedPodsImmediately() {
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);

    TaskWorkerState busyPod = new TaskWorkerState("namespace-A", 2, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", busyPod);

    leaser.syncDiscovery(Collections.singletonList(discoverableAt("10.0.0.2", 11015)));

    // Evicted even though requests are in flight.
    assertNull(leaser.getRegistry().get("10.0.0.1:11015"));
    assertNotNull(leaser.getRegistry().get("10.0.0.2:11015"));
    assertEquals("10.0.0.2:11015", leaser.acquireSlot("namespace-A"));

    // Late releases are no-ops.
    leaser.releaseSlot("10.0.0.1:11015");
    leaser.releaseSlot("10.0.0.1:11015");
    assertNull(leaser.getRegistry().get("10.0.0.1:11015"));
  }

  @Test
  public void testRediscoveredPodStartsFresh() {
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);
    TaskWorkerState oldState = new TaskWorkerState("namespace-A", 2, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", oldState);

    leaser.syncDiscovery(Collections.emptyList());
    leaser.syncDiscovery(Collections.singletonList(discoverableAt("10.0.0.1", 11015)));

    // Old counts are not carried over.
    TaskWorkerState newState = leaser.getRegistry().get("10.0.0.1:11015");
    assertNotNull(newState);
    assertNotSame(oldState, newState);
    assertNull(newState.getLeasedNamespace());
    assertEquals(0, newState.getInflightRequests());

    // A late release for the old entry is floored at zero.
    leaser.releaseSlot("10.0.0.1:11015");
    assertEquals(0, newState.getInflightRequests());
  }

  @Test
  public void testReleaseSlotKeepsLeaseAndOtherCounts() {
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);

    // Three requests are routed to the same pod; one of them finishes.
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 3, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", pod);

    leaser.releaseSlot("10.0.0.1:11015");

    // Only that request's slot is released; the pod stays registered and leased.
    assertSame(pod, leaser.getRegistry().get("10.0.0.1:11015"));
    assertEquals(2, pod.getInflightRequests());
    assertEquals("namespace-A", pod.getLeasedNamespace());
  }

  @Test
  public void testWarmMatchPicksLeastLoadedPod() {
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);
    TaskWorkerState busy = new TaskWorkerState("namespace-A", 7, null, 0L);
    TaskWorkerState light = new TaskWorkerState("namespace-A", 2, null, 0L);
    TaskWorkerState other = new TaskWorkerState("namespace-B", 0, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", busy);
    leaser.getRegistry().put("10.0.0.2:11015", light);
    leaser.getRegistry().put("10.0.0.3:11015", other);

    assertEquals("10.0.0.2:11015", leaser.acquireSlot("namespace-A"));
    assertEquals(3, light.getInflightRequests());
    assertEquals(7, busy.getInflightRequests());
    assertEquals(0, other.getInflightRequests());
  }

  @Test
  public void testWarmMatchFillsEvenlyWithoutClaimingFreshPods() {
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);
    TaskWorkerState first = new TaskWorkerState("namespace-A", 0, null, 0L);
    TaskWorkerState second = new TaskWorkerState("namespace-A", 0, null, 0L);
    TaskWorkerState fresh = new TaskWorkerState(null, 0, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", first);
    leaser.getRegistry().put("10.0.0.2:11015", second);
    leaser.getRegistry().put("10.0.0.3:11015", fresh);

    for (int i = 0; i < 10; i++) {
      assertNotNull(leaser.acquireSlot("namespace-A"));
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
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);
    TaskWorkerState freshPod = new TaskWorkerState(null, 0, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", freshPod);

    EmbeddedChannel channel = new EmbeddedChannel(
        new TaskWorkerManagerFrontendHandler(
            leaser, mockDiscovery, new TaskWorkerManagerDrainState(), true));
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
  public void testPingIsAnsweredWithoutLeasing() {
    DiscoveryServiceClient mockDiscovery = mock(DiscoveryServiceClient.class);
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);
    TaskWorkerState freshPod = new TaskWorkerState(null, 0, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", freshPod);

    EmbeddedChannel channel = new EmbeddedChannel(
        new TaskWorkerManagerFrontendHandler(
            leaser, mockDiscovery, new TaskWorkerManagerDrainState(), true));
    // The readiness probe sends no namespace header; a query string must not defeat the match.
    channel.writeInbound(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
        Constants.TaskWorkerManager.PING_PATH + "?probe=readiness"));

    FullHttpResponse response = channel.readOutbound();
    assertEquals(HttpResponseStatus.OK, response.status());
    assertEquals(0, response.content().readableBytes());
    response.release();

    // Answered by the manager itself: nothing leased and discovery never consulted.
    assertNull(freshPod.getLeasedNamespace());
    assertEquals(0, freshPod.getInflightRequests());
    Mockito.verify(mockDiscovery, Mockito.never()).discover(Mockito.anyString());

    channel.writeInbound(new DefaultLastHttpContent());
    assertFalse(channel.isActive());
  }

  @Test
  public void testOnlyGetPingSkipsTheNamespaceCheck() {
    DiscoveryServiceClient mockDiscovery = mock(DiscoveryServiceClient.class);
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);

    EmbeddedChannel channel = new EmbeddedChannel(
        new TaskWorkerManagerFrontendHandler(
            leaser, mockDiscovery, new TaskWorkerManagerDrainState(), true));
    channel.writeInbound(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
        Constants.TaskWorkerManager.PING_PATH));

    // A task request to the same path still needs a namespace.
    FullHttpResponse response = channel.readOutbound();
    assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
    response.release();
    Mockito.verify(mockDiscovery, Mockito.never()).discover(Mockito.anyString());

    channel.writeInbound(new DefaultLastHttpContent());
    assertFalse(channel.isActive());
  }

  @Test
  public void testDrainingAnswers503WithoutLeasing() {
    DiscoveryServiceClient mockDiscovery = mock(DiscoveryServiceClient.class);
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);
    TaskWorkerState freshPod = new TaskWorkerState(null, 0, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", freshPod);
    TaskWorkerManagerDrainState drainState = new TaskWorkerManagerDrainState();
    drainState.startDraining();

    EmbeddedChannel channel = new EmbeddedChannel(
        new TaskWorkerManagerFrontendHandler(leaser, mockDiscovery, drainState, true));
    HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api");
    req.headers().set(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, "namespace-A");
    channel.writeInbound(req);

    // A 503 is what App Fabric retries; the body says why.
    FullHttpResponse response = channel.readOutbound();
    assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE, response.status());
    assertEquals(TaskWorkerManagerFrontendHandler.MANAGER_DRAINING_BODY,
        response.content().toString(StandardCharsets.UTF_8));
    assertEquals("close", response.headers().get("Connection"));
    response.release();

    // Never given a worker, so it doesn't hold up the shutdown.
    assertNull(freshPod.getLeasedNamespace());
    assertEquals(0, drainState.getInFlight());
    Mockito.verify(mockDiscovery, Mockito.never()).discover(Mockito.anyString());

    channel.writeInbound(new DefaultLastHttpContent());
    assertFalse(channel.isActive());
  }

  @Test
  public void testRequestCountsAsInFlightUntilItsConnectionCloses() {
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);
    TaskWorkerManagerDrainState drainState = new TaskWorkerManagerDrainState();
    EmbeddedChannel channel = new EmbeddedChannel(new TaskWorkerManagerFrontendHandler(
        leaser, discoveryOf(discoverableAt("127.0.0.1", 11015)), drainState, true));
    HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api");
    req.headers().set(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, "namespace-A");

    // The EmbeddedChannel can't host the worker connection, so the connect fails and the request
    // is answered 503; it still counts until the caller's connection closes.
    channel.writeInbound(req);
    assertWorkerUnreachableResponse(channel.readOutbound());
    assertEquals(1, drainState.getInFlight());

    channel.writeInbound(new DefaultLastHttpContent());
    assertFalse(channel.isActive());
    assertEquals(0, drainState.getInFlight());
  }

  @Test
  public void testMalformedWorkerAddressReturns502AndReleasesSlot() {
    TaskWorkerLeaser leaser = new TaskWorkerLeaser(
        CConfiguration.create(), System::nanoTime) {
      @Override
      String acquireSlot(String namespace) {
        getRegistry().put("malformed-no-port", new TaskWorkerState(namespace, 1, null, 0L));
        return "malformed-no-port";
      }
    };
    TaskWorkerManagerDrainState drainState = new TaskWorkerManagerDrainState();
    EmbeddedChannel channel = new EmbeddedChannel(new TaskWorkerManagerFrontendHandler(
        leaser, discoveryOf(discoverableAt("127.0.0.1", 11015)), drainState, true));
    HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api");
    req.headers().set(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, "namespace-A");

    channel.writeInbound(req);

    FullHttpResponse response = channel.readOutbound();
    assertEquals(HttpResponseStatus.BAD_GATEWAY, response.status());
    response.release();
    assertEquals(0, leaser.getRegistry().get("malformed-no-port").getInflightRequests());

    channel.writeInbound(new DefaultLastHttpContent());
    assertFalse(channel.isActive());
    assertEquals(0, drainState.getInFlight());
  }

  @Test
  public void testPipelinedSecondRequestIsRejectedWith400() {
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);
    TaskWorkerManagerDrainState drainState = new TaskWorkerManagerDrainState();
    EmbeddedChannel channel = new EmbeddedChannel(new TaskWorkerManagerFrontendHandler(
        leaser, discoveryOf(discoverableAt("127.0.0.1", 11015)), drainState, true));

    HttpRequest first = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api");
    first.headers().set(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, "namespace-A");
    channel.writeInbound(first);
    // Drain the 503 from the first request's failed EmbeddedChannel connect.
    assertWorkerUnreachableResponse(channel.readOutbound());
    assertTrue(channel.isActive());

    // A second pipelined HttpRequest on the same connection is rejected with 400 and closes.
    HttpRequest second = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api");
    second.headers().set(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, "namespace-A");
    channel.writeInbound(second);

    FullHttpResponse secondResponse = channel.readOutbound();
    assertEquals(HttpResponseStatus.BAD_REQUEST, secondResponse.status());
    secondResponse.release();
    assertFalse(channel.isActive());
    assertEquals(0, drainState.getInFlight());
  }

  @Test
  public void testFrontendExceptionCaughtClosesConnection() {
    EmbeddedChannel channel = new EmbeddedChannel(new TaskWorkerManagerFrontendHandler(
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime),
        mock(DiscoveryServiceClient.class),
        new TaskWorkerManagerDrainState(), true));

    channel.pipeline().fireExceptionCaught(new RuntimeException("boom"));

    assertFalse(channel.isActive());
  }

  @Test
  public void testSyncDiscoveryRecordsNodeName() {
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);

    leaser.syncDiscovery(Arrays.asList(
        discoverableAt("10.0.0.1", 11015, "node-a"),
        discoverableAt("10.0.0.2", 11015)));

    assertEquals("node-a", leaser.getRegistry().get("10.0.0.1:11015").getNodeName());
    assertNull(leaser.getRegistry().get("10.0.0.2:11015").getNodeName());
  }

  @Test
  public void testHotNamespaceSpreadsBusyPodsAcrossNodes() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.TaskWorker.REQUEST_LIMIT, 10);
    TaskWorkerLeaser leaser = new TaskWorkerLeaser(cConf, System::nanoTime);
    // 10 / 10 / 5 workers, as on the benchmark cluster.
    Map<String, String> nodeByPod = new HashMap<>();
    for (int i = 0; i < 25; i++) {
      String node = i < 10 ? "node-a" : i < 20 ? "node-b" : "node-c";
      String address = "10.0.0." + i + ":11015";
      nodeByPod.put(address, node);
      leaser.getRegistry().put(address, new TaskWorkerState(null, 0, node, 0L));
    }

    for (int i = 0; i < 50; i++) {
      assertNotNull(leaser.acquireSlot("namespace-A"));
    }

    Map<String, Integer> busyPodsPerNode = new HashMap<>();
    for (Map.Entry<String, TaskWorkerState> entry : leaser.getRegistry().entrySet()) {
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
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);
    leaser.getRegistry().put(
        "10.0.0.1:11015", new TaskWorkerState("namespace-B", 8, "node-a", 0L));
    TaskWorkerState freshOnBusyNode = new TaskWorkerState(null, 0, "node-a", 0L);
    TaskWorkerState freshOnIdleNode = new TaskWorkerState(null, 0, "node-b", 0L);
    leaser.getRegistry().put("10.0.0.2:11015", freshOnBusyNode);
    leaser.getRegistry().put("10.0.0.3:11015", freshOnIdleNode);

    assertEquals("10.0.0.3:11015", leaser.acquireSlot("namespace-A"));
    assertNull(freshOnBusyNode.getLeasedNamespace());
  }

  @Test
  public void testWarmTieBreaksOnNodeLoad() {
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);
    leaser.getRegistry().put(
        "10.0.0.1:11015", new TaskWorkerState("namespace-B", 8, "node-a", 0L));
    TaskWorkerState warmOnBusyNode = new TaskWorkerState("namespace-A", 3, "node-a", 0L);
    TaskWorkerState warmOnIdleNode = new TaskWorkerState("namespace-A", 3, "node-b", 0L);
    leaser.getRegistry().put("10.0.0.2:11015", warmOnBusyNode);
    leaser.getRegistry().put("10.0.0.3:11015", warmOnIdleNode);

    assertEquals("10.0.0.3:11015", leaser.acquireSlot("namespace-A"));
    assertEquals(3, warmOnBusyNode.getInflightRequests());
    assertEquals(4, warmOnIdleNode.getInflightRequests());
  }

  @Test
  public void testIdleStealPrefersLeastLoadedNodeOverLru() {
    AtomicLong clock = new AtomicLong(300L);
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    leaser.getRegistry().put(
        "10.0.0.1:11015", new TaskWorkerState("namespace-B", 5, "node-a", 100L));
    // Older lastActivityTime (100ns < 200ns), so LRU alone would pick it.
    TaskWorkerState idleOnBusyNode = new TaskWorkerState("namespace-C", 0, "node-a", 100L);
    TaskWorkerState idleOnIdleNode = new TaskWorkerState("namespace-D", 0, "node-b", 200L);
    leaser.getRegistry().put("10.0.0.2:11015", idleOnBusyNode);
    leaser.getRegistry().put("10.0.0.3:11015", idleOnIdleNode);

    assertEquals("10.0.0.3:11015", leaser.acquireSlot("namespace-A"));
    assertEquals("namespace-C", idleOnBusyNode.getLeasedNamespace());
  }

  @Test
  public void testSyncDiscoveryAndAcquireSlotStampInjectedClockForLruSteal() {
    AtomicLong clock = new AtomicLong(100L);
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    leaser.syncDiscovery(Arrays.asList(
        discoverableAt("10.0.0.1", 11015, "node-a"),
        discoverableAt("10.0.0.2", 11015, "node-a")));

    TaskWorkerState pod1 = leaser.getRegistry().get("10.0.0.1:11015");
    TaskWorkerState pod2 = leaser.getRegistry().get("10.0.0.2:11015");
    assertNotNull(pod1);
    assertNotNull(pod2);
    assertEquals(100L, pod1.getLastActivityTime());
    assertEquals(100L, pod2.getLastActivityTime());

    clock.set(200L);
    String firstClaim = leaser.acquireSlot("namespace-A");
    assertNotNull(firstClaim);
    TaskWorkerState firstPod = leaser.getRegistry().get(firstClaim);
    assertEquals(200L, firstPod.getLastActivityTime());

    clock.set(250L);
    assertEquals(firstClaim, leaser.acquireSlot("namespace-A"));
    assertEquals(250L, firstPod.getLastActivityTime());
    assertEquals(2, firstPod.getInflightRequests());

    clock.set(300L);
    String secondClaim = leaser.acquireSlot("namespace-B");
    assertNotNull(secondClaim);
    assertNotEquals(firstClaim, secondClaim);
    TaskWorkerState secondPod = leaser.getRegistry().get(secondClaim);
    assertEquals(300L, secondPod.getLastActivityTime());

    clock.set(400L);
    leaser.releaseSlot(firstClaim);
    assertEquals(400L, firstPod.getLastActivityTime());
    clock.set(450L);
    leaser.releaseSlot(firstClaim);
    assertEquals(450L, firstPod.getLastActivityTime());
    assertEquals(0, firstPod.getInflightRequests());

    clock.set(500L);
    leaser.releaseSlot(secondClaim);
    assertEquals(500L, secondPod.getLastActivityTime());
    assertEquals(0, secondPod.getInflightRequests());

    // Equal node load (0 vs 0 on node-a): LRU steals firstClaim (450ns < 500ns).
    clock.set(600L);
    assertEquals(firstClaim, leaser.acquireSlot("namespace-C"));
    assertEquals(600L, firstPod.getLastActivityTime());

    // Release firstClaim at 700ns so secondClaim (500ns) becomes the older idle pod.
    clock.set(700L);
    leaser.releaseSlot(firstClaim);
    assertEquals(700L, firstPod.getLastActivityTime());

    clock.set(800L);
    assertEquals(secondClaim, leaser.acquireSlot("namespace-D"));
    assertEquals(800L, secondPod.getLastActivityTime());
  }

  @Test
  public void testLeaseManagerHonorsConfiguredRequestLimit() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.TaskWorker.REQUEST_LIMIT, 2);
    TaskWorkerLeaser leaser = new TaskWorkerLeaser(cConf, () -> 100L);
    TaskWorkerState pod1 = new TaskWorkerState(null, 0, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", pod1);

    assertEquals("10.0.0.1:11015", leaser.acquireSlot("namespace-A"));
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("namespace-A"));
    assertEquals(2, pod1.getInflightRequests());
    // Saturated at configured limit 2 (not 10).
    assertNull(leaser.acquireSlot("namespace-A"));

    TaskWorkerState pod2 = new TaskWorkerState(null, 0, null, 0L);
    leaser.getRegistry().put("10.0.0.2:11015", pod2);
    assertEquals("10.0.0.2:11015", leaser.acquireSlot("namespace-A"));
    assertEquals("10.0.0.2:11015", leaser.acquireSlot("namespace-A"));
    assertNull(leaser.acquireSlot("namespace-A"));

    leaser.releaseSlot("10.0.0.1:11015");
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("namespace-A"));
  }

  @Test
  public void testTaskWorkerManagerWireHeaderNamesMatchProtocolContract() {
    assertEquals("X-CDAP-Namespace", Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE);
    assertEquals("X-CDAP-Leased-Namespace", Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE);
    assertEquals("X-CDAP-Task-Worker-Status",
        Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS);
    assertEquals("RUNNING", TaskWorkerStatus.RUNNING.name());
    assertEquals("TERMINATING", TaskWorkerStatus.TERMINATING.name());
  }

  @Test
  public void testBackoffDoublesThenCaps() {
    assertEquals(TimeUnit.SECONDS.toNanos(10), TaskWorkerState.backoffNanos(1));
    assertEquals(TimeUnit.SECONDS.toNanos(20), TaskWorkerState.backoffNanos(2));
    assertEquals(TimeUnit.SECONDS.toNanos(30), TaskWorkerState.backoffNanos(3));
    assertEquals(TimeUnit.SECONDS.toNanos(30), TaskWorkerState.backoffNanos(1000));
  }

  @Test
  public void testUnavailablePodIsSkippedUntilBackoffExpires() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    // The restarting pod is idle, so least-loaded selection would otherwise pick it first.
    TaskWorkerState restarting = new TaskWorkerState("namespace-A", 1, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", restarting);
    leaser.getRegistry(
        ).put("10.0.0.2:11015", new TaskWorkerState("namespace-B", 10, null, 0L));

    leaser.markUnavailable("10.0.0.1:11015", "a failed connect");

    // The slot is released and the lease dropped, since the pod restarts without one.
    assertEquals(0, restarting.getInflightRequests());
    assertNull(restarting.getLeasedNamespace());
    // Direct CAS attempts during a race after snapshotting must also reject a backing-off pod.
    assertFalse(restarting.tryAcquireWarmLease("namespace-A", 10, clock.get()));
    assertFalse(restarting.tryClaimFreshLease("namespace-A", 10, clock.get()));
    assertFalse(restarting.tryStealIdleLease("namespace-A", 10, clock.get()));
    // Neither a fresh claim nor a steal may pick it while it backs off.
    assertNull(leaser.acquireSlot("namespace-A"));
    assertNull(leaser.acquireSlot("namespace-C"));

    // Once the backoff expires, the next request probes it.
    clock.set(TimeUnit.SECONDS.toNanos(10));
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("namespace-A"));
  }

  @Test
  public void testBackoffGrowsUntilConnectSucceeds() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState(null, 0, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);

    leaser.markUnavailable("worker1:8080", "a failed connect");
    // The probe at 10s fails too, so the next backoff is 20s.
    clock.set(TimeUnit.SECONDS.toNanos(10));
    leaser.markUnavailable("worker1:8080", "a failed connect");
    assertFalse(pod.isAvailable(TimeUnit.SECONDS.toNanos(29)));
    assertTrue(pod.isAvailable(TimeUnit.SECONDS.toNanos(30)));

    // The probe at 30s completes its TLS handshake, which resets the backoff.
    clock.set(TimeUnit.SECONDS.toNanos(30));
    leaser.markReachable("worker1:8080");

    leaser.markUnavailable("worker1:8080", "a failed connect");
    assertFalse(pod.isAvailable(TimeUnit.SECONDS.toNanos(39)));
    assertTrue(pod.isAvailable(TimeUnit.SECONDS.toNanos(40)));
  }

  @Test
  public void testLateResponseKeepsBackoff() {
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), () -> 0L);
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 2, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);
    leaser.markUnavailable("worker1:8080", "a TERMINATING status");

    // A task the terminating worker accepted earlier completes, without a status.
    EmbeddedChannel inboundClientChannel = new EmbeddedChannel();
    EmbeddedChannel workerChannel = new EmbeddedChannel(
        backendHandler(inboundClientChannel, leaser, "worker1:8080"));
    workerChannel.writeInbound(
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
    assertNotNull(inboundClientChannel.readOutbound());

    // The worker is still about to restart, so it stays skipped.
    assertFalse(pod.isAvailable(TimeUnit.SECONDS.toNanos(9)));
  }

  @Test
  public void testConcurrentFailuresStartOneBackoff() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 4, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);

    // Four requests were already connecting when the pod went down.
    for (int i = 0; i < 4; i++) {
      leaser.markUnavailable("worker1:8080", "a failed connect");
    }

    // Every slot is released, but the backoff is still the first one.
    assertEquals(0, pod.getInflightRequests());
    assertFalse(pod.isAvailable(TimeUnit.SECONDS.toNanos(9)));
    assertTrue(pod.isAvailable(TimeUnit.SECONDS.toNanos(10)));

    // The failed probe at 10s is the second failure, not the fifth.
    clock.set(TimeUnit.SECONDS.toNanos(10));
    leaser.markUnavailable("worker1:8080", "a failed connect");
    assertFalse(pod.isAvailable(TimeUnit.SECONDS.toNanos(29)));
    assertTrue(pod.isAvailable(TimeUnit.SECONDS.toNanos(30)));
  }

  @Test
  public void testBackingOffPodStillCountsTowardNodeLoad() {
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), () -> 0L);
    leaser.getRegistry().put(
        "10.0.0.1:11015", new TaskWorkerState("namespace-B", 4, "node-a", 0L));
    leaser.getRegistry().put("10.0.0.2:11015", new TaskWorkerState(null, 0, "node-a", 0L));
    leaser.getRegistry().put("10.0.0.3:11015", new TaskWorkerState(null, 0, "node-b", 0L));
    leaser.getRegistry().put(
        "10.0.0.4:11015", new TaskWorkerState("namespace-C", 2, "node-b", 0L));

    // Leaves three tasks still running on node-a, against two on node-b.
    leaser.markUnavailable("10.0.0.1:11015", "a TERMINATING status");

    assertEquals("10.0.0.3:11015", leaser.acquireSlot("namespace-A"));
  }

  @Test
  public void testTerminatingRejectionBacksOffPod() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 1, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);
    EmbeddedChannel inboundClientChannel = new EmbeddedChannel();
    EmbeddedChannel workerChannel = backendChannel(inboundClientChannel, leaser);

    writeResponse(workerChannel,
        workerResponse(HttpResponseStatus.TOO_MANY_REQUESTS, TaskWorkerStatus.TERMINATING.name()));

    assertFalse(pod.isAvailable(clock.get()));
    assertEquals(0, pod.getInflightRequests());
    assertNull(pod.getLeasedNamespace());
    // The 429 still reaches AppFabric, which retries.
    HttpResponse relayedClientResponse = inboundClientChannel.readOutbound();
    assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, relayedClientResponse.status());
    inboundClientChannel.finishAndReleaseAll();
  }

  @Test
  public void testTerminatingSuccessBacksOffPodAndRelaysTheResult() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    // Two tasks in flight: this one, and a sibling that is still running.
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 2, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);
    EmbeddedChannel inboundClientChannel = new EmbeddedChannel();
    EmbeddedChannel workerChannel = backendChannel(inboundClientChannel, leaser);

    // An isolated worker answers its last task, then restarts.
    HttpResponse head = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    head.headers().set(Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS,
        TaskWorkerStatus.TERMINATING.name());
    workerChannel.writeInbound(head);

    // Out of rotation at once, not after the next request is rejected.
    assertFalse(pod.isAvailable(clock.get()));
    assertNull(pod.getLeasedNamespace());
    assertEquals(1, pod.getInflightRequests());
    assertNull(leaser.acquireSlot("namespace-A"));

    // The end of the body doesn't release the slot a second time.
    ByteBuf body = Unpooled.copiedBuffer("result", StandardCharsets.UTF_8);
    workerChannel.writeInbound(new DefaultLastHttpContent(body));
    assertEquals(1, pod.getInflightRequests());

    // The caller still gets the task's result.
    HttpResponse relayedHead = inboundClientChannel.readOutbound();
    assertEquals(HttpResponseStatus.OK, relayedHead.status());
    LastHttpContent relayedBody = inboundClientChannel.readOutbound();
    assertEquals("result", relayedBody.content().toString(StandardCharsets.UTF_8));
    relayedBody.release();
  }

  @Test
  public void testTerminatingMismatchBacksOffInsteadOfAdoptingTheLease() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 1, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);
    EmbeddedChannel inboundClientChannel = new EmbeddedChannel();
    EmbeddedChannel workerChannel = backendChannel(inboundClientChannel, leaser);
    HttpResponse rejection = workerResponse(
        HttpResponseStatus.TOO_MANY_REQUESTS, TaskWorkerStatus.TERMINATING.name());
    rejection.headers().set(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE, "namespace-B");

    writeResponse(workerChannel, rejection);

    // The restart wipes the lease, so the worker's namespace isn't adopted, and the pod backs off
    // for a restart (10s) rather than cooling down for a rejection (5s).
    TaskWorkerState.State state = pod.snapshot();
    assertNull(state.leasedNamespace);
    assertEquals(0, state.inflightRequests);
    assertEquals(1, state.failures);
    assertFalse(state.hasRejectionDeadline);
    assertFalse(state.isAvailableFor("namespace-B", 0L));
    assertFalse(pod.isAvailable(TaskWorkerState.REJECTION_SKIP_NANOS));
    assertTrue(pod.isAvailable(TimeUnit.SECONDS.toNanos(10)));
    inboundClientChannel.finishAndReleaseAll();
  }

  @Test
  public void testTerminatingServerErrorBacksOffPod() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 1, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);
    EmbeddedChannel inboundClientChannel = new EmbeddedChannel();
    EmbeddedChannel workerChannel = backendChannel(inboundClientChannel, leaser);

    // A task that threw: the isolated worker restarts after answering 500.
    writeResponse(workerChannel, workerResponse(
        HttpResponseStatus.INTERNAL_SERVER_ERROR, TaskWorkerStatus.TERMINATING.name()));

    assertFalse(pod.isAvailable(clock.get()));
    assertEquals(0, pod.getInflightRequests());
    HttpResponse relayed = inboundClientChannel.readOutbound();
    assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, relayed.status());
    inboundClientChannel.finishAndReleaseAll();
  }

  @Test
  public void testRunningRejectionCoolsDownAndAdoptsTheLease() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 1, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);
    EmbeddedChannel inboundClientChannel = new EmbeddedChannel();
    EmbeddedChannel workerChannel = backendChannel(inboundClientChannel, leaser);
    HttpResponse rejection =
        workerResponse(HttpResponseStatus.TOO_MANY_REQUESTS, TaskWorkerStatus.RUNNING.name());
    rejection.headers().set(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE, "namespace-B");

    writeResponse(workerChannel, rejection);

    TaskWorkerState.State state = pod.snapshot();
    assertEquals("namespace-B", state.leasedNamespace);
    assertEquals(0, state.inflightRequests);
    assertEquals(0, state.failures);
    assertTrue(state.isAvailableFor("namespace-B", 0L));
    assertFalse(state.isAvailableFor("namespace-A", TaskWorkerState.REJECTION_SKIP_NANOS - 1));
    assertFalse(pod.isAvailable(TaskWorkerState.REJECTION_SKIP_NANOS - 1));
    assertTrue(pod.isAvailable(TaskWorkerState.REJECTION_SKIP_NANOS));
    inboundClientChannel.finishAndReleaseAll();
  }

  @Test
  public void testRunningSuccessKeepsPodInRotation() {
    AtomicLong clock = new AtomicLong(100L);
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 1, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);
    EmbeddedChannel inboundClientChannel = new EmbeddedChannel();
    EmbeddedChannel workerChannel = backendChannel(inboundClientChannel, leaser);

    writeResponse(workerChannel,
        workerResponse(HttpResponseStatus.OK, TaskWorkerStatus.RUNNING.name()));

    TaskWorkerState.State state = pod.snapshot();
    assertEquals("namespace-A", state.leasedNamespace);
    assertEquals(0, state.inflightRequests);
    assertEquals(100L, state.lastActivityTime);
    assertTrue(pod.isAvailable(clock.get()));
    inboundClientChannel.finishAndReleaseAll();
  }

  @Test
  public void testUnknownStatusIsTreatedAsRunning() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 1, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);
    EmbeddedChannel inboundClientChannel = new EmbeddedChannel();
    EmbeddedChannel workerChannel = backendChannel(inboundClientChannel, leaser);

    // Only the exact TERMINATING name takes a pod out of rotation.
    writeResponse(workerChannel, workerResponse(HttpResponseStatus.OK, "terminating"));

    assertTrue(pod.isAvailable(clock.get()));
    assertEquals("namespace-A", pod.getLeasedNamespace());
    assertEquals(0, pod.getInflightRequests());
    inboundClientChannel.finishAndReleaseAll();
  }

  private static EmbeddedChannel backendChannel(EmbeddedChannel inboundClientChannel,
      TaskWorkerLeaser leaser) {
    return new EmbeddedChannel(
        backendHandler(inboundClientChannel, leaser, "worker1:8080"));
  }

  /** Builds the handler for a namespace-A request sent to {@code workerAddress}. */
  private static TaskWorkerManagerBackendHandler backendHandler(Channel callerChannel,
      TaskWorkerLeaser leaser, String workerAddress) {
    return new TaskWorkerManagerBackendHandler(
        callerChannel, leaser, workerAddress, "namespace-A");
  }

  /** Builds the head of a worker response carrying {@code status} in the worker status header. */
  private static HttpResponse workerResponse(HttpResponseStatus code, String status) {
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, code);
    response.headers().set(Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS, status);
    return response;
  }

  /** Feeds a bodiless response the way HttpClientCodec decodes it: the head, then the end. */
  private static void writeResponse(EmbeddedChannel workerChannel, HttpResponse head) {
    workerChannel.writeInbound(head, LastHttpContent.EMPTY_LAST_CONTENT);
  }

  @Test
  public void testFailedConnectBacksOffPodAndAnswers503() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    EmbeddedChannel channel = new EmbeddedChannel(new TaskWorkerManagerFrontendHandler(leaser,
        discoveryOf(discoverableAt("127.0.0.1", 11015)), new TaskWorkerManagerDrainState(), true));
    HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api");
    req.headers().set(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, "namespace-A");
    // An EmbeddedChannel's event loop can't register the NIO worker channel, so the connect
    // fails at once and runs the failure path.
    channel.writeInbound(req);

    TaskWorkerState pod = leaser.getRegistry().get("127.0.0.1:11015");
    assertNotNull(pod);
    assertFalse(pod.isAvailable(clock.get()));
    assertEquals(0, pod.getInflightRequests());
    assertWorkerUnreachableResponse(channel.readOutbound());
    // Reads resume so the rest of the body drains, and the last chunk closes the connection.
    assertTrue(channel.config().isAutoRead());
    assertTrue(channel.isActive());
    ByteBuf body = Unpooled.wrappedBuffer(new byte[16]);
    channel.writeInbound(new DefaultLastHttpContent(body));
    assertEquals(0, body.refCnt());
    assertFalse(channel.isActive());
  }

  @Test
  public void testFailedConnectAfterFullRequestAnswers503AndCloses() {
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);
    EmbeddedChannel channel = new EmbeddedChannel(new TaskWorkerManagerFrontendHandler(leaser,
        discoveryOf(discoverableAt("127.0.0.1", 11015)), new TaskWorkerManagerDrainState(), true));
    DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
        "/api", Unpooled.wrappedBuffer(new byte[16]));
    req.headers().set(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, "namespace-A");

    channel.writeInbound(req);

    assertEquals(0, req.refCnt());
    assertWorkerUnreachableResponse(channel.readOutbound());
    assertFalse(channel.isActive());
  }

  @Test
  public void testWorkerUnreachableEventAnswers503Once() {
    EmbeddedChannel channel = new EmbeddedChannel(
        new TaskWorkerManagerFrontendHandler(
            new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime),
            mock(DiscoveryServiceClient.class), new TaskWorkerManagerDrainState(), true));

    channel.pipeline(
        ).fireUserEventTriggered(TaskWorkerManagerFrontendHandler.WorkerUnreachableEvent.INSTANCE);
    channel.pipeline(
        ).fireUserEventTriggered(TaskWorkerManagerFrontendHandler.WorkerUnreachableEvent.INSTANCE);

    assertWorkerUnreachableResponse(channel.readOutbound());
    assertNull(channel.readOutbound());
  }

  @Test
  public void testForwardedRequestAsksWorkerToClose() {
    DiscoveryServiceClient mockDiscovery = mock(DiscoveryServiceClient.class);
    Discoverable worker = discoverableAt("127.0.0.1", 11015);
    ServiceDiscovered serviceDiscovered = mock(ServiceDiscovered.class);
    when(serviceDiscovered.iterator()).thenReturn(Collections.singletonList(worker).iterator());
    when(mockDiscovery.discover(Mockito.anyString())).thenReturn(serviceDiscovered);
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);

    EmbeddedChannel channel = new EmbeddedChannel(
        new TaskWorkerManagerFrontendHandler(
            leaser, mockDiscovery, new TaskWorkerManagerDrainState(), true));
    HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api");
    req.headers().set(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, "namespace-A");
    assertTrue(HttpUtil.isKeepAlive(req));

    channel.writeInbound(req);

    // The header is marked before it is queued for the worker, whatever the connect outcome.
    assertFalse(HttpUtil.isKeepAlive(req));
  }

  @Test
  public void testBackendPropagatesChannelInactive() {
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 1, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);
    AtomicBoolean nextHandlerSawInactive = new AtomicBoolean();

    EmbeddedChannel inboundClientChannel = new EmbeddedChannel();
    EmbeddedChannel workerChannel = new EmbeddedChannel(
        backendHandler(inboundClientChannel, leaser, "worker1:8080"),
        new ChannelInboundHandlerAdapter() {
          @Override
          public void channelInactive(ChannelHandlerContext ctx) {
            nextHandlerSawInactive.set(true);
          }
        });

    workerChannel.close();

    assertTrue(nextHandlerSawInactive.get());
    assertEquals(0, pod.getInflightRequests());
  }

  @Test
  public void testFreshClaimJoinsSameNamespaceWinner() {
    // Another request for namespace-A claimed the fresh pod after this request's snapshot.
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 1, null, 0L);

    assertTrue(pod.tryClaimFreshLease("namespace-A", 10, 1L));
    assertEquals("namespace-A", pod.getLeasedNamespace());
    assertEquals(2, pod.getInflightRequests());

    // A different namespace or a full pod still fails.
    assertFalse(pod.tryClaimFreshLease("namespace-B", 10, 2L));
    TaskWorkerState fullPod = new TaskWorkerState("namespace-A", 10, null, 0L);
    assertFalse(fullPod.tryClaimFreshLease("namespace-A", 10, 2L));
  }

  @Test
  public void testIdleStealJoinsSameNamespaceWinner() {
    // Another request for namespace-A stole the idle pod after this request's snapshot.
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 1, null, 0L);

    assertTrue(pod.tryStealIdleLease("namespace-A", 10, 1L));
    assertEquals("namespace-A", pod.getLeasedNamespace());
    assertEquals(2, pod.getInflightRequests());

    // A different namespace or a full pod still fails.
    assertFalse(pod.tryStealIdleLease("namespace-B", 10, 2L));
    TaskWorkerState fullPod = new TaskWorkerState("namespace-A", 10, null, 0L);
    assertFalse(fullPod.tryStealIdleLease("namespace-A", 10, 2L));
  }

  @Test
  public void testClientLeavingMidConnectReleasesSlotWithoutBackoff() throws Exception {
    NioEventLoopGroup group = new NioEventLoopGroup(1);
    try (ServerSocket worker = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
      DiscoveryServiceClient mockDiscovery = mock(DiscoveryServiceClient.class);
      Discoverable discoverable = discoverableAt("127.0.0.1", worker.getLocalPort());
      ServiceDiscovered serviceDiscovered = mock(ServiceDiscovered.class);
      when(serviceDiscovered.iterator())
          .thenReturn(Collections.singletonList(discoverable).iterator());
      when(mockDiscovery.discover(Mockito.anyString())).thenReturn(serviceDiscovered);
      AtomicLong clock = new AtomicLong();
      TaskWorkerLeaser leaser =
          new TaskWorkerLeaser(CConfiguration.create(), clock::get);

      // A LocalChannel can run on a NIO event loop, so the frontend can open a real worker
      // connection.
      LocalAddress managerAddress = new LocalAddress("manager-mid-connect-test");
      CompletableFuture<Channel> accepted = new CompletableFuture<>();
      Channel server = new ServerBootstrap().group(group).channel(LocalServerChannel.class)
          .childHandler(new ChannelInitializer<LocalChannel>() {
            @Override
            protected void initChannel(LocalChannel ch) {
              ch.pipeline().addLast(
                  new TaskWorkerManagerFrontendHandler(
                      leaser, mockDiscovery, new TaskWorkerManagerDrainState(), true));
              accepted.complete(ch);
            }
          }).bind(managerAddress).sync().channel();
      Channel client = new Bootstrap().group(group).channel(LocalChannel.class)
          .handler(new ChannelInboundHandlerAdapter()).connect(managerAddress).sync().channel();
      Channel clientChannel = accepted.get(10, TimeUnit.SECONDS);

      HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api");
      req.headers().set(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, "namespace-A");
      // One event loop task: the connect listener can't run until the client is already gone.
      clientChannel.eventLoop().submit(() -> {
        clientChannel.pipeline().fireChannelRead(req);
        clientChannel.close();
      }).sync();

      // The worker sees its connection closed rather than left open with no request on it.
      try (Socket workerSide = worker.accept()) {
        workerSide.setSoTimeout(10_000);
        InputStream in = workerSide.getInputStream();
        byte[] buf = new byte[4096];
        while (in.read(buf) >= 0) {
          // Discard the TLS ClientHello until the manager closes the connection.
        }
      }
      TaskWorkerState pod = leaser.getRegistry().get("127.0.0.1:" + worker.getLocalPort());
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (pod.getInflightRequests() != 0 && System.nanoTime() < deadline) {
        TimeUnit.MILLISECONDS.sleep(20);
      }
      assertEquals(0, pod.getInflightRequests());
      assertTrue(pod.isAvailable(clock.get()));
      assertEquals("namespace-A", pod.getLeasedNamespace());

      client.close().sync();
      server.close().sync();
    } finally {
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
    }
  }

  @Test
  public void testConnectKeepsBackpressureWhenFlushFillsWorkerBuffer() throws Exception {
    NioEventLoopGroup group = new NioEventLoopGroup(1);
    try (ServerSocket worker = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
      DiscoveryServiceClient mockDiscovery = mock(DiscoveryServiceClient.class);
      Discoverable discoverable = discoverableAt("127.0.0.1", worker.getLocalPort());
      ServiceDiscovered serviceDiscovered = mock(ServiceDiscovered.class);
      when(serviceDiscovered.iterator())
          .thenReturn(Collections.singletonList(discoverable).iterator());
      when(mockDiscovery.discover(Mockito.anyString())).thenReturn(serviceDiscovered);
      TaskWorkerLeaser leaser =
          new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);

      LocalAddress managerAddress = new LocalAddress("manager-connect-backpressure-test");
      AtomicInteger callerReads = new AtomicInteger();
      CompletableFuture<Channel> accepted = new CompletableFuture<>();
      Channel server = new ServerBootstrap().group(group).channel(LocalServerChannel.class)
          .childHandler(new ChannelInitializer<LocalChannel>() {
            @Override
            protected void initChannel(LocalChannel ch) {
              ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                  callerReads.incrementAndGet();
                  ctx.fireChannelRead(msg);
                }
              });
              ch.pipeline().addLast(
                  new TaskWorkerManagerFrontendHandler(
                      leaser, mockDiscovery, new TaskWorkerManagerDrainState(), true));
              accepted.complete(ch);
            }
          }).bind(managerAddress).sync().channel();
      Channel client = new Bootstrap().group(group).channel(LocalChannel.class)
          .handler(new ChannelInboundHandlerAdapter()).connect(managerAddress).sync().channel();
      Channel clientChannel = accepted.get(10, TimeUnit.SECONDS);

      HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api");
      req.headers().set(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, "namespace-A");
      // One event loop task, so the whole body is queued before the worker connect completes.
      clientChannel.eventLoop().submit(() -> {
        // Sent through the transport, so the caller's pending read is used up by the header.
        client.writeAndFlush(req);
        // Twice the worker channel's default 64 KiB high write watermark.
        for (int i = 0; i < 4; i++) {
          clientChannel.pipeline().fireChannelRead(
              new DefaultHttpContent(Unpooled.wrappedBuffer(new byte[32 * 1024])));
        }
        // Stays in the caller's inbound buffer until the manager reads from the caller again.
        client.writeAndFlush(new DefaultLastHttpContent(Unpooled.wrappedBuffer(new byte[1024])));
      }).sync();

      // The worker never reads past the ClientHello, so the body stays queued on the manager.
      try (Socket workerSide = worker.accept()) {
        workerSide.setSoTimeout(10_000);
        // The ClientHello means the connect listener has already drained the queue.
        assertTrue(workerSide.getInputStream().read() >= 0);
        // Submitted after the listener and any writability event it queued, so both have run.
        boolean autoRead = clientChannel.eventLoop()
            .submit(() -> clientChannel.config().isAutoRead())
            .get(10, TimeUnit.SECONDS);
        int reads = clientChannel.eventLoop().submit(callerReads::get).get(10, TimeUnit.SECONDS);
        assertFalse(autoRead);
        assertEquals("The last chunk must wait while the worker is unwritable", 5, reads);

        client.close().sync();
        server.close().sync();
      }
    } finally {
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
    }
  }

  @Test
  public void testHandshakeFailureBacksOffPodAndReleasesOneSlot() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 2, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);
    EmbeddedChannel workerChannel = new EmbeddedChannel(
        backendHandler(new EmbeddedChannel(), leaser, "worker1:8080"));

    SSLHandshakeException cause = new SSLHandshakeException("bad certificate");
    workerChannel.pipeline().fireUserEventTriggered(new SslHandshakeCompletionEvent(cause));
    // SslHandler raises the same failure through exceptionCaught right after the event.
    workerChannel.pipeline().fireExceptionCaught(cause);

    assertFalse(pod.isAvailable(clock.get()));
    assertEquals("Only this request's slot is released", 1, pod.getInflightRequests());
  }

  @Test
  public void testHandshakeSuccessClearsBackoff() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 2, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);
    leaser.markUnavailable("worker1:8080", "a failed connect");
    assertFalse(pod.isAvailable(clock.get()));
    EmbeddedChannel workerChannel = new EmbeddedChannel(
        backendHandler(new EmbeddedChannel(), leaser, "worker1:8080"));

    workerChannel.pipeline().fireUserEventTriggered(SslHandshakeCompletionEvent.SUCCESS);

    assertTrue(pod.isAvailable(clock.get()));
    assertEquals("A handshake doesn't release the slot", 1, pod.getInflightRequests());
  }

  @Test
  public void testCallerLeavingDuringHandshakeDoesNotBackOff() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 1, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);
    EmbeddedChannel inboundClientChannel = new EmbeddedChannel();
    EmbeddedChannel workerChannel = new EmbeddedChannel(
        backendHandler(inboundClientChannel, leaser, "worker1:8080"));

    // The caller left, so the frontend closes the worker mid-handshake and the handshake fails.
    inboundClientChannel.close();
    workerChannel.pipeline().fireUserEventTriggered(
        new SslHandshakeCompletionEvent(new SSLException("SSLEngine closed already")));
    workerChannel.close();

    assertTrue(pod.isAvailable(clock.get()));
    assertEquals(0, pod.getInflightRequests());
    assertEquals("namespace-A", pod.getLeasedNamespace());
  }

  @Test
  public void testHandshakeFailureHandsTheReplyToTheFrontend() {
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), System::nanoTime);
    TaskWorkerState pod = new TaskWorkerState("namespace-A", 1, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);
    AtomicInteger unreachableEvents = new AtomicInteger();
    EmbeddedChannel inboundClientChannel = new EmbeddedChannel(new ChannelInboundHandlerAdapter() {
      @Override
      public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt == TaskWorkerManagerFrontendHandler.WorkerUnreachableEvent.INSTANCE) {
          unreachableEvents.incrementAndGet();
        }
      }
    });
    EmbeddedChannel workerChannel = new EmbeddedChannel(
        backendHandler(inboundClientChannel, leaser, "worker1:8080"));

    workerChannel.pipeline().fireUserEventTriggered(
        new SslHandshakeCompletionEvent(new SSLHandshakeException("bad certificate")));
    workerChannel.close();

    assertEquals(1, unreachableEvents.get());
    assertTrue("The frontend closes the caller after its 503", inboundClientChannel.isActive());
    assertEquals(0, pod.getInflightRequests());
  }

  @Test
  public void testWorkerFailingTlsIsBackedOffAndAnswered503() throws Exception {
    NioEventLoopGroup group = new NioEventLoopGroup(1);
    try (ServerSocket worker = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
      DiscoveryServiceClient mockDiscovery =
          discoveryOf(discoverableAt("127.0.0.1", worker.getLocalPort()));
      AtomicLong clock = new AtomicLong();
      TaskWorkerLeaser leaser =
          new TaskWorkerLeaser(CConfiguration.create(), clock::get);

      LocalAddress managerAddress = new LocalAddress("manager-tls-failure-test");
      CompletableFuture<Channel> accepted = new CompletableFuture<>();
      Channel server = new ServerBootstrap().group(group).channel(LocalServerChannel.class)
          .childHandler(new ChannelInitializer<LocalChannel>() {
            @Override
            protected void initChannel(LocalChannel ch) {
              ch.pipeline().addLast(
                  new TaskWorkerManagerFrontendHandler(
                      leaser, mockDiscovery, new TaskWorkerManagerDrainState(), true));
              accepted.complete(ch);
            }
          }).bind(managerAddress).sync().channel();
      CompletableFuture<Object> callerResponse = new CompletableFuture<>();
      Channel client = new Bootstrap().group(group).channel(LocalChannel.class)
          .handler(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
              // The server pipeline has no codec, so the response arrives as an object.
              if (!callerResponse.complete(msg)) {
                ReferenceCountUtil.release(msg);
              }
            }
          }).connect(managerAddress).sync().channel();
      Channel clientChannel = accepted.get(10, TimeUnit.SECONDS);

      HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api");
      req.headers().set(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, "namespace-A");
      clientChannel.eventLoop().submit(() -> {
        clientChannel.pipeline().fireChannelRead(req);
        clientChannel.pipeline().fireChannelRead(new DefaultLastHttpContent());
      }).sync();

      // The worker accepts TCP but answers the ClientHello in plaintext, so TLS fails.
      try (Socket workerSide = worker.accept()) {
        workerSide.setSoTimeout(10_000);
        InputStream in = workerSide.getInputStream();
        byte[] buf = new byte[4096];
        assertTrue(in.read(buf) > 0);
        workerSide.getOutputStream().write(
            "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n"
                .getBytes(StandardCharsets.US_ASCII));
        while (in.read(buf) >= 0) {
          // Wait for the manager to close the connection.
        }
      }

      assertWorkerUnreachableResponse(callerResponse.get(10, TimeUnit.SECONDS));
      assertTrue("The caller's connection is closed",
          client.closeFuture().await(10, TimeUnit.SECONDS));
      TaskWorkerState pod = leaser.getRegistry().get("127.0.0.1:" + worker.getLocalPort());
      assertFalse(pod.isAvailable(clock.get()));
      assertEquals(0, pod.getInflightRequests());

      server.close().sync();
    } finally {
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
    }
  }

  @Test
  public void testPlainHttpWorkerModeForwardsRequestAndClearsBackoff() throws Exception {
    NioEventLoopGroup group = new NioEventLoopGroup(1);
    try (ServerSocket worker = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
      String workerAddress = "127.0.0.1:" + worker.getLocalPort();
      DiscoveryServiceClient mockDiscovery =
          discoveryOf(discoverableAt("127.0.0.1", worker.getLocalPort()));
      AtomicLong clock = new AtomicLong();
      TaskWorkerLeaser leaser =
          new TaskWorkerLeaser(CConfiguration.create(), clock::get);
      TaskWorkerState pod = new TaskWorkerState(null, 0, null, 0L);
      leaser.getRegistry().put(workerAddress, pod);
      // Put the pod in backoff and advance past the window so this request probes it.
      leaser.markUnavailable(workerAddress, "a failed connect");
      clock.set(TimeUnit.SECONDS.toNanos(10));

      LocalAddress managerAddress = new LocalAddress("manager-plain-http-test");
      CompletableFuture<Channel> accepted = new CompletableFuture<>();
      Channel server = new ServerBootstrap().group(group).channel(LocalServerChannel.class)
          .childHandler(new ChannelInitializer<LocalChannel>() {
            @Override
            protected void initChannel(LocalChannel ch) {
              ch.pipeline().addLast(new TaskWorkerManagerFrontendHandler(
                  leaser, mockDiscovery, new TaskWorkerManagerDrainState(), false));
              accepted.complete(ch);
            }
          }).bind(managerAddress).sync().channel();
      CompletableFuture<HttpResponse> callerHeader = new CompletableFuture<>();
      CompletableFuture<LastHttpContent> callerEnd = new CompletableFuture<>();
      Channel client = new Bootstrap().group(group).channel(LocalChannel.class)
          .handler(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
              if (msg instanceof HttpResponse) {
                callerHeader.complete((HttpResponse) msg);
              } else if (msg instanceof LastHttpContent) {
                callerEnd.complete((LastHttpContent) msg);
              } else {
                ReferenceCountUtil.release(msg);
              }
            }
          }).connect(managerAddress).sync().channel();
      Channel clientChannel = accepted.get(10, TimeUnit.SECONDS);

      HttpRequest req =
          new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/worker/run");
      req.headers().set(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, "namespace-A");
      clientChannel.eventLoop().submit(() -> {
        clientChannel.pipeline().fireChannelRead(req);
        clientChannel.pipeline().fireChannelRead(new DefaultLastHttpContent());
      }).sync();

      try (Socket workerSide = worker.accept()) {
        workerSide.setSoTimeout(10_000);
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(workerSide.getInputStream(), StandardCharsets.US_ASCII));
        assertEquals("POST /worker/run HTTP/1.1", reader.readLine());
        workerSide.getOutputStream().write(
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
                .getBytes(StandardCharsets.US_ASCII));
        workerSide.getOutputStream().flush();
      }

      assertEquals(HttpResponseStatus.OK, callerHeader.get(10, TimeUnit.SECONDS).status());
      LastHttpContent end = callerEnd.get(10, TimeUnit.SECONDS);
      end.release();
      // Backoff was cleared on TCP connect and slot released on LastHttpContent.
      clock.set(TimeUnit.SECONDS.toNanos(11));
      assertTrue(pod.isAvailable(clock.get()));
      assertEquals(0, pod.getInflightRequests());

      client.close().sync();
      server.close().sync();
    } finally {
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
    }
  }

  @Test
  public void testSameNamespaceRejectionSkipsFullPod() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState podA = new TaskWorkerState("ns-A", 1, null, 0L);
    TaskWorkerState podB = new TaskWorkerState(null, 0, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", podA);
    leaser.getRegistry().put("10.0.0.2:11015", podB);

    relayRejection(leaser, "10.0.0.1:11015", "ns-A", "ns-A");

    assertEquals("ns-A", podA.getLeasedNamespace());
    assertEquals(0, podA.getInflightRequests());
    assertEquals("10.0.0.2:11015", leaser.acquireSlot("ns-A"));
  }

  @Test
  public void testRejectionCooldownExpiresAtExactBoundary() {
    AtomicLong clock = new AtomicLong(1_000L);
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState podA = new TaskWorkerState("ns-A", 1, null, 500L);
    leaser.getRegistry().put("10.0.0.1:11015", podA);

    relayRejection(leaser, "10.0.0.1:11015", "ns-A", "ns-A");

    clock.set(1_000L + TaskWorkerState.REJECTION_SKIP_NANOS - 1L);
    assertFalse(podA.isAvailable(clock.get()));
    assertNull(leaser.acquireSlot("ns-A"));

    clock.set(1_000L + TaskWorkerState.REJECTION_SKIP_NANOS);
    assertTrue(podA.isAvailable(clock.get()));
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("ns-A"));
  }

  @Test
  public void testMismatchRejectionAdoptsNamespaceAndCoolsDownOnlyOthers() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState podA = new TaskWorkerState("ns-A", 1, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", podA);

    relayRejection(leaser, "10.0.0.1:11015", "ns-A", "ns-B");

    assertEquals("ns-B", podA.getLeasedNamespace());
    assertEquals(0, podA.getInflightRequests());
    // The worker turned ns-A away for ns-B, so only ns-B is let in before the 5s pass.
    assertNull(leaser.acquireSlot("ns-A"));
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("ns-B"));
    leaser.releaseSlot("10.0.0.1:11015");
    clock.set(TaskWorkerState.REJECTION_SKIP_NANOS - 1L);
    assertNull(leaser.acquireSlot("ns-A"));

    clock.set(TaskWorkerState.REJECTION_SKIP_NANOS);
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("ns-A"));
  }

  @Test
  public void testHeaderlessRejectionClearsUnconfirmedClaim() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState unconfirmed = new TaskWorkerState("ns-B", 1, null, 0L);
    TaskWorkerState sharedInflight = new TaskWorkerState("ns-B", 2, null, 0L);
    TaskWorkerState backingOff = new TaskWorkerState("ns-B", 2, null, 0L);
    TaskWorkerState emptyHeaderShared = new TaskWorkerState("ns-B", 2, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", unconfirmed);
    leaser.getRegistry().put("10.0.0.2:11015", sharedInflight);
    leaser.getRegistry().put("10.0.0.3:11015", backingOff);
    leaser.getRegistry().put("10.0.0.4:11015", emptyHeaderShared);

    // Headerless 429 when inflight drops to 0 clears the speculative claim.
    relayRejection(leaser, "10.0.0.1:11015", "ns-B", null);
    assertNull(unconfirmed.getLeasedNamespace());
    assertEquals(0, unconfirmed.getInflightRequests());
    assertFalse(unconfirmed.isAvailable(clock.get()));

    // Headerless 429 when sibling requests remain in flight preserves current.leasedNamespace.
    relayRejection(leaser, "10.0.0.2:11015", "ns-B", null);
    assertEquals("ns-B", sharedInflight.getLeasedNamespace());
    assertEquals(1, sharedInflight.getInflightRequests());
    assertFalse(sharedInflight.isAvailable(clock.get()));

    // Empty X-CDAP-Leased-Namespace behaves identically to an omitted header across 2 -> 1 -> 0.
    relayRejection(leaser, "10.0.0.4:11015", "ns-B", "");
    assertEquals("ns-B", emptyHeaderShared.getLeasedNamespace());
    assertEquals(1, emptyHeaderShared.getInflightRequests());
    relayRejection(leaser, "10.0.0.4:11015", "ns-B", "");
    assertNull(emptyHeaderShared.getLeasedNamespace());
    assertEquals(0, emptyHeaderShared.getInflightRequests());

    // A rejection arriving while the pod is already backing off keeps leasedNamespace null and
    // preserves the longer 10s unreachable backoff past the 5s rejection cooldown.
    leaser.markUnavailable("10.0.0.3:11015", "a failed connect");
    relayRejection(leaser, "10.0.0.3:11015", "ns-A", "ns-B");
    assertNull(backingOff.getLeasedNamespace());
    assertEquals(0, backingOff.getInflightRequests());
    assertNull(leaser.acquireSlot("ns-B"));
    assertFalse(backingOff.isAvailable(TaskWorkerState.REJECTION_SKIP_NANOS));
    assertFalse(backingOff.isAvailable(TimeUnit.SECONDS.toNanos(10) - 1L));
    assertTrue(backingOff.isAvailable(TimeUnit.SECONDS.toNanos(10)));
  }

  @Test
  public void testConcurrentBackoffDecrementDropsAdoptedNamespace() {
    TaskWorkerState pod = new TaskWorkerState("ns-A", 3, null, 0L);

    long expiry = pod.markUnavailable(0L);
    assertEquals(TimeUnit.SECONDS.toNanos(10), expiry);
    assertNull(pod.getLeasedNamespace());
    assertEquals(2, pod.getInflightRequests());

    // At expiry, a probe rejection adopts "ns-B" and starts a 5s rejection cooldown while
    // unavailableUntil is still stamped at expiry with failures == 1.
    pod.markRejected("ns-B", "ns-A", expiry);
    assertEquals("ns-B", pod.getLeasedNamespace());
    assertEquals(1, pod.getInflightRequests());

    // A concurrent failure timestamped 1ns before expiry enters the active-backoff CAS branch and
    // must drop the adopted "ns-B" lease while preserving the 5s rejection cooldown.
    assertEquals(0L, pod.markUnavailable(expiry - 1L));
    assertNull(pod.getLeasedNamespace());
    assertEquals(0, pod.getInflightRequests());
    assertFalse(pod.isAvailable(expiry));
    assertFalse(pod.isAvailable(expiry + TaskWorkerState.REJECTION_SKIP_NANOS - 1L));
    assertTrue(pod.isAvailable(expiry + TaskWorkerState.REJECTION_SKIP_NANOS));
  }

  @Test
  public void testRejectionDuringCooldownRestartsWindow() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState podA = new TaskWorkerState("ns-A", 2, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", podA);

    relayRejection(leaser, "10.0.0.1:11015", "ns-A", "ns-A");
    clock.set(TimeUnit.SECONDS.toNanos(3));
    relayRejection(leaser, "10.0.0.1:11015", "ns-A", "ns-A");

    long restartedExpiry = TimeUnit.SECONDS.toNanos(3) + TaskWorkerState.REJECTION_SKIP_NANOS;
    clock.set(restartedExpiry - 1L);
    assertFalse(podA.isAvailable(clock.get()));
    assertNull(leaser.acquireSlot("ns-A"));

    clock.set(restartedExpiry);
    assertTrue(podA.isAvailable(clock.get()));
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("ns-A"));
  }

  @Test
  public void testReachableDoesNotClearCooldown() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState("ns-A", 1, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);

    // Start backoff at 0s (failures = 1, unavailableUntil = 10s).
    leaser.markUnavailable("worker1:8080", "a failed connect");

    // At 10s backoff expires while failures == 1; a probe gets rejected (cooldown until 15s).
    clock.set(TimeUnit.SECONDS.toNanos(10));
    relayRejection(leaser, "worker1:8080", "ns-A", "ns-A");

    // At 11s a concurrent connection finishes its TLS handshake (executes State.withoutBackoff).
    clock.set(TimeUnit.SECONDS.toNanos(11));
    EmbeddedChannel secondWorkerChannel = new EmbeddedChannel(
        backendHandler(new EmbeddedChannel(), leaser, "worker1:8080"));
    secondWorkerChannel.pipeline().fireUserEventTriggered(SslHandshakeCompletionEvent.SUCCESS);

    clock.set(TimeUnit.SECONDS.toNanos(14));
    assertFalse(pod.isAvailable(clock.get()));
    assertNull(leaser.acquireSlot("ns-A"));

    clock.set(TimeUnit.SECONDS.toNanos(15));
    assertTrue(pod.isAvailable(clock.get()));
    assertEquals("worker1:8080", leaser.acquireSlot("ns-A"));

    // Repeat with failures == 0: 429 on channel 1 at 15s, TLS handshake on channel 2 at 16s.
    relayRejection(leaser, "worker1:8080", "ns-A", "ns-A");
    clock.set(TimeUnit.SECONDS.toNanos(16));
    EmbeddedChannel thirdWorkerChannel = new EmbeddedChannel(
        backendHandler(new EmbeddedChannel(), leaser, "worker1:8080"));
    thirdWorkerChannel.pipeline().fireUserEventTriggered(SslHandshakeCompletionEvent.SUCCESS);
    assertFalse(pod.isAvailable(TimeUnit.SECONDS.toNanos(19)));
    assertTrue(pod.isAvailable(TimeUnit.SECONDS.toNanos(20)));
  }

  @Test
  public void testInFlightCompletionDoesNotClearCooldown() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState("ns-A", 2, null, 0L);
    leaser.getRegistry().put("worker1:8080", pod);

    // Request 1 is rejected with 429 at 0s; request 2 is still running on the worker.
    relayRejection(leaser, "worker1:8080", "ns-A", "ns-A");
    assertEquals(1, pod.getInflightRequests());

    // At 1s request 2 finishes with 200 OK + LastHttpContent (recordActivity + releaseSlot).
    clock.set(TimeUnit.SECONDS.toNanos(1));
    EmbeddedChannel callerChannel = new EmbeddedChannel();
    EmbeddedChannel workerChannel = new EmbeddedChannel(
        backendHandler(callerChannel, leaser, "worker1:8080"));
    workerChannel.writeInbound(
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
    workerChannel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);
    assertNotNull(callerChannel.readOutbound());
    assertNotNull(callerChannel.readOutbound());
    assertEquals(0, pod.getInflightRequests());

    clock.set(TaskWorkerState.REJECTION_SKIP_NANOS - 1L);
    assertFalse(pod.isAvailable(clock.get()));
    assertNull(leaser.acquireSlot("ns-A"));

    clock.set(TaskWorkerState.REJECTION_SKIP_NANOS);
    assertTrue(pod.isAvailable(clock.get()));
    assertEquals("worker1:8080", leaser.acquireSlot("ns-A"));
  }

  @Test
  public void testRejectionCooldownHandlesNegativeAndWrappingNanoTime() {
    // Fresh pod at negative System.nanoTime() must be available (hasRejectionDeadline == false).
    long negStart = -TimeUnit.SECONDS.toNanos(100);
    TaskWorkerState pod = new TaskWorkerState("ns-A", 1, null, negStart);
    assertTrue(pod.isAvailable(negStart));
    assertTrue(pod.isAvailable(0L));

    // Rejection at -5s sets rejectedUntil == 0L exactly; unavailable at -1ns, available at 0ns.
    pod.markRejected("ns-A", "ns-A", -TaskWorkerState.REJECTION_SKIP_NANOS);
    assertFalse(pod.isAvailable(-1L));
    assertTrue(pod.isAvailable(0L));

    // Rejection at Long.MAX_VALUE - 2s wraps rejectedUntil into Long.MIN_VALUE + 3s - 1ns.
    long nearMax = Long.MAX_VALUE - TimeUnit.SECONDS.toNanos(2);
    pod.markRejected("ns-A", "ns-A", nearMax);
    assertFalse(pod.isAvailable(Long.MAX_VALUE));
    assertFalse(pod.isAvailable(nearMax + TaskWorkerState.REJECTION_SKIP_NANOS - 1L));
    assertTrue(pod.isAvailable(nearMax + TaskWorkerState.REJECTION_SKIP_NANOS));
  }

  @Test
  public void testCoolingDownPodStillCountsTowardNodeLoad() {
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), () -> 0L);
    leaser.getRegistry().put(
        "10.0.0.1:11015", new TaskWorkerState("ns-B", 4, "node-a", 0L));
    leaser.getRegistry().put("10.0.0.2:11015", new TaskWorkerState(null, 0, "node-a", 0L));
    leaser.getRegistry().put("10.0.0.3:11015", new TaskWorkerState(null, 0, "node-b", 0L));
    leaser.getRegistry().put(
        "10.0.0.4:11015", new TaskWorkerState("ns-C", 2, "node-b", 0L));

    // Leaves three active tasks on node-a against two on node-b while 10.0.0.1 cools down.
    leaser.markRejected("10.0.0.1:11015", "ns-B", "ns-B");

    assertEquals("10.0.0.3:11015", leaser.acquireSlot("ns-A"));
  }

  @Test
  public void testEvictedPodRejectionAndFrontendAllCoolingDown429() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);

    // Part 1: 429 and 200 responses from a pod already evicted from podRegistry cause no NPE.
    relayRejection(leaser, "evicted:8080", "ns-A", "ns-A");
    EmbeddedChannel evictedCaller = new EmbeddedChannel();
    EmbeddedChannel evictedWorker = new EmbeddedChannel(
        backendHandler(evictedCaller, leaser, "evicted:8080"));
    evictedWorker.writeInbound(
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
    evictedWorker.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);
    evictedWorker.close();
    HttpResponse evictedOk = evictedCaller.readOutbound();
    assertNotNull(evictedOk);
    assertEquals(HttpResponseStatus.OK, evictedOk.status());
    LastHttpContent evictedEnd = evictedCaller.readOutbound();
    assertNotNull(evictedEnd);
    evictedEnd.release();

    // Part 2: When every discovered pod is cooling down, frontend answers 429 and releases drain.
    TaskWorkerState coolingPod = new TaskWorkerState("ns-A", 1, null, 0L);
    leaser.getRegistry().put("127.0.0.1:8080", coolingPod);
    leaser.markRejected("127.0.0.1:8080", "ns-A", "ns-A");

    TaskWorkerManagerDrainState drainState = new TaskWorkerManagerDrainState();
    EmbeddedChannel channel = new EmbeddedChannel(new TaskWorkerManagerFrontendHandler(
        leaser, discoveryOf(discoverableAt("127.0.0.1", 8080)), drainState, true));
    HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api");
    req.headers().set(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, "ns-A");
    channel.writeInbound(req);

    FullHttpResponse response = channel.readOutbound();
    assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, response.status());
    response.release();
    assertEquals(0, drainState.getInFlight());

    channel.writeInbound(new DefaultLastHttpContent());
    assertFalse(channel.isActive());
  }

  @Test
  public void testMismatchLetsNamedHolderWarmMatchAtOnce() {
    AtomicLong clock = new AtomicLong(1_000L);
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    // The manager speculatively leased the pod to ns-B, but the worker is running for ns-A.
    TaskWorkerState pod = new TaskWorkerState("ns-B", 1, null, 500L);
    TaskWorkerState spare = new TaskWorkerState(null, 0, null, 500L);
    leaser.getRegistry().put("10.0.0.1:11015", pod);
    leaser.getRegistry().put("10.0.0.2:11015", spare);

    relayRejection(leaser, "10.0.0.1:11015", "ns-B", "ns-A");

    // With no time passed, the holder goes back to its own pod instead of claiming the spare one.
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("ns-A"));
    assertEquals("ns-A", pod.getLeasedNamespace());
    assertEquals(1, pod.getInflightRequests());
    assertNull(spare.getLeasedNamespace());
    assertEquals(0, spare.getInflightRequests());
  }

  @Test
  public void testMismatchKeepsOtherNamespacesOutUntilCooldownExpires() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState("ns-B", 1, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", pod);

    relayRejection(leaser, "10.0.0.1:11015", "ns-B", "ns-A");

    // The pod is idle, so only the cooldown keeps the rejected namespace, and any other, off it.
    clock.set(TaskWorkerState.REJECTION_SKIP_NANOS - 1L);
    assertNull(leaser.acquireSlot("ns-B"));
    assertNull(leaser.acquireSlot("ns-C"));
    assertFalse(pod.tryClaimFreshLease("ns-B", 10, clock.get()));
    assertFalse(pod.tryStealIdleLease("ns-B", 10, clock.get()));

    // The holder using its pod in the meantime does not open it to the others.
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("ns-A"));
    leaser.releaseSlot("10.0.0.1:11015");
    assertEquals(0, pod.getInflightRequests());
    assertNull(leaser.acquireSlot("ns-B"));

    // After 5s the idle pod is a steal candidate again.
    clock.set(TaskWorkerState.REJECTION_SKIP_NANOS);
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("ns-B"));
    assertEquals("ns-B", pod.getLeasedNamespace());
  }

  @Test
  public void testRejectionNamingRequesterCoolsDownHolderAndOthers() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState("ns-A", 1, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", pod);

    // The worker turns away its own holder, so it is full rather than leased to someone else.
    relayRejection(leaser, "10.0.0.1:11015", "ns-A", "ns-A");

    assertEquals("ns-A", pod.getLeasedNamespace());
    assertNull(leaser.acquireSlot("ns-A"));
    assertNull(leaser.acquireSlot("ns-B"));
    clock.set(TaskWorkerState.REJECTION_SKIP_NANOS - 1L);
    assertNull(leaser.acquireSlot("ns-A"));
    assertNull(leaser.acquireSlot("ns-B"));

    clock.set(TaskWorkerState.REJECTION_SKIP_NANOS);
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("ns-A"));
  }

  @Test
  public void testRejectionWithoutHolderCoolsDownEveryNamespace() {
    for (String header : Arrays.asList(null, "")) {
      AtomicLong clock = new AtomicLong();
      TaskWorkerLeaser leaser =
          new TaskWorkerLeaser(CConfiguration.create(), clock::get);
      TaskWorkerState pod = new TaskWorkerState("ns-B", 1, null, 0L);
      leaser.getRegistry().put("10.0.0.1:11015", pod);

      relayRejection(leaser, "10.0.0.1:11015", "ns-B", header);

      assertNull(pod.getLeasedNamespace());
      clock.set(TaskWorkerState.REJECTION_SKIP_NANOS - 1L);
      assertNull(leaser.acquireSlot("ns-A"));
      assertNull(leaser.acquireSlot("ns-B"));

      clock.set(TaskWorkerState.REJECTION_SKIP_NANOS);
      assertEquals("10.0.0.1:11015", leaser.acquireSlot("ns-B"));
    }

    // A speculative claim kept because a sibling request is still in flight is not a holder the
    // worker confirmed, so it waits out the cooldown too.
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState shared = new TaskWorkerState("ns-B", 2, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", shared);

    relayRejection(leaser, "10.0.0.1:11015", "ns-A", null);

    assertEquals("ns-B", shared.getLeasedNamespace());
    clock.set(TaskWorkerState.REJECTION_SKIP_NANOS - 1L);
    assertNull(leaser.acquireSlot("ns-B"));
    clock.set(TaskWorkerState.REJECTION_SKIP_NANOS);
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("ns-B"));
  }

  @Test
  public void testFullWorkerRejectingHolderAfterMismatchRestoresFullCooldown() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    TaskWorkerState pod = new TaskWorkerState("ns-B", 1, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", pod);

    // The worker is full of ns-A's tasks, which the manager can't tell from a lease mismatch.
    relayRejection(leaser, "10.0.0.1:11015", "ns-B", "ns-A");

    // So ns-A gets one more attempt 1s later, which the worker rejects naming ns-A itself.
    clock.set(TimeUnit.SECONDS.toNanos(1));
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("ns-A"));
    relayRejection(leaser, "10.0.0.1:11015", "ns-A", "ns-A");
    assertEquals(0, pod.getInflightRequests());

    // That restarts the window from 1s and no longer lets the holder through.
    assertNull(leaser.acquireSlot("ns-A"));
    long restartedExpiry = TimeUnit.SECONDS.toNanos(1) + TaskWorkerState.REJECTION_SKIP_NANOS;
    clock.set(restartedExpiry - 1L);
    assertNull(leaser.acquireSlot("ns-A"));
    assertNull(leaser.acquireSlot("ns-B"));

    clock.set(restartedExpiry);
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("ns-A"));
  }

  @Test
  public void testLaterMismatchDoesNotReopenFullWorkerToHolder() {
    AtomicLong clock = new AtomicLong();
    TaskWorkerLeaser leaser =
        new TaskWorkerLeaser(CConfiguration.create(), clock::get);
    // Two ns-B requests are in flight to a worker that is full of ns-A's tasks.
    TaskWorkerState pod = new TaskWorkerState("ns-B", 2, null, 0L);
    leaser.getRegistry().put("10.0.0.1:11015", pod);

    relayRejection(leaser, "10.0.0.1:11015", "ns-B", "ns-A");
    clock.set(TimeUnit.SECONDS.toNanos(1));
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("ns-A"));
    relayRejection(leaser, "10.0.0.1:11015", "ns-A", "ns-A");
    assertNull(leaser.acquireSlot("ns-A"));

    // The second ns-B rejection arrives at 2s: it extends the cooldown but must not relax it.
    clock.set(TimeUnit.SECONDS.toNanos(2));
    relayRejection(leaser, "10.0.0.1:11015", "ns-B", "ns-A");
    assertEquals("ns-A", pod.getLeasedNamespace());
    assertEquals(0, pod.getInflightRequests());
    assertNull(leaser.acquireSlot("ns-A"));
    long extendedExpiry = TimeUnit.SECONDS.toNanos(2) + TaskWorkerState.REJECTION_SKIP_NANOS;
    clock.set(extendedExpiry - 1L);
    assertNull(leaser.acquireSlot("ns-A"));
    assertNull(leaser.acquireSlot("ns-B"));

    // Once the full cooldown is over, a mismatch lets the holder through again.
    clock.set(extendedExpiry);
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("ns-B"));
    relayRejection(leaser, "10.0.0.1:11015", "ns-B", "ns-A");
    assertEquals("10.0.0.1:11015", leaser.acquireSlot("ns-A"));
  }

  @Test
  public void testRejectionRelayedByManagerOpensPodOnlyToAnotherHolder() throws Exception {
    NioEventLoopGroup group = new NioEventLoopGroup(1);
    try (ServerSocket worker = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
      // Fail rather than hang if the manager doesn't route a request to the worker.
      worker.setSoTimeout(10_000);
      String workerAddress = "127.0.0.1:" + worker.getLocalPort();
      DiscoveryServiceClient mockDiscovery =
          discoveryOf(discoverableAt("127.0.0.1", worker.getLocalPort()));
      AtomicLong clock = new AtomicLong();
      TaskWorkerLeaser leaser =
          new TaskWorkerLeaser(CConfiguration.create(), clock::get);
      LocalAddress managerAddress = new LocalAddress("manager-rejection-cooldown-test");
      Channel server = new ServerBootstrap().group(group).channel(LocalServerChannel.class)
          .childHandler(new ChannelInitializer<LocalChannel>() {
            @Override
            protected void initChannel(LocalChannel ch) {
              ch.pipeline().addLast(new TaskWorkerManagerFrontendHandler(
                  leaser, mockDiscovery, new TaskWorkerManagerDrainState(), false));
            }
          }).bind(managerAddress).sync().channel();

      // The worker is full of ns-A's tasks that this manager doesn't know of, so ns-B claims it.
      assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS,
          sendRequestRejectedByWorker(group, managerAddress, worker, "ns-B", "ns-A"));
      TaskWorkerState pod = leaser.getRegistry().get(workerAddress);
      assertEquals("ns-A", pod.getLeasedNamespace());
      assertEquals(0, pod.getInflightRequests());
      assertNull(leaser.acquireSlot("ns-B"));

      // ns-A is routed back to its pod at once, and the worker rejects it naming ns-A itself.
      assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS,
          sendRequestRejectedByWorker(group, managerAddress, worker, "ns-A", "ns-A"));
      assertEquals("ns-A", pod.getLeasedNamespace());
      assertEquals(0, pod.getInflightRequests());

      // Now the pod is known to be full, so ns-A waits out the cooldown like everyone else.
      clock.set(TaskWorkerState.REJECTION_SKIP_NANOS - 1L);
      assertNull(leaser.acquireSlot("ns-A"));
      assertNull(leaser.acquireSlot("ns-B"));
      clock.set(TaskWorkerState.REJECTION_SKIP_NANOS);
      assertEquals(workerAddress, leaser.acquireSlot("ns-A"));

      server.close().sync();
    } finally {
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
    }
  }

  /**
   * Sends a request for {@code namespace} through the manager and has the worker answer 429 with
   * {@code leasedNamespace} as its lease holder.
   *
   * @return the status the caller gets back
   */
  private static HttpResponseStatus sendRequestRejectedByWorker(NioEventLoopGroup group,
      LocalAddress managerAddress, ServerSocket worker, String namespace, String leasedNamespace)
      throws Exception {
    CompletableFuture<HttpResponseStatus> callerStatus = new CompletableFuture<>();
    Channel client = new Bootstrap().group(group).channel(LocalChannel.class)
        .handler(new ChannelInboundHandlerAdapter() {
          @Override
          public void channelRead(ChannelHandlerContext ctx, Object msg) {
            // The server pipeline has no codec, so the response arrives as objects.
            if (msg instanceof HttpResponse) {
              callerStatus.complete(((HttpResponse) msg).status());
            }
            ReferenceCountUtil.release(msg);
          }
        }).connect(managerAddress).sync().channel();
    HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/worker/run");
    req.headers().set(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, namespace);
    client.write(req);
    client.writeAndFlush(new DefaultLastHttpContent()).sync();

    try (Socket workerSide = worker.accept()) {
      workerSide.setSoTimeout(10_000);
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(workerSide.getInputStream(), StandardCharsets.US_ASCII));
      assertEquals("POST /worker/run HTTP/1.1", reader.readLine());
      String rejection = "HTTP/1.1 429 Too Many Requests\r\n"
          + Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE + ": " + leasedNamespace + "\r\n"
          + "Content-Length: 0\r\nConnection: close\r\n\r\n";
      workerSide.getOutputStream().write(rejection.getBytes(StandardCharsets.US_ASCII));
      workerSide.getOutputStream().flush();
    }

    HttpResponseStatus status = callerStatus.get(10, TimeUnit.SECONDS);
    assertTrue("The caller's connection is closed",
        client.closeFuture().await(10, TimeUnit.SECONDS));
    return status;
  }

  @Test
  public void testAtomicSnapshotPreventsTornCandidateUnderConcurrentContention() throws Exception {
    TaskWorkerState pod = new TaskWorkerState(null, 0, null, 10L);
    TaskWorkerState.State initial = pod.snapshot();
    assertTrue(pod.tryClaimFreshLease("ns-A", 10, 50L));
    assertNull(initial.leasedNamespace);
    assertEquals(0, initial.inflightRequests);
    assertEquals(10L, initial.lastActivityTime);
    TaskWorkerState.State claimed = pod.snapshot();
    assertEquals("ns-A", claimed.leasedNamespace);
    assertEquals(1, claimed.inflightRequests);
    assertEquals(50L, claimed.lastActivityTime);

    int threads = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      for (String initialOwner : Arrays.asList(null, "ns-B")) {
        for (int round = 0; round < 100; round++) {
          TaskWorkerLeaser leaser =
              new TaskWorkerLeaser(CConfiguration.create(), () -> 100L);
          TaskWorkerState contendedPod = new TaskWorkerState(initialOwner, 0, null, 0L);
          leaser.getRegistry().put("10.0.0.1:11015", contendedPod);

          CyclicBarrier startBarrier = new CyclicBarrier(threads);
          List<Future<String>> results = new ArrayList<>(threads);
          for (int t = 0; t < threads; t++) {
            results.add(pool.submit(() -> {
              startBarrier.await(5, TimeUnit.SECONDS);
              return leaser.acquireSlot("ns-A");
            }));
          }
          for (Future<String> result : results) {
            assertEquals("10.0.0.1:11015", result.get(5, TimeUnit.SECONDS));
          }
          assertEquals(threads, contendedPod.getInflightRequests());
          assertEquals("ns-A", contendedPod.getLeasedNamespace());
        }
      }
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * Relays a worker's 429 to a {@code requestNamespace} request, with {@code leasedNamespace} in
   * the leased namespace header unless it is {@code null}.
   */
  private static void relayRejection(TaskWorkerLeaser leaser, String workerAddress,
      String requestNamespace, String leasedNamespace) {
    HttpResponseStatus status = HttpResponseStatus.TOO_MANY_REQUESTS;
    EmbeddedChannel callerChannel = new EmbeddedChannel();
    EmbeddedChannel workerChannel = new EmbeddedChannel(new TaskWorkerManagerBackendHandler(
        callerChannel, leaser, workerAddress, requestNamespace));
    DefaultHttpResponse rejection = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
    if (leasedNamespace != null) {
      rejection.headers().set(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE, leasedNamespace);
    }
    workerChannel.writeInbound(rejection);
    workerChannel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);
    workerChannel.close();

    HttpResponse relayedHeader = callerChannel.readOutbound();
    assertNotNull(relayedHeader);
    assertEquals(status, relayedHeader.status());
    assertEquals(
        leasedNamespace,
        relayedHeader.headers().get(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE));
    LastHttpContent relayedEnd = callerChannel.readOutbound();
    assertNotNull(relayedEnd);
    relayedEnd.release();
  }

  private static void assertWorkerUnreachableResponse(Object msg) {
    assertTrue("Expected a full response, got " + msg, msg instanceof FullHttpResponse);
    FullHttpResponse response = (FullHttpResponse) msg;
    try {
      assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE, response.status());
      assertEquals(TaskWorkerManagerFrontendHandler.WORKER_UNREACHABLE_BODY,
          response.content().toString(StandardCharsets.UTF_8));
      assertEquals("close", response.headers().get("Connection"));
    } finally {
      response.release();
    }
  }

  private static DiscoveryServiceClient discoveryOf(Discoverable discoverable) {
    DiscoveryServiceClient discovery = mock(DiscoveryServiceClient.class);
    ServiceDiscovered serviceDiscovered = mock(ServiceDiscovered.class);
    when(serviceDiscovered.iterator()).thenAnswer(
        invocation -> Collections.singletonList(discoverable).iterator());
    when(discovery.discover(Mockito.anyString())).thenReturn(serviceDiscovered);
    return discovery;
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
