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

import com.google.gson.Gson;
import com.google.inject.Guice;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.ArtifactLocalizer;
import io.cdap.cdap.common.conf.Constants.TaskWorker;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.features.Feature;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.AbstractHttpResponder;
import io.cdap.http.BodyProducer;
import io.cdap.http.ChunkResponder;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.File;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Tests the task worker's concurrency limit in each deployment mode, and that failed tasks give
 * back their slot and lease.
 */
public class TaskWorkerConcurrencyModeTest {

  private static final int CONFIGURED_LIMIT = 7;
  private static final String NAMESPACE = "ns1";
  private static final Gson GSON = new Gson();

  /** A launcher that throws while {@code failure} is non-null, then succeeds so slots can be probed. */
  private static final class ToggleableLauncher extends RunnableTaskLauncher {

    private final AtomicReference<Throwable> failure;

    private ToggleableLauncher(Throwable initialFailure) {
      super(Guice.createInjector());
      this.failure = new AtomicReference<>(initialFailure);
    }

    private void clearFailure() {
      failure.set(null);
    }

    @Override
    public void launchRunnableTask(RunnableTaskContext context) throws Exception {
      Throwable t = failure.get();
      if (t == null) {
        return;
      }
      if (t instanceof Error) {
        throw (Error) t;
      }
      throw (Exception) t;
    }
  }

  private static CConfiguration newCConf(boolean authorizationEnabled,
      boolean userCodeIsolationEnabled, boolean managerEnabled) {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, authorizationEnabled);
    cConf.setBoolean(TaskWorker.USER_CODE_ISOLATION_ENABLED, userCodeIsolationEnabled);
    cConf.setInt(TaskWorker.REQUEST_LIMIT, CONFIGURED_LIMIT);
    cConf.setBoolean("feature.task.worker.manager.enabled", managerEnabled);
    // Keep the periodic restart thread, and the System.exit it ends in, out of the test.
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_DURATION_SECOND, 0);
    return cConf;
  }

  private static TaskWorkerHttpHandlerInternal newHandler(boolean authorizationEnabled,
      boolean userCodeIsolationEnabled, boolean managerEnabled) {
    return newHandler(newCConf(authorizationEnabled, userCodeIsolationEnabled, managerEnabled));
  }

  private static TaskWorkerHttpHandlerInternal newHandler(CConfiguration cConf) {
    return new TaskWorkerHttpHandlerInternal(cConf,
        new CallbackLauncher(() -> { }), className -> { }, new NoOpMetricsCollectionService());
  }

  private static FullHttpRequest runRequest() {
    return runRequest(NAMESPACE);
  }

  private static FullHttpRequest runRequest(String namespace) {
    return requestWithBody(GSON.toJson(RunnableTaskRequest.getBuilder("com.example.SomeTask")
        .withNamespace(namespace)
        .build()));
  }

  private static FullHttpRequest requestWithBody(String body) {
    return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
        "/v3Internal/worker/run",
        Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
  }

  /**
   * Asserts that {@code handler} admits exactly {@code expectedLimit} concurrent requests and
   * applies, or skips, the namespace lease as expected.
   */
  private static void assertConcurrencyAndLeasing(TaskWorkerHttpHandlerInternal handler,
      int expectedLimit, boolean expectStickyLease) {
    List<BodyProducer> inFlight = new ArrayList<>();
    HttpResponder first = Mockito.mock(HttpResponder.class);
    handler.run(runRequest("ns_probe_1"), first);
    ArgumentCaptor<BodyProducer> firstProducer = ArgumentCaptor.forClass(BodyProducer.class);
    ArgumentCaptor<HttpHeaders> firstHeaders = ArgumentCaptor.forClass(HttpHeaders.class);
    Mockito.verify(first).sendContent(
        Mockito.eq(HttpResponseStatus.OK), firstProducer.capture(), firstHeaders.capture());
    inFlight.add(firstProducer.getValue());

    if (expectStickyLease) {
      Assert.assertEquals("RUNNING",
          firstHeaders.getValue().get(Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS));
      StatusCapturingResponder otherNs = new StatusCapturingResponder();
      handler.run(runRequest("ns_probe_2"), otherNs);
      Assert.assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, otherNs.status);
      Assert.assertEquals("ns_probe_1",
          otherNs.headers.get(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE));
    } else {
      Assert.assertNull(
          firstHeaders.getValue().get(Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS));
      if (expectedLimit > 1) {
        // Without a sticky lease, a second namespace is admitted alongside the first.
        HttpResponder secondNs = Mockito.mock(HttpResponder.class);
        handler.run(runRequest("ns_probe_2"), secondNs);
        ArgumentCaptor<BodyProducer> secondProducer = ArgumentCaptor.forClass(BodyProducer.class);
        Mockito.verify(secondNs).sendContent(
            Mockito.eq(HttpResponseStatus.OK), secondProducer.capture(),
            Mockito.any(HttpHeaders.class));
        inFlight.add(secondProducer.getValue());
      }
    }

    while (inFlight.size() < expectedLimit) {
      HttpResponder next = Mockito.mock(HttpResponder.class);
      handler.run(runRequest("ns_probe_1"), next);
      ArgumentCaptor<BodyProducer> producer = ArgumentCaptor.forClass(BodyProducer.class);
      Mockito.verify(next).sendContent(
          Mockito.eq(HttpResponseStatus.OK), producer.capture(), Mockito.any(HttpHeaders.class));
      inFlight.add(producer.getValue());
    }

    StatusCapturingResponder overLimit = new StatusCapturingResponder();
    handler.run(runRequest("ns_probe_1"), overLimit);
    Assert.assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, overLimit.status);

    for (BodyProducer producer : inFlight) {
      finishBody(producer);
    }
  }

  /**
   * Asserts that {@code handler} has exactly {@code expectedAvailable} open slots for a fresh
   * namespace, so no slot or lease was leaked or released twice.
   */
  private static void assertRemainingSlotsAvailable(TaskWorkerHttpHandlerInternal handler,
      String probeNamespace, int expectedAvailable) {
    List<BodyProducer> inFlight = new ArrayList<>();
    for (int i = 0; i < expectedAvailable; i++) {
      HttpResponder responder = Mockito.mock(HttpResponder.class);
      handler.run(runRequest(probeNamespace), responder);
      ArgumentCaptor<BodyProducer> producer = ArgumentCaptor.forClass(BodyProducer.class);
      Mockito.verify(responder).sendContent(
          Mockito.eq(HttpResponseStatus.OK), producer.capture(), Mockito.any(HttpHeaders.class));
      inFlight.add(producer.getValue());
    }

    StatusCapturingResponder overLimit = new StatusCapturingResponder();
    handler.run(runRequest(probeNamespace), overLimit);
    Assert.assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, overLimit.status);

    for (BodyProducer producer : inFlight) {
      finishBody(producer);
    }
  }

  @Test
  public void testNonRbacUsesConfiguredLimit() {
    // No per-task credential exists, so there is nothing for concurrent tasks to corrupt.
    TaskWorkerHttpHandlerInternal handler = newHandler(false, false, false);
    assertConcurrencyAndLeasing(handler, CONFIGURED_LIMIT, false);
  }

  @Test
  public void testRbacWithoutManagerIsClampedToOneTask() {
    TaskWorkerHttpHandlerInternal handler = newHandler(true, true, false);
    assertConcurrencyAndLeasing(handler, 1, false);
  }

  @Test
  public void testRbacWithManagerUsesConfiguredLimit() {
    TaskWorkerHttpHandlerInternal handler = newHandler(true, true, true);
    assertConcurrencyAndLeasing(handler, CONFIGURED_LIMIT, true);
  }

  @Test
  public void testSingleWorkerKeepsLeaseAndConfiguredLimit() {
    // Clients call a single worker directly, so its own lease keeps namespaces apart.
    CConfiguration cConf = newCConf(true, true, true);
    cConf.setInt(TaskWorker.CONTAINER_COUNT, 1);
    TaskWorkerHttpHandlerInternal handler = newHandler(cConf);
    assertConcurrencyAndLeasing(handler, CONFIGURED_LIMIT, true);
  }

  @Test
  public void testManagerFlagWithoutRbacFallsBackToNonRbacBehaviour() {
    // Flag without RBAC must behave exactly like non-RBAC, matching RemoteTaskExecutor's routing.
    TaskWorkerHttpHandlerInternal handler = newHandler(false, false, true);
    assertConcurrencyAndLeasing(handler, CONFIGURED_LIMIT, false);
  }

  @Test
  public void testIsolationDisabledOnRbacStillHonoursTheConfiguredLimit() {
    // Documents existing behaviour; CDF never turns isolation off on RBAC instances.
    TaskWorkerHttpHandlerInternal handler = newHandler(true, false, false);
    assertConcurrencyAndLeasing(handler, CONFIGURED_LIMIT, false);
  }

  @Test
  public void testErrorFromUserCodeReleasesSlotAndLease() {
    ToggleableLauncher launcher = new ToggleableLauncher(
        new NoClassDefFoundError("simulated linkage failure in a user artifact"));
    TaskWorkerHttpHandlerInternal handler = new TaskWorkerHttpHandlerInternal(
        newCConf(true, true, true), launcher, className -> { }, new NoOpMetricsCollectionService());

    try {
      handler.run(runRequest(), Mockito.mock(HttpResponder.class));
      Assert.fail("An Error must not be swallowed");
    } catch (NoClassDefFoundError expected) {
      // The handler releases what it holds and rethrows.
    }

    launcher.clearFailure();
    assertRemainingSlotsAvailable(handler, "ns_after", CONFIGURED_LIMIT);
  }

  @Test
  public void testExceptionFromUserCodeReleasesSlotAndLease() {
    ToggleableLauncher launcher = new ToggleableLauncher(
        new IllegalStateException("simulated task failure"));
    TaskWorkerHttpHandlerInternal handler = new TaskWorkerHttpHandlerInternal(
        newCConf(true, true, true), launcher, className -> { }, new NoOpMetricsCollectionService());

    handler.run(runRequest(), Mockito.mock(HttpResponder.class));

    launcher.clearFailure();
    assertRemainingSlotsAvailable(handler, "ns_after", CONFIGURED_LIMIT);
  }

  @Test
  public void testMissingTaskClassReleasesSlotAndLease() {
    ToggleableLauncher launcher = new ToggleableLauncher(
        new ClassNotFoundException("com.example.SomeTask"));
    TaskWorkerHttpHandlerInternal handler = new TaskWorkerHttpHandlerInternal(
        newCConf(true, true, true), launcher, className -> { }, new NoOpMetricsCollectionService());

    handler.run(runRequest(), Mockito.mock(HttpResponder.class));

    launcher.clearFailure();
    assertRemainingSlotsAvailable(handler, "ns_after", CONFIGURED_LIMIT);
  }

  @Test
  public void testUnparseableRequestReleasesTheSlotAndTakesNoLease() {
    ToggleableLauncher launcher = new ToggleableLauncher(
        new IllegalStateException("never reached"));
    TaskWorkerHttpHandlerInternal handler = new TaskWorkerHttpHandlerInternal(
        newCConf(true, true, true), launcher, className -> { }, new NoOpMetricsCollectionService());

    handler.run(requestWithBody("this is not json"), Mockito.mock(HttpResponder.class));

    launcher.clearFailure();
    assertRemainingSlotsAvailable(handler, "ns_after", CONFIGURED_LIMIT);
  }

  @Test
  public void testFailuresDoNotAccumulateAcrossRequests() {
    ToggleableLauncher launcher = new ToggleableLauncher(
        new NoClassDefFoundError("simulated linkage failure in a user artifact"));
    TaskWorkerHttpHandlerInternal handler = new TaskWorkerHttpHandlerInternal(
        newCConf(true, true, true), launcher, className -> { }, new NoOpMetricsCollectionService());

    for (int i = 0; i < CONFIGURED_LIMIT; i++) {
      try {
        handler.run(runRequest(), Mockito.mock(HttpResponder.class));
        Assert.fail("Expected the Error to propagate");
      } catch (NoClassDefFoundError expected) {
        // Expected.
      }
    }

    launcher.clearFailure();
    assertRemainingSlotsAvailable(handler, "ns_after", CONFIGURED_LIMIT);
  }

  @Test
  public void testOtherNamespaceIsRejectedWithTheLeaseHolder() {
    // The responder is a mock, so the first response never finishes and its lease stays held.
    TaskWorkerHttpHandlerInternal handler = new TaskWorkerHttpHandlerInternal(
        newCConf(true, true, true), new CallbackLauncher(() -> { }), className -> { },
        new NoOpMetricsCollectionService());
    handler.run(runRequest(NAMESPACE), Mockito.mock(HttpResponder.class));

    HttpResponder rejected = Mockito.mock(HttpResponder.class);
    handler.run(runRequest("ns2"), rejected);

    ArgumentCaptor<HttpHeaders> headers = ArgumentCaptor.forClass(HttpHeaders.class);
    Mockito.verify(rejected).sendStatus(Mockito.eq(HttpResponseStatus.TOO_MANY_REQUESTS),
        headers.capture());
    // The manager adopts this header to fix its routing table.
    Assert.assertEquals(NAMESPACE,
        headers.getValue().get(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE));
    // A running worker's rejection keeps it in rotation; the manager only cools it down.
    Assert.assertEquals("RUNNING",
        headers.getValue().get(Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS));
    // Only 1 slot is held by the first admitted task; the other CONFIGURED_LIMIT - 1 remain open.
    assertRemainingSlotsAvailable(handler, NAMESPACE, CONFIGURED_LIMIT - 1);
  }

  @Test
  public void testFullLeasedWorkerIncludesLeasedNamespaceHeaderOn429() {
    TaskWorkerHttpHandlerInternal handler = new TaskWorkerHttpHandlerInternal(
        newCConf(true, true, true), new CallbackLauncher(() -> { }), className -> { },
        new NoOpMetricsCollectionService());
    for (int i = 0; i < CONFIGURED_LIMIT; i++) {
      handler.run(runRequest("ns_a"), Mockito.mock(HttpResponder.class));
    }

    // (limit + 1)th request from the same namespace gets 429 with X-CDAP-Leased-Namespace: ns_a.
    StatusCapturingResponder sameNsRejected = new StatusCapturingResponder();
    handler.run(runRequest("ns_a"), sameNsRejected);
    Assert.assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, sameNsRejected.status);
    Assert.assertEquals("ns_a",
        sameNsRejected.headers.get(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE));

    // Speculative claim from ns_b after a manager restart also learns that ns_a holds the full pod.
    StatusCapturingResponder crossNsRejected = new StatusCapturingResponder();
    handler.run(runRequest("ns_b"), crossNsRejected);
    Assert.assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, crossNsRejected.status);
    Assert.assertEquals("ns_a",
        crossNsRejected.headers.get(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE));

    // Direct (non-leased) full pod still answers plain 429 without NPE or lease header.
    TaskWorkerHttpHandlerInternal directHandler = new TaskWorkerHttpHandlerInternal(
        newCConf(false, false, false), new CallbackLauncher(() -> { }), className -> { },
        new NoOpMetricsCollectionService());
    for (int i = 0; i < CONFIGURED_LIMIT; i++) {
      directHandler.run(runRequest("ns_a"), Mockito.mock(HttpResponder.class));
    }
    StatusCapturingResponder directRejected = new StatusCapturingResponder();
    directHandler.run(runRequest("ns_a"), directRejected);
    Assert.assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, directRejected.status);
    Assert.assertNull(
        directRejected.headers.get(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE));
    Assert.assertNull(
        directRejected.headers.get(Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS));

    // Never-leased pod at limit 0 omits X-CDAP-Leased-Namespace and releases its rejected slot.
    CConfiguration zeroLimitConf = newCConf(true, true, true);
    zeroLimitConf.setInt(TaskWorker.REQUEST_LIMIT, 0);
    TaskWorkerHttpHandlerInternal neverLeasedHandler = new TaskWorkerHttpHandlerInternal(
        zeroLimitConf, new CallbackLauncher(() -> { }), className -> { },
        new NoOpMetricsCollectionService());
    StatusCapturingResponder neverLeasedRejected = new StatusCapturingResponder();
    neverLeasedHandler.run(runRequest("ns_a"), neverLeasedRejected);
    Assert.assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, neverLeasedRejected.status);
    Assert.assertNull(
        neverLeasedRejected.headers.get(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE));

    // Previously-leased idle pod (currentLease == ns_old, activeTaskCount == 0) must omit the
    // stale finished namespace if a burst exceeds concurrentRequestLimit before admitTask runs.
    CConfiguration oneSlotConf = newCConf(true, true, true);
    oneSlotConf.setInt(TaskWorker.REQUEST_LIMIT, 1);
    TaskWorkerHttpHandlerInternal idleLeasedHandler = new TaskWorkerHttpHandlerInternal(
        oneSlotConf, new CallbackLauncher(() -> { }), className -> { },
        new NoOpMetricsCollectionService());
    idleLeasedHandler.run(runRequest("ns_old"), completingResponder());

    StatusCapturingResponder burstOverflowRejected = new StatusCapturingResponder();
    AtomicBoolean nestedSent = new AtomicBoolean(false);
    String secondNamespaceJson = GSON.toJson(
        RunnableTaskRequest.getBuilder("com.example.SomeTask").withNamespace("ns_b").build());
    FullHttpRequest preAdmissionBurstRequest = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1, HttpMethod.POST, "/v3Internal/worker/run",
        Unpooled.copiedBuffer(secondNamespaceJson, StandardCharsets.UTF_8)) {
      @Override
      public ByteBuf content() {
        if (nestedSent.compareAndSet(false, true)) {
          idleLeasedHandler.run(runRequest("ns_b"), burstOverflowRejected);
        }
        return super.content();
      }
    };
    idleLeasedHandler.run(preAdmissionBurstRequest, completingResponder());
    Assert.assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, burstOverflowRejected.status);
    Assert.assertNull(
        burstOverflowRejected.headers.get(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE));
    assertRemainingSlotsAvailable(idleLeasedHandler, "ns_after", 1);
  }

  @Test
  public void testRejected429SlotTriggersDeferredDrainStopOnce() {
    // Case 1: Concurrency-limit 429 (limit 1, same namespace) holds the transient slot.
    assertDeferredDrainStopWaitsForRejected429(1, "ns_a");
    // Case 2: REJECTED_MISMATCH 429 (limit 2, different namespace) holds the transient slot.
    assertDeferredDrainStopWaitsForRejected429(2, "ns_b");
  }

  private static void assertDeferredDrainStopWaitsForRejected429(
      int requestLimit, String rejectedNamespace) {
    CConfiguration cConf = newCConf(true, true, true);
    cConf.setInt(TaskWorker.REQUEST_LIMIT, requestLimit);
    cConf.setInt(TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 1);
    List<String> stopped = new ArrayList<>();
    TaskWorkerHttpHandlerInternal handler = new TaskWorkerHttpHandlerInternal(
        cConf, new CallbackLauncher(() -> { }), stopped::add, new NoOpMetricsCollectionService());

    HttpResponder activeResponder = Mockito.mock(HttpResponder.class);
    handler.run(runRequest("ns_a"), activeResponder);
    ArgumentCaptor<BodyProducer> activeProducer = ArgumentCaptor.forClass(BodyProducer.class);
    Mockito.verify(activeResponder).sendContent(
        Mockito.eq(HttpResponseStatus.OK), activeProducer.capture(), Mockito.any(HttpHeaders.class));

    StatusCapturingResponder rejected = new StatusCapturingResponder(() -> {
      finishBody(activeProducer.getValue());
      Assert.assertTrue("Stopper must wait until the 429 slot releases", stopped.isEmpty());
    });
    handler.run(runRequest(rejectedNamespace), rejected);
    Assert.assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, rejected.status);
    Assert.assertEquals("ns_a",
        rejected.headers.get(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE));
    Assert.assertEquals(Collections.singletonList(""), stopped);
  }

  @Test
  public void testWipeHoldsSlotUntilCompleteAgainstConcurrentRejected429() throws Exception {
    final CountDownLatch rejectedEnteredSend = new CountDownLatch(1);
    final CountDownLatch releaseRejectedSlot = new CountDownLatch(1);
    final CountDownLatch rejectedFinished = new CountDownLatch(1);
    final AtomicReference<Throwable> clearContextFailure = new AtomicReference<>();
    final List<String> stopped = new ArrayList<>();

    NettyHttpService sidecar = NettyHttpService.builder("metadata-sidecar")
        .setHost(InetAddress.getLoopbackAddress().getHostName())
        .setHttpHandlers(new StubSidecarHandler(() -> {
          try {
            releaseRejectedSlot.countDown();
            Assert.assertTrue(rejectedFinished.await(5, TimeUnit.SECONDS));
            Assert.assertTrue("Stopper must not fire while credential wipe is still running",
                stopped.isEmpty());
          } catch (Throwable t) {
            clearContextFailure.set(t);
          }
        }))
        .build();
    sidecar.start();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      CConfiguration cConf = newCConf(true, true, true);
      cConf.setBoolean("feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(),
          true);
      cConf.setInt(ArtifactLocalizer.PORT, sidecar.getBindAddress().getPort());
      cConf.setInt(TaskWorker.REQUEST_LIMIT, 2);
      cConf.setInt(TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 1);

      TaskWorkerHttpHandlerInternal handler = new TaskWorkerHttpHandlerInternal(
          cConf, new CallbackLauncher(() -> { }), stopped::add, new NoOpMetricsCollectionService());

      HttpResponder firstResponder = Mockito.mock(HttpResponder.class);
      handler.run(runRequest("ns_a"), firstResponder);
      ArgumentCaptor<BodyProducer> firstProducer = ArgumentCaptor.forClass(BodyProducer.class);
      Mockito.verify(firstResponder).sendContent(
          Mockito.eq(HttpResponseStatus.OK), firstProducer.capture(),
          Mockito.any(HttpHeaders.class));

      HttpResponder secondResponder = Mockito.mock(HttpResponder.class);
      handler.run(runRequest("ns_a"), secondResponder);
      ArgumentCaptor<BodyProducer> secondProducer = ArgumentCaptor.forClass(BodyProducer.class);
      Mockito.verify(secondResponder).sendContent(
          Mockito.eq(HttpResponseStatus.OK), secondProducer.capture(),
          Mockito.any(HttpHeaders.class));

      StatusCapturingResponder overflowResponder = new StatusCapturingResponder(() -> {
        rejectedEnteredSend.countDown();
        try {
          Assert.assertTrue(releaseRejectedSlot.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      });
      final Future<?> rejectedFuture = executor.submit(() -> {
        try {
          handler.run(runRequest("ns_a"), overflowResponder);
        } finally {
          rejectedFinished.countDown();
        }
      });
      Assert.assertTrue(rejectedEnteredSend.await(5, TimeUnit.SECONDS));

      // Task 1 finishes: activeTaskCount drops 2 -> 1 (no wipe yet) and mustRestart flips to true.
      finishBody(firstProducer.getValue());
      Assert.assertTrue(stopped.isEmpty());

      // Task 2 finishes: enters wipe() while overflowResponder releases its rejected 429 slot.
      finishBody(secondProducer.getValue());
      rejectedFuture.get(5, TimeUnit.SECONDS);

      if (clearContextFailure.get() != null) {
        throw new AssertionError("Failure inside credential wipe callback",
            clearContextFailure.get());
      }
      Assert.assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, overflowResponder.status);
      Assert.assertEquals(Collections.singletonList("com.example.SomeTask"), stopped);
    } finally {
      releaseRejectedSlot.countDown();
      executor.shutdownNow();
      stopQuietly(sidecar);
    }
  }

  @Test
  public void testDrainingFlipAfterEntryRejectsAsTerminatingAndStopsOnce() throws Exception {
    // 1. mustRestart flips to true at killAfterRequestCount while a racing request is reading body
    // bytes before admitTask: must answer 429 + a TERMINATING status without launching user code.
    CConfiguration killConf = newCConf(true, true, true);
    killConf.setInt(TaskWorker.REQUEST_LIMIT, 2);
    killConf.setInt(TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 1);
    final List<String> killStopped = new ArrayList<>();
    final AtomicInteger killLaunches = new AtomicInteger();
    TaskWorkerHttpHandlerInternal killHandler = new TaskWorkerHttpHandlerInternal(
        killConf, new CallbackLauncher(killLaunches::incrementAndGet), killStopped::add,
        new NoOpMetricsCollectionService());

    HttpResponder firstResponder = Mockito.mock(HttpResponder.class);
    killHandler.run(runRequest("ns_a"), firstResponder);
    final ArgumentCaptor<BodyProducer> firstProducer = ArgumentCaptor.forClass(BodyProducer.class);
    Mockito.verify(firstResponder).sendContent(
        Mockito.eq(HttpResponseStatus.OK), firstProducer.capture(), Mockito.any(HttpHeaders.class));

    final AtomicBoolean flippedDuringParse = new AtomicBoolean(false);
    String racingBody = GSON.toJson(
        RunnableTaskRequest.getBuilder("com.example.SomeTask").withNamespace("ns_b").build());
    FullHttpRequest racingRequest = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1, HttpMethod.POST, "/v3Internal/worker/run",
        Unpooled.copiedBuffer(racingBody, StandardCharsets.UTF_8)) {
      @Override
      public ByteBuf content() {
        if (flippedDuringParse.compareAndSet(false, true)) {
          finishBody(firstProducer.getValue());
          Assert.assertTrue("Stopper must wait while racing request holds slot",
              killStopped.isEmpty());
        }
        return super.content();
      }
    };

    StatusCapturingResponder racingRejected = new StatusCapturingResponder();
    killHandler.run(racingRequest, racingRejected);
    Assert.assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, racingRejected.status);
    Assert.assertEquals("TERMINATING",
        racingRejected.headers.get(Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS));
    Assert.assertNull(
        racingRejected.headers.get(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE));
    Assert.assertEquals(1, killLaunches.get());
    Assert.assertEquals(Collections.singletonList(""), killStopped);

    // 2. Failed credential wipe sets mustRestart while a racing request is queued before admitTask:
    // must answer 429 + a TERMINATING status rather than calling provision() and returning 500.
    final NettyHttpService sidecar = NettyHttpService.builder("metadata-sidecar")
        .setHost(InetAddress.getLoopbackAddress().getHostName())
        .setHttpHandlers(new StubSidecarHandler())
        .build();
    sidecar.start();
    try {
      CConfiguration wipeConf = newCConf(true, true, true);
      wipeConf.setBoolean("feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(),
          true);
      wipeConf.setInt(ArtifactLocalizer.PORT, sidecar.getBindAddress().getPort());
      wipeConf.setInt(TaskWorker.REQUEST_LIMIT, 2);
      wipeConf.setInt(TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 0);
      final List<String> wipeStopped = new ArrayList<>();
      final AtomicInteger wipeLaunches = new AtomicInteger();
      TaskWorkerHttpHandlerInternal wipeHandler = new TaskWorkerHttpHandlerInternal(
          wipeConf, new CallbackLauncher(wipeLaunches::incrementAndGet), wipeStopped::add,
          new NoOpMetricsCollectionService());

      HttpResponder activeResponder = Mockito.mock(HttpResponder.class);
      wipeHandler.run(runRequest("ns_a"), activeResponder);
      final ArgumentCaptor<BodyProducer> activeProducer =
          ArgumentCaptor.forClass(BodyProducer.class);
      Mockito.verify(activeResponder).sendContent(
          Mockito.eq(HttpResponseStatus.OK), activeProducer.capture(),
          Mockito.any(HttpHeaders.class));

      final AtomicBoolean failedWipeTriggered = new AtomicBoolean(false);
      FullHttpRequest queuedBehindWipeRequest = new DefaultFullHttpRequest(
          HttpVersion.HTTP_1_1, HttpMethod.POST, "/v3Internal/worker/run",
          Unpooled.copiedBuffer(racingBody, StandardCharsets.UTF_8)) {
        @Override
        public ByteBuf content() {
          if (failedWipeTriggered.compareAndSet(false, true)) {
            stopQuietly(sidecar);
            finishBody(activeProducer.getValue());
            Assert.assertTrue(wipeStopped.isEmpty());
          }
          return super.content();
        }
      };

      StatusCapturingResponder postWipeRejected = new StatusCapturingResponder();
      wipeHandler.run(queuedBehindWipeRequest, postWipeRejected);
      Assert.assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS, postWipeRejected.status);
      Assert.assertEquals("TERMINATING",
          postWipeRejected.headers.get(Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS));
      Assert.assertNull(
          postWipeRejected.headers.get(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE));
      Assert.assertEquals(1, wipeLaunches.get());
      Assert.assertEquals(Collections.singletonList(""), wipeStopped);
    } finally {
      stopQuietly(sidecar);
    }
  }

  @Test
  public void testFailedCredentialWipeRestartsThePod() throws Exception {
    NettyHttpService sidecar = NettyHttpService.builder("metadata-sidecar")
        .setHost(InetAddress.getLoopbackAddress().getHostName())
        .setHttpHandlers(new StubSidecarHandler())
        .build();
    sidecar.start();
    try {
      CConfiguration cConf = newCConf(true, true, true);
      cConf.setBoolean("feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(),
          true);
      cConf.setInt(ArtifactLocalizer.PORT, sidecar.getBindAddress().getPort());
      // Only the failed wipe may restart the pod, not the request count.
      cConf.setInt(TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 0);
      List<String> stopped = new ArrayList<>();

      // Provisioning succeeds, then the sidecar goes away so the wipe after the task fails.
      TaskWorkerHttpHandlerInternal handler = new TaskWorkerHttpHandlerInternal(cConf,
          new CallbackLauncher(() -> {
            stopQuietly(sidecar);
            throw new IllegalStateException("simulated task failure");
          }), stopped::add, new NoOpMetricsCollectionService());
      handler.run(runRequest(NAMESPACE), Mockito.mock(HttpResponder.class));

      Assert.assertEquals("A credential that can't be wiped must not outlive its namespace",
          Collections.singletonList("com.example.SomeTask"), stopped);
    } finally {
      stopQuietly(sidecar);
    }
  }

  @Test
  public void testDirectPodStopsAtTheKillCountAndRefusesNewTasks() {
    // RBAC on, flag off: stops at once without draining, and admits nothing while it stops.
    CConfiguration cConf = newCConf(true, true, false);
    cConf.setInt(TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 1);
    List<String> stopped = new ArrayList<>();
    TaskWorkerHttpHandlerInternal handler = new TaskWorkerHttpHandlerInternal(cConf,
        new CallbackLauncher(() -> { }), stopped::add, new NoOpMetricsCollectionService());

    handler.run(runRequest(), completingResponder());
    Assert.assertEquals(Collections.singletonList("com.example.SomeTask"), stopped);

    HttpResponder next = completingResponder();
    handler.run(runRequest(), next);
    // A direct pod answers as before the task worker manager, without a status header.
    Mockito.verify(next).sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
    Mockito.verify(next, Mockito.never()).sendStatus(Mockito.any(HttpResponseStatus.class),
        Mockito.any(HttpHeaders.class));
    Assert.assertEquals(Collections.singletonList("com.example.SomeTask"), stopped);
  }

  @Test
  public void testLeasedPodDrainsAtTheKillCountAndReportsTerminating() {
    CConfiguration cConf = newCConf(true, true, true);
    cConf.setInt(TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 1);
    List<String> stopped = new ArrayList<>();
    TaskWorkerHttpHandlerInternal handler = new TaskWorkerHttpHandlerInternal(cConf,
        new CallbackLauncher(() -> { }), stopped::add, new NoOpMetricsCollectionService());

    handler.run(runRequest(), completingResponder());
    Assert.assertEquals(Collections.singletonList("com.example.SomeTask"), stopped);

    HttpResponder next = Mockito.mock(HttpResponder.class);
    handler.run(runRequest(), next);
    ArgumentCaptor<HttpHeaders> headers = ArgumentCaptor.forClass(HttpHeaders.class);
    Mockito.verify(next).sendStatus(Mockito.eq(HttpResponseStatus.TOO_MANY_REQUESTS),
        headers.capture());
    Assert.assertEquals("TERMINATING",
        headers.getValue().get(Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS));
  }

  @Test
  public void testLeasedTaskResponsesReportTheWorkerStatus() {
    CConfiguration cConf = newCConf(true, true, true);
    cConf.setInt(TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 2);
    List<String> stopped = new ArrayList<>();
    TaskWorkerHttpHandlerInternal handler = new TaskWorkerHttpHandlerInternal(cConf,
        new CallbackLauncher(() -> { }), stopped::add, new NoOpMetricsCollectionService());

    HttpResponder first = completingResponder();
    handler.run(runRequest(), first);
    Assert.assertEquals("RUNNING", sentContentHeaders(first)
        .get(Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS));
    Assert.assertTrue(stopped.isEmpty());

    // The task that reaches the kill count says so on its own 200, before the pod drains.
    HttpResponder second = completingResponder();
    handler.run(runRequest(), second);
    Assert.assertEquals("TERMINATING", sentContentHeaders(second)
        .get(Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS));
    Assert.assertEquals(Collections.singletonList("com.example.SomeTask"), stopped);
  }

  @Test
  public void testLeasedFailedTaskReportsTheWorkerStatus() {
    CConfiguration cConf = newCConf(true, true, true);
    cConf.setInt(TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 1);
    List<String> stopped = new ArrayList<>();
    TaskWorkerHttpHandlerInternal handler = new TaskWorkerHttpHandlerInternal(cConf,
        new CallbackLauncher(() -> {
          throw new IllegalStateException("task failed");
        }), stopped::add, new NoOpMetricsCollectionService());

    // User code may have run and the kill count is reached, so the 500 reports TERMINATING.
    StatusCapturingResponder failed = new StatusCapturingResponder();
    handler.run(runRequest(), failed);
    Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, failed.status);
    Assert.assertEquals("TERMINATING",
        failed.headers.get(Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS));
  }

  @Test
  public void testDirectTaskResponsesCarryNoWorkerStatus() {
    TaskWorkerHttpHandlerInternal handler = new TaskWorkerHttpHandlerInternal(
        newCConf(false, false, false), new CallbackLauncher(() -> { }), className -> { },
        new NoOpMetricsCollectionService());

    HttpResponder responder = completingResponder();
    handler.run(runRequest(), responder);

    Assert.assertNull(sentContentHeaders(responder)
        .get(Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS));
  }

  /** Returns the headers of the single body response sent through the mock {@code responder}. */
  private static HttpHeaders sentContentHeaders(HttpResponder responder) {
    ArgumentCaptor<HttpHeaders> headers = ArgumentCaptor.forClass(HttpHeaders.class);
    Mockito.verify(responder).sendContent(Mockito.eq(HttpResponseStatus.OK),
        Mockito.any(BodyProducer.class), headers.capture());
    return headers.getValue();
  }

  /** A responder that finishes the response body at once, as a real channel would. */
  private static HttpResponder completingResponder() {
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    Mockito.doAnswer(invocation -> {
      ((BodyProducer) invocation.getArguments()[1]).finished();
      return null;
    }).when(responder).sendContent(Mockito.any(HttpResponseStatus.class),
        Mockito.any(BodyProducer.class), Mockito.any(HttpHeaders.class));
    return responder;
  }

  private static void finishBody(BodyProducer producer) {
    try {
      producer.finished();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static void stopQuietly(NettyHttpService service) {
    try {
      service.stop();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /** A launcher that runs a callback instead of a task; the callback may throw. */
  private static final class CallbackLauncher extends RunnableTaskLauncher {

    private final Runnable callback;

    private CallbackLauncher(Runnable callback) {
      super(Guice.createInjector());
      this.callback = callback;
    }

    @Override
    public void launchRunnableTask(RunnableTaskContext context) {
      callback.run();
    }
  }

  /** Accepts the metadata sidecar's set and clear context calls. */
  @Path("/")
  public static final class StubSidecarHandler extends AbstractHttpHandler {

    private final Runnable onClear;

    public StubSidecarHandler() {
      this(() -> { });
    }

    public StubSidecarHandler(Runnable onClear) {
      this.onClear = onClear;
    }

    @PUT
    @Path("/set-context")
    public void setContext(FullHttpRequest request, HttpResponder responder) {
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @DELETE
    @Path("/clear-context")
    public void clearContext(FullHttpRequest request, HttpResponder responder) {
      try {
        onClear.run();
      } finally {
        responder.sendStatus(HttpResponseStatus.OK);
      }
    }
  }

  /** Captures status and headers across both 1-arg and 2-arg {@link HttpResponder#sendStatus}. */
  private static final class StatusCapturingResponder extends AbstractHttpResponder {

    private final Runnable onSend;
    private HttpResponseStatus status;
    private HttpHeaders headers = EmptyHttpHeaders.INSTANCE;

    private StatusCapturingResponder() {
      this(() -> { });
    }

    private StatusCapturingResponder(Runnable onSend) {
      this.onSend = onSend;
    }

    @Override
    public ChunkResponder sendChunkStart(HttpResponseStatus status, HttpHeaders headers) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void sendContent(HttpResponseStatus status, ByteBuf content, HttpHeaders headers) {
      this.status = status;
      this.headers = headers;
      onSend.run();
    }

    @Override
    public void sendContent(HttpResponseStatus status, BodyProducer bodyProducer,
        HttpHeaders headers) {
      this.status = status;
      this.headers = headers;
      onSend.run();
    }

    @Override
    public void sendFile(File file, HttpHeaders headers) {
      throw new UnsupportedOperationException();
    }
  }
}
