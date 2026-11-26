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
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.GcpMetadataTaskContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Tests that every way a task can fail gives its slot back to the task worker exactly once.
 */
public class TaskWorkerFailureHandlingTest {

  private static final int REQUEST_LIMIT = 3;
  private static final String TASK_CLASS = "com.example.SomeTask";
  private static final Gson GSON = new Gson();
  private static final String SET_CONTEXT = "set-context";
  private static final String CLEAR_CONTEXT = "clear-context";

  private static CConfiguration newCConf(boolean userCodeIsolationEnabled) {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(TaskWorker.USER_CODE_ISOLATION_ENABLED, userCodeIsolationEnabled);
    cConf.setInt(TaskWorker.REQUEST_LIMIT, REQUEST_LIMIT);
    // Keep the periodic restart thread, and the System.exit it ends in, out of the test.
    cConf.setInt(TaskWorker.CONTAINER_KILL_AFTER_DURATION_SECOND, 0);
    cConf.setInt(TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 0);
    return cConf;
  }

  private static TaskWorkerHttpHandlerInternal newHandler(CConfiguration cConf,
      Runnable launch, Consumer<String> stopper) {
    return new TaskWorkerHttpHandlerInternal(cConf, new CallbackLauncher(launch), stopper,
        new NoOpMetricsCollectionService());
  }

  private static TaskWorkerHttpHandlerInternal newHandler(Runnable launch) {
    return newHandler(newCConf(false), launch, className -> { });
  }

  private static FullHttpRequest runRequest() {
    return runRequest("ns1");
  }

  private static FullHttpRequest runRequest(String namespace) {
    return requestWithBody(GSON.toJson(RunnableTaskRequest.getBuilder(TASK_CLASS)
        .withNamespace(namespace)
        .build()));
  }

  private static FullHttpRequest requestWithBody(String body) {
    return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
        "/v3Internal/worker/run", Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
  }

  private static void throwError() {
    throw new NoClassDefFoundError("simulated linkage failure in a user artifact");
  }

  /**
   * Asserts that {@code handler} has exactly {@code expectedAvailable} open slots, so none was
   * leaked or released twice.
   */
  private static void assertAvailableSlots(TaskWorkerHttpHandlerInternal handler,
      String namespace, int expectedAvailable) {
    List<BodyProducer> inFlight = new ArrayList<>();
    for (int i = 0; i < expectedAvailable; i++) {
      HttpResponder responder = Mockito.mock(HttpResponder.class);
      handler.run(runRequest(namespace), responder);
      ArgumentCaptor<BodyProducer> producer = ArgumentCaptor.forClass(BodyProducer.class);
      Mockito.verify(responder).sendContent(
          Mockito.eq(HttpResponseStatus.OK), producer.capture(), Mockito.any(HttpHeaders.class));
      inFlight.add(producer.getValue());
    }

    HttpResponder overLimit = Mockito.mock(HttpResponder.class);
    handler.run(runRequest(namespace), overLimit);
    Mockito.verify(overLimit, Mockito.never()).sendContent(
        Mockito.eq(HttpResponseStatus.OK), Mockito.any(BodyProducer.class),
        Mockito.any(HttpHeaders.class));

    for (BodyProducer producer : inFlight) {
      try {
        producer.finished();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Test
  public void testErrorFromTaskReleasesTheSlot() {
    AtomicBoolean shouldThrow = new AtomicBoolean(true);
    TaskWorkerHttpHandlerInternal handler = newHandler(() -> {
      if (shouldThrow.get()) {
        throwError();
      }
    });

    try {
      handler.run(runRequest(), Mockito.mock(HttpResponder.class));
      Assert.fail("Expected the Error to propagate");
    } catch (NoClassDefFoundError expected) {
      // The handler releases the slot and rethrows.
    }

    shouldThrow.set(false);
    assertAvailableSlots(handler, "ns1", REQUEST_LIMIT);
  }

  @Test
  public void testRepeatedErrorsDoNotWedgeThePod() {
    AtomicBoolean shouldThrow = new AtomicBoolean(true);
    TaskWorkerHttpHandlerInternal handler = newHandler(() -> {
      if (shouldThrow.get()) {
        throwError();
      }
    });

    for (int i = 0; i < REQUEST_LIMIT; i++) {
      try {
        handler.run(runRequest(), Mockito.mock(HttpResponder.class));
        Assert.fail("Expected the Error to propagate");
      } catch (NoClassDefFoundError expected) {
        // Expected.
      }
    }

    // If each failure leaked a slot, this request would now be rejected with a 429.
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    try {
      handler.run(runRequest(), responder);
    } catch (NoClassDefFoundError expected) {
      // Expected: admitted and run again.
    }
    Mockito.verify(responder, Mockito.never()).sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
    shouldThrow.set(false);
    assertAvailableSlots(handler, "ns1", REQUEST_LIMIT);
  }

  @Test
  public void testUnparseableRequestReleasesTheSlot() {
    TaskWorkerHttpHandlerInternal handler = newHandler(() -> { });
    HttpResponder responder = Mockito.mock(HttpResponder.class);

    handler.run(requestWithBody("this is not json"), responder);

    Mockito.verify(responder).sendString(Mockito.eq(HttpResponseStatus.INTERNAL_SERVER_ERROR),
        Mockito.anyString(), Mockito.any(HttpHeaders.class));
    assertAvailableSlots(handler, "ns1", REQUEST_LIMIT);
  }

  @Test
  public void testFailedErrorResponseReleasesTheSlotOnce() {
    AtomicBoolean shouldThrow = new AtomicBoolean(true);
    TaskWorkerHttpHandlerInternal handler = newHandler(() -> {
      if (shouldThrow.get()) {
        throw new IllegalStateException("simulated task failure");
      }
    });
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    Mockito.doThrow(new IllegalStateException("simulated closed channel"))
        .when(responder).sendString(Mockito.any(HttpResponseStatus.class), Mockito.anyString(),
            Mockito.any(HttpHeaders.class));

    try {
      handler.run(runRequest(), responder);
      Assert.fail("Expected the send failure to propagate");
    } catch (IllegalStateException expected) {
      Assert.assertEquals("simulated closed channel", expected.getMessage());
    }

    shouldThrow.set(false);
    assertAvailableSlots(handler, "ns1", REQUEST_LIMIT);
  }

  @Test
  public void testFailedCredentialWipeReleasesTheSlotOnce() throws Exception {
    // A failed wipe sets mustRestart = true and calls stopIfRestartPendingAndIdle, which only
    // fires when runningRequestCount == 0 (a double-release to -1 would miss zero and never call
    // stopper).
    NettyHttpService sidecar = startSidecar();
    try {
      CConfiguration cConf = sidecarCConf(newCConf(false), sidecar);
      List<String> stopped = new ArrayList<>();
      // Provisioning succeeds, then the sidecar goes away so the wipe after the task fails.
      TaskWorkerHttpHandlerInternal handler = newHandler(cConf, () -> {
        stopQuietly(sidecar);
        throw new IllegalStateException("simulated task failure");
      }, stopped::add);

      HttpResponder responder = Mockito.mock(HttpResponder.class);
      handler.run(runRequest(), responder);

      Assert.assertEquals(
          "A failed wipe must release the slot once so stopIfRestartPendingAndIdle sees zero",
          Collections.singletonList(""), stopped);
      Mockito.verify(responder, Mockito.times(1)).sendString(
          Mockito.eq(HttpResponseStatus.INTERNAL_SERVER_ERROR), Mockito.anyString(),
          Mockito.any(HttpHeaders.class));
    } finally {
      stopQuietly(sidecar);
    }
  }

  @Test
  public void testTaskThatThrowsCountsTowardTheRestart() {
    CConfiguration cConf = newCConf(true);
    cConf.setInt(TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 1);
    List<String> stopped = new ArrayList<>();
    TaskWorkerHttpHandlerInternal handler = newHandler(cConf, () -> {
      throw new IllegalStateException("simulated task failure");
    }, stopped::add);

    handler.run(runRequest(), Mockito.mock(HttpResponder.class));

    Assert.assertEquals("User code may have run, so isolation must restart the pod",
        Collections.singletonList(TASK_CLASS), stopped);
  }

  @Test
  public void testFailedCredentialWipeRestartsThePod() throws Exception {
    // The wipe runs after a failed task's completion, which has already checked the restart flag,
    // so the wipe failure has to stop the pod itself.
    NettyHttpService sidecar = startSidecar();
    try {
      CConfiguration cConf = sidecarCConf(newCConf(true), sidecar);
      List<String> stopped = new ArrayList<>();
      // Provisioning succeeds, then the sidecar goes away so the wipe after the task fails.
      TaskWorkerHttpHandlerInternal handler = newHandler(cConf, () -> {
        stopQuietly(sidecar);
        throw new IllegalStateException("simulated task failure");
      }, stopped::add);

      handler.run(runRequest(), Mockito.mock(HttpResponder.class));

      Assert.assertEquals("A credential that can't be wiped must not outlive its namespace",
          Collections.singletonList(""), stopped);

      HttpResponder next = Mockito.mock(HttpResponder.class);
      handler.run(runRequest(), next);
      Mockito.verify(next).sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
    } finally {
      stopQuietly(sidecar);
    }
  }

  @Test
  public void testFailedCredentialWipeDefersStopWhileATaskRuns() throws Exception {
    NettyHttpService sidecar = startSidecar();
    try {
      CConfiguration cConf = sidecarCConf(newCConf(false), sidecar);
      List<String> stopped = new ArrayList<>();
      AtomicInteger launches = new AtomicInteger();
      // The first task succeeds and keeps its slot until its response is sent. The second loses
      // the sidecar mid-task, so its wipe fails while the first is still holding a slot.
      TaskWorkerHttpHandlerInternal handler = newHandler(cConf, () -> {
        if (launches.incrementAndGet() == 2) {
          stopQuietly(sidecar);
          throw new IllegalStateException("simulated task failure");
        }
      }, stopped::add);

      HttpResponder running = Mockito.mock(HttpResponder.class);
      handler.run(runRequest(), running);
      ArgumentCaptor<BodyProducer> producer = ArgumentCaptor.forClass(BodyProducer.class);
      Mockito.verify(running).sendContent(Mockito.eq(HttpResponseStatus.OK), producer.capture(),
          Mockito.any(HttpHeaders.class));

      handler.run(runRequest(), Mockito.mock(HttpResponder.class));

      Assert.assertTrue("Stopping now would kill the task that is still running",
          stopped.isEmpty());

      producer.getValue().finished();

      Assert.assertEquals("The last running task must stop the pod once it finishes",
          Collections.singletonList(TASK_CLASS), stopped);
    } finally {
      stopQuietly(sidecar);
    }
  }

  @Test
  public void testFailedCredentialProvisioningUnwindsLeaseAndSlot() throws Exception {
    NettyHttpService sidecar = startSidecar();
    try {
      CConfiguration cConf = newCConf(false);
      cConf.setBoolean(
          "feature." + Feature.TASK_WORKER_MANAGER.getFeatureFlagString(), true);
      cConf.setBoolean("security.authorization.enabled", true);
      cConf.setBoolean(
          "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), true);
      // GcpMetadataTaskContextUtil reads ArtifactLocalizer.PORT from cConf on each call, so
      // pointing at an unused port fails admission now and can be switched to the sidecar below.
      cConf.setInt(ArtifactLocalizer.PORT, 1);

      TaskWorkerHttpHandlerInternal handler = newHandler(cConf, () -> { }, className -> { });
      HttpResponder responder = Mockito.mock(HttpResponder.class);

      handler.run(runRequest("ns_failed"), responder);

      Mockito.verify(responder).sendString(Mockito.eq(HttpResponseStatus.INTERNAL_SERVER_ERROR),
          Mockito.anyString(), Mockito.any(HttpHeaders.class));

      // Point cConf at the live sidecar and prove a different namespace can immediately claim all
      // REQUEST_LIMIT slots (neither the slot nor the ns_failed lease remained held).
      cConf.setInt(ArtifactLocalizer.PORT, sidecar.getBindAddress().getPort());
      assertAvailableSlots(handler, "ns_other", REQUEST_LIMIT);
    } finally {
      stopQuietly(sidecar);
    }
  }

  @Test
  public void testLeaseProvisioningClearsSidecarContextBeforeSettingIt() throws Exception {
    RecordingSidecarHandler sidecarHandler = new RecordingSidecarHandler();
    NettyHttpService sidecar = startSidecar(sidecarHandler);
    try {
      AtomicInteger launches = new AtomicInteger();
      TaskWorkerHttpHandlerInternal handler =
          newHandler(leasedSidecarCConf(sidecar), launches::incrementAndGet, className -> { });

      HttpResponder first = Mockito.mock(HttpResponder.class);
      handler.run(runRequest("ns1"), first);
      Mockito.verify(first).sendContent(Mockito.eq(HttpResponseStatus.OK),
          Mockito.any(BodyProducer.class), Mockito.any(HttpHeaders.class));
      Assert.assertEquals("A previous lease may have left the sidecar's context behind",
          Arrays.asList(CLEAR_CONTEXT, SET_CONTEXT), sidecarHandler.calls);

      // A second task without a user credential leaves the existing sidecar context untouched.
      HttpResponder second = Mockito.mock(HttpResponder.class);
      handler.run(runRequest("ns1"), second);
      Mockito.verify(second).sendContent(Mockito.eq(HttpResponseStatus.OK),
          Mockito.any(BodyProducer.class), Mockito.any(HttpHeaders.class));
      Assert.assertEquals(Arrays.asList(CLEAR_CONTEXT, SET_CONTEXT), sidecarHandler.calls);

      // A third task carrying a fresh user credential refreshes /set-context without clearing the
      // sidecar's cached namespace token.
      SecurityRequestContext.setUserCredential(
          new Credential("fresh-user-token", Credential.CredentialType.EXTERNAL_BEARER));
      try {
        HttpResponder third = Mockito.mock(HttpResponder.class);
        handler.run(runRequest("ns1"), third);
        Mockito.verify(third).sendContent(Mockito.eq(HttpResponseStatus.OK),
            Mockito.any(BodyProducer.class), Mockito.any(HttpHeaders.class));
      } finally {
        SecurityRequestContext.setUserCredential(null);
      }
      Assert.assertEquals(Arrays.asList(CLEAR_CONTEXT, SET_CONTEXT, SET_CONTEXT),
          sidecarHandler.calls);
      Assert.assertEquals("fresh-user-token",
          sidecarHandler.contexts.get(1).getUserCredential().getValue());
      Assert.assertEquals(3, launches.get());
    } finally {
      stopQuietly(sidecar);
    }
  }

  @Test
  public void testNon200FromClearContextOnWipeRestartsPod() throws Exception {
    RecordingSidecarHandler sidecarHandler = new RecordingSidecarHandler();
    NettyHttpService sidecar = startSidecar(sidecarHandler);
    try {
      // 1. Leased pod: provision succeeds, then /clear-context answers 503 when the lease ends.
      List<String> leasedStopped = new ArrayList<>();
      TaskWorkerHttpHandlerInternal leased =
          newHandler(leasedSidecarCConf(sidecar), () -> { }, leasedStopped::add);
      HttpResponder first = Mockito.mock(HttpResponder.class);
      leased.run(runRequest("ns1"), first);
      ArgumentCaptor<BodyProducer> producer = ArgumentCaptor.forClass(BodyProducer.class);
      Mockito.verify(first).sendContent(Mockito.eq(HttpResponseStatus.OK), producer.capture(),
          Mockito.any(HttpHeaders.class));

      sidecarHandler.clearContextStatus = HttpResponseStatus.SERVICE_UNAVAILABLE;
      producer.getValue().finished();
      Assert.assertEquals(Collections.singletonList(TASK_CLASS), leasedStopped);

      // 2. Unleased pod: task throws, then /clear-context answers 503 during finally wipe.
      List<String> unleasedStopped = new ArrayList<>();
      TaskWorkerHttpHandlerInternal unleased = newHandler(sidecarCConf(newCConf(true), sidecar),
          () -> {
            throw new IllegalStateException("task failed");
          }, unleasedStopped::add);
      unleased.run(runRequest("ns1"), Mockito.mock(HttpResponder.class));
      Assert.assertEquals(Collections.singletonList(""), unleasedStopped);
    } finally {
      stopQuietly(sidecar);
    }
  }

  @Test
  public void testMissingTaskClassOnUnleasedPodDoesNotRestartAtKillCountOne() {
    CConfiguration cConf = newCConf(true);
    cConf.setInt(TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 1);
    List<String> stopped = new ArrayList<>();
    RunnableTaskLauncher missingClassLauncher =
        Mockito.mock(RunnableTaskLauncher.class, invocation -> {
          throw new ClassNotFoundException(TASK_CLASS);
        });
    TaskWorkerHttpHandlerInternal handler = new TaskWorkerHttpHandlerInternal(
        cConf, missingClassLauncher, stopped::add, new NoOpMetricsCollectionService());

    HttpResponder responder = Mockito.mock(HttpResponder.class);
    handler.run(runRequest(), responder);
    Mockito.verify(responder).sendString(Mockito.eq(HttpResponseStatus.BAD_REQUEST),
        Mockito.anyString(), Mockito.any(HttpHeaders.class));
    Assert.assertTrue(stopped.isEmpty());
  }

  @Test
  public void testNon200FromSetContextFailsProvisioning() throws Exception {
    RecordingSidecarHandler sidecarHandler = new RecordingSidecarHandler();
    sidecarHandler.setContextStatus = HttpResponseStatus.SERVICE_UNAVAILABLE;
    NettyHttpService sidecar = startSidecar(sidecarHandler);
    try {
      assertNon200FailsProvisioning(sidecar, sidecarHandler,
          Arrays.asList(CLEAR_CONTEXT, SET_CONTEXT));
    } finally {
      stopQuietly(sidecar);
    }
  }

  @Test
  public void testNon200FromClearContextFailsProvisioning() throws Exception {
    RecordingSidecarHandler sidecarHandler = new RecordingSidecarHandler();
    sidecarHandler.clearContextStatus = HttpResponseStatus.SERVICE_UNAVAILABLE;
    NettyHttpService sidecar = startSidecar(sidecarHandler);
    try {
      // The context is never set when the stale one could not be cleared.
      assertNon200FailsProvisioning(sidecar, sidecarHandler,
          Collections.singletonList(CLEAR_CONTEXT));
    } finally {
      stopQuietly(sidecar);
    }
  }

  /**
   * Asserts that a task whose lease provisioning gets a non-200 from the sidecar is answered with
   * a 500 without running, and leaves neither its lease nor its slot behind.
   */
  private static void assertNon200FailsProvisioning(NettyHttpService sidecar,
      RecordingSidecarHandler sidecarHandler, List<String> expectedCalls) {
    AtomicInteger launches = new AtomicInteger();
    TaskWorkerHttpHandlerInternal handler =
        newHandler(leasedSidecarCConf(sidecar), launches::incrementAndGet, className -> { });
    HttpResponder responder = Mockito.mock(HttpResponder.class);

    handler.run(runRequest("ns_failed"), responder);

    Mockito.verify(responder).sendString(Mockito.eq(HttpResponseStatus.INTERNAL_SERVER_ERROR),
        Mockito.anyString(), Mockito.any(HttpHeaders.class));
    Assert.assertEquals("The task must not run without its namespace's identity",
        0, launches.get());
    Assert.assertEquals(expectedCalls, sidecarHandler.calls);

    // Once the sidecar answers 200 again, a different namespace can claim every slot.
    sidecarHandler.setContextStatus = HttpResponseStatus.OK;
    sidecarHandler.clearContextStatus = HttpResponseStatus.OK;
    assertAvailableSlots(handler, "ns_other", REQUEST_LIMIT);
  }

  @Test
  public void testThrowingSendStatusOn429ReleasesSlotWithoutSending500() {
    // 1. Direct concurrency-limit 429 where sendStatus throws: release all held slots afterward
    // and verify all REQUEST_LIMIT slots are available (none leaked, none double-decremented).
    TaskWorkerHttpHandlerInternal directHandler = newHandler(() -> { });
    List<BodyProducer> directHeld = new ArrayList<>();
    for (int i = 0; i < REQUEST_LIMIT; i++) {
      HttpResponder responder = Mockito.mock(HttpResponder.class);
      directHandler.run(runRequest("ns1"), responder);
      ArgumentCaptor<BodyProducer> producer = ArgumentCaptor.forClass(BodyProducer.class);
      Mockito.verify(responder).sendContent(
          Mockito.eq(HttpResponseStatus.OK), producer.capture(), Mockito.any(HttpHeaders.class));
      directHeld.add(producer.getValue());
    }
    assertThrowing429PropagatesWithoutSending500(directHandler, "ns1");
    for (BodyProducer producer : directHeld) {
      try {
        producer.finished();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    assertAvailableSlots(directHandler, "ns1", REQUEST_LIMIT);

    // 2. Leased REJECTED_MISMATCH 429 where sendStatus throws must not enter failTask / send 500,
    // and must leave REQUEST_LIMIT - 1 slots open for the lease holder.
    CConfiguration leasedConf = newCConf(true);
    leasedConf.setBoolean("security.authorization.enabled", true);
    leasedConf.setBoolean(
        "feature." + Feature.TASK_WORKER_MANAGER.getFeatureFlagString(), true);
    leasedConf.setInt(TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 0);
    List<String> stopped = new ArrayList<>();
    TaskWorkerHttpHandlerInternal leasedHandler = newHandler(leasedConf, () -> { }, stopped::add);

    leasedHandler.run(runRequest("ns1"), Mockito.mock(HttpResponder.class));
    assertThrowing429PropagatesWithoutSending500(leasedHandler, "ns2");
    Assert.assertTrue(stopped.isEmpty());

    // 3. A leased concurrency-limit 429 whose sendStatus throws must leave the slot count at
    // REQUEST_LIMIT, so the next request still gets a normal 429.
    for (int i = 1; i < REQUEST_LIMIT; i++) {
      HttpResponder responder = Mockito.mock(HttpResponder.class);
      leasedHandler.run(runRequest("ns1"), responder);
      Mockito.verify(responder).sendContent(
          Mockito.eq(HttpResponseStatus.OK), Mockito.any(BodyProducer.class),
          Mockito.any(HttpHeaders.class));
    }
    assertThrowing429PropagatesWithoutSending500(leasedHandler, "ns1");
    HttpResponder stillFull = Mockito.mock(HttpResponder.class);
    leasedHandler.run(runRequest("ns1"), stillFull);
    Mockito.verify(stillFull).sendStatus(
        Mockito.eq(HttpResponseStatus.TOO_MANY_REQUESTS), Mockito.any(HttpHeaders.class));
  }

  private static void assertThrowing429PropagatesWithoutSending500(
      TaskWorkerHttpHandlerInternal handler, String namespace) {
    HttpResponder throwingResponder = Mockito.mock(HttpResponder.class);
    Mockito.doThrow(new IllegalStateException("simulated closed channel"))
        .when(throwingResponder).sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
    Mockito.doThrow(new IllegalStateException("simulated closed channel"))
        .when(throwingResponder).sendStatus(
            Mockito.eq(HttpResponseStatus.TOO_MANY_REQUESTS), Mockito.any(HttpHeaders.class));
    try {
      handler.run(runRequest(namespace), throwingResponder);
      Assert.fail("Expected sendStatus failure to propagate");
    } catch (IllegalStateException expected) {
      Assert.assertEquals("simulated closed channel", expected.getMessage());
    }
    Mockito.verify(throwingResponder, Mockito.never()).sendString(
        Mockito.any(HttpResponseStatus.class), Mockito.anyString(), Mockito.any(HttpHeaders.class));
  }

  @Test
  public void testStopIfRestartPendingAndIdleStopsAtMostOnce() throws Exception {
    NettyHttpService sidecar = startSidecar();
    try {
      CConfiguration cConf = sidecarCConf(newCConf(false), sidecar);
      List<String> stopped = new ArrayList<>();
      AtomicInteger launches = new AtomicInteger();
      TaskWorkerHttpHandlerInternal[] handlerRef = new TaskWorkerHttpHandlerInternal[1];
      // Two overlapping tasks are in flight when the sidecar dies, so the outer task reaches
      // stopIfRestartPendingAndIdle twice with no request running.
      handlerRef[0] = newHandler(cConf, () -> {
        if (launches.incrementAndGet() == 1) {
          handlerRef[0].run(runRequest(), Mockito.mock(HttpResponder.class));
        } else {
          stopQuietly(sidecar);
        }
        throw new IllegalStateException("simulated task failure");
      }, stopped::add);

      handlerRef[0].run(runRequest(), Mockito.mock(HttpResponder.class));

      Assert.assertEquals(Collections.singletonList(TASK_CLASS), stopped);
    } finally {
      stopQuietly(sidecar);
    }
  }

  @Test
  public void testFailedWarmCredentialRefreshPreservesActiveLease() throws Exception {
    RecordingSidecarHandler sidecarHandler = new RecordingSidecarHandler();
    NettyHttpService sidecar = startSidecar(sidecarHandler);
    try {
      CConfiguration cConf = leasedSidecarCConf(sidecar);
      AtomicInteger launches = new AtomicInteger();
      TaskWorkerHttpHandlerInternal handler = newHandler(cConf, launches::incrementAndGet,
          className -> { });

      // Admit task 1 for ns1 and keep its BodyProducer in flight so the lease stays active.
      HttpResponder firstResponder = Mockito.mock(HttpResponder.class);
      handler.run(runRequest("ns1"), firstResponder);
      ArgumentCaptor<BodyProducer> producerCaptor = ArgumentCaptor.forClass(BodyProducer.class);
      Mockito.verify(firstResponder).sendContent(Mockito.eq(HttpResponseStatus.OK),
          producerCaptor.capture(), Mockito.any(HttpHeaders.class));
      int clearCountAfterProvision = Collections.frequency(sidecarHandler.calls, CLEAR_CONTEXT);

      // Fail /set-context during warm refresh for task 2 on the same namespace.
      sidecarHandler.setContextStatus = HttpResponseStatus.SERVICE_UNAVAILABLE;
      SecurityRequestContext.setUserCredential(
          new Credential("refreshed-token", Credential.CredentialType.EXTERNAL_BEARER));
      try {
        HttpResponder secondResponder = Mockito.mock(HttpResponder.class);
        handler.run(runRequest("ns1"), secondResponder);
        Mockito.verify(secondResponder).sendString(
            Mockito.eq(HttpResponseStatus.INTERNAL_SERVER_ERROR), Mockito.anyString(),
            Mockito.any(HttpHeaders.class));
      } finally {
        SecurityRequestContext.reset();
      }

      Assert.assertEquals("Failed warm refresh must not launch task 2", 1, launches.get());
      Assert.assertEquals("Failed warm refresh must not wipe the active lease",
          clearCountAfterProvision, Collections.frequency(sidecarHandler.calls, CLEAR_CONTEXT));

      // Active lease for ns1 must remain intact against a competing namespace.
      HttpResponder competingResponder = Mockito.mock(HttpResponder.class);
      handler.run(runRequest("ns2"), competingResponder);
      ArgumentCaptor<HttpHeaders> rejectHeaders = ArgumentCaptor.forClass(HttpHeaders.class);
      Mockito.verify(competingResponder).sendStatus(
          Mockito.eq(HttpResponseStatus.TOO_MANY_REQUESTS), rejectHeaders.capture());
      Assert.assertEquals("ns1",
          rejectHeaders.getValue().get(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE));

      // Finishing task 1 releases the lease and all slots cleanly.
      producerCaptor.getValue().finished();
      sidecarHandler.setContextStatus = HttpResponseStatus.OK;
      assertAvailableSlots(handler, "ns_other", REQUEST_LIMIT);
    } finally {
      stopQuietly(sidecar);
    }
  }

  @Test
  public void testFailedUnleasedCredentialSetupSkipsTaskAndReleasesSlotOnce() throws Exception {
    RecordingSidecarHandler sidecarHandler = new RecordingSidecarHandler();
    sidecarHandler.setContextStatus = HttpResponseStatus.SERVICE_UNAVAILABLE;
    NettyHttpService sidecar = startSidecar(sidecarHandler);
    try {
      CConfiguration cConf = sidecarCConf(newCConf(false), sidecar);
      AtomicInteger launches = new AtomicInteger();
      TaskWorkerHttpHandlerInternal handler = newHandler(cConf, launches::incrementAndGet,
          className -> { });

      HttpResponder responder = Mockito.mock(HttpResponder.class);
      handler.run(runRequest("ns1"), responder);

      Mockito.verify(responder).sendString(
          Mockito.eq(HttpResponseStatus.INTERNAL_SERVER_ERROR), Mockito.anyString(),
          Mockito.any(HttpHeaders.class));
      Assert.assertEquals("Task must not launch when /set-context fails", 0, launches.get());
      Assert.assertEquals(Arrays.asList(SET_CONTEXT, CLEAR_CONTEXT), sidecarHandler.calls);

      sidecarHandler.setContextStatus = HttpResponseStatus.OK;
      assertAvailableSlots(handler, "ns1", REQUEST_LIMIT);
    } finally {
      stopQuietly(sidecar);
    }
  }

  static NettyHttpService startSidecar() throws Exception {
    return startSidecar(new StubSidecarHandler());
  }

  private static NettyHttpService startSidecar(AbstractHttpHandler sidecarHandler)
      throws Exception {
    NettyHttpService sidecar = NettyHttpService.builder("metadata-sidecar")
        .setHost(InetAddress.getLoopbackAddress().getHostName())
        .setHttpHandlers(sidecarHandler)
        .build();
    sidecar.start();
    return sidecar;
  }

  static CConfiguration sidecarCConf(CConfiguration cConf, NettyHttpService sidecar) {
    cConf.setBoolean("feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), true);
    cConf.setInt(ArtifactLocalizer.PORT, sidecar.getBindAddress().getPort());
    return cConf;
  }

  /** A worker that leases its pod to one namespace and provisions its credential in the sidecar. */
  private static CConfiguration leasedSidecarCConf(NettyHttpService sidecar) {
    CConfiguration cConf = sidecarCConf(newCConf(false), sidecar);
    cConf.setBoolean("feature." + Feature.TASK_WORKER_MANAGER.getFeatureFlagString(), true);
    cConf.setBoolean("security.authorization.enabled", true);
    return cConf;
  }

  static void stopQuietly(NettyHttpService service) {
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

    @PUT
    @Path("/set-context")
    public void setContext(FullHttpRequest request, HttpResponder responder) {
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @DELETE
    @Path("/clear-context")
    public void clearContext(FullHttpRequest request, HttpResponder responder) {
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  /** Records the metadata sidecar's set and clear context calls in order. */
  @Path("/")
  public static final class RecordingSidecarHandler extends AbstractHttpHandler {

    private final List<String> calls = Collections.synchronizedList(new ArrayList<>());
    private volatile HttpResponseStatus setContextStatus = HttpResponseStatus.OK;
    private volatile HttpResponseStatus clearContextStatus = HttpResponseStatus.OK;

    private final List<GcpMetadataTaskContext> contexts =
        Collections.synchronizedList(new ArrayList<>());

    @PUT
    @Path("/set-context")
    public void setContext(FullHttpRequest request, HttpResponder responder) {
      calls.add(SET_CONTEXT);
      contexts.add(GSON.fromJson(request.content().toString(StandardCharsets.UTF_8),
          GcpMetadataTaskContext.class));
      responder.sendStatus(setContextStatus);
    }

    @DELETE
    @Path("/clear-context")
    public void clearContext(FullHttpRequest request, HttpResponder responder) {
      calls.add(CLEAR_CONTEXT);
      responder.sendStatus(clearContextStatus);
    }
  }
}
