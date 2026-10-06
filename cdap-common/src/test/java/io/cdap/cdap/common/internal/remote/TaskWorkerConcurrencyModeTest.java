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
import java.util.Collections;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import org.apache.twill.discovery.InMemoryDiscoveryService;
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

  /** A launcher that fails in a chosen way instead of running anything. */
  private static final class ThrowingLauncher extends RunnableTaskLauncher {

    private final Throwable failure;

    private ThrowingLauncher(Throwable failure) {
      super(Guice.createInjector());
      this.failure = failure;
    }

    @Override
    public void launchRunnableTask(RunnableTaskContext context) throws Exception {
      if (failure instanceof Error) {
        throw (Error) failure;
      }
      throw (Exception) failure;
    }
  }

  private static CConfiguration newCConf(boolean authorizationEnabled,
      boolean userCodeIsolationEnabled, boolean proxyEnabled) {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, authorizationEnabled);
    cConf.setBoolean(TaskWorker.USER_CODE_ISOLATION_ENABLED, userCodeIsolationEnabled);
    cConf.setInt(TaskWorker.REQUEST_LIMIT, CONFIGURED_LIMIT);
    cConf.setBoolean("feature.rbac.task.worker.manager.enabled", proxyEnabled);
    // Keep the periodic restart thread, and the System.exit it ends in, out of the test.
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_DURATION_SECOND, 0);
    return cConf;
  }

  private static TaskWorkerHttpHandlerInternal newHandler(boolean authorizationEnabled,
      boolean userCodeIsolationEnabled, boolean proxyEnabled) {
    return newHandler(newCConf(authorizationEnabled, userCodeIsolationEnabled, proxyEnabled));
  }

  private static TaskWorkerHttpHandlerInternal newHandler(CConfiguration cConf) {
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    return new TaskWorkerHttpHandlerInternal(cConf, discoveryService, discoveryService,
        className -> { }, new NoOpMetricsCollectionService());
  }

  /** Builds a leased (RBAC plus proxy) handler whose tasks always fail in the given way. */
  private static TaskWorkerHttpHandlerInternal newFailingHandler(boolean proxyEnabled,
      Throwable failure) {
    return new TaskWorkerHttpHandlerInternal(newCConf(proxyEnabled, proxyEnabled, proxyEnabled),
        new ThrowingLauncher(failure), className -> { }, new NoOpMetricsCollectionService());
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

  @Test
  public void testNonRbacUsesConfiguredLimit() {
    // No per-task credential exists, so there is nothing for concurrent tasks to corrupt.
    TaskWorkerHttpHandlerInternal handler = newHandler(false, false, false);

    Assert.assertEquals(CONFIGURED_LIMIT, handler.getConcurrentRequestLimit());
    Assert.assertNull("Leasing a non-RBAC pod to a namespace would cost throughput and buy nothing",
        handler.getStickyLeaseManager());
  }

  @Test
  public void testRbacWithoutProxyIsClampedToOneTask() {
    TaskWorkerHttpHandlerInternal handler = newHandler(true, true, false);

    Assert.assertEquals("Without a lease, the single mutable credential context bounds the pod "
        + "to one task", 1, handler.getConcurrentRequestLimit());
    Assert.assertNull(handler.getStickyLeaseManager());
  }

  @Test
  public void testRbacWithProxyUsesConfiguredLimit() {
    TaskWorkerHttpHandlerInternal handler = newHandler(true, true, true);

    Assert.assertEquals(CONFIGURED_LIMIT, handler.getConcurrentRequestLimit());
    Assert.assertNotNull("The lease is what makes the raised limit safe",
        handler.getStickyLeaseManager());
  }

  @Test
  public void testSingleWorkerKeepsLeaseAndConfiguredLimit() {
    // Clients call a single worker directly, so its own lease keeps namespaces apart.
    CConfiguration cConf = newCConf(true, true, true);
    cConf.setInt(TaskWorker.CONTAINER_COUNT, 1);
    TaskWorkerHttpHandlerInternal handler = newHandler(cConf);

    Assert.assertEquals(CONFIGURED_LIMIT, handler.getConcurrentRequestLimit());
    Assert.assertNotNull(handler.getStickyLeaseManager());
  }

  @Test
  public void testProxyFlagWithoutRbacFallsBackToNonRbacBehaviour() {
    // Flag without RBAC must behave exactly like non-RBAC, matching RemoteTaskExecutor's routing.
    TaskWorkerHttpHandlerInternal handler = newHandler(false, false, true);

    Assert.assertEquals(CONFIGURED_LIMIT, handler.getConcurrentRequestLimit());
    Assert.assertNull(handler.getStickyLeaseManager());
  }

  @Test
  public void testIsolationDisabledOnRbacStillHonoursTheConfiguredLimit() {
    // Documents existing behaviour; CDF never turns isolation off on RBAC instances.
    TaskWorkerHttpHandlerInternal handler = newHandler(true, false, false);

    Assert.assertEquals(CONFIGURED_LIMIT, handler.getConcurrentRequestLimit());
    Assert.assertNull(handler.getStickyLeaseManager());
  }

  @Test
  public void testErrorFromUserCodeReleasesSlotAndLease() {
    // An Error is not an Exception, so it walks past every catch clause written with task failures
    // in mind. A NoClassDefFoundError out of a half-built user artifact is not exotic.
    TaskWorkerHttpHandlerInternal handler = newFailingHandler(true,
        new NoClassDefFoundError("simulated linkage failure in a user artifact"));
    StickyLeaseManager leaseManager = handler.getStickyLeaseManager();
    Assert.assertNotNull(leaseManager);

    try {
      handler.run(runRequest(), Mockito.mock(HttpResponder.class));
      Assert.fail("An Error must not be swallowed: returning a tidy 500 is not worth pretending "
          + "an OutOfMemoryError did not happen");
    } catch (NoClassDefFoundError expected) {
      // The handler releases what it holds and rethrows.
    }

    Assert.assertEquals("The slot is what every admission check reads. Leaking it wedges the pod "
            + "at its limit until the periodic restart timer fires, which is two hours away by "
            + "default", 0, handler.getRunningRequestCount());
    Assert.assertEquals("The credential wipe only fires when the count reaches zero, so a leaked "
            + "lease strands a namespaced credential on an idle pod",
        0, leaseManager.getActiveTaskCount());
  }

  @Test
  public void testErrorFromUserCodeReleasesSlotWithoutALease() {
    // The same failure on a pod that runs without the proxy. No lease to release here, but the
    // slot still has to come back or the pod stops accepting work.
    TaskWorkerHttpHandlerInternal handler = newFailingHandler(false,
        new NoClassDefFoundError("simulated linkage failure in a user artifact"));
    Assert.assertNull(handler.getStickyLeaseManager());

    try {
      handler.run(runRequest(), Mockito.mock(HttpResponder.class));
      Assert.fail("Expected the Error to propagate");
    } catch (NoClassDefFoundError expected) {
      // Expected.
    }

    Assert.assertEquals(0, handler.getRunningRequestCount());
  }

  @Test
  public void testExceptionFromUserCodeReleasesSlotAndLease() {
    TaskWorkerHttpHandlerInternal handler = newFailingHandler(true,
        new IllegalStateException("simulated task failure"));
    StickyLeaseManager leaseManager = handler.getStickyLeaseManager();
    Assert.assertNotNull(leaseManager);

    handler.run(runRequest(), Mockito.mock(HttpResponder.class));

    Assert.assertEquals(0, handler.getRunningRequestCount());
    Assert.assertEquals("A task that threw still ran under the lease and still has to give it "
        + "back", 0, leaseManager.getActiveTaskCount());
  }

  @Test
  public void testMissingTaskClassReleasesSlotAndLease() {
    TaskWorkerHttpHandlerInternal handler = newFailingHandler(true,
        new ClassNotFoundException("com.example.SomeTask"));
    StickyLeaseManager leaseManager = handler.getStickyLeaseManager();
    Assert.assertNotNull(leaseManager);

    handler.run(runRequest(), Mockito.mock(HttpResponder.class));

    Assert.assertEquals(0, handler.getRunningRequestCount());
    Assert.assertEquals(0, leaseManager.getActiveTaskCount());
  }

  @Test
  public void testUnparseableRequestReleasesTheSlotAndTakesNoLease() {
    TaskWorkerHttpHandlerInternal handler = newFailingHandler(true,
        new IllegalStateException("never reached"));
    StickyLeaseManager leaseManager = handler.getStickyLeaseManager();
    Assert.assertNotNull(leaseManager);

    handler.run(requestWithBody("this is not json"), Mockito.mock(HttpResponder.class));

    Assert.assertEquals(0, handler.getRunningRequestCount());
    Assert.assertEquals(0, leaseManager.getActiveTaskCount());
    Assert.assertNull("A request that never parsed must not leave the pod advertising a lease, or "
            + "the proxy will route matching traffic to a pod that never served that namespace",
        leaseManager.getCurrentLease());
  }

  @Test
  public void testFailuresDoNotAccumulateAcrossRequests() {
    // The failure that matters is not one bad task, it is the pod quietly losing a slot per bad
    // task until it has none left.
    TaskWorkerHttpHandlerInternal handler = newFailingHandler(true,
        new NoClassDefFoundError("simulated linkage failure in a user artifact"));
    StickyLeaseManager leaseManager = handler.getStickyLeaseManager();
    Assert.assertNotNull(leaseManager);

    for (int i = 0; i < CONFIGURED_LIMIT; i++) {
      try {
        handler.run(runRequest(), Mockito.mock(HttpResponder.class));
        Assert.fail("Expected the Error to propagate");
      } catch (NoClassDefFoundError expected) {
        // Expected.
      }
    }

    Assert.assertEquals("As many failures as the pod has slots: if each one leaked, the pod would "
        + "now reject every request it ever receives", 0, handler.getRunningRequestCount());
    Assert.assertEquals(0, leaseManager.getActiveTaskCount());
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
    // The proxy adopts this header to fix its routing table.
    Assert.assertEquals(NAMESPACE,
        headers.getValue().get(Constants.Gateway.HEADER_LEASED_NAMESPACE));
    Assert.assertEquals("Only the admitted task holds a slot", 1, handler.getRunningRequestCount());
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
  public void testFailedWipeAfterFailedTaskRestartsDirectWorker() throws Exception {
    // Without a lease the wipe runs after a failed task's completion, which has already checked
    // the restart flag, so the wipe failure has to stop the pod itself.
    NettyHttpService sidecar = NettyHttpService.builder("metadata-sidecar")
        .setHost(InetAddress.getLoopbackAddress().getHostName())
        .setHttpHandlers(new StubSidecarHandler())
        .build();
    sidecar.start();
    try {
      CConfiguration cConf = newCConf(true, true, false);
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
      Assert.assertNull(handler.getStickyLeaseManager());
      handler.run(runRequest(NAMESPACE), Mockito.mock(HttpResponder.class));

      Assert.assertEquals("Otherwise the pod refuses every task while holding the credential "
          + "until the periodic restart", Collections.singletonList(""), stopped);
      Assert.assertEquals(0, handler.getRunningRequestCount());
    } finally {
      stopQuietly(sidecar);
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
}
