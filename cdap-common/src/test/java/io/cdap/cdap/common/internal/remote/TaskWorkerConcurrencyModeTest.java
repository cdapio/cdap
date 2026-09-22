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
import io.cdap.cdap.common.conf.Constants.TaskWorker;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.http.HttpResponder;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests how much work a task worker pod accepts: the limit it resolves across the three deployment
 * modes, and whether it gives that capacity back when a task fails.
 *
 * <p>The mode resolution is the security-critical branch. On an RBAC instance the pod holds a
 * single mutable namespaced credential, so running more than one task at a time is only safe when
 * something coordinates ownership of it. Getting this wrong in the permissive direction means user
 * code in one namespace can obtain a token for another.
 *
 * <p>The failure paths are the other half of the same property. A slot or a lease that is taken
 * and never released does not fail the request that hit it; it removes capacity from the pod for
 * the rest of the pod's life, and on an RBAC instance it also pins the namespaced credential in
 * the sidecar, because the wipe only fires when the active task count reaches zero.
 */
public class TaskWorkerConcurrencyModeTest {

  private static final int CONFIGURED_LIMIT = 7;
  private static final String NAMESPACE = "ns1";
  private static final Gson GSON = new Gson();

  /**
   * A launcher that fails in a chosen way instead of running anything. Driving the real launcher
   * would mean shipping a class that fails in the exact way under test and trusting the
   * container's classloading to cooperate.
   */
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
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    return new TaskWorkerHttpHandlerInternal(
        newCConf(authorizationEnabled, userCodeIsolationEnabled, proxyEnabled),
        discoveryService, discoveryService, className -> { },
        new NoOpMetricsCollectionService());
  }

  /**
   * Builds a handler whose tasks always fail in the given way. RBAC and the proxy move together
   * here because that is the only combination CDF deploys with a lease.
   */
  private static TaskWorkerHttpHandlerInternal newFailingHandler(boolean proxyEnabled,
      Throwable failure) {
    return new TaskWorkerHttpHandlerInternal(newCConf(proxyEnabled, proxyEnabled, proxyEnabled),
        new ThrowingLauncher(failure), className -> { }, new NoOpMetricsCollectionService());
  }

  private static FullHttpRequest runRequest() {
    return requestWithBody(GSON.toJson(RunnableTaskRequest.getBuilder("com.example.SomeTask")
        .withNamespace(NAMESPACE)
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
  public void testProxyFlagWithoutRbacFallsBackToNonRbacBehaviour() {
    // The proxy is only deployed on RBAC instances. If the flag is somehow on without RBAC, the
    // worker must land on exactly the non-RBAC configuration, matching what RemoteClientFactory
    // decides on the client side. Any other outcome means the two ends of the wire disagree about
    // how many tasks a pod accepts.
    TaskWorkerHttpHandlerInternal handler = newHandler(false, false, true);

    Assert.assertEquals(CONFIGURED_LIMIT, handler.getConcurrentRequestLimit());
    Assert.assertNull(handler.getStickyLeaseManager());
  }

  @Test
  public void testIsolationDisabledOnRbacStillHonoursTheConfiguredLimit() {
    // Documents today's behaviour rather than endorsing it: an RBAC instance that explicitly turns
    // isolation off and runs without the proxy gets concurrent tasks sharing one credential
    // context. CDF never produces this combination, since it only sets the isolation key on
    // non-RBAC instances.
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
}
