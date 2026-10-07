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
import java.util.function.Consumer;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests that every way a task can fail gives its slot back to the task worker exactly once.
 */
public class TaskWorkerFailureHandlingTest {

  private static final int REQUEST_LIMIT = 3;
  private static final String TASK_CLASS = "com.example.SomeTask";
  private static final Gson GSON = new Gson();

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
    return requestWithBody(GSON.toJson(RunnableTaskRequest.getBuilder(TASK_CLASS)
        .withNamespace("ns1")
        .build()));
  }

  private static FullHttpRequest requestWithBody(String body) {
    return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
        "/v3Internal/worker/run", Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
  }

  private static void throwError() {
    throw new NoClassDefFoundError("simulated linkage failure in a user artifact");
  }

  @Test
  public void testErrorFromTaskReleasesTheSlot() {
    TaskWorkerHttpHandlerInternal handler =
        newHandler(TaskWorkerFailureHandlingTest::throwError);

    try {
      handler.run(runRequest(), Mockito.mock(HttpResponder.class));
      Assert.fail("Expected the Error to propagate");
    } catch (NoClassDefFoundError expected) {
      // The handler releases the slot and rethrows.
    }

    Assert.assertEquals(0, handler.getRunningRequestCount());
  }

  @Test
  public void testRepeatedErrorsDoNotWedgeThePod() {
    TaskWorkerHttpHandlerInternal handler =
        newHandler(TaskWorkerFailureHandlingTest::throwError);

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
    Assert.assertEquals(0, handler.getRunningRequestCount());
  }

  @Test
  public void testUnparseableRequestReleasesTheSlot() {
    TaskWorkerHttpHandlerInternal handler = newHandler(() -> { });

    handler.run(requestWithBody("this is not json"), Mockito.mock(HttpResponder.class));

    Assert.assertEquals(0, handler.getRunningRequestCount());
  }

  @Test
  public void testFailedErrorResponseReleasesTheSlotOnce() {
    TaskWorkerHttpHandlerInternal handler = newHandler(() -> {
      throw new IllegalStateException("simulated task failure");
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

    Assert.assertEquals(0, handler.getRunningRequestCount());
  }

  @Test
  public void testFailedCredentialWipeReleasesTheSlotOnce() throws Exception {
    NettyHttpService sidecar = startSidecar();
    try {
      CConfiguration cConf = sidecarCConf(newCConf(false), sidecar);
      // Provisioning succeeds, then the sidecar goes away so the wipe after the task fails.
      TaskWorkerHttpHandlerInternal handler = newHandler(cConf, () -> {
        stopQuietly(sidecar);
        throw new IllegalStateException("simulated task failure");
      }, className -> { });

      HttpResponder responder = Mockito.mock(HttpResponder.class);
      handler.run(runRequest(), responder);

      Assert.assertEquals("A failed wipe must not release the failed task's slot a second time",
          0, handler.getRunningRequestCount());
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

  static NettyHttpService startSidecar() throws Exception {
    NettyHttpService sidecar = NettyHttpService.builder("metadata-sidecar")
        .setHost(InetAddress.getLoopbackAddress().getHostName())
        .setHttpHandlers(new StubSidecarHandler())
        .build();
    sidecar.start();
    return sidecar;
  }

  static CConfiguration sidecarCConf(CConfiguration cConf, NettyHttpService sidecar) {
    cConf.setBoolean("feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), true);
    cConf.setInt(ArtifactLocalizer.PORT, sidecar.getBindAddress().getPort());
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
}
