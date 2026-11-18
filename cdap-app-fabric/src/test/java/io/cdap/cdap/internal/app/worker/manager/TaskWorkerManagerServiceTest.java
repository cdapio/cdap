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

package io.cdap.cdap.internal.app.worker.manager;

import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.HttpsURLConnection;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@link TaskWorkerManagerService}: draining on stop and TLS configuration.
 */
public class TaskWorkerManagerServiceTest {

  private static final String RUN_PATH = "/v3Internal/worker/run";
  private static final String TASK_RESULT = "task-result";

  private CConfiguration cConf;
  private InMemoryDiscoveryService discoveryService;
  private BlockingWorkerHandler workerHandler;
  private NettyHttpService worker;
  private Cancellable cancelWorkerDiscovery;
  private ExecutorService callers;

  @Before
  public void setUp() throws Exception {
    cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorkerManager.ADDRESS, "127.0.0.1");
    cConf.setInt(Constants.TaskWorkerManager.PORT, 0);
    discoveryService = new InMemoryDiscoveryService();
    callers = Executors.newCachedThreadPool();

    workerHandler = new BlockingWorkerHandler();
    worker = NettyHttpService.builder("fake-task-worker")
        .setHost("127.0.0.1")
        .setPort(0)
        .setExecThreadPoolSize(2)
        .setHttpHandlers(workerHandler)
        .build();
    worker.start();
    cancelWorkerDiscovery = discoveryService.register(
        new Discoverable(Constants.Service.TASK_WORKER, worker.getBindAddress()));
  }

  @After
  public void tearDown() throws Exception {
    // Let any task still blocked in the fake worker return, so the worker can stop.
    workerHandler.release.countDown();
    cancelWorkerDiscovery.cancel();
    worker.stop();
    callers.shutdownNow();
  }

  @Test
  public void testStopWaitsForInFlightRequests() throws Exception {
    cConf.setLong(Constants.TaskWorkerManager.DRAIN_TIMEOUT_SECS, 30);
    TaskWorkerManagerService service = startService();
    Future<String> caller = callers.submit(() -> runTask());
    Assert.assertTrue("The task never reached the worker",
        workerHandler.received.await(10, TimeUnit.SECONDS));

    ListenableFuture<Service.State> stopped = service.stop();

    // Deregistered and draining, but still running while the request is in flight.
    waitUntilDeregistered();
    TimeUnit.MILLISECONDS.sleep(300);
    Assert.assertFalse(stopped.isDone());
    Assert.assertFalse(caller.isDone());

    // The worker answers: the response still reaches the caller, and only then does the manager stop.
    workerHandler.release.countDown();
    Assert.assertEquals(TASK_RESULT, caller.get(10, TimeUnit.SECONDS));
    Assert.assertEquals(Service.State.TERMINATED, stopped.get(10, TimeUnit.SECONDS));
  }

  @Test
  public void testRequestOnAnOpenConnectionIsRefusedWhileDraining() throws Exception {
    cConf.setLong(Constants.TaskWorkerManager.DRAIN_TIMEOUT_SECS, 30);
    TaskWorkerManagerService service = startService();
    // A task in flight keeps the manager draining while the late request arrives.
    Future<String> caller = callers.submit(() -> runTask());
    Assert.assertTrue("The task never reached the worker",
        workerHandler.received.await(10, TimeUnit.SECONDS));

    // Connected before the stop closes the listening socket, so the connection stays open. The
    // pause lets the manager accept it first.
    InetSocketAddress manager = managerDiscoverable().getSocketAddress();
    try (Socket lateClient = new Socket(manager.getHostString(), manager.getPort())) {
      lateClient.setSoTimeout((int) TimeUnit.SECONDS.toMillis(10));
      TimeUnit.MILLISECONDS.sleep(500);

      ListenableFuture<Service.State> stopped = service.stop();
      waitUntilDeregistered();

      OutputStream out = lateClient.getOutputStream();
      out.write(("POST " + RUN_PATH + " HTTP/1.1\r\n"
          + "Host: " + manager.getHostString() + "\r\n"
          + Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE + ": ns1\r\n"
          + "Content-Length: 2\r\n"
          + "\r\n"
          + "{}").getBytes(StandardCharsets.UTF_8));
      out.flush();
      BufferedReader in = new BufferedReader(
          new InputStreamReader(lateClient.getInputStream(), StandardCharsets.UTF_8));
      String statusLine = in.readLine();

      // Answered by the manager, so App Fabric retries it elsewhere and no worker ever saw it.
      Assert.assertNotNull("The manager closed the connection without answering", statusLine);
      Assert.assertTrue("Unexpected status line: " + statusLine, statusLine.contains(" 503 "));
      Assert.assertEquals(1, workerHandler.requests.get());
      Assert.assertFalse(stopped.isDone());

      workerHandler.release.countDown();
      Assert.assertEquals(TASK_RESULT, caller.get(10, TimeUnit.SECONDS));
      Assert.assertEquals(Service.State.TERMINATED, stopped.get(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testStopGivesUpAtTheDrainTimeout() throws Exception {
    cConf.setLong(Constants.TaskWorkerManager.DRAIN_TIMEOUT_SECS, 1);
    TaskWorkerManagerService service = startService();
    Future<String> caller = callers.submit(() -> runTask());
    Assert.assertTrue("The task never reached the worker",
        workerHandler.received.await(10, TimeUnit.SECONDS));

    long start = System.currentTimeMillis();
    Assert.assertEquals(Service.State.TERMINATED, service.stop().get(10, TimeUnit.SECONDS));

    Assert.assertTrue(System.currentTimeMillis() - start >= TimeUnit.SECONDS.toMillis(1));
    // The manager closed the request it gave up on, so the caller sees a failure, not a result.
    try {
      caller.get(10, TimeUnit.SECONDS);
      Assert.fail("Expected the caller's request to fail once the manager stopped");
    } catch (ExecutionException e) {
      Assert.assertTrue("Unexpected failure: " + e.getCause(), e.getCause() instanceof IOException);
    }
  }

  @Test
  public void testStopIsImmediateWhenNothingIsInFlight() throws Exception {
    cConf.setLong(Constants.TaskWorkerManager.DRAIN_TIMEOUT_SECS, 30);
    TaskWorkerManagerService service = startService();

    long start = System.currentTimeMillis();
    Assert.assertEquals(Service.State.TERMINATED, service.stop().get(10, TimeUnit.SECONDS));

    Assert.assertTrue(System.currentTimeMillis() - start < TimeUnit.SECONDS.toMillis(10));
  }

  @Test
  public void testStopIsImmediateAfterRequestsComplete() throws Exception {
    cConf.setLong(Constants.TaskWorkerManager.DRAIN_TIMEOUT_SECS, 30);
    TaskWorkerManagerService service = startService();
    workerHandler.release.countDown();
    Assert.assertEquals(TASK_RESULT, runTask());

    long start = System.currentTimeMillis();
    Assert.assertEquals(Service.State.TERMINATED, service.stop().get(10, TimeUnit.SECONDS));

    // A finished request no longer counts as in flight, so the stop doesn't wait for the timeout.
    Assert.assertTrue(System.currentTimeMillis() - start < TimeUnit.SECONDS.toMillis(10));
  }

  @Test
  public void testInternalSslEnabledServesHttpsAndRegistersHttpsScheme() throws Exception {
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, true);
    TaskWorkerManagerService service = startService();
    try {
      Discoverable discoverable = managerDiscoverable();
      Assert.assertEquals(URIScheme.HTTPS, URIScheme.getScheme(discoverable));

      URL pingUrl =
          URIScheme.createURI(discoverable, "%s", Constants.TaskWorkerManager.PING_PATH).toURL();
      HttpsURLConnection conn = (HttpsURLConnection) pingUrl.openConnection();
      new HttpsEnabler().setTrustAll(true).enable(conn);
      try {
        Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      } finally {
        conn.disconnect();
      }
    } finally {
      service.stopAndWait();
    }
  }

  @Test
  public void testInjectorProvidesOneInstance() {
    // Without a module binding it, the class's own scope must keep one lease table per process.
    Injector injector = Guice.createInjector(binder -> {
      binder.bind(CConfiguration.class).toInstance(cConf);
      binder.bind(SConfiguration.class).toInstance(SConfiguration.create());
      binder.bind(DiscoveryService.class).toInstance(discoveryService);
      binder.bind(DiscoveryServiceClient.class)
          .annotatedWith(Names.named(TaskWorkerManagerService.WORKER_DISCOVERY))
          .toInstance(discoveryService);
    });

    Assert.assertSame(injector.getInstance(TaskWorkerManagerService.class),
        injector.getInstance(TaskWorkerManagerService.class));
  }

  @Test(expected = ConfigurationException.class)
  public void testInjectorDoesNotFallBackToUnnamedDiscoveryClient() {
    // The unnamed client resolves one ClusterIP for the whole pool, so the manager must never get it.
    Injector injector = Guice.createInjector(binder -> {
      binder.bind(CConfiguration.class).toInstance(cConf);
      binder.bind(SConfiguration.class).toInstance(SConfiguration.create());
      binder.bind(DiscoveryService.class).toInstance(discoveryService);
      binder.bind(DiscoveryServiceClient.class).toInstance(discoveryService);
    });

    injector.getInstance(TaskWorkerManagerService.class);
  }

  private TaskWorkerManagerService startService() {
    TaskWorkerManagerService service = new TaskWorkerManagerService(
        cConf, SConfiguration.create(), discoveryService, discoveryService);
    service.startAndWait();
    Assert.assertTrue(discoveryService.discover(Constants.Service.TASK_WORKER_MANAGER)
        .iterator().hasNext());
    return service;
  }

  private Discoverable managerDiscoverable() {
    return discoveryService.discover(Constants.Service.TASK_WORKER_MANAGER).iterator().next();
  }

  /**
   * Sends a task request through the manager, the way App Fabric does, and returns the response body.
   *
   * @throws IOException if the request fails or the manager answers anything other than 200
   */
  private String runTask() throws IOException {
    InetSocketAddress manager = managerDiscoverable().getSocketAddress();
    URL url = new URL("http", manager.getHostString(), manager.getPort(), RUN_PATH);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setRequestProperty(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, "ns1");
      conn.getOutputStream().write("{}".getBytes(StandardCharsets.UTF_8));
      int status = conn.getResponseCode();
      if (status != HttpURLConnection.HTTP_OK) {
        throw new IOException("Unexpected status " + status + " from the manager");
      }
      try (InputStream body = conn.getInputStream()) {
        return new String(ByteStreams.toByteArray(body), StandardCharsets.UTF_8);
      }
    } finally {
      conn.disconnect();
    }
  }

  private void waitUntilDeregistered() throws InterruptedException {
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
    while (discoveryService.discover(Constants.Service.TASK_WORKER_MANAGER).iterator().hasNext()) {
      Assert.assertTrue("Still registered after 10s", System.currentTimeMillis() < deadline);
      TimeUnit.MILLISECONDS.sleep(20);
    }
  }

  /** A task worker that holds every task until {@link #release} opens. */
  public static final class BlockingWorkerHandler extends AbstractHttpHandler {

    private final CountDownLatch received = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicInteger requests = new AtomicInteger();

    @POST
    @Path(RUN_PATH)
    public void run(FullHttpRequest request, HttpResponder responder) throws InterruptedException {
      requests.incrementAndGet();
      received.countDown();
      release.await();
      responder.sendString(HttpResponseStatus.OK, TASK_RESULT);
    }
  }
}
