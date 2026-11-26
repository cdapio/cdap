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
import io.cdap.cdap.common.service.Services;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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
 * Unit test for {@link TaskWorkerManagerService}: stop, TLS and injection.
 */
public class TaskWorkerManagerServiceTest {

  private static final String RUN_PATH = "/v3Internal/worker/run";
  private static final String TASK_RESULT = "task-result";

  private CConfiguration cConf;
  private InMemoryDiscoveryService discoveryService;
  private CountingWorkerHandler workerHandler;
  private NettyHttpService worker;
  private Cancellable cancelWorkerDiscovery;

  @Before
  public void setUp() throws Exception {
    cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorkerManager.ADDRESS, "127.0.0.1");
    cConf.setInt(Constants.TaskWorkerManager.PORT, 0);
    discoveryService = new InMemoryDiscoveryService();

    workerHandler = new CountingWorkerHandler();
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
    cancelWorkerDiscovery.cancel();
    worker.stop();
  }

  @Test
  public void testStopDeregistersAndTerminates() throws Exception {
    TaskWorkerManagerService service = startService();
    Assert.assertEquals(TASK_RESULT, runTask());
    InetSocketAddress manager = managerDiscoverable().getSocketAddress();

    Assert.assertEquals(Service.State.TERMINATED,
        Services.stop(service).get(10, TimeUnit.SECONDS));

    Assert.assertFalse(discoveryService.discover(Constants.Service.TASK_WORKER_MANAGER)
        .iterator().hasNext());
    // The server socket is closed, so new connections are refused.
    try (Socket socket = new Socket(manager.getHostString(), manager.getPort())) {
      Assert.fail("Expected the manager's port to refuse connections after stop");
    } catch (IOException expected) {
      // expected
    }
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
      Services.stopAndWait(service);
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
    Services.startAndWait(service);
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

  /** A task worker that counts the tasks it receives and answers each with {@link #TASK_RESULT}. */
  public static final class CountingWorkerHandler extends AbstractHttpHandler {

    private final AtomicInteger requests = new AtomicInteger();

    @POST
    @Path(RUN_PATH)
    public void run(FullHttpRequest request, HttpResponder responder) {
      requests.incrementAndGet();
      responder.sendString(HttpResponseStatus.OK, TASK_RESULT);
    }
  }
}
