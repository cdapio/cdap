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

import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.ServiceException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.encryption.AeadCipher;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.security.spi.encryption.CipherException;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests {@link RemoteTaskExecutor} routing with the task worker proxy. The stub worker is registered
 * under one service name, so a task only succeeds if the executor resolved that service.
 */
public class RemoteTaskExecutorRbacProxyTest {

  private static final String TENANT_NAMESPACE = "tenant-a";
  private static final String TASK_RESULT = "success";
  private static final String PLAIN_TOKEN = "plain-token";

  private static NettyHttpService httpService;
  private static StubWorkerHandler workerHandler;
  private static AeadCipher mockAeadCipher;

  private InMemoryDiscoveryService discoveryService;
  private final List<Cancellable> registrations = new ArrayList<>();

  @BeforeClass
  public static void init() throws Exception {
    mockAeadCipher = createMockAeadCipher();
    workerHandler = new StubWorkerHandler();
    httpService = new CommonNettyHttpServiceBuilder(CConfiguration.create(), "test",
        new NoOpMetricsCollectionService(), false, auditLogContexts -> {
    }, mockAeadCipher)
        .setHttpHandlers(workerHandler)
        .build();
    httpService.start();
  }

  @AfterClass
  public static void cleanup() throws Exception {
    httpService.stop();
  }

  @Before
  public void beforeTest() {
    discoveryService = new InMemoryDiscoveryService();
    workerHandler.reset();
  }

  @After
  public void afterTest() {
    registrations.forEach(Cancellable::cancel);
    registrations.clear();
    // The credential lives in a thread local and surefire reuses the thread across tests.
    SecurityRequestContext.reset();
  }

  @Test
  public void testRoutesToTaskWorkerManagerWhenProxyEnabled() throws Exception {
    // Only the proxy is discoverable. A successful run therefore proves the executor targeted
    // task.worker.manager rather than task.worker.
    register(Constants.Service.TASK_WORKER_MANAGER);

    byte[] result = newExecutor(proxyEnabledConf()).runTask(taskRequest(TENANT_NAMESPACE));

    Assert.assertEquals(TASK_RESULT, new String(result, StandardCharsets.UTF_8));
    Assert.assertEquals(1, workerHandler.getRequestCount());
  }

  @Test
  public void testRoutesToTaskWorkerWhenFeatureFlagDisabled() throws Exception {
    CConfiguration cConf = proxyEnabledConf();
    cConf.setBoolean(featureFlagKey(), false);

    assertTargetsTaskWorkerOnly(cConf);
  }

  @Test
  public void testRoutesToTaskWorkerWhenRbacDisabled() throws Exception {
    CConfiguration cConf = proxyEnabledConf();
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, false);

    assertTargetsTaskWorkerOnly(cConf);
  }

  @Test
  public void testSingleWorkerIsCalledDirectly() throws Exception {
    assertTargetsTaskWorkerOnly(singleWorkerConf());
    // The worker reads the namespace from the request body, so no routing header is sent.
    Assert.assertEquals(Collections.singletonList(null), workerHandler.getNamespaceHeaders());
  }

  @Test
  public void testSingleWorkerReadTimeoutIsNotRetried() {
    CConfiguration cConf = singleWorkerConf();
    register(Constants.Service.TASK_WORKER);
    workerHandler.setResponseDelayMillis(1000L);
    RemoteTaskExecutor executor = new RemoteTaskExecutor(cConf, new NoOpMetricsCollectionService(),
        newClientFactory(cConf), RemoteTaskExecutor.Type.TASK_WORKER,
        new HttpRequestConfig(1000, 200, false), mockAeadCipher);

    try {
      executor.runTask(taskRequest(TENANT_NAMESPACE));
      Assert.fail("Expected the read timeout to surface");
    } catch (Exception e) {
      Assert.assertTrue("Expected SocketTimeoutException but got " + e.getClass(),
          e instanceof SocketTimeoutException);
    }

    // The worker may already be running the task, so a retry could run it twice.
    Assert.assertEquals(1, workerHandler.getRequestCount());
  }

  @Test
  public void testNamespaceHeaderSentWhenProxyEnabled() throws Exception {
    register(Constants.Service.TASK_WORKER_MANAGER);

    newExecutor(proxyEnabledConf()).runTask(taskRequest(TENANT_NAMESPACE));

    Assert.assertEquals(Collections.singletonList(TENANT_NAMESPACE),
        workerHandler.getNamespaceHeaders());
  }

  @Test
  public void testNamespaceHeaderAbsentWhenProxyDisabled() throws Exception {
    CConfiguration cConf = proxyEnabledConf();
    cConf.setBoolean(featureFlagKey(), false);
    register(Constants.Service.TASK_WORKER);

    newExecutor(cConf).runTask(taskRequest(TENANT_NAMESPACE));

    // Without the proxy there is nothing to route, so the header must not be attached at all.
    Assert.assertEquals(Collections.singletonList(null), workerHandler.getNamespaceHeaders());
  }

  @Test
  public void testSystemNamespaceUnwrapsToEmbeddedNamespace() throws Exception {
    register(Constants.Service.TASK_WORKER_MANAGER);

    RunnableTaskRequest embedded = RunnableTaskRequest.getBuilder("EmbeddedTask")
        .withNamespace(TENANT_NAMESPACE)
        .build();
    RunnableTaskRequest request = RunnableTaskRequest.getBuilder("SystemAppTask")
        .withNamespace(NamespaceId.SYSTEM.getNamespace())
        .withEmbeddedTaskRequest(embedded)
        .build();

    newExecutor(proxyEnabledConf()).runTask(request);

    // The lease must be taken against the tenant that actually owns the user code, not "system".
    Assert.assertEquals(Collections.singletonList(TENANT_NAMESPACE),
        workerHandler.getNamespaceHeaders());
  }

  @Test
  public void testEmbeddedNamespaceIsUsedWhateverTheOuterNamespace() throws Exception {
    register(Constants.Service.TASK_WORKER_MANAGER);

    RunnableTaskRequest embedded = RunnableTaskRequest.getBuilder("EmbeddedTask")
        .withNamespace(TENANT_NAMESPACE)
        .build();
    RunnableTaskRequest request = RunnableTaskRequest.getBuilder("OuterTask")
        .withNamespace("other")
        .withEmbeddedTaskRequest(embedded)
        .build();

    newExecutor(proxyEnabledConf()).runTask(request);

    // Must match TaskWorkerHttpHandlerInternal, which admits the task under the embedded namespace.
    Assert.assertEquals(Collections.singletonList(TENANT_NAMESPACE),
        workerHandler.getNamespaceHeaders());
  }

  @Test
  public void testSystemNamespaceWithoutEmbeddedRequestKeepsSystemNamespace() throws Exception {
    register(Constants.Service.TASK_WORKER_MANAGER);

    newExecutor(proxyEnabledConf()).runTask(
        taskRequest(NamespaceId.SYSTEM.getNamespace()));

    Assert.assertEquals(Collections.singletonList(NamespaceId.SYSTEM.getNamespace()),
        workerHandler.getNamespaceHeaders());
  }

  @Test
  public void testNullNamespaceRequestDoesNotSendNamespaceHeader() throws Exception {
    register(Constants.Service.TASK_WORKER_MANAGER);
    RunnableTaskRequest requestWithoutNamespace = RunnableTaskRequest.getBuilder("SomeTask")
        .withParam("param")
        .build();

    newExecutor(proxyEnabledConf()).runTask(requestWithoutNamespace);

    Assert.assertEquals(Collections.singletonList(null), workerHandler.getNamespaceHeaders());
  }

  @Test
  public void testEmptyBodyBadRequestSurfacesStatusInsteadOfNpe() {
    register(Constants.Service.TASK_WORKER_MANAGER);
    workerHandler.setStatusCode(HttpResponseStatus.BAD_REQUEST.code());

    try {
      newExecutor(proxyEnabledConf()).runTask(taskRequest(TENANT_NAMESPACE));
      Assert.fail("Expected a 400 response to fail the task");
    } catch (ServiceException e) {
      Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), e.getStatusCode());
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("400"));
    } catch (Exception e) {
      Assert.fail("Expected ServiceException but got " + e);
    }

    Assert.assertEquals(1, workerHandler.getRequestCount());
  }

  @Test
  public void testSaturationResponseIsRetriedThenSurfacedAsTooManyRequests() {
    register(Constants.Service.TASK_WORKER_MANAGER);
    workerHandler.setStatusCode(HttpResponseStatus.TOO_MANY_REQUESTS.code());

    try {
      newExecutor(proxyEnabledConf()).runTask(taskRequest(TENANT_NAMESPACE));
      Assert.fail("Expected a saturation failure once the retry budget was exhausted");
    } catch (Exception e) {
      Assert.assertTrue("Expected ServiceException but got " + e.getClass(),
          e instanceof ServiceException);
      Assert.assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS.code(),
          ((ServiceException) e).getStatusCode());
    }

    // A 429 must be retried rather than failing the first time, otherwise a momentarily saturated
    // cluster would surface as a hard pipeline failure.
    Assert.assertTrue("Expected more than one attempt, got " + workerHandler.getRequestCount(),
        workerHandler.getRequestCount() > 1);
  }

  @Test
  public void testSaturationWithoutLeasesFailsAsBefore() throws Exception {
    CConfiguration rbacOff = proxyEnabledConf();
    rbacOff.setBoolean(Constants.Security.Authorization.ENABLED, false);
    for (CConfiguration cConf : Arrays.asList(flagOffConf(), rbacOff)) {
      register(Constants.Service.TASK_WORKER);
      assertSaturationFailsAsBefore(newExecutor(cConf));
      registrations.forEach(Cancellable::cancel);
      registrations.clear();
    }
    // System workers never lease, even with the manager on.
    register(Constants.Service.SYSTEM_WORKER);
    assertSaturationFailsAsBefore(new RemoteTaskExecutor(proxyEnabledConf(),
        new NoOpMetricsCollectionService(), newClientFactory(proxyEnabledConf()),
        RemoteTaskExecutor.Type.SYSTEM_WORKER, mockAeadCipher));
  }

  /** Asserts a 429 is retried and then surfaces as the plain retryable error, as on develop. */
  private void assertSaturationFailsAsBefore(RemoteTaskExecutor executor) {
    workerHandler.reset();
    workerHandler.setStatusCode(HttpResponseStatus.TOO_MANY_REQUESTS.code());

    try {
      executor.runTask(taskRequest(TENANT_NAMESPACE));
      Assert.fail("Expected a failure once the retry budget was exhausted");
    } catch (Exception e) {
      Assert.assertTrue("Expected RetryableException but got " + e,
          e instanceof RetryableException);
      Assert.assertTrue(e.getMessage(), e.getMessage().startsWith("Received response code 429"));
    }
    Assert.assertTrue("Expected more than one attempt, got " + workerHandler.getRequestCount(),
        workerHandler.getRequestCount() > 1);
  }

  @Test
  public void testGatewayErrorsAreNotRetried() throws Exception {
    // Same as develop with the flag off; behind the proxy a 502 is a discovery bug and a 504
    // never comes from the proxy, so neither is worth a retry.
    for (CConfiguration cConf : Arrays.asList(proxyEnabledConf(), flagOffConf())) {
      String serviceName = TaskWorkerManager.isProxyEnabled(cConf)
          ? Constants.Service.TASK_WORKER_MANAGER : Constants.Service.TASK_WORKER;
      register(serviceName);
      for (HttpResponseStatus status : Arrays.asList(HttpResponseStatus.BAD_GATEWAY,
          HttpResponseStatus.GATEWAY_TIMEOUT)) {
        workerHandler.reset();
        workerHandler.setStatusCode(status.code());

        try {
          newExecutor(cConf).runTask(taskRequest(TENANT_NAMESPACE));
          Assert.fail("Expected " + status + " to fail the task via " + serviceName);
        } catch (ServiceException e) {
          Assert.assertEquals(status.code(), e.getStatusCode());
        }

        Assert.assertEquals(status + " via " + serviceName, 1, workerHandler.getRequestCount());
      }
      registrations.forEach(Cancellable::cancel);
      registrations.clear();
    }
  }

  @Test
  public void testProxyWorkerUnreachableIsRetriedThenReportedAsNotRun() {
    register(Constants.Service.TASK_WORKER_MANAGER);
    workerHandler.setStatusCode(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
    workerHandler.setResponseBody(ProxyFrontendHandler.WORKER_UNREACHABLE_BODY);

    try {
      newExecutor(proxyEnabledConf()).runTask(taskRequest(TENANT_NAMESPACE));
      Assert.fail("Expected the task to fail once the retry budget was exhausted");
    } catch (Exception e) {
      Assert.assertTrue("Expected ServiceException but got " + e.getClass(),
          e instanceof ServiceException);
      Assert.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
          ((ServiceException) e).getStatusCode());
      // The proxy answered, so the error must point at the workers, not the proxy deployment.
      Assert.assertTrue("Error should blame the task workers, got: " + e.getMessage(),
          e.getMessage().contains("could not reach a task worker"));
    }

    Assert.assertTrue("Expected more than one attempt, got " + workerHandler.getRequestCount(),
        workerHandler.getRequestCount() > 1);
  }

  @Test
  public void testProxyReadTimeoutIsNotRetried() {
    CConfiguration cConf = proxyEnabledConf();
    register(Constants.Service.TASK_WORKER_MANAGER);
    workerHandler.setResponseDelayMillis(1000L);
    RemoteTaskExecutor executor = new RemoteTaskExecutor(cConf, new NoOpMetricsCollectionService(),
        newClientFactory(cConf), RemoteTaskExecutor.Type.TASK_WORKER,
        new HttpRequestConfig(1000, 200, false), mockAeadCipher);

    try {
      executor.runTask(taskRequest(TENANT_NAMESPACE));
      Assert.fail("Expected the read timeout to surface");
    } catch (Exception e) {
      Assert.assertTrue("Expected SocketTimeoutException but got " + e.getClass(),
          e instanceof SocketTimeoutException);
    }

    // The worker behind the proxy may already be running the task, so a retry could run it twice.
    Assert.assertEquals(1, workerHandler.getRequestCount());
  }

  @Test
  public void testSystemWorkerConnectionResetIsNotRetried() throws Exception {
    // Develop behaviour: a reset may come after the system worker started the task.
    try (ServerSocket resettingWorker = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
      AtomicInteger connections = new AtomicInteger();
      Thread acceptor = new Thread(() -> {
        while (!resettingWorker.isClosed()) {
          try (Socket socket = resettingWorker.accept()) {
            connections.incrementAndGet();
            socket.setSoLinger(true, 0);
          } catch (IOException e) {
            return;
          }
        }
      });
      acceptor.setDaemon(true);
      acceptor.start();
      registrations.add(discoveryService.register(new Discoverable(Constants.Service.SYSTEM_WORKER,
          new InetSocketAddress(InetAddress.getLoopbackAddress(), resettingWorker.getLocalPort()))));
      CConfiguration cConf = flagOffConf();
      RemoteTaskExecutor executor = new RemoteTaskExecutor(cConf,
          new NoOpMetricsCollectionService(), newClientFactory(cConf),
          RemoteTaskExecutor.Type.SYSTEM_WORKER, mockAeadCipher);

      try {
        executor.runTask(taskRequest(TENANT_NAMESPACE));
        Assert.fail("Expected the reset to surface");
      } catch (SocketException expected) {
        // The develop predicate doesn't retry SocketException for system workers.
      }

      // HttpURLConnection itself may resend a POST once on a reset (sun.net.http.retryPost);
      // anything more would come from the executor's retry loop.
      Assert.assertTrue("Expected at most 2 connections, got " + connections.get(),
          connections.get() <= 2);
    }
  }

  @Test
  public void testForbiddenIsNotRetried() {
    register(Constants.Service.TASK_WORKER_MANAGER);
    workerHandler.setStatusCode(HttpResponseStatus.FORBIDDEN.code());

    try {
      newExecutor(proxyEnabledConf()).runTask(taskRequest(TENANT_NAMESPACE));
      Assert.fail("Expected a 403 to fail the task");
    } catch (Exception e) {
      Assert.assertTrue("Expected UnauthorizedException but got " + e.getClass(),
          e instanceof UnauthorizedException);
    }

    Assert.assertEquals(1, workerHandler.getRequestCount());
  }

  @Test
  public void testProxyUnreachableFailsWithoutFallingBackToTaskWorker() {
    // Only the worker is discoverable; the executor must not bypass the proxy.
    register(Constants.Service.TASK_WORKER);

    try {
      newExecutor(proxyEnabledConf()).runTask(taskRequest(TENANT_NAMESPACE));
      Assert.fail("Expected the task to fail rather than bypass the proxy");
    } catch (Exception e) {
      Assert.assertTrue("Expected ServiceException but got " + e.getClass(),
          e instanceof ServiceException);
      Assert.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
          ((ServiceException) e).getStatusCode());
      // The operator needs to know which service is missing, so it has to be named.
      Assert.assertTrue("Error should name the proxy, got: " + e.getMessage(),
          e.getMessage().contains(Constants.Service.TASK_WORKER_MANAGER));
    }

    Assert.assertEquals("The task worker must not receive a direct request", 0,
        workerHandler.getRequestCount());
  }

  @Test
  public void testSystemWorkerTypeIsNeverProxied() throws Exception {
    // System worker traffic runs trusted platform code, so it must bypass the namespace proxy even
    // on an RBAC instance.
    register(Constants.Service.SYSTEM_WORKER);

    RemoteTaskExecutor executor = new RemoteTaskExecutor(proxyEnabledConf(),
        new NoOpMetricsCollectionService(), newClientFactory(proxyEnabledConf()),
        RemoteTaskExecutor.Type.SYSTEM_WORKER, mockAeadCipher);

    byte[] result = executor.runTask(taskRequest(TENANT_NAMESPACE));

    Assert.assertEquals(TASK_RESULT, new String(result, StandardCharsets.UTF_8));
    Assert.assertEquals(Collections.singletonList(null), workerHandler.getNamespaceHeaders());
  }

  /** Guards the credential restore in {@code runTask}, so a retry doesn't re-encrypt ciphertext. */
  @Test
  public void testUserCredentialIsRestoredAfterAFailedAttempt() {
    register(Constants.Service.TASK_WORKER_MANAGER);
    // A 503 makes RemoteClient.execute() throw, which is the path that used to skip the restore.
    workerHandler.setStatusCode(HttpResponseStatus.SERVICE_UNAVAILABLE.code());

    Credential original = new Credential(PLAIN_TOKEN, Credential.CredentialType.EXTERNAL);
    SecurityRequestContext.setUserCredential(original);
    RecordingAeadCipher cipher = new RecordingAeadCipher();

    try {
      newExecutor(proxyEnabledConf(), cipher).runTask(taskRequest(TENANT_NAMESPACE));
      Assert.fail("Expected the run to fail once the retry budget was exhausted");
    } catch (Exception expected) {
      // How the run fails is covered elsewhere; this test only cares about the credential.
    }

    List<String> encryptedValues = cipher.getEncryptedValues();
    Assert.assertTrue("Expected more than one attempt, got " + encryptedValues.size(),
        encryptedValues.size() > 1);
    Assert.assertEquals(Collections.nCopies(encryptedValues.size(), PLAIN_TOKEN), encryptedValues);
    // The original instance must be back on the thread local, not a re-wrapped copy.
    Assert.assertSame(original, SecurityRequestContext.getUserCredential());
  }

  @Test
  public void testProxyConnectTimeoutIsRetried() throws Exception {
    register(Constants.Service.TASK_WORKER_MANAGER);
    CConfiguration cConf = proxyEnabledConf();
    AtomicInteger executeCalls = new AtomicInteger();
    Credential original = new Credential(PLAIN_TOKEN, Credential.CredentialType.EXTERNAL);
    SecurityRequestContext.setUserCredential(original);
    RecordingAeadCipher cipher = new RecordingAeadCipher();

    byte[] result = newExecutor(cConf, cipher, failingClientFactory(cConf,
        new SocketTimeoutException("connect timed out"), 1, executeCalls, new ArrayList<>()))
        .runTask(taskRequest(TENANT_NAMESPACE));

    Assert.assertEquals(TASK_RESULT, new String(result, StandardCharsets.UTF_8));
    Assert.assertEquals(2, executeCalls.get());
    // The failed attempt never connected, so only the retry reached the worker.
    Assert.assertEquals(1, workerHandler.getRequestCount());
    Assert.assertEquals(Arrays.asList(PLAIN_TOKEN, PLAIN_TOKEN), cipher.getEncryptedValues());
    Assert.assertSame(original, SecurityRequestContext.getUserCredential());
  }

  @Test
  public void testConnectTimeoutIsNotRetriedWhenProxyDisabled() {
    CConfiguration cConf = proxyEnabledConf();
    cConf.setBoolean(featureFlagKey(), false);
    register(Constants.Service.TASK_WORKER);
    AtomicInteger executeCalls = new AtomicInteger();

    try {
      newExecutor(cConf, mockAeadCipher, failingClientFactory(cConf,
          new SocketTimeoutException("connect timed out"), Integer.MAX_VALUE, executeCalls,
          new ArrayList<>())).runTask(taskRequest(TENANT_NAMESPACE));
      Assert.fail("Expected the connect timeout to surface");
    } catch (Exception e) {
      // Same as before the proxy existed: a direct task worker call fails on the first timeout.
      Assert.assertTrue("Expected SocketTimeoutException but got " + e.getClass(),
          e instanceof SocketTimeoutException);
    }
    Assert.assertEquals(1, executeCalls.get());
  }

  @Test
  public void testProxyConnectTimeoutFailsAsUnreachableOnceRetriesRunOut() {
    register(Constants.Service.TASK_WORKER_MANAGER);
    CConfiguration cConf = proxyEnabledConf();
    AtomicInteger executeCalls = new AtomicInteger();

    try {
      newExecutor(cConf, mockAeadCipher, failingClientFactory(cConf,
          new SocketTimeoutException("connect timed out"), Integer.MAX_VALUE, executeCalls,
          new ArrayList<>())).runTask(taskRequest(TENANT_NAMESPACE));
      Assert.fail("Expected the run to fail once the retry budget was exhausted");
    } catch (ServiceException e) {
      Assert.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.code(), e.getStatusCode());
      Assert.assertTrue(e.getMessage(), e.getMessage().startsWith(
          "Could not reach the " + Constants.Service.TASK_WORKER_MANAGER + " proxy"));
    } catch (Exception e) {
      Assert.fail("Expected ServiceException but got " + e);
    }
    Assert.assertTrue("Expected retries, got " + executeCalls.get(), executeCalls.get() > 1);
    Assert.assertEquals(0, workerHandler.getRequestCount());
  }

  @Test
  public void testProxyConnectTimeoutIsCappedOnlyWhenProxyEnabled() throws Exception {
    register(Constants.Service.TASK_WORKER_MANAGER);
    register(Constants.Service.TASK_WORKER);
    HttpRequestConfig callerConfig = new HttpRequestConfig(60000, 120000, false, 1024);

    CConfiguration proxyConf = proxyEnabledConf();
    proxyConf.setInt(Constants.TaskWorkerManager.CLIENT_CONNECT_TIMEOUT_MS, 5000);
    List<HttpRequestConfig> proxyConfigs = new ArrayList<>();
    new RemoteTaskExecutor(proxyConf, new NoOpMetricsCollectionService(),
        failingClientFactory(proxyConf, null, 0, new AtomicInteger(), proxyConfigs),
        RemoteTaskExecutor.Type.TASK_WORKER, callerConfig, mockAeadCipher);
    HttpRequestConfig capped = proxyConfigs.get(0);
    Assert.assertEquals(5000, capped.getConnectTimeout());
    Assert.assertEquals(120000, capped.getReadTimeout());
    Assert.assertEquals(1024, capped.getFixedLengthStreamingThreshold());

    CConfiguration directConf = proxyEnabledConf();
    directConf.setBoolean(featureFlagKey(), false);
    List<HttpRequestConfig> directConfigs = new ArrayList<>();
    new RemoteTaskExecutor(directConf, new NoOpMetricsCollectionService(),
        failingClientFactory(directConf, null, 0, new AtomicInteger(), directConfigs),
        RemoteTaskExecutor.Type.TASK_WORKER, callerConfig, mockAeadCipher);
    Assert.assertSame(callerConfig, directConfigs.get(0));
  }

  @Test
  public void testCapConnectTimeout() {
    HttpRequestConfig shortConfig = new HttpRequestConfig(2000, 120000, true, 512);
    Assert.assertSame(shortConfig, RemoteTaskExecutor.capConnectTimeout(shortConfig, 5000));

    HttpRequestConfig longConfig = new HttpRequestConfig(60000, 120000, true, 512);
    HttpRequestConfig capped = RemoteTaskExecutor.capConnectTimeout(longConfig, 5000);
    Assert.assertEquals(5000, capped.getConnectTimeout());
    Assert.assertEquals(120000, capped.getReadTimeout());
    Assert.assertTrue(capped.isVerifySSLCert());
    Assert.assertEquals(512, capped.getFixedLengthStreamingThreshold());

    // 0 means wait forever, so it is capped as well.
    HttpRequestConfig unbounded = new HttpRequestConfig(0, 120000, true, 512);
    Assert.assertEquals(5000,
        RemoteTaskExecutor.capConnectTimeout(unbounded, 5000).getConnectTimeout());
  }

  @Test
  public void testIsConnectTimeout() {
    Assert.assertTrue(RemoteTaskExecutor.isConnectTimeout(
        new SocketTimeoutException("connect timed out")));
    Assert.assertTrue(RemoteTaskExecutor.isConnectTimeout(
        new SocketTimeoutException("Connect timed out")));
    Assert.assertFalse(RemoteTaskExecutor.isConnectTimeout(
        new SocketTimeoutException("Read timed out")));
    Assert.assertFalse(RemoteTaskExecutor.isConnectTimeout(new SocketTimeoutException()));
  }

  /** Asserts the executor talks to {@code task.worker}, not {@code task.worker.manager}. */
  private void assertTargetsTaskWorkerOnly(CConfiguration cConf) throws Exception {
    register(Constants.Service.TASK_WORKER);

    byte[] result = newExecutor(cConf).runTask(taskRequest(TENANT_NAMESPACE));

    Assert.assertEquals(TASK_RESULT, new String(result, StandardCharsets.UTF_8));
    Assert.assertEquals(1, workerHandler.getRequestCount());
  }

  private static String featureFlagKey() {
    return "feature." + Feature.RBAC_TASK_WORKER_MANAGER.getFeatureFlagString();
  }

  /** Builds a configuration with the flag and RBAC on, and a short retry budget. */
  private static CConfiguration proxyEnabledConf() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(featureFlagKey(), true);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    cConf.set(Constants.Service.TASK_WORKER + "." + Constants.Retry.TYPE, "fixed.delay");
    cConf.setLong(Constants.Service.TASK_WORKER + "." + Constants.Retry.DELAY_BASE_MS, 10L);
    cConf.setLong(Constants.Service.TASK_WORKER + "." + Constants.Retry.MAX_TIME_SECS, 2L);
    cConf.set(Constants.Service.SYSTEM_WORKER + "." + Constants.Retry.TYPE, "fixed.delay");
    cConf.setLong(Constants.Service.SYSTEM_WORKER + "." + Constants.Retry.DELAY_BASE_MS, 10L);
    cConf.setLong(Constants.Service.SYSTEM_WORKER + "." + Constants.Retry.MAX_TIME_SECS, 2L);
    return cConf;
  }

  /** Builds a configuration with RBAC on and the flag off, which must behave like develop. */
  private static CConfiguration flagOffConf() {
    CConfiguration cConf = proxyEnabledConf();
    cConf.setBoolean(featureFlagKey(), false);
    return cConf;
  }

  /** Builds a proxy enabled configuration with a single task worker. */
  private static CConfiguration singleWorkerConf() {
    CConfiguration cConf = proxyEnabledConf();
    cConf.setInt(Constants.TaskWorker.CONTAINER_COUNT, 1);
    return cConf;
  }

  private RemoteClientFactory newClientFactory(CConfiguration cConf) {
    return new RemoteClientFactory(discoveryService, new NoOpInternalAuthenticator(),
        new NoOpRemoteAuthenticator(), cConf);
  }

  private RemoteTaskExecutor newExecutor(CConfiguration cConf) {
    return newExecutor(cConf, mockAeadCipher);
  }

  private RemoteTaskExecutor newExecutor(CConfiguration cConf, AeadCipher aeadCipher,
      RemoteClientFactory clientFactory) {
    return new RemoteTaskExecutor(cConf, new NoOpMetricsCollectionService(), clientFactory,
        RemoteTaskExecutor.Type.TASK_WORKER, aeadCipher);
  }

  /**
   * A client factory whose clients throw {@code failure} on the first {@code failures} calls to
   * {@link RemoteClient#execute(HttpRequest)} and behave normally after that. Counts the calls in
   * {@code executeCalls} and records each requested {@link HttpRequestConfig} in {@code configs}.
   */
  private RemoteClientFactory failingClientFactory(CConfiguration cConf, IOException failure,
      int failures, AtomicInteger executeCalls, List<HttpRequestConfig> configs) {
    RemoteClientFactory factory = Mockito.spy(newClientFactory(cConf));
    Mockito.doAnswer(createCall -> {
      configs.add(createCall.getArgument(1));
      RemoteClient client = Mockito.spy((RemoteClient) createCall.callRealMethod());
      Mockito.doAnswer(executeCall -> {
        if (executeCalls.getAndIncrement() < failures) {
          throw failure;
        }
        return executeCall.callRealMethod();
      }).when(client).execute(Mockito.any(HttpRequest.class));
      return client;
    }).when(factory).createRemoteClient(Mockito.anyString(), Mockito.any(HttpRequestConfig.class),
        Mockito.anyString());
    return factory;
  }

  private RemoteTaskExecutor newExecutor(CConfiguration cConf, AeadCipher aeadCipher) {
    return new RemoteTaskExecutor(cConf, new NoOpMetricsCollectionService(),
        newClientFactory(cConf), RemoteTaskExecutor.Type.TASK_WORKER, aeadCipher);
  }

  private void register(String serviceName) {
    registrations.add(
        discoveryService.register(URIScheme.createDiscoverable(serviceName, httpService)));
  }

  private static RunnableTaskRequest taskRequest(String namespace) {
    return RunnableTaskRequest.getBuilder("SomeTask")
        .withParam("param")
        .withNamespace(namespace)
        .build();
  }

  private static AeadCipher createMockAeadCipher() {
    return new AeadCipher() {
      @Override
      public byte[] encrypt(byte[] plainData, byte[] associatedData) throws CipherException {
        return new byte[0];
      }

      @Override
      public byte[] decrypt(byte[] cipherData, byte[] associatedData) throws CipherException {
        return new byte[0];
      }
    };
  }

  /** An {@link AeadCipher} that records each plaintext and returns a fixed marker. */
  private static final class RecordingAeadCipher implements AeadCipher {

    private static final byte[] CIPHER_TEXT = "encrypted".getBytes(StandardCharsets.UTF_8);

    private final List<String> encryptedValues = Collections.synchronizedList(new ArrayList<>());

    @Override
    public byte[] encrypt(byte[] plainData, byte[] associatedData) throws CipherException {
      encryptedValues.add(new String(plainData, StandardCharsets.UTF_8));
      return CIPHER_TEXT;
    }

    @Override
    public byte[] decrypt(byte[] cipherData, byte[] associatedData) throws CipherException {
      return new byte[0];
    }

    List<String> getEncryptedValues() {
      return new ArrayList<>(encryptedValues);
    }
  }

  /** A stub task worker that records namespace headers; its status and delay are configurable. */
  @Path(Constants.Gateway.INTERNAL_API_VERSION_3)
  public static final class StubWorkerHandler extends AbstractHttpHandler {

    private final List<String> namespaceHeaders =
        Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger requestCount = new AtomicInteger();
    private final AtomicInteger statusCode =
        new AtomicInteger(HttpResponseStatus.OK.code());
    private final AtomicLong responseDelayMillis = new AtomicLong();
    private final AtomicReference<String> responseBody = new AtomicReference<>();

    @POST
    @Path("/worker/run")
    public void runWorkerTask(FullHttpRequest request, HttpResponder responder) {
      handle(request, responder);
    }

    @POST
    @Path("/system/run")
    public void runSystemTask(FullHttpRequest request, HttpResponder responder) {
      handle(request, responder);
    }

    private void handle(FullHttpRequest request, HttpResponder responder) {
      requestCount.incrementAndGet();
      namespaceHeaders.add(request.headers().get(Constants.Gateway.HEADER_CDAP_NAMESPACE));

      long delayMillis = responseDelayMillis.get();
      if (delayMillis > 0) {
        Uninterruptibles.sleepUninterruptibly(delayMillis, TimeUnit.MILLISECONDS);
      }

      int code = statusCode.get();
      if (code != HttpResponseStatus.OK.code()) {
        String body = responseBody.get();
        if (body == null) {
          responder.sendStatus(HttpResponseStatus.valueOf(code));
        } else {
          responder.sendString(HttpResponseStatus.valueOf(code), body);
        }
        return;
      }
      responder.sendString(HttpResponseStatus.OK, TASK_RESULT);
    }

    void reset() {
      namespaceHeaders.clear();
      requestCount.set(0);
      statusCode.set(HttpResponseStatus.OK.code());
      responseDelayMillis.set(0L);
      responseBody.set(null);
    }

    void setResponseBody(String body) {
      responseBody.set(body);
    }

    void setStatusCode(int code) {
      statusCode.set(code);
    }

    void setResponseDelayMillis(long millis) {
      responseDelayMillis.set(millis);
    }

    int getRequestCount() {
      return requestCount.get();
    }

    List<String> getNamespaceHeaders() {
      return new ArrayList<>(namespaceHeaders);
    }
  }
}
