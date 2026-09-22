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

import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.ServiceException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.encryption.AeadCipher;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.encryption.CipherException;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests how {@link RemoteTaskExecutor} routes task worker traffic once the RBAC netty proxy is in
 * play.
 *
 * <p>The routing decision is deliberately asserted through discovery rather than by reading private
 * state: the stub worker is registered under exactly one service name, so a task only completes if
 * the executor resolved that service. Registering under {@code task.worker.manager} alone and getting a
 * successful run is proof that the request went through the proxy, and not to the worker directly.
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
  public void testSystemNamespaceWithoutEmbeddedRequestKeepsSystemNamespace() throws Exception {
    register(Constants.Service.TASK_WORKER_MANAGER);

    newExecutor(proxyEnabledConf()).runTask(
        taskRequest(NamespaceId.SYSTEM.getNamespace()));

    Assert.assertEquals(Collections.singletonList(NamespaceId.SYSTEM.getNamespace()),
        workerHandler.getNamespaceHeaders());
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
  public void testProxyUnreachableFailsWithoutFallingBackToTaskWorker() {
    // Only the worker is discoverable; the proxy is not. The executor must never reach the worker
    // directly, because the proxy owns the per-namespace pod lease and a direct request would
    // place a task the routing registry knows nothing about.
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
  public void testRemoteClientFactoryPropagatesProxyFlag() {
    CConfiguration cConf = proxyEnabledConf();
    RemoteClient proxyClient = newClientFactory(cConf).createRemoteClient(
        Constants.Service.TASK_WORKER_MANAGER, new DefaultHttpRequestConfig(false),
        Constants.Gateway.INTERNAL_API_VERSION_3);
    register(Constants.Service.TASK_WORKER_MANAGER);

    // Resolution against the proxy only succeeds when the factory wired the flag through to the
    // client; otherwise the client would never be built against task.worker.manager at all.
    Assert.assertNotNull(proxyClient.resolve("/worker/run", TENANT_NAMESPACE));
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

  /**
   * Guards the credential restore in {@code runTask}. Every attempt encrypts whatever credential is
   * on the thread local, so if a failed attempt leaves its own ciphertext behind, the next attempt
   * encrypts that ciphertext again and the worker receives a token it cannot decrypt.
   */
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

  /**
   * Asserts that the executor built with the given configuration talks to {@code task.worker} and
   * not to {@code task.worker.manager}.
   */
  private void assertTargetsTaskWorkerOnly(CConfiguration cConf) throws Exception {
    register(Constants.Service.TASK_WORKER);

    byte[] result = newExecutor(cConf).runTask(taskRequest(TENANT_NAMESPACE));

    Assert.assertEquals(TASK_RESULT, new String(result, StandardCharsets.UTF_8));
    Assert.assertEquals(1, workerHandler.getRequestCount());
  }

  private static String featureFlagKey() {
    return "feature." + Feature.RBAC_TASK_WORKER_MANAGER.getFeatureFlagString();
  }

  /**
   * Builds a configuration with both the feature flag and instance level RBAC turned on, and a
   * retry budget short enough to keep the tests fast.
   */
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

  private RemoteClientFactory newClientFactory(CConfiguration cConf) {
    return new RemoteClientFactory(discoveryService, new NoOpInternalAuthenticator(),
        new NoOpRemoteAuthenticator(), cConf);
  }

  private RemoteTaskExecutor newExecutor(CConfiguration cConf) {
    return newExecutor(cConf, mockAeadCipher);
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

  /**
   * An {@link AeadCipher} that records every plaintext it is asked to encrypt. The ciphertext is a
   * fixed marker rather than a function of the input: a credential that gets encrypted twice still
   * shows up as a different recorded value on the second call, but the value cannot compound. That
   * matters, because real re-encryption grows the credential on every pass, and a growing value
   * would make a regression here hang instead of failing.
   */
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

  /**
   * Stands in for a task worker. It records the routing header of every inbound request so tests
   * can assert on what the executor actually put on the wire, and its response status is
   * controllable so the saturation path can be exercised.
   */
  @Path(Constants.Gateway.INTERNAL_API_VERSION_3)
  public static final class StubWorkerHandler extends AbstractHttpHandler {

    private final List<String> namespaceHeaders =
        Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger requestCount = new AtomicInteger();
    private final AtomicInteger statusCode =
        new AtomicInteger(HttpResponseStatus.OK.code());

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

      int code = statusCode.get();
      if (code != HttpResponseStatus.OK.code()) {
        responder.sendStatus(HttpResponseStatus.valueOf(code));
        return;
      }
      responder.sendString(HttpResponseStatus.OK, TASK_RESULT);
    }

    void reset() {
      namespaceHeaders.clear();
      requestCount.set(0);
      statusCode.set(HttpResponseStatus.OK.code());
    }

    void setStatusCode(int code) {
      statusCode.set(code);
    }

    int getRequestCount() {
      return requestCount.get();
    }

    List<String> getNamespaceHeaders() {
      return new ArrayList<>(namespaceHeaders);
    }
  }
}
