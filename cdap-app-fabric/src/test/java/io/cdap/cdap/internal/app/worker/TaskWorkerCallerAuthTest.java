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

package io.cdap.cdap.internal.app.worker;

import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.encryption.NoOpAeadCipher;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.service.Services;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests that {@link TaskWorkerService} runs only tasks sent with an internal token of a CDAP system
 * service.
 */
public class TaskWorkerCallerAuthTest {

  private static final Gson GSON = new Gson();
  private static final MetricsCollectionService METRICS = new NoOpMetricsCollectionService();
  private static final long ONE_MINUTE = TimeUnit.MINUTES.toMillis(1);

  private TaskWorkerService taskWorkerService;
  private TokenManager tokenManager;
  private Codec<AccessToken> accessTokenCodec;

  @After
  public void afterTest() {
    if (taskWorkerService != null) {
      Services.stopAndWait(taskWorkerService);
    }
  }

  @Test
  public void testRunsTaskWithSystemToken() throws IOException {
    startService(createConf(true));

    HttpResponse response = runTask(internalToken(tokenManager, "cdap",
        UserIdentity.IdentifierType.INTERNAL, ONE_MINUTE));

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals("0", response.getResponseBodyAsString());
    Services.stopAndWait(taskWorkerService);
    Assert.assertFalse(tokenManager.isRunning());
  }

  @Test
  public void testRejectsTaskWithoutSystemToken() throws IOException {
    CConfiguration cConf = createConf(true);
    startService(cConf);
    TokenManager otherTokenManager = createSecurityInjector(cConf).getInstance(TokenManager.class);
    Services.startAndWait(otherTokenManager);
    try {
      String systemToken = internalToken(tokenManager, "cdap", UserIdentity.IdentifierType.INTERNAL,
          ONE_MINUTE);

      List<String> callerTokens = Arrays.asList(
          null,
          "CDAP-Internal not-a-token",
          "CDAP-Internal AAAA",
          systemToken.replace("CDAP-Internal", "CDAP-External"),
          internalToken(tokenManager, "cdap", UserIdentity.IdentifierType.INTERNAL, -ONE_MINUTE),
          internalToken(otherTokenManager, "cdap", UserIdentity.IdentifierType.INTERNAL, ONE_MINUTE),
          internalToken(tokenManager, "user", UserIdentity.IdentifierType.EXTERNAL, ONE_MINUTE),
          // A program run token.
          internalToken(tokenManager, Constants.Security.Authentication.RUNTIME_IDENTITY,
              UserIdentity.IdentifierType.INTERNAL, TimeUnit.DAYS.toMillis(365)));
      for (String callerToken : callerTokens) {
        Assert.assertEquals(String.valueOf(callerToken), HttpURLConnection.HTTP_FORBIDDEN,
            runTask(callerToken).getResponseCode());
      }

      // Rejected tasks don't take the only task slot.
      Assert.assertEquals(HttpURLConnection.HTTP_OK, runTask(systemToken).getResponseCode());
    } finally {
      Services.stopAndWait(otherTokenManager);
    }
  }

  @Test
  public void testNoCheckWithoutInternalAuth() throws IOException {
    startService(createConf(false));

    Assert.assertEquals(HttpURLConnection.HTTP_OK, runTask(null).getResponseCode());
    Assert.assertFalse(tokenManager.isRunning());
  }

  private CConfiguration createConf(boolean internalAuthEnabled) {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorker.ADDRESS, "localhost");
    cConf.setInt(Constants.TaskWorker.PORT, 0);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);
    cConf.setInt(Constants.ArtifactLocalizer.PORT, -1);
    cConf.setBoolean(Constants.TaskWorker.USER_CODE_ISOLATION_ENABLED, false);
    cConf.setInt(Constants.TaskWorker.REQUEST_LIMIT, 1);
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_DURATION_SECOND, 0);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, internalAuthEnabled);
    return cConf;
  }

  private void startService(CConfiguration cConf) {
    Injector injector = createSecurityInjector(cConf);
    tokenManager = injector.getInstance(TokenManager.class);
    accessTokenCodec = injector.getInstance(Key.get(new TypeLiteral<Codec<AccessToken>>() {
    }));
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    taskWorkerService = new TaskWorkerService(cConf, SConfiguration.create(), discoveryService,
        discoveryService, METRICS, new CommonNettyHttpServiceFactory(cConf, METRICS,
            auditLogContexts -> { }, new NoOpAeadCipher()), tokenManager, accessTokenCodec);
    Services.startAndWait(taskWorkerService);
  }

  private static Injector createSecurityInjector(CConfiguration cConf) {
    return Guice.createInjector(new ConfigModule(cConf), new IOModule(),
        new CoreSecurityRuntimeModule().getInMemoryModules());
  }

  private String internalToken(TokenManager signer, String username,
      UserIdentity.IdentifierType identifierType, long lifetimeMillis) throws IOException {
    long now = System.currentTimeMillis();
    UserIdentity identity = new UserIdentity(username, identifierType, Collections.emptyList(),
        now, now + lifetimeMillis);
    return "CDAP-Internal " + Base64.getEncoder().encodeToString(
        accessTokenCodec.encode(signer.signIdentifier(identity)));
  }

  private HttpResponse runTask(@Nullable String callerToken) throws IOException {
    InetSocketAddress address = taskWorkerService.getBindAddress();
    RunnableTaskRequest request = RunnableTaskRequest.getBuilder(
        TaskWorkerServiceTest.TestRunnableClass.class.getName())
        .withParam("0").withNamespace("testNamespace").build();
    URL url = new URL(String.format("http://%s:%d/v3Internal/worker/run", address.getHostName(),
        address.getPort()));
    HttpRequest.Builder builder = HttpRequest.post(url).withBody(GSON.toJson(request));
    if (callerToken != null) {
      builder.addHeader(Constants.Security.Headers.WORKER_CALLER_TOKEN, callerToken);
    }
    return HttpRequests.execute(builder.build(), new DefaultHttpRequestConfig(false));
  }
}
