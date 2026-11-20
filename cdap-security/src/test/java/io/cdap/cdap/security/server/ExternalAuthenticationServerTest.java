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

package io.cdap.cdap.security.server;

import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import java.io.File;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.auth.login.Configuration;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

/**
 * Fast unit tests for {@link ExternalAuthenticationServer}.
 */
public class ExternalAuthenticationServerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testStartStopStatusAndExecutorThreadNaming() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Security.AUTH_SERVER_BIND_ADDRESS, "127.0.0.1");
    cConf.setInt(Constants.Security.AUTH_SERVER_BIND_PORT, 0);

    Injector injector = Guice.createInjector(
        new IOModule(),
        new CoreSecurityRuntimeModule().getStandaloneModules(),
        new ConfigModule(cConf),
        new InMemoryDiscoveryModule());
    TokenManager tokenManager = injector.getInstance(TokenManager.class);
    AccessTokenCodec tokenCodec = injector.getInstance(AccessTokenCodec.class);
    GrantAccessToken grantAccessToken = new GrantAccessToken(tokenManager, tokenCodec, cConf);

    File realmFile = TEMP_FOLDER.newFile("realm.properties");
    Map<String, Object> handlers = new HashMap<>();
    handlers.put(
        ExternalAuthenticationServer.HandlerType.AUTHENTICATION_HANDLER,
        new TestAuthHandler(realmFile.getAbsolutePath()));
    handlers.put(ExternalAuthenticationServer.HandlerType.GRANT_TOKEN_HANDLER, grantAccessToken);

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    ExternalAuthenticationServer server = new ExternalAuthenticationServer(
        cConf, SConfiguration.create(), discoveryService, handlers,
        new AuditLogHandler(LoggerFactory.getLogger(ExternalAuthenticationServerTest.class)));

    Executor executor = server.executor(Service.State.STARTING);
    final AtomicReference<String> threadName = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(1);
    executor.execute(new Runnable() {
      @Override
      public void run() {
        threadName.set(Thread.currentThread().getName());
        latch.countDown();
      }
    });
    Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
    Assert.assertEquals("ExternalAuthenticationServer-1", threadName.get());

    server.startAndWait();
    try {
      Assert.assertTrue(server.isRunning());
      InetSocketAddress address = server.getSocketAddress();
      Assert.assertTrue(address.getPort() > 0);

      boolean registered = false;
      for (Discoverable discoverable : discoveryService.discover(Constants.Service.EXTERNAL_AUTHENTICATION)) {
        if (discoverable.getSocketAddress().equals(address)) {
          registered = true;
          break;
        }
      }
      Assert.assertTrue(registered);

      URL statusUrl = new URL(String.format(
          "http://%s:%d%s", address.getHostString(), address.getPort(), Constants.EndPoints.STATUS));
      HttpURLConnection connection = (HttpURLConnection) statusUrl.openConnection();
      try {
        Assert.assertEquals(200, connection.getResponseCode());
      } finally {
        connection.disconnect();
      }
    } finally {
      server.stopAndWait();
    }

    Assert.assertEquals(Service.State.TERMINATED, server.state());
    try {
      server.getSocketAddress();
      Assert.fail("Expected IllegalStateException after server stopped");
    } catch (IllegalStateException expected) {
      // expected
    }
  }

  private static final class TestAuthHandler extends AbstractAuthenticationHandler {
    private final String realmPath;
    private IdentityService identityService;

    TestAuthHandler(String realmPath) {
      this.realmPath = realmPath;
    }

    @Override
    protected LoginService getHandlerLoginService() {
      HashLoginService loginService = new HashLoginService("test-realm", realmPath);
      loginService.setIdentityService(getHandlerIdentityService());
      return loginService;
    }

    @Override
    protected Authenticator getHandlerAuthenticator() {
      return new BasicAuthenticator();
    }

    @Override
    protected IdentityService getHandlerIdentityService() {
      if (identityService == null) {
        identityService = new DefaultIdentityService();
      }
      return identityService;
    }

    @Override
    protected Configuration getLoginModuleConfiguration() {
      return null;
    }
  }
}
