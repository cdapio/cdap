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

package io.cdap.cdap.security.runtime;

import com.google.common.util.concurrent.AbstractIdleService;
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
import io.cdap.cdap.security.server.AbstractAuthenticationHandler;
import io.cdap.cdap.security.server.AuditLogHandler;
import io.cdap.cdap.security.server.ExternalAuthenticationServer;
import io.cdap.cdap.security.server.GrantAccessToken;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.Configuration;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.zookeeper.ZKClientService;
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
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link AuthenticationServerMain}.
 */
public class AuthenticationServerMainTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testStartAndStopWithSecurityDisabled() throws Exception {
    AuthenticationServerMain serverMain = new AuthenticationServerMain();
    serverMain.init(new String[0]);
    serverMain.start();
    serverMain.stop();
    serverMain.destroy();
  }

  @Test
  public void testStartAndStopStartsAndStopsExternalAuthServer() throws Exception {
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

    ExternalAuthenticationServer authServer = new ExternalAuthenticationServer(
        cConf, SConfiguration.create(), new InMemoryDiscoveryService(), handlers,
        new AuditLogHandler(LoggerFactory.getLogger(AuthenticationServerMainTest.class)));

    final AbstractIdleService backingService = new AbstractIdleService() {
      @Override
      protected void startUp() {
      }

      @Override
      protected void shutDown() {
      }
    };
    ZKClientService zkClientService = Mockito.mock(ZKClientService.class, invocation -> {
      Method method = invocation.getMethod();
      if (method.getDeclaringClass().isAssignableFrom(Service.class)) {
        return method.invoke(backingService, invocation.getArguments());
      }
      if ("getConnectString".equals(method.getName())) {
        return "127.0.0.1:2181";
      }
      return Mockito.RETURNS_DEFAULTS.answer(invocation);
    });

    AuthenticationServerMain serverMain = new AuthenticationServerMain();
    setField(serverMain, "configuration", cConf);
    setField(serverMain, "zkClientService", zkClientService);
    setField(serverMain, "authServer", authServer);

    serverMain.start();
    try {
      Assert.assertTrue(zkClientService.isRunning());
      Assert.assertTrue(authServer.isRunning());
      Assert.assertTrue(authServer.getSocketAddress().getPort() > 0);
    } finally {
      serverMain.stop();
      serverMain.destroy();
    }

    Assert.assertEquals(Service.State.TERMINATED, authServer.state());
    Assert.assertEquals(Service.State.TERMINATED, zkClientService.state());
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static final class TestAuthHandler extends AbstractAuthenticationHandler {
    private final String realmPath;
    private IdentityService identityService;

    TestAuthHandler(String realmPath) throws IOException {
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
