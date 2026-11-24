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
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Base64;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link GrantAccessToken}.
 */
public class GrantAccessTokenTest {

  @Test
  public void testInitGrantAndDestroyLifecycle() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    Injector injector = Guice.createInjector(
        new IOModule(),
        new CoreSecurityRuntimeModule().getStandaloneModules(),
        new ConfigModule(cConf),
        new InMemoryDiscoveryModule());
    final TokenManager tokenManager = injector.getInstance(TokenManager.class);
    AccessTokenCodec tokenCodec = injector.getInstance(AccessTokenCodec.class);
    GrantAccessToken grantAccessToken = new GrantAccessToken(tokenManager, tokenCodec, cConf);

    Assert.assertFalse(tokenManager.isRunning());
    grantAccessToken.init();
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() {
        return tokenManager.isRunning();
      }
    }, 5, TimeUnit.SECONDS);

    // Calling init() again while TokenManager is already running must be a safe no-op.
    grantAccessToken.init();
    Assert.assertTrue(tokenManager.isRunning());

    try {
      HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
      Mockito.when(request.getUserPrincipal()).thenReturn(new Principal() {
        @Override
        public String getName() {
          return "alice";
        }
      });

      final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
      ServletOutputStream servletOut = new ServletOutputStream() {
        @Override
        public boolean isReady() {
          return true;
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {
        }

        @Override
        public void write(int b) {
          outBytes.write(b);
        }
      };
      HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
      Mockito.when(response.getOutputStream()).thenReturn(servletOut);

      Assert.assertEquals(200, grantAccessToken.token(request, response).getStatus());
      JsonObject json = new JsonParser().parse(new String(outBytes.toByteArray(), StandardCharsets.UTF_8))
          .getAsJsonObject();
      Assert.assertEquals(
          ExternalAuthenticationServer.ResponseFields.TOKEN_TYPE_BODY,
          json.get(ExternalAuthenticationServer.ResponseFields.TOKEN_TYPE).getAsString());
      String encodedToken = json.get(ExternalAuthenticationServer.ResponseFields.ACCESS_TOKEN).getAsString();
      AccessToken decoded = tokenCodec.decode(Base64.getDecoder().decode(encodedToken));
      tokenManager.validateSecret(decoded);
      Assert.assertEquals("alice", decoded.getIdentifier().getUsername());
    } finally {
      grantAccessToken.destroy();
    }

    Tasks.waitFor(Service.State.TERMINATED, new Callable<Service.State>() {
      @Override
      public Service.State call() {
        return tokenManager.state();
      }
    }, 5, TimeUnit.SECONDS);
  }
}
