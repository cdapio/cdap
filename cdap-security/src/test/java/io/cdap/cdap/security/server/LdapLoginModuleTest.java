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

import io.cdap.cdap.common.conf.Constants;
import java.lang.reflect.Field;
import java.security.KeyManagementException;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.net.SocketFactory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContextSpi;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link LdapLoginModule}.
 */
public class LdapLoginModuleTest {

  private static final String BROKEN_SSL_PROVIDER_NAME = "CdapTestBrokenSslProvider";
  private static final KeyManagementException EXPECTED_SSL_EXCEPTION =
      new KeyManagementException("Simulated SSL init failure");

  @After
  public void tearDown() {
    Security.removeProvider(BROKEN_SSL_PROVIDER_NAME);
    setLdapSslVerify(true);
  }

  @Test
  public void testGetEnvironmentAndTrustAllSslSocketFactory() throws Exception {
    setLdapSslVerify(true);
    LdapLoginModule moduleVerifyEnabled = createConfiguredModule();
    Hashtable<Object, Object> envEnabled = moduleVerifyEnabled.getEnvironment();
    Assert.assertNull(envEnabled.get("java.naming.ldap.factory.socket"));

    setLdapSslVerify(false);
    LdapLoginModule moduleVerifyDisabled = createConfiguredModule();
    Hashtable<Object, Object> envDisabled = moduleVerifyDisabled.getEnvironment();
    Assert.assertEquals(
        LdapLoginModule.TrustAllSslSocketFactory.class.getName(),
        envDisabled.get("java.naming.ldap.factory.socket"));

    SocketFactory socketFactory = LdapLoginModule.TrustAllSslSocketFactory.getDefault();
    Assert.assertTrue(socketFactory instanceof LdapLoginModule.TrustAllSslSocketFactory);
  }

  @Test
  public void testTrustAllSslSocketFactoryPropagatesGeneralSecurityException() {
    Provider brokenProvider = new BrokenSslProvider();
    Security.insertProviderAt(brokenProvider, 1);
    try {
      LdapLoginModule.TrustAllSslSocketFactory.getDefault();
      Assert.fail("Expected RuntimeException wrapping KeyManagementException");
    } catch (RuntimeException caught) {
      Assert.assertSame(EXPECTED_SSL_EXCEPTION, caught.getCause());
    } finally {
      Security.removeProvider(BROKEN_SSL_PROVIDER_NAME);
    }
  }

  private static void setLdapSslVerify(boolean verify) {
    Map<String, String> props = new HashMap<>();
    props.put(Constants.Security.LOGIN_MODULE_CLASS_NAME, LdapLoginModule.class.getName());
    props.put("debug", "false");
    props.put("hostname", "127.0.0.1");
    props.put("port", "636");
    props.put("userBaseDn", "dc=example,dc=com");
    props.put("userRdnAttribute", "cn");
    props.put("userObjectClass", "inetorgperson");
    props.put("useLdaps", "true");
    props.put("ldapsVerifyCertificate", Boolean.toString(verify));

    LdapAuthenticationHandler handler = new LdapAuthenticationHandler();
    handler.handlerProps = props;
    handler.getLoginModuleConfiguration().getAppConfigurationEntry("jaas");
  }

  private static LdapLoginModule createConfiguredModule() throws Exception {
    LdapLoginModule module = new LdapLoginModule();
    Field contextFactoryField =
        org.eclipse.jetty.jaas.spi.LdapLoginModule.class.getDeclaredField("_contextFactory");
    contextFactoryField.setAccessible(true);
    contextFactoryField.set(module, "com.sun.jndi.ldap.LdapCtxFactory");
    return module;
  }

  private static final class BrokenSslProvider extends Provider {
    BrokenSslProvider() {
      super(BROKEN_SSL_PROVIDER_NAME, 1.0, "Test provider failing SSLContext.init");
      put("SSLContext.SSL", BrokenSslContextSpi.class.getName());
    }
  }

  /**
   * Public {@link SSLContextSpi} instantiated reflectively by JCA to simulate {@link KeyManagementException}.
   */
  public static final class BrokenSslContextSpi extends SSLContextSpi {
    @Override
    protected void engineInit(KeyManager[] km, TrustManager[] tm, SecureRandom sr) throws KeyManagementException {
      throw EXPECTED_SSL_EXCEPTION;
    }

    @Override
    protected SSLSocketFactory engineGetSocketFactory() {
      return null;
    }

    @Override
    protected SSLServerSocketFactory engineGetServerSocketFactory() {
      return null;
    }

    @Override
    protected SSLEngine engineCreateSSLEngine() {
      return null;
    }

    @Override
    protected SSLEngine engineCreateSSLEngine(String host, int port) {
      return null;
    }

    @Override
    protected SSLSessionContext engineGetServerSessionContext() {
      return null;
    }

    @Override
    protected SSLSessionContext engineGetClientSessionContext() {
      return null;
    }
  }
}
