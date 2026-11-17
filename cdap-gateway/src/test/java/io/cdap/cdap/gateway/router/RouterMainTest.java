/*
 * Copyright © 2014-2023 Cask Data, Inc.
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

package io.cdap.cdap.gateway.router;

import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Enumeration;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test RouterMain.
 */
public class RouterMainTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testGuiceInjection() {
    CConfiguration cConf = CConfiguration.create();

    Injector injector = RouterMain.createGuiceInjector(cConf);
    Assert.assertNotNull(injector);

    NettyRouter router = injector.getInstance(NettyRouter.class);
    Assert.assertNotNull(router);
  }

  @Test
  public void testInitStartAndStop() throws Exception {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().build();
    zkServer.startAsync().awaitRunning();
    try {
      String siteXml = "<configuration>"
          + "<property><name>" + Constants.Zookeeper.QUORUM + "</name><value>"
          + zkServer.getConnectionStr() + "</value></property>"
          + "<property><name>" + Constants.Router.ADDRESS + "</name><value>"
          + InetAddress.getLoopbackAddress().getHostAddress() + "</value></property>"
          + "<property><name>" + Constants.Router.ROUTER_PORT + "</name><value>0</value></property>"
          + "<property><name>" + Constants.Router.CCONF_RELOAD_INTERVAL_SECONDS
          + "</name><value>0</value></property>"
          + "</configuration>";

      RouterMain routerMain = new RouterMain();
      runWithSiteXml(siteXml, () -> {
        routerMain.init(new String[0]);
        routerMain.start();
        try {
          Assert.assertNotNull(routerMain);
        } finally {
          routerMain.stop();
          routerMain.destroy();
        }
      });
    } finally {
      zkServer.stopAsync().awaitTerminated();
    }
  }

  @Test
  public void testInitPropagatesUncheckedAndWrapsCheckedThrowable() throws Exception {
    String invalidIntXml = "<configuration>"
        + "<property><name>" + Constants.Router.SERVER_BOSS_THREADS
        + "</name><value>not-an-integer</value></property>"
        + "</configuration>";

    try {
      runWithSiteXml(invalidIntXml, () -> new RouterMain().init(new String[0]));
      Assert.fail("Expected ProvisionException directly");
    } catch (ProvisionException expected) {
      Assert.assertNotNull(expected.getCause());
    }

    String badKerberosXml = "<configuration>"
        + "<property><name>" + Constants.Security.ENABLED + "</name><value>true</value></property>"
        + "<property><name>" + Constants.Router.ROUTER_AUDIT_PATH_CHECK_ENABLED
        + "</name><value>false</value></property>"
        + "<property><name>" + Constants.Security.KERBEROS_ENABLED
        + "</name><value>true</value></property>"
        + "<property><name>" + Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL
        + "</name><value>cdap/localhost@EXAMPLE.COM</value></property>"
        + "<property><name>" + Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH
        + "</name><value>/nonexistent/path/cdap.keytab</value></property>"
        + "</configuration>";

    try {
      runWithSiteXml(badKerberosXml, () -> new RouterMain().init(new String[0]));
      Assert.fail("Expected IllegalArgumentException from unreadable Kerberos keytab");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue(expected.getMessage().contains("cdap.keytab"));
    }

    Error loaderError = new AssertionError("simulated classloader error");
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(new ClassLoader(previous) {
      @Override
      public URL getResource(String name) {
        if ("cdap-site.xml".equals(name)) {
          throw loaderError;
        }
        return super.getResource(name);
      }
    });
    try {
      new RouterMain().init(new String[0]);
      Assert.fail("Expected Error propagated directly");
    } catch (AssertionError actual) {
      Assert.assertSame(loaderError, actual);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private void runWithSiteXml(String siteXml, ThrowingRunnable action) throws Exception {
    File siteFile = new File(tempFolder.newFolder(), "cdap-site.xml");
    Files.write(siteFile.toPath(), siteXml.getBytes(StandardCharsets.UTF_8));
    URL fileUrl = siteFile.toURI().toURL();

    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    ClassLoader customLoader = new ClassLoader(previous) {
      @Override
      public URL getResource(String name) {
        if ("cdap-site.xml".equals(name)) {
          return fileUrl;
        }
        return super.getResource(name);
      }

      @Override
      public Enumeration<URL> getResources(String name) throws IOException {
        if ("cdap-site.xml".equals(name)) {
          return Collections.enumeration(Collections.singletonList(fileUrl));
        }
        return super.getResources(name);
      }
    };

    Thread.currentThread().setContextClassLoader(customLoader);
    try {
      action.run();
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }
}
