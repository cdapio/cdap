/*
 * Copyright © 2016-2023 Cask Data, Inc.
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

package io.cdap.cdap.master.startup;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link MasterStartupTool}.
 */
public class MasterStartupToolTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testInjector() throws Exception {
    MasterStartupTool.createInjector(CConfiguration.create(), new Configuration());
    // should not throw exception
  }

  @Test
  public void testMainChecksDisabledAndLoginExceptionPropagation() throws Exception {
    CConfiguration disabledConf = CConfiguration.create();
    disabledConf.setBoolean(Constants.Startup.CHECKS_ENABLED, false);
    runWithSiteConf(disabledConf, () -> MasterStartupTool.main(new String[0]));

    javax.security.auth.login.Configuration prevJaas = getJaasConfigurationQuietly();
    String prevRealm = System.getProperty("java.security.krb5.realm");
    String prevKdc = System.getProperty("java.security.krb5.kdc");
    System.setProperty("java.security.krb5.realm", "EXAMPLE.COM");
    System.setProperty("java.security.krb5.kdc", "localhost");
    Configuration krbHadoopConf = new Configuration(false);
    krbHadoopConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    try {
      UserGroupInformation.setConfiguration(krbHadoopConf);

      CConfiguration missingPrincipalConf = CConfiguration.create();
      missingPrincipalConf.setBoolean(Constants.Startup.CHECKS_ENABLED, true);
      missingPrincipalConf.setBoolean(Constants.Security.ENABLED, true);
      missingPrincipalConf.setBoolean(Constants.Security.KERBEROS_ENABLED, true);
      try {
        runWithSiteConf(missingPrincipalConf, () -> MasterStartupTool.main(new String[0]));
        Assert.fail("Expected IllegalArgumentException directly");
      } catch (IllegalArgumentException expected) {
        Assert.assertNotNull(expected.getMessage());
      }

      File emptyKeytab = tempFolder.newFile("empty-startup.keytab");
      CConfiguration invalidKeytabConf = CConfiguration.create();
      invalidKeytabConf.setBoolean(Constants.Startup.CHECKS_ENABLED, true);
      invalidKeytabConf.setBoolean(Constants.Security.ENABLED, true);
      invalidKeytabConf.setBoolean(Constants.Security.KERBEROS_ENABLED, true);
      invalidKeytabConf.set(
          Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL, "cdap/localhost@EXAMPLE.COM");
      invalidKeytabConf.set(
          Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH, emptyKeytab.getAbsolutePath());
      try {
        runWithSiteConf(invalidKeytabConf, () -> MasterStartupTool.main(new String[0]));
        Assert.fail("Expected RuntimeException wrapping IOException");
      } catch (RuntimeException actual) {
        Assert.assertTrue(
            "Expected IOException cause, got: " + actual.getCause(),
            actual.getCause() instanceof IOException);
      }
    } finally {
      UserGroupInformation.setConfiguration(new Configuration());
      javax.security.auth.login.Configuration.setConfiguration(prevJaas);
      restoreProperty("java.security.krb5.realm", prevRealm);
      restoreProperty("java.security.krb5.kdc", prevKdc);
      System.clearProperty(Constants.External.Zookeeper.ENV_AUTH_PROVIDER_1);
      System.clearProperty(Constants.External.Zookeeper.ENV_ALLOW_SASL_FAILED_CLIENTS);
      System.clearProperty("zookeeper.sasl.clientconfig");
    }
  }

  private static void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  private static javax.security.auth.login.Configuration getJaasConfigurationQuietly() {
    try {
      return javax.security.auth.login.Configuration.getConfiguration();
    } catch (SecurityException e) {
      return null;
    }
  }

  private void runWithSiteConf(CConfiguration siteConf, Runnable action) throws Exception {
    File siteFile = new File(tempFolder.newFolder(), "cdap-site.xml");
    try (FileOutputStream out = new FileOutputStream(siteFile)) {
      siteConf.writeXml(out);
    }
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
