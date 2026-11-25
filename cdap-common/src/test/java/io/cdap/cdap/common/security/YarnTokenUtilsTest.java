/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.common.security;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link YarnTokenUtils}.
 */
public class YarnTokenUtilsTest {

  @After
  public void tearDown() {
    UserGroupInformation.setConfiguration(new Configuration());
    System.clearProperty("java.security.krb5.realm");
    System.clearProperty("java.security.krb5.kdc");
  }

  @Test
  public void testObtainTokenNoOpWhenSecurityDisabled() {
    UserGroupInformation.setConfiguration(new Configuration());
    Assert.assertFalse(UserGroupInformation.isSecurityEnabled());

    Credentials credentials = new Credentials();
    Credentials returned = YarnTokenUtils.obtainToken(new YarnConfiguration(), credentials);
    Assert.assertSame(credentials, returned);
    Assert.assertTrue(returned.getAllTokens().isEmpty());
  }

  @Test
  public void testObtainTokenPropagatesUncheckedAndCheckedFailuresWhenSecurityEnabled()
      throws Exception {
    System.setProperty("java.security.krb5.realm", "EXAMPLE.COM");
    System.setProperty("java.security.krb5.kdc", "127.0.0.1");
    Configuration secConf = new Configuration();
    secConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    secConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL,
        "RULE:[1:$1@$0](.*@EXAMPLE\\.COM)s/@.*//\nDEFAULT");
    UserGroupInformation.setConfiguration(secConf);
    Assert.assertTrue(UserGroupInformation.isSecurityEnabled());

    // 1. Passing null configuration triggers an unchecked exception in yarnClient.init(null)
    // which must be rethrown unwrapped (not double-wrapped in RuntimeException).
    try {
      YarnTokenUtils.obtainToken(null, new Credentials());
      Assert.fail("Expected unchecked RuntimeException");
    } catch (RuntimeException caught) {
      Assert.assertFalse(RuntimeException.class.equals(caught.getClass()));
    }

    // 2. Connecting to a closed local RM port with 0 retries triggers a checked IOException
    // wrapped in RuntimeException.
    int closedPort;
    try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
      closedPort = socket.getLocalPort();
    }

    YarnConfiguration failFastConf = new YarnConfiguration(secConf);
    failFastConf.set(YarnConfiguration.RM_ADDRESS, "127.0.0.1:" + closedPort);
    failFastConf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, 0L);
    failFastConf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, 10L);
    failFastConf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    failFastConf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, 0);

    try {
      YarnTokenUtils.obtainToken(failFastConf, new Credentials());
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException caught) {
      boolean hasIoCause = false;
      for (Throwable cur = caught.getCause(); cur != null; cur = cur.getCause()) {
        if (cur instanceof IOException) {
          hasIoCause = true;
          break;
        }
      }
      Assert.assertTrue(hasIoCause);
    }
  }
}
