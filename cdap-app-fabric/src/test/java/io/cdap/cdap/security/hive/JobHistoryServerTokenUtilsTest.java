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

package io.cdap.cdap.security.hive;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link JobHistoryServerTokenUtils}.
 */
public class JobHistoryServerTokenUtilsTest {

  @Test
  public void testObtainTokenReturnsSameCredentialsWhenSecurityDisabled() {
    Configuration conf = new Configuration();
    UserGroupInformation.setConfiguration(conf);
    Credentials credentials = new Credentials();
    Credentials result = JobHistoryServerTokenUtils.obtainToken(conf, credentials);
    Assert.assertSame(credentials, result);
  }

  @Test
  public void testObtainTokenHostAndPortValidationAndExceptionPropagation() {
    String prevRealm = System.getProperty("java.security.krb5.realm");
    String prevKdc = System.getProperty("java.security.krb5.kdc");
    System.setProperty("java.security.krb5.realm", "EXAMPLE.COM");
    System.setProperty("java.security.krb5.kdc", "localhost");
    Configuration kerberosConf = new Configuration();
    kerberosConf.set("hadoop.security.authentication", "kerberos");
    kerberosConf.set("hadoop.security.auth_to_local", "DEFAULT");
    try {
      UserGroupInformation.setConfiguration(kerberosConf);
      Credentials credentials = new Credentials();

      // Missing mapreduce.jobhistory.address fails in HostAndPort.fromString with NPE.
      Configuration missingHostConf = new Configuration(false);
      try {
        JobHistoryServerTokenUtils.obtainToken(missingHostConf, credentials);
        Assert.fail("Expected NullPointerException when mapreduce.jobhistory.address is missing");
      } catch (NullPointerException expected) {
        // Expected.
      }

      // Invalid port fails in HostAndPort.fromString with IllegalArgumentException.
      Configuration invalidPortConf = new Configuration(false);
      invalidPortConf.set("mapreduce.jobhistory.address", "127.0.0.1:99999");
      try {
        JobHistoryServerTokenUtils.obtainToken(invalidPortConf, credentials);
        Assert.fail("Expected IllegalArgumentException for out-of-range port");
      } catch (IllegalArgumentException expected) {
        // Expected.
      }

      // Checked IOException (missing YARN RM principal) is wrapped in RuntimeException.
      Configuration missingRmPrincipalConf = new Configuration(false);
      missingRmPrincipalConf.set("mapreduce.jobhistory.address", "127.0.0.1:10020");
      try {
        JobHistoryServerTokenUtils.obtainToken(missingRmPrincipalConf, credentials);
        Assert.fail("Expected RuntimeException wrapping IOException");
      } catch (RuntimeException caught) {
        Assert.assertTrue(caught.getCause() instanceof IOException);
      }

      // Valid host:port entering try block: unchecked RuntimeException propagates unwrapped.
      IllegalStateException uncheckedFailure = new IllegalStateException("simulated config error");
      Configuration throwingConf = new Configuration(false) {
        @Override
        public String get(String name) {
          if ("mapreduce.jobhistory.address".equals(name)) {
            return "127.0.0.1:10020";
          }
          throw uncheckedFailure;
        }
      };
      try {
        JobHistoryServerTokenUtils.obtainToken(throwingConf, credentials);
        Assert.fail("Expected IllegalStateException to propagate unwrapped");
      } catch (IllegalStateException caught) {
        Assert.assertSame(uncheckedFailure, caught);
      }
    } finally {
      UserGroupInformation.setConfiguration(new Configuration());
      if (prevRealm == null) {
        System.clearProperty("java.security.krb5.realm");
      } else {
        System.setProperty("java.security.krb5.realm", prevRealm);
      }
      if (prevKdc == null) {
        System.clearProperty("java.security.krb5.kdc");
      } else {
        System.setProperty("java.security.krb5.kdc", prevKdc);
      }
    }
  }
}
