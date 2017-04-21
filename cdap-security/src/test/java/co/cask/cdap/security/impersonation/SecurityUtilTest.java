/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.security.impersonation;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Tests for {@link SecurityUtil}
 */
public class SecurityUtilTest {

  @Test
  public void testExpandPrincipal() throws Exception {
    String localHostname = InetAddress.getLocalHost().getCanonicalHostName();
    Assert.assertNull(SecurityUtil.expandPrincipal(null));
    Assert.assertEquals("user/" + localHostname + "@REALM.NET", SecurityUtil.expandPrincipal("user/_HOST@REALM.NET"));
    Assert.assertEquals("user/abc.com@REALM.NET", SecurityUtil.expandPrincipal("user/abc.com@REALM.NET"));
    Assert.assertEquals("_HOST/abc.com@REALM.NET", SecurityUtil.expandPrincipal("_HOST/abc.com@REALM.NET"));
    Assert.assertEquals("_HOST/" + localHostname + "@REALM.NET", SecurityUtil.expandPrincipal("_HOST/_HOST@REALM.NET"));
    Assert.assertEquals("user/_host@REALM.NET", SecurityUtil.expandPrincipal("user/_host@REALM.NET"));
  }

  @Test
  public void isKerberosEnabled() throws Exception {
    CConfiguration kerbConf = CConfiguration.create();
    kerbConf.set(Constants.Security.KERBEROS_ENABLED, "true");
    kerbConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL, "prinicpal@REALM.NET");
    kerbConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH, "/path/to/keytab");
    Assert.assertTrue(SecurityUtil.isKerberosEnabled(kerbConf));

    CConfiguration noPrincipalConf = CConfiguration.create();
    kerbConf.set(Constants.Security.KERBEROS_ENABLED, "false");
    noPrincipalConf.unset(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL);
    noPrincipalConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH, "/path/to/keytab");
    Assert.assertFalse(SecurityUtil.isKerberosEnabled(noPrincipalConf));

    CConfiguration noKeyTabConf = CConfiguration.create();
    kerbConf.set(Constants.Security.KERBEROS_ENABLED, "false");
    noKeyTabConf.unset(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH);
    noKeyTabConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL, "prinicpal@REALM.NET");
    Assert.assertFalse(SecurityUtil.isKerberosEnabled(noKeyTabConf));
  }

  @Test
  public void testGetKeytabURIforUser() throws IOException {
    String user = "alice";
    String confPath = "/dir1/dir2/${name}/${name}.keytab";
    String expectedPath = "/dir1/dir2/alice/alice.keytab";
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Security.KEYTAB_PATH, confPath);
    cConf.set("user", "blah blah");

    String path = SecurityUtil.getKeytabURIforPrincipal(user, cConf);
    Assert.assertEquals(expectedPath, path);
  }

  @Test
  public void testNoKeytabPath() throws IOException {
    String user = "alice";
    CConfiguration cConf = CConfiguration.create();
    // this should throw a null pointer exception with proper message as what to set
    try {
      SecurityUtil.getKeytabURIforPrincipal(user, cConf);
      Assert.fail();
    } catch (NullPointerException e) {
      // expected
      Assert.assertTrue(e.getMessage().contains(Constants.Security.KEYTAB_PATH));
    }
  }
}
