/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.kerberos;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;

/**
 *
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
  }

  @Test
  public void testStripPrincipal() throws Exception {
    Assert.assertNull(SecurityUtil.stripPrincipal(null));
    Assert.assertEquals("abc", SecurityUtil.stripPrincipal("abc/host@REALM.COM"));
    Assert.assertEquals("abc", SecurityUtil.stripPrincipal("abc"));
    Assert.assertEquals("abc", SecurityUtil.stripPrincipal("abc@REALM.COM"));
    Assert.assertEquals("abc", SecurityUtil.stripPrincipal("abc/host"));
  }
}
