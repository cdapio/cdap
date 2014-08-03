/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.common.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test Networks.
 */
public class NetworksTest {
  @Test
  public void testNormalizeHost() throws Exception {
    Assert.assertEquals("www1_abc_com", Networks.normalizeWebappDiscoveryName("www1.abc.com:80"));
    Assert.assertEquals("www_abc_com", Networks.normalizeWebappDiscoveryName("www.abc.com:80"));

    Assert.assertEquals("www1_abc_com_8080", Networks.normalizeWebappDiscoveryName("www1.abc.com:8080"));
    Assert.assertEquals("www_abc_com_8080", Networks.normalizeWebappDiscoveryName("www.abc.com:8080"));

    Assert.assertEquals("www1_abc_com_path", Networks.normalizeWebappDiscoveryName("www1.abc.com:80/path"));
    Assert.assertEquals("www_abc_com_path", Networks.normalizeWebappDiscoveryName("www.abc.com:80/path"));

    Assert.assertEquals("www1_abc_com_8080_path", Networks.normalizeWebappDiscoveryName("www1.abc.com:8080/path"));
    Assert.assertEquals("www_abc_com_8080_path", Networks.normalizeWebappDiscoveryName("www.abc.com:8080/path/"));

    Assert.assertEquals("www1_abc%40def_com", Networks.normalizeWebappDiscoveryName("www1.abc@def.com:80"));
    Assert.assertEquals("www_abc%40def_com", Networks.normalizeWebappDiscoveryName("www.abc@def.com:80"));

    Assert.assertEquals("www1_abc_def_com_8080", Networks.normalizeWebappDiscoveryName("www1.abc/def.com:8080"));
    Assert.assertEquals("www_abc%40def_com_8080", Networks.normalizeWebappDiscoveryName("www.abc@def.com:8080"));
  }
}
