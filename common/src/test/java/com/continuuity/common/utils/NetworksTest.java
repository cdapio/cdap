package com.continuuity.common.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test Networks.
 */
public class NetworksTest {
  @Test
  public void testNormalizeHost() throws Exception {
    Assert.assertEquals("www1_abc_com", Networks.normalizeWebappHost("www1.abc.com:80"));
    Assert.assertEquals("www_abc_com", Networks.normalizeWebappHost("www.abc.com:80"));

    Assert.assertEquals("www1_abc_com_8080", Networks.normalizeWebappHost("www1.abc.com:8080"));
    Assert.assertEquals("www_abc_com_8080", Networks.normalizeWebappHost("www.abc.com:8080"));

    Assert.assertEquals("www1_abc%40def_com", Networks.normalizeWebappHost("www1.abc@def.com:80"));
    Assert.assertEquals("www_abc%40def_com", Networks.normalizeWebappHost("www.abc@def.com:80"));

    Assert.assertEquals("www1_abc_def_com_8080", Networks.normalizeWebappHost("www1.abc/def.com:8080"));
    Assert.assertEquals("www_abc%40def_com_8080", Networks.normalizeWebappHost("www.abc@def.com:8080"));
  }
}
