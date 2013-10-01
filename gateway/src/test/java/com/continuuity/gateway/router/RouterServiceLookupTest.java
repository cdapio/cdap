package com.continuuity.gateway.router;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests RouterServiceLookup.
 */
public class RouterServiceLookupTest {
  @Test
  public void testNormalizeHost() throws Exception {
    Assert.assertEquals("www1_abc_com", RouterServiceLookup.normalizeHost("www1.abc.com:80"));
    Assert.assertEquals("www_abc_com", RouterServiceLookup.normalizeHost("www.abc.com:80"));

    Assert.assertEquals("www1_abc_com%3A8080", RouterServiceLookup.normalizeHost("www1.abc.com:8080"));
    Assert.assertEquals("www_abc_com%3A8080", RouterServiceLookup.normalizeHost("www.abc.com:8080"));

    Assert.assertEquals("www1_abc%40def_com", RouterServiceLookup.normalizeHost("www1.abc@def.com:80"));
    Assert.assertEquals("www_abc%40def_com", RouterServiceLookup.normalizeHost("www.abc@def.com:80"));

    Assert.assertEquals("www1_abc_def_com%3A8080", RouterServiceLookup.normalizeHost("www1.abc/def.com:8080"));
    Assert.assertEquals("www_abc%40def_com%3A8080", RouterServiceLookup.normalizeHost("www.abc@def.com:8080"));
  }
}
