package com.continuuity.gateway.router;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests RouterServiceLookup.
 */
public class RouterServiceLookupTest {
  @Test
  public void testNormalizeHost() throws Exception {
    Assert.assertEquals("www1.abc.com", RouterServiceLookup.normalizeHost("www1.abc.com:80"));
    Assert.assertEquals("www.abc.com", RouterServiceLookup.normalizeHost("www.abc.com:80"));

    Assert.assertEquals("www1.abc.com%3A8080", RouterServiceLookup.normalizeHost("www1.abc.com:8080"));
    Assert.assertEquals("www.abc.com%3A8080", RouterServiceLookup.normalizeHost("www.abc.com:8080"));

    Assert.assertEquals("www1.abc%40def.com", RouterServiceLookup.normalizeHost("www1.abc@def.com:80"));
    Assert.assertEquals("www.abc%40def.com", RouterServiceLookup.normalizeHost("www.abc@def.com:80"));

    Assert.assertEquals("www1.abc%40def.com%3A8080", RouterServiceLookup.normalizeHost("www1.abc@def.com:8080"));
    Assert.assertEquals("www.abc%40def.com%3A8080", RouterServiceLookup.normalizeHost("www.abc@def.com:8080"));
  }
}
