package com.continuuity.gateway.router.handlers;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test inbound handler.
 */
public class InboundHandlerTest {

  @Test
  public void testNormalizeHost() throws Exception {
    Assert.assertEquals("abc.com", InboundHandler.normalizeHost("www.abc.com:80"));
    Assert.assertEquals("abc.com", InboundHandler.normalizeHost("www.abc.com"));
    Assert.assertEquals("abc.com", InboundHandler.normalizeHost("abc.com:80"));

    Assert.assertEquals("www1.abc.com", InboundHandler.normalizeHost("www1.abc.com:80"));
    Assert.assertEquals("1www.abc.com", InboundHandler.normalizeHost("1www.abc.com:80"));

    Assert.assertEquals("www1.abc.com:8080", InboundHandler.normalizeHost("www1.abc.com:8080"));
    Assert.assertEquals("abc.com:8080", InboundHandler.normalizeHost("abc.com:8080"));
  }
}
