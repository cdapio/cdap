package com.continuuity.gateway.router;

import com.continuuity.common.conf.CConfiguration;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DatasetsProxyRuleTest {
  private static final HttpVersion VERSION = HttpVersion.HTTP_1_1;

  private static DatasetsProxyRule rule = new DatasetsProxyRule(CConfiguration.create());

  @Test
  public void testApplyingProxyRules() {
    assertChange("/v2/data/datasets/continuuity.user.myTable", "/v2/data/datasets/myTable");
    assertChange("/v2/data/datasets/continuuity.user.myTable/admin", "/v2/data/datasets/myTable/admin");
    assertChange("/v2/data/datasets/continuuity.user.myTable/admin/truncate",
                 "/v2/data/datasets/myTable/admin/truncate");
    assertSame("/v2/data/types/myType");
    assertSame("/v2/metrics");
    assertSame("/v2/metrics/data/datasets/myTable");
  }

  private void assertSame(String original) {
    assertChange(original, original);
  }

  private void assertChange(String expected, String original) {
    for (String method : new String[] {"GET", "POST", "DELETE", "PUT"}) {
      HttpRequest changed = rule.apply(new DefaultHttpRequest(VERSION, new HttpMethod(method), original));
      Assert.assertEquals(expected, changed.getUri());
    }
  }
}
