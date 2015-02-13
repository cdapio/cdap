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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.conf.CConfiguration;
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
  public void testApplyingProxyRulesV2() {
    assertChange("/v2/data/explore/datasets/cdap.user.myTable/schema",
                 "/v2/data/explore/datasets/myTable/schema");
    assertChange("/v2/data/datasets/cdap.user.myTable", "/v2/data/datasets/myTable");
    assertChange("/v2/data/datasets/cdap.user.myTable/admin", "/v2/data/datasets/myTable/admin");
    assertChange("/v2/data/datasets/cdap.user.myTable/admin/truncate",
                 "/v2/data/datasets/myTable/admin/truncate");
    assertSame("/v2/data/types/myType");
    assertSame("/v2/metrics");
    assertSame("/v2/metrics/data/datasets/myTable");
  }

  @Test
  public void testApplyingProxyRulesV3() {
    assertChange("/v3/namespaces/myspace/data/explore/datasets/cdap.user.myTable/schema",
                 "/v3/namespaces/myspace/data/explore/datasets/myTable/schema");
    assertChange("/v3/namespaces/myspace/data/datasets/cdap.user.myTable",
                 "/v3/namespaces/myspace/data/datasets/myTable");
    assertChange("/v3/namespaces/myspace/data/datasets/cdap.user.myTable/admin",
                 "/v3/namespaces/myspace/data/datasets/myTable/admin");
    assertChange("/v3/namespaces/myspace/data/datasets/cdap.user.myTable/admin/truncate",
                 "/v3/namespaces/myspace/data/datasets/myTable/admin/truncate");
    assertSame("/v3/namespaces/myspace/data/types/myType");
    assertSame("/v3/namespaces/myspace/metrics");
    assertSame("/v3/namespaces/myspace/metrics/data/datasets/myTable");
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
