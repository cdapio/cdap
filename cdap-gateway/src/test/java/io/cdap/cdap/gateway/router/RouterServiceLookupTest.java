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

package io.cdap.cdap.gateway.router;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import javax.annotation.Nullable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that the router refuses to forward the internal {@code /v1} API however the request URI is
 * spelled. {@link RouterPathLookupTest} covers the path decision itself. These tests go through
 * {@link RouterServiceLookup}, which normalizes the URI the same way before it asks
 * {@link RouterPathLookup}, so they pin that normalization as well.
 */
public class RouterServiceLookupTest {

  private static final HttpVersion VERSION = HttpVersion.HTTP_1_1;

  private static RouterServiceLookup serviceLookup;

  @BeforeClass
  public static void init() {
    serviceLookup = new RouterServiceLookup(CConfiguration.create(), new InMemoryDiscoveryService(),
        new RouterPathLookup());
  }

  @Test
  public void testInternalApiIsNotRoutedForAnyMethod() {
    for (HttpMethod method : ImmutableList.of(HttpMethod.GET, HttpMethod.PUT, HttpMethod.POST,
        HttpMethod.DELETE, HttpMethod.PATCH, HttpMethod.OPTIONS, HttpMethod.HEAD)) {
      assertNotRouted(method, "/v1/namespaces/system/topics");
      assertNotRouted(method, "/v1/execute/listPrivileges");
    }
  }

  @Test
  public void testInternalApiIsNotRoutedWhateverTheSpelling() {
    for (String uri : ImmutableList.of(
        "/v1/namespaces/system/topics?limit=1",
        // percent-encoded characters, including an encoded slash
        "/%76%31/namespaces/system/topics",
        "/v1%2Fnamespaces%2Fsystem%2Ftopics",
        // dot segments that normalize into /v1
        "/./v1/namespaces/system/topics",
        "/a/../v1/namespaces/system/topics",
        "/v3/../v1/namespaces/system/topics",
        "/v1/x/../namespaces/system/topics",
        // absolute-form request target
        "http://example.com/v1/namespaces/system/topics",
        "http://example.com/v1/namespaces/system/topics?limit=1")) {
      assertNotRouted(HttpMethod.GET, uri);
    }
  }

  @Test
  public void testOtherPathsAreStillRouted() {
    // Without these, the tests above would also pass if nothing was routable at all.
    for (String uri : ImmutableList.of(
        "/v3/namespaces",
        "/v3Internal/namespaces/default/apps/a/states/s",
        "/v1x/a")) {
      Assert.assertNotNull(uri, lookup(HttpMethod.GET, uri));
    }
  }

  @Nullable
  private EndpointStrategy lookup(HttpMethod method, String uri) {
    return serviceLookup.getDiscoverable(new DefaultHttpRequest(VERSION, method, uri));
  }

  private void assertNotRouted(HttpMethod method, String uri) {
    Assert.assertNull(method + " " + uri, lookup(method, uri));
  }
}
