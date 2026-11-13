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
import java.net.URI;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RouterServiceLookupInternalApiTest {

  private static final HttpVersion VERSION = HttpVersion.HTTP_1_1;
  private static final List<HttpMethod> ALL_HTTP_METHODS = ImmutableList.of(
      HttpMethod.GET, HttpMethod.PUT, HttpMethod.POST, HttpMethod.DELETE, HttpMethod.PATCH,
      HttpMethod.OPTIONS, HttpMethod.HEAD, HttpMethod.TRACE, HttpMethod.CONNECT);
  private static final List<HttpMethod> NON_GET_HTTP_METHODS = ImmutableList.of(
      HttpMethod.PUT, HttpMethod.POST, HttpMethod.DELETE, HttpMethod.PATCH, HttpMethod.OPTIONS,
      HttpMethod.HEAD, HttpMethod.TRACE, HttpMethod.CONNECT);
  private static final String FILE_PATH = "/location/cdap/twill/app/run/launcher.jar";
  private static final String ARTIFACT_PATH =
      "/v3Internal/namespaces/default/artifacts/cdap-data-pipeline/versions/6.12.0";
  private static final String SYSTEM_ARTIFACT_PATH =
      "/v3Internal/namespaces/system/artifacts/cdap-data-pipeline/versions/6.12.0";

  private static RouterServiceLookup serviceLookup;

  @BeforeClass
  public static void init() {
    serviceLookup = new RouterServiceLookup(CConfiguration.create(), new InMemoryDiscoveryService(),
        new RouterPathLookup());
  }

  @Test
  public void testInternalApiWithQueryStringIsNotRouted() {
    assertNotRouted(ALL_HTTP_METHODS, "/v3Internal" + FILE_PATH + "?unused=1");
  }

  @Test
  public void testInternalApiWithRepeatedSlashesIsNotRouted() {
    assertNotRouted(ALL_HTTP_METHODS, "///v3Internal" + FILE_PATH, "/v3Internal/" + FILE_PATH);
  }

  @Test
  public void testPercentEncodedInternalApiIsNotRouted() {
    assertNotRouted(ALL_HTTP_METHODS,
        "/%76%33%49%6e%74%65%72%6e%61%6c" + FILE_PATH,
        "/v3Internal%2Flocation%2Fcdap%2Ftwill%2Fapp%2Frun%2Flauncher.jar",
        "/%2Fv3Internal" + FILE_PATH);
  }

  @Test
  public void testInternalApiReachedThroughDotSegmentsIsNotRouted() {
    assertNotRouted(ALL_HTTP_METHODS,
        "/./v3Internal" + FILE_PATH,
        "/a/../v3Internal" + FILE_PATH,
        "/v3/../v3Internal" + FILE_PATH,
        "/v3Internal/x/.." + FILE_PATH);
  }

  @Test
  public void testAbsoluteFormInternalApiIsNotRouted() {
    assertNotRouted(ALL_HTTP_METHODS,
        "http://example.com/v3Internal" + FILE_PATH,
        "http://example.com/v3Internal" + FILE_PATH + "?unused=1");
  }

  @Test
  public void testTetheredPeerArtifactReadsAreStillRouted() {
    assertRouted(HttpMethod.GET,
        ARTIFACT_PATH,
        ARTIFACT_PATH + "?scope=user",
        ARTIFACT_PATH + "/download?scope=user",
        SYSTEM_ARTIFACT_PATH + "/download?scope=system");
  }

  @Test
  public void testTetheredPeerArtifactPathsAreNotRoutedForNonGetMethods() {
    assertNotRouted(NON_GET_HTTP_METHODS, ARTIFACT_PATH, ARTIFACT_PATH + "/download");
  }

  @Test
  public void testArtifactLocationIsNotRouted() {
    assertNotRouted(ALL_HTTP_METHODS, ARTIFACT_PATH + "/location");
  }

  @Test
  public void testOtherPathsAreStillRouted() {
    assertRouted(HttpMethod.GET, "/v3/namespaces", "/v3Internalx" + FILE_PATH);
  }

  @Test
  public void testLeadingDoubleSlashMakesV3InternalTheAuthorityNotThePath() {
    String uri = "//v3Internal" + FILE_PATH;
    Assert.assertEquals(FILE_PATH, URI.create(uri).normalize().getPath());
    assertRouted(HttpMethod.GET, uri);
  }

  private void assertRouted(HttpMethod method, String... uris) {
    for (String uri : uris) {
      Assert.assertNotNull(method + " " + uri, lookup(method, uri));
    }
  }

  private void assertNotRouted(List<HttpMethod> methods, String... uris) {
    for (String uri : uris) {
      for (HttpMethod method : methods) {
        Assert.assertNull(method + " " + uri, lookup(method, uri));
      }
    }
  }

  @Nullable
  private EndpointStrategy lookup(HttpMethod method, String uri) {
    return serviceLookup.getDiscoverable(new DefaultHttpRequest(VERSION, method, uri));
  }
}
