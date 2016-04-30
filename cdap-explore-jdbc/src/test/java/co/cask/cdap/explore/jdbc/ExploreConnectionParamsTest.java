/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.explore.jdbc;

import co.cask.cdap.common.conf.Constants;
import com.google.common.collect.ImmutableMultimap;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for explore connection params.
 */
public class ExploreConnectionParamsTest {
  private static final String BASE = String.format("%s%s:%d", Constants.Explore.Jdbc.URL_PREFIX, "foobar", 10000);

  @Test
  public void testParseFetchSize() {
    ExploreConnectionParams connectionParams = ExploreConnectionParams.parseConnectionUrl(BASE + "?fetch.size=500");
    Assert.assertEquals(500, connectionParams.getFetchSize());
    connectionParams = ExploreConnectionParams.parseConnectionUrl(BASE + "?fetchSize=12asd");
    Assert.assertEquals(ExploreConnectionParams.DEFAULT_FETCH_SIZE, connectionParams.getFetchSize());
  }

  @Test
  public void parseConnectionUrlTest() {
    ExploreConnectionParams connectionParams;

    connectionParams = ExploreConnectionParams.parseConnectionUrl(BASE);
    Assert.assertEquals("foobar", connectionParams.getHost());
    Assert.assertEquals(10000, connectionParams.getPort());
    Assert.assertEquals(ImmutableMultimap.of(), connectionParams.getExtraInfos());

    connectionParams = ExploreConnectionParams.parseConnectionUrl(BASE + "?auth.token=foo");
    Assert.assertEquals("foobar", connectionParams.getHost());
    Assert.assertEquals(10000, connectionParams.getPort());
    Assert.assertEquals(
      ImmutableMultimap.of(ExploreConnectionParams.Info.EXPLORE_AUTH_TOKEN, "foo"),
      connectionParams.getExtraInfos());

    connectionParams = ExploreConnectionParams.parseConnectionUrl(BASE + "?auth.token=foo&foo2=bar2");
    Assert.assertEquals("foobar", connectionParams.getHost());
    Assert.assertEquals(10000, connectionParams.getPort());
    Assert.assertEquals(
      ImmutableMultimap.of(ExploreConnectionParams.Info.EXPLORE_AUTH_TOKEN, "foo"),
      connectionParams.getExtraInfos());

    connectionParams = ExploreConnectionParams.parseConnectionUrl(
      BASE + "?foo2=bar2&auth.token=foo&ssl.enabled=true&verify.ssl.cert=false");
    Assert.assertEquals("foobar", connectionParams.getHost());
    Assert.assertEquals(10000, connectionParams.getPort());
    Assert.assertEquals(
      ImmutableMultimap.of(ExploreConnectionParams.Info.EXPLORE_AUTH_TOKEN, "foo",
                           ExploreConnectionParams.Info.SSL_ENABLED, "true",
                           ExploreConnectionParams.Info.VERIFY_SSL_CERT, "false"),
      connectionParams.getExtraInfos());

    connectionParams = ExploreConnectionParams.parseConnectionUrl(BASE + "?foo2=bar2&auth.token");
    Assert.assertEquals("foobar", connectionParams.getHost());
    Assert.assertEquals(10000, connectionParams.getPort());
    Assert.assertEquals(ImmutableMultimap.of(), connectionParams.getExtraInfos());

    connectionParams = ExploreConnectionParams.parseConnectionUrl(BASE + "?foo2=bar2&auth.token=foo,bar");
    Assert.assertEquals("foobar", connectionParams.getHost());
    Assert.assertEquals(10000, connectionParams.getPort());
    Assert.assertEquals(
      ImmutableMultimap.of(ExploreConnectionParams.Info.EXPLORE_AUTH_TOKEN, "foo",
                           ExploreConnectionParams.Info.EXPLORE_AUTH_TOKEN, "bar"),
      connectionParams.getExtraInfos());

    // Test that we don't decode URL more than once
    connectionParams = ExploreConnectionParams.parseConnectionUrl(
      BASE + "?" +
        ExploreConnectionParams.Info.EXPLORE_AUTH_TOKEN.getName() +
        "=AgxqdWxpZW4AyIOOuPRRyJPy%2BPhR4o%2B35wl%3DAA3");
    Assert.assertEquals(
      ImmutableMultimap.of(ExploreConnectionParams.Info.EXPLORE_AUTH_TOKEN,
                           "AgxqdWxpZW4AyIOOuPRRyJPy+PhR4o+35wl=AA3"),
      connectionParams.getExtraInfos());
  }
}
