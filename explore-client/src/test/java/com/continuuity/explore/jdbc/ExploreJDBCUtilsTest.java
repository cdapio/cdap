/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.explore.jdbc;

import com.continuuity.common.conf.Constants;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

public class ExploreJDBCUtilsTest {

  @Test
  public void parseConnectionUrlTest() throws Exception {
    ExploreJDBCUtils.ConnectionParams params;

    // Simple connection URL
    params = ExploreJDBCUtils.parseConnectionUrl(Constants.Explore.Jdbc.URL_PREFIX + "foo:10000");

    Assert.assertEquals("foo", params.getHost());
    Assert.assertEquals(10000, params.getPort());
    Assert.assertEquals(ImmutableMap.of(), params.getExtraInfos());

    // With extra parameters
    params = ExploreJDBCUtils.parseConnectionUrl(Constants.Explore.Jdbc.URL_PREFIX +
                                                   "foobar:10001?reactor.auth.token=bar");

    Assert.assertEquals("foobar", params.getHost());
    Assert.assertEquals(10001, params.getPort());
    Assert.assertEquals(ImmutableMap.of(ExploreJDBCUtils.ConnectionParams.Info.EXPLORE_AUTH_TOKEN, "bar"),
                        params.getExtraInfos());

    // With unrecognized parameters
    params = ExploreJDBCUtils.parseConnectionUrl(Constants.Explore.Jdbc.URL_PREFIX + "foo:10000?key=value");

    Assert.assertEquals("foo", params.getHost());
    Assert.assertEquals(10000, params.getPort());
    Assert.assertEquals(ImmutableMap.of(), params.getExtraInfos());
  }
}
