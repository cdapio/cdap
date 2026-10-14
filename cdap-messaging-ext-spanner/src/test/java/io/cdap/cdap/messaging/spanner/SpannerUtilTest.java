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

package io.cdap.cdap.messaging.spanner;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.SpannerOptions;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SpannerUtil}.
 */
public class SpannerUtilTest {

  private static final String PROJECT = "test-project";
  private static final String DEFAULT_HOST = "https://spanner.googleapis.com";
  private static final String PREPROD_HOST = "https://preprod-spanner.sandbox.googleapis.com";
  // Both differ from the localhost:9010 that CI exports as SPANNER_EMULATOR_HOST, which
  // SpannerOptions.Builder reads by default, so a passing test cannot come from the environment.
  private static final String CONF_EMULATOR_HOST = "localhost:9998";
  private static final String ENV_EMULATOR_HOST = "localhost:9999";

  @Test
  public void testHostIsSetWhenConfigured() {
    Map<String, String> conf = new HashMap<>();
    conf.put(SpannerUtil.HOST, PREPROD_HOST);

    Assert.assertEquals(PREPROD_HOST, buildOptions(conf).getHost());
  }

  @Test
  public void testDefaultHostWhenNotConfigured() {
    Assert.assertEquals(DEFAULT_HOST, buildOptions(new HashMap<>()).getHost());
  }

  @Test
  public void testDefaultHostWhenEmpty() {
    Map<String, String> conf = new HashMap<>();
    conf.put(SpannerUtil.HOST, "");

    Assert.assertEquals(DEFAULT_HOST, buildOptions(conf).getHost());
  }

  @Test
  public void testEmulatorTakesPrecedenceOverHost() {
    Map<String, String> conf = new HashMap<>();
    conf.put(SpannerUtil.SPANNER_EMULATOR_HOST, CONF_EMULATOR_HOST);
    conf.put(SpannerUtil.HOST, PREPROD_HOST);

    SpannerOptions options =
        SpannerUtil.createSpannerOptionsBuilder(conf, PROJECT, null, null).build();

    Assert.assertEquals("http://" + CONF_EMULATOR_HOST, options.getHost());
  }

  @Test
  public void testEnvEmulatorTakesPrecedenceOverHost() {
    Map<String, String> conf = new HashMap<>();
    conf.put(SpannerUtil.HOST, PREPROD_HOST);

    SpannerOptions options =
        SpannerUtil.createSpannerOptionsBuilder(conf, PROJECT, null, ENV_EMULATOR_HOST).build();

    Assert.assertEquals("http://" + ENV_EMULATOR_HOST, options.getHost());
  }

  @Test
  public void testEmulatorPropertyTakesPrecedenceOverEnvEmulator() {
    Map<String, String> conf = new HashMap<>();
    conf.put(SpannerUtil.SPANNER_EMULATOR_HOST, CONF_EMULATOR_HOST);

    SpannerOptions options =
        SpannerUtil.createSpannerOptionsBuilder(conf, PROJECT, null, ENV_EMULATOR_HOST).build();

    Assert.assertEquals("http://" + CONF_EMULATOR_HOST, options.getHost());
  }

  private static SpannerOptions buildOptions(Map<String, String> conf) {
    // NoCredentials skips the Application Default Credentials lookup, which the host does not
    // depend on.
    return SpannerUtil.createSpannerOptionsBuilder(conf, PROJECT, NoCredentials.getInstance(), null)
        // A SPANNER_EMULATOR_HOST environment variable would replace the host when building.
        .setEmulatorHost(null)
        .build();
  }
}
