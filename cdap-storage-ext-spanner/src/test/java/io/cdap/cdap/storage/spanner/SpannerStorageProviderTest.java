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

package io.cdap.cdap.storage.spanner;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.SpannerOptions;
import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SpannerStorageProvider}.
 */
public class SpannerStorageProviderTest {

  private static final String ENDPOINT = "https://spanner.example.com";
  private static final String EMULATOR_HOST = "localhost:9999";
  private static final Map<String, String> ENDPOINT_CONF =
      Collections.singletonMap("endpoint", ENDPOINT);

  @Test
  public void testEndpointWhenConfigured() {
    Assert.assertEquals(ENDPOINT, SpannerStorageProvider.getEndpoint(ENDPOINT_CONF, null));
  }

  @Test
  public void testNoEndpointWhenNotConfigured() {
    Assert.assertNull(SpannerStorageProvider.getEndpoint(Collections.emptyMap(), null));
  }

  @Test
  public void testNoEndpointWhenEmpty() {
    Assert.assertNull(
        SpannerStorageProvider.getEndpoint(Collections.singletonMap("endpoint", ""), null));
  }

  @Test
  public void testNoEndpointWithEmulator() {
    Assert.assertNull(SpannerStorageProvider.getEndpoint(ENDPOINT_CONF, EMULATOR_HOST));
  }

  /**
   * Pins the client library behavior that the provider works around: setting the host clears the
   * emulator host, so the endpoint must not be set when an emulator is.
   */
  @Test
  public void testSetHostClearsEmulatorHost() {
    SpannerOptions options = SpannerOptions.newBuilder()
        .setProjectId("test-project")
        .setCredentials(NoCredentials.getInstance())
        .setEmulatorHost(EMULATOR_HOST)
        .setHost(ENDPOINT)
        .build();

    Assert.assertEquals(ENDPOINT, options.getHost());
  }
}
