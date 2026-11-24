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

package io.cdap.cdap.app.runtime.spark;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link BasicSparkClientContext}.
 */
public class BasicSparkClientContextTest {

  @Test
  public void testLocalizeValidUriAndOpaqueUriPropagatesUriSyntaxException() {
    SparkRuntimeContext runtimeContext = SparkRuntimeContextTest.createRuntimeContext(
        CConfiguration.create(), new Configuration());
    BasicSparkClientContext clientContext = new BasicSparkClientContext(runtimeContext);

    clientContext.localize("data.zip", URI.create("file:///tmp/archive.zip"), true);
    LocalizeResource resource = clientContext.getLocalizeResources().get("data.zip");
    Assert.assertNotNull(resource);
    Assert.assertTrue(resource.isArchive());
    Assert.assertEquals("data.zip", resource.getURI().getFragment());

    // An opaque URI has a scheme ("urn") with null authority/path/query, causing new URI(...) to throw
    // URISyntaxException inside localize(), exercising Throwables.propagate(e).
    try {
      clientContext.localize("invalid", URI.create("urn:opaque-part"), false);
      Assert.fail("Expected RuntimeException wrapping URISyntaxException");
    } catch (RuntimeException caught) {
      Assert.assertTrue(caught.getCause() instanceof URISyntaxException);
    }
  }
}
