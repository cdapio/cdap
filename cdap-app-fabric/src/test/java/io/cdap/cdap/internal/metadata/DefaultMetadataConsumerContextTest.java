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

package io.cdap.cdap.internal.metadata;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.security.auth.P4saTokenEndpointAuth;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link DefaultMetadataConsumerContext}.
 */
public class DefaultMetadataConsumerContextTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testAugmentsTokenEndpointOnlyWhenKeyfileExistsWithoutMutatingConfig()
      throws Exception {
    String rawEndpoint = "http://appfabric:11017/v3Internal/system/p4sa/token";
    String propKey =
        Constants.MetadataConsumer.METADATA_CONSUMER_PREFIX + ".dataplex.token.endpoint";

    // 1. Missing keyfile (Zero-P4SA worker pod): endpoint is not augmented.
    CConfiguration workerConf = CConfiguration.create();
    workerConf.set(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH,
        new File(tempFolder.getRoot(), "missing.json").getAbsolutePath());
    workerConf.set(propKey, rawEndpoint);
    DefaultMetadataConsumerContext workerCtx = new DefaultMetadataConsumerContext(
        workerConf, "dataplex", new NoOpMetricsCollectionService(), "default", "myApp");
    Assert.assertEquals(rawEndpoint, workerCtx.getProperties().get("token.endpoint"));

    // 2. Existing keyfile (system pod): endpoint points to loopback bridge and cConf is unmutated.
    File keyFile = tempFolder.newFile("tokenManagerKey.json");
    byte[] rawKey = "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8);
    String json = String.format(
        "{\"encodedKey\":\"%s\",\"algorithm\":\"AES\",\"keyId\":1,\"expiration\":%d}",
        Base64.getEncoder().encodeToString(rawKey), Long.MAX_VALUE);
    Files.write(keyFile.toPath(), json.getBytes(StandardCharsets.UTF_8));

    CConfiguration systemConf = CConfiguration.create();
    systemConf.set(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH, keyFile.getAbsolutePath());
    systemConf.set(propKey, rawEndpoint);
    DefaultMetadataConsumerContext systemCtx = new DefaultMetadataConsumerContext(
        systemConf, "dataplex", new NoOpMetricsCollectionService(), "default", "myApp");
    String expectedHmac = P4saTokenEndpointAuth.computeToken(systemConf);
    String bridgeEndpoint = systemCtx.getProperties().get("token.endpoint");
    Assert.assertTrue(bridgeEndpoint.startsWith("http://127.0.0.1:"));
    Assert.assertTrue(bridgeEndpoint.endsWith("/token"));
    Assert.assertFalse(bridgeEndpoint.contains(expectedHmac));
    Assert.assertEquals(rawEndpoint, systemConf.get(propKey));
  }
}
