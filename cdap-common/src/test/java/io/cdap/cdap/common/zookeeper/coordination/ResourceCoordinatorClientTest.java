/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.common.zookeeper.coordination;

import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClients;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link ResourceCoordinatorClient}.
 */
public class ResourceCoordinatorClientTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static InMemoryZKServer zkServer;

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(TEMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();
  }

  @AfterClass
  public static void finish() {
    zkServer.stopAndWait();
  }

  @Test
  public void testSubmitFetchModifyDeleteAndCorruptRequirement() throws Exception {
    ZKClientService zkClientService =
        ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClientService.startAndWait();
    try {
      ZKClient zkClient = ZKClients.namespace(zkClientService, "/client-test");
      ResourceCoordinatorClient client = new ResourceCoordinatorClient(zkClient);
      client.startAndWait();
      try {
        // Fetching non-existent requirement returns null via NODE_DATA_TO_REQUIREMENT.
        Assert.assertNull(client.fetchRequirement("missing-res").get(5, TimeUnit.SECONDS));

        // Deleting non-existent requirement succeeds and returns resourceName.
        Assert.assertEquals("missing-res",
            client.deleteRequirement("missing-res").get(5, TimeUnit.SECONDS));

        ResourceRequirement initial = ResourceRequirement.builder("res-1")
            .addPartitions("p", 2, 1)
            .build();
        Assert.assertEquals(initial, client.submitRequirement(initial).get(5, TimeUnit.SECONDS));
        Assert.assertEquals(initial, client.fetchRequirement("res-1").get(5, TimeUnit.SECONDS));

        ResourceRequirement modified = ResourceRequirement.builder("res-1")
            .addPartitions("p", 3, 2)
            .build();
        Assert.assertEquals(modified,
            client.modifyRequirement("res-1", existing -> modified).get(5, TimeUnit.SECONDS));
        Assert.assertEquals(modified, client.fetchRequirement("res-1").get(5, TimeUnit.SECONDS));

        Assert.assertEquals("res-1", client.deleteRequirement("res-1").get(5, TimeUnit.SECONDS));
        Assert.assertNull(client.fetchRequirement("res-1").get(5, TimeUnit.SECONDS));

        // Corrupt JSON in requirement node triggers JsonSyntaxException inside
        // NODE_DATA_TO_REQUIREMENT, which must be rethrown unwrapped (not wrapped in RuntimeException).
        zkClient.create(CoordinationConstants.REQUIREMENTS_PATH + "/corrupt-res",
            "{{invalid-json".getBytes(StandardCharsets.UTF_8),
            CreateMode.PERSISTENT, true).get(5, TimeUnit.SECONDS);

        try {
          client.fetchRequirement("corrupt-res").get(5, TimeUnit.SECONDS);
          Assert.fail("Expected ExecutionException caused by JsonSyntaxException");
        } catch (ExecutionException e) {
          Assert.assertTrue(e.getCause() instanceof JsonSyntaxException);
        }
      } finally {
        client.stopAndWait();
      }
    } finally {
      zkClientService.stopAndWait();
    }
  }
}
