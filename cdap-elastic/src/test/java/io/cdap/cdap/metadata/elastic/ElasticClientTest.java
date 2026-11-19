/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.metadata.elastic;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the way OpenSearch is set up for "integration" tests (mvn verify).
 */
public class ElasticClientTest {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticClientTest.class);

  @Test
  public void testCreateDelete() throws IOException {
    String elasticPort = System.getProperty("elastic.http.port", "9200");
    LOG.info("OpenSearch port is {}", elasticPort);
    try (IndexRestClient client = new IndexRestClient("localhost:" + elasticPort, null, null, true)) {
      JsonObject index = new JsonObject();
      index.addProperty("number_of_shards", 1);
      index.addProperty("number_of_replicas", 1);
      JsonObject settings = new JsonObject();
      settings.add("index", index);
      JsonObject createRequest = new JsonObject();
      createRequest.add("settings", settings);

      String indexName = "test" + new Random(System.currentTimeMillis()).nextInt();
      IndexRestClient.Response created = client.execute(
          "PUT", Collections.singletonList(indexName), Collections.emptyMap(), createRequest);
      Assert.assertTrue(created.toString(),
          created.ensureSuccess().getJson().get("acknowledged").getAsBoolean());
      try {
        Assert.assertEquals(200, client.execute("HEAD", indexName).getStatus());
      } finally {
        IndexRestClient.Response deleted = client.execute("DELETE", indexName);
        Assert.assertTrue(deleted.toString(),
            deleted.ensureSuccess().getJson().get("acknowledged").getAsBoolean());
      }
      Assert.assertEquals(404, client.execute("HEAD", indexName).getStatus());
    }
  }
}
