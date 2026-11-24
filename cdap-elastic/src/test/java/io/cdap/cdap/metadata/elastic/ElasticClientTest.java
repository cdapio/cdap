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

import java.io.IOException;
import java.util.Random;
import org.apache.hc.core5.http.HttpHost;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.json.jsonb.JsonbJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the way OpenSearch is set up for "integration" tests (mvn verify).
 */
public class ElasticClientTest {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticClientTest.class);

  @Test
  public void testCreateDelete() throws IOException {
    int elasticPort = Integer.valueOf(System.getProperty("elastic.http.port", "9200"));
    LOG.info("OpenSearch port is {}", System.getProperty("elastic.http.port"));
    try (ApacheHttpClient5Transport transport = ApacheHttpClient5TransportBuilder
           .builder(new HttpHost("http", "localhost", elasticPort))
           .setMapper(new JsonbJsonpMapper()).build()) {
      OpenSearchClient client = new OpenSearchClient(transport);

      String indexName = "test" + new Random(System.currentTimeMillis()).nextInt();
      Assert.assertTrue(client.indices().create(c -> c.index(indexName)
                                                  .settings(s -> s.numberOfShards(1).numberOfReplicas(1)))
                          .acknowledged());
      try {
        Assert.assertTrue(client.indices().exists(e -> e.index(indexName)).value());
      } finally {
        Assert.assertTrue(client.indices().delete(d -> d.index(indexName)).acknowledged());
      }
      Assert.assertFalse(client.indices().exists(e -> e.index(indexName)).value());
    }
  }
}
