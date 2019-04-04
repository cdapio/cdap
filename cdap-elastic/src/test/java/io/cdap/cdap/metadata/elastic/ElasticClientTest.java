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

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * Tests the way Elasticsearch is set up for "integration" tests (mvn verify).
 */
public class ElasticClientTest {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticClientTest.class);

  @Test
  public void testCreateDelete() throws IOException {
    int elasticPort = Integer.valueOf(System.getProperty("elastic.http.port", "9200"));
    LOG.info("Elasticsearch port is {}", System.getProperty("elastic.http.port"));
    try (RestHighLevelClient client =
           new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", elasticPort)))) {

      String indexName = "test" + new Random(System.currentTimeMillis()).nextInt();
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
      createIndexRequest.settings(Settings.builder()
                                    .put("index.number_of_shards", 1)
                                    .put("index.number_of_replicas", 1));
      Assert.assertTrue(client.indices().create(createIndexRequest, RequestOptions.DEFAULT).isAcknowledged());
      try {
        GetIndexRequest request = new GetIndexRequest().indices(indexName);
        Assert.assertTrue(client.indices().exists(request, RequestOptions.DEFAULT));
      } finally {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
        Assert.assertTrue(client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT).isAcknowledged());
      }
      GetIndexRequest request = new GetIndexRequest().indices(indexName);
      Assert.assertFalse(client.indices().exists(request, RequestOptions.DEFAULT));
    }
  }
}
