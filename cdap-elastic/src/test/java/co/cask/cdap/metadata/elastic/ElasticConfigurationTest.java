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

package co.cask.cdap.metadata.elastic;

import co.cask.cdap.common.conf.CConfiguration;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import javax.annotation.Nullable;

/**
 * Tests that the Elasticsearch metadata storage creates indexes with the right settings.
 */
public class ElasticConfigurationTest {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticConfigurationTest.class);

  @Test
  public void testIndexSetup() throws IOException {

    String indexName = "idx" + new Random(System.currentTimeMillis()).nextInt();
    String elasticPort = System.getProperty("elastic.http.port");
    elasticPort = (elasticPort != null && !elasticPort.isEmpty()) ? elasticPort : "9200";
    LOG.info("Elasticsearch port is {}, index name is {}", elasticPort, indexName);

    CConfiguration cConf = CConfiguration.create();
    cConf.set(ElasticsearchMetadataStorage.CONF_ELASTIC_INDEX_NAME, indexName);
    cConf.set(ElasticsearchMetadataStorage.CONF_ELASTIC_HOSTS, "localhost:" + elasticPort);

    // shards defaults to 5, and replicas default 1 in Elasticsearch
    // max result window defaults to 10000 but this default is not returned in the settings
    testIndexConfig(cConf, indexName, elasticPort, 5, 1, null);

    cConf.setInt(ElasticsearchMetadataStorage.CONF_ELASTIC_WINDOW_SIZE, 100);

    testIndexConfig(cConf, indexName, elasticPort, 5, 1, 100);

    cConf.setInt(ElasticsearchMetadataStorage.CONF_ELASTIC_NUM_SHARDS, 4);
    cConf.setInt(ElasticsearchMetadataStorage.CONF_ELASTIC_NUM_REPLICAS, 2);
    cConf.setInt(ElasticsearchMetadataStorage.CONF_ELASTIC_WINDOW_SIZE, 100);

    testIndexConfig(cConf, indexName, elasticPort, 4, 2, 100);
  }

  private void testIndexConfig(CConfiguration cConf, String indexName, String elasticPort,
                               @Nullable Integer shards, @Nullable Integer replicas, @Nullable Integer windowSize)
    throws IOException {
    try (ElasticsearchMetadataStorage store = new ElasticsearchMetadataStorage(cConf)) {
      store.createIndex();
      try {
        try (RestHighLevelClient client = new RestHighLevelClient(
          RestClient.builder(new HttpHost("localhost", Integer.parseInt(elasticPort))))) {
          GetIndexResponse response =
            client.indices().get(new GetIndexRequest().indices(indexName), RequestOptions.DEFAULT);
          if (windowSize != null) {
            Assert.assertEquals(String.valueOf(windowSize), response.getSetting(indexName, "index.max_result_window"));
          } else {
            Assert.assertNull(response.getSetting(indexName, "index.max_result_window"));
          }
          Assert.assertEquals(String.valueOf(replicas), response.getSetting(indexName, "index.number_of_replicas"));
          Assert.assertEquals(String.valueOf(shards), response.getSetting(indexName, "index.number_of_shards"));
        }
      } finally {
        store.dropIndex();
      }
    }
  }

}
