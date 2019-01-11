/*
 * Copyright 2019 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.elastic;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.metadata.store.MetadataStoreTest;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ElasticMetadataStoreTest extends MetadataStoreTest {

  private static ElasticMetadataStore elasticStore;

  @BeforeClass
  public static void createIndex() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(ElasticMetadataStore.CONF_INDEX_NAME, "idx" + new Random(System.currentTimeMillis()).nextInt());
    elasticStore = new ElasticMetadataStore(cConf);
    store = elasticStore;
  }

  @AfterClass
  public static void dropIndex() throws IOException {
    if (elasticStore != null) {
      try {
        elasticStore.deleteIndex();
      } finally {
        elasticStore.close();
      }
    }
  }

  @Test
  public void testCreateDelete() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(ElasticMetadataStore.CONF_INDEX_NAME, "idx" + new Random(System.currentTimeMillis()).nextInt());
    try (ElasticMetadataStore store = new ElasticMetadataStore(cConf)) {
      store.ensureIndexCreated();
      store.deleteIndex();
    }
  }

  @Test
  public void testSetGetProperties() {
    Map<String, String> properties = ImmutableMap.of("a", "b", "x", "y");
    MetadataEntity entity = NamespaceId.DEFAULT.app("foo").service("bar").toMetadataEntity();
    store.setProperties(MetadataScope.SYSTEM, entity, properties);
    Map<String, String> retrieved = store.getProperties(MetadataScope.SYSTEM, entity);
    Assert.assertEquals(properties, retrieved);

    Map<String, String> additionalProperties = ImmutableMap.of("c", "d", "x", "z");
    store.setProperties(MetadataScope.SYSTEM, entity, additionalProperties);
    retrieved = store.getProperties(MetadataScope.SYSTEM, entity);
    Map<String, String> expected = new HashMap<>(properties);
    expected.putAll(additionalProperties);
    Assert.assertEquals(expected, retrieved);
  }


}
