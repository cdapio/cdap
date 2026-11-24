/*
 * Copyright © 2023 Cask Data, Inc.
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

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.SConfiguration;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.CheckedConsumer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ElasticsearchMetadataStorage#close()}.
 */
public class ElasticsearchMetadataStorageCloseTest {

  @Test
  public void testCloseWhenClientIsNullDoesNotThrow() {
    ElasticsearchMetadataStorage storage =
        new ElasticsearchMetadataStorage(CConfiguration.create(), SConfiguration.create());
    storage.close();
  }

  @Test
  public void testCloseDelegatesAndSwallowsIoException() throws Exception {
    ElasticsearchMetadataStorage storage =
        new ElasticsearchMetadataStorage(CConfiguration.create(), SConfiguration.create());

    AtomicBoolean normalClosed = new AtomicBoolean(false);
    injectClient(storage, new StubRestHighLevelClient(client -> {
      normalClosed.set(true);
      client.close();
    }));
    storage.close();
    Assert.assertTrue(normalClosed.get());

    AtomicBoolean failingClosed = new AtomicBoolean(false);
    injectClient(storage, new StubRestHighLevelClient(client -> {
      failingClosed.set(true);
      client.close();
      throw new IOException("simulated elasticsearch close failure");
    }));
    storage.close();
    Assert.assertTrue(failingClosed.get());
  }

  private void injectClient(ElasticsearchMetadataStorage storage, RestHighLevelClient client)
      throws Exception {
    Field field = ElasticsearchMetadataStorage.class.getDeclaredField("client");
    field.setAccessible(true);
    field.set(storage, client);
  }

  private static final class StubRestHighLevelClient extends RestHighLevelClient {
    StubRestHighLevelClient(CheckedConsumer<RestClient, IOException> doClose) {
      super(
          RestClient.builder(new HttpHost("127.0.0.1", 9200, "http")).build(),
          doClose,
          Collections.emptyList());
    }
  }
}
