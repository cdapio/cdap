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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonParseException;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.io.Closeables;
import io.cdap.cdap.common.metadata.Cursor;
import io.cdap.cdap.common.metadata.MetadataConflictException;
import io.cdap.cdap.common.security.KeyStores;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataChange;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.MetadataMutation.Drop;
import io.cdap.cdap.spi.metadata.MetadataMutation.Update;
import io.cdap.cdap.spi.metadata.MetadataRecord;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MetadataStorageTest;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.SearchResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.hc.core5.http.HttpHost;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.jsonb.JsonbJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchMetadataStorageTest extends MetadataStorageTest {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchMetadataStorageTest.class);

  private static ElasticsearchMetadataStorage elasticStore;

  @Override
  protected MetadataStorage getMetadataStorage() {
    return elasticStore;
  }

  @BeforeClass
  public static void createIndex() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Config.CONF_ELASTIC_INDEX_NAME,
              "idx" + new Random(System.currentTimeMillis()).nextInt());
    cConf.set(Config.CONF_ELASTIC_SCROLL_TIMEOUT, "2s");
    cConf.setInt(Config.CONF_ELASTIC_NUM_REPLICAS, 1);
    cConf.setInt(Config.CONF_ELASTIC_NUM_SHARDS, 1);
    cConf.setInt(Config.CONF_ELASTIC_WINDOW_SIZE, 128);
    String elasticPort = System.getProperty("elastic.http.port");
    if (elasticPort != null && !elasticPort.isEmpty()) {
      LOG.info("OpenSearch port is {}", elasticPort);
      cConf.set(Config.CONF_ELASTIC_HOSTS, "localhost:" + elasticPort);
    }
    SConfiguration sConf = SConfiguration.create();
    elasticStore = new ElasticsearchMetadataStorage(cConf, sConf);
    elasticStore.createIndex();
  }

  @AfterClass
  public static void dropIndex() throws IOException {
    if (elasticStore != null) {
      try {
        elasticStore.dropIndex();
      } finally {
        Closeables.closeQuietly(elasticStore);
      }
    }
  }

  @Test
  public void testFiltering() {
    ScopedName sys = new ScopedName(MetadataScope.SYSTEM, "s");
    ScopedName user = new ScopedName(MetadataScope.USER, "u");
    String sval = "S";
    String uval = "U";
    Metadata before = new Metadata(tags(sys, user), props(sys, sval, user, uval));

    // test selection to remove
    Assert.assertEquals(new Metadata(tags(sys), props(user, uval)),
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.DISCARD,
                          MetadataKind.NONE,
                          MetadataScope.NONE,
                          ImmutableSet.of(new ScopedNameOfKind(MetadataKind.TAG, user),
                                          new ScopedNameOfKind(MetadataKind.PROPERTY, sys))));

    // test selection is not affected by scopes or kinds
    Assert.assertEquals(new Metadata(tags(sys), props(user, uval)),
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.DISCARD,
                          MetadataKind.ALL,
                          MetadataScope.ALL,
                          ImmutableSet.of(new ScopedNameOfKind(MetadataKind.TAG, user),
                                          new ScopedNameOfKind(MetadataKind.PROPERTY, sys))));

    // test selection to keep
    Assert.assertEquals(new Metadata(tags(user), props(sys, sval)),
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.KEEP,
                          MetadataKind.NONE,
                          MetadataScope.NONE,
                          ImmutableSet.of(new ScopedNameOfKind(MetadataKind.TAG, user),
                                          new ScopedNameOfKind(MetadataKind.PROPERTY, sys))));

    // test selection is not affected by scopes or kinds
    Assert.assertEquals(new Metadata(tags(user), props(sys, sval)),
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.KEEP,
                          MetadataKind.ALL,
                          MetadataScope.ALL,
                          ImmutableSet.of(new ScopedNameOfKind(MetadataKind.TAG, user),
                                          new ScopedNameOfKind(MetadataKind.PROPERTY, sys))));

    // test removing nothing
    Assert.assertEquals(before,
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.DISCARD,
                          MetadataKind.NONE,
                          MetadataScope.NONE,
                          null));
    Assert.assertEquals(before,
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.DISCARD,
                          MetadataKind.NONE,
                          MetadataScope.ALL,
                          null));
    Assert.assertEquals(before,
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.DISCARD,
                          MetadataKind.ALL,
                          MetadataScope.NONE,
                          null));

    // test keeping all
    Assert.assertEquals(before,
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.KEEP,
                          MetadataKind.ALL,
                          MetadataScope.ALL,
                          null));

    // test removing all
    Assert.assertEquals(Metadata.EMPTY,
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.DISCARD,
                          MetadataKind.ALL,
                          MetadataScope.ALL,
                          null));

    // test keeping nothing
    Assert.assertEquals(Metadata.EMPTY,
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.KEEP,
                          MetadataKind.NONE,
                          MetadataScope.NONE,
                          null));
    // test keeping nothing
    Assert.assertEquals(Metadata.EMPTY,
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.KEEP,
                          MetadataKind.ALL,
                          MetadataScope.NONE,
                          null));
    // test keeping nothing
    Assert.assertEquals(Metadata.EMPTY,
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.KEEP,
                          MetadataKind.NONE,
                          MetadataScope.ALL,
                          null));

    // test removing all SYSTEM
    Assert.assertEquals(new Metadata(tags(user), props(user, uval)),
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.DISCARD,
                          MetadataKind.ALL,
                          Collections.singleton(MetadataScope.SYSTEM),
                          null));
    // test removing all USER
    Assert.assertEquals(new Metadata(tags(sys), props(sys, sval)),
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.DISCARD,
                          MetadataKind.ALL,
                          Collections.singleton(MetadataScope.USER),
                          null));
    // test keeping all SYSTEM
    Assert.assertEquals(new Metadata(tags(sys), props(sys, sval)),
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.KEEP,
                          MetadataKind.ALL,
                          Collections.singleton(MetadataScope.SYSTEM),
                          null));
    // test keeping all USER
    Assert.assertEquals(new Metadata(tags(user), props(user, uval)),
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.KEEP,
                          MetadataKind.ALL,
                          Collections.singleton(MetadataScope.USER),
                          null));

    // test removing all tags
    Assert.assertEquals(new Metadata(tags(), props(sys, sval, user, uval)),
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.DISCARD,
                          Collections.singleton(MetadataKind.TAG),
                          MetadataScope.ALL,
                          null));

    // test removing all properties
    Assert.assertEquals(new Metadata(tags(sys, user), props()),
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.DISCARD,
                          Collections.singleton(MetadataKind.PROPERTY),
                          MetadataScope.ALL,
                          null));

    // test keeping all tags
    Assert.assertEquals(new Metadata(tags(sys, user), props()),
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.KEEP,
                          Collections.singleton(MetadataKind.TAG),
                          MetadataScope.ALL,
                          null));

    // test keeping all properties
    Assert.assertEquals(new Metadata(tags(), props(sys, sval, user, uval)),
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.KEEP,
                          Collections.singleton(MetadataKind.PROPERTY),
                          MetadataScope.ALL,
                          null));

    // test removing all tags in SYSTEM scope
    Assert.assertEquals(new Metadata(tags(user), props(sys, sval, user, uval)),
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.DISCARD,
                          Collections.singleton(MetadataKind.TAG),
                          Collections.singleton(MetadataScope.SYSTEM),
                          null));

    // test removing all properties in USER scope
    Assert.assertEquals(new Metadata(tags(sys, user), props(sys, sval)),
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.DISCARD,
                          Collections.singleton(MetadataKind.PROPERTY),
                          Collections.singleton(MetadataScope.USER),
                          null));

    // test keeping all tags in SYSTEM scope
    Assert.assertEquals(new Metadata(tags(sys), props()),
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.KEEP,
                          Collections.singleton(MetadataKind.TAG),
                          Collections.singleton(MetadataScope.SYSTEM),
                          null));

    // test keeping all properties in USER scope
    Assert.assertEquals(new Metadata(tags(), props(user, uval)),
                        ElasticsearchMetadataStorage.filterMetadata(
                          before,
                          ElasticsearchMetadataStorage.KEEP,
                          Collections.singleton(MetadataKind.PROPERTY),
                          Collections.singleton(MetadataScope.USER),
                          null));
  }

  @Override
  protected List<String> getAdditionalTTLQueries() {
    return ImmutableList.of("ttl:0003600", "TtL:03600", "TtL:03600.00");
  }

  @Test
  public void testScrollTimeout() throws IOException, InterruptedException {
    MetadataStorage mds = getMetadataStorage();
    MutationOptions options = MutationOptions.builder().setAsynchronous(false).build();

    List<MetadataRecord> records = IntStream.range(0, 20).boxed().map(i -> new MetadataRecord(
      MetadataEntity.ofDataset("ns" + i, "ds" + i),
      new Metadata(MetadataScope.USER, tags("tag", "t" + i), props("p", "v" + i)))).collect(Collectors.toList());
    mds.batch(records.stream().map(r -> new Update(r.getEntity(), r.getMetadata())).collect(Collectors.toList()),
              options);

    SearchRequest request = SearchRequest.of("t*").setCursorRequested(true).setLimit(5).build();
    SearchResponse response = mds.search(request);
    Assert.assertEquals(5, response.getResults().size());
    Assert.assertNotNull(response.getCursor());

    SearchRequest request2 = SearchRequest.of("t*").setCursor(response.getCursor()).build();
    SearchResponse response2 = mds.search(request2);
    Assert.assertEquals(5, response2.getResults().size());

    // it works despite a cursor and an offset (which is equal to the cursor's)
    SearchRequest request2a = SearchRequest.of("t*").setCursor(response.getCursor()).setOffset(5).build();
    SearchResponse response2a = mds.search(request2a);
    Assert.assertEquals(5, response2a.getOffset());
    Assert.assertEquals(5, response2a.getLimit());
    Assert.assertEquals(response2.getResults(), response2a.getResults());

    // it works despite a cursor and an offset (which is different from the cursor's)
    SearchRequest request2b = SearchRequest.of("t*").setCursor(response.getCursor()).setOffset(8).build();
    SearchResponse response2b = mds.search(request2b);
    Assert.assertEquals(5, response2b.getOffset());
    Assert.assertEquals(5, response2b.getLimit());
    Assert.assertEquals(response2.getResults(), response2b.getResults());

    // sleep 1 sec longer than the configured scroll timeout to invalidate cursor
    TimeUnit.SECONDS.sleep(3);
    SearchResponse response3 = mds.search(request2);
    Assert.assertEquals(response2.getResults(), response3.getResults());
    Assert.assertEquals(response2.getCursor(), response3.getCursor());

    // it works despite an expired cursor and an offset (which is different from the cursor's)
    SearchRequest request3a = SearchRequest.of("t*").setCursor(response.getCursor()).setOffset(8).build();
    SearchResponse response3a = mds.search(request3a);
    Assert.assertEquals(5, response3a.getOffset());
    Assert.assertEquals(5, response3a.getLimit());
    Assert.assertEquals(response2.getResults(), response3a.getResults());

    // create a nonsense cursor and search with that
    Cursor cursor = Cursor.fromString(response.getCursor());
    cursor = new Cursor(cursor, cursor.getOffset(), "nosuchcursor");
    SearchRequest request4 = SearchRequest.of("t*").setCursor(cursor.toString()).build();
    SearchResponse response4 = mds.search(request4);
    // compare only the results, not the entire response (the cursor is different)
    Assert.assertEquals(response2.getResults(), response4.getResults());

    // clean up
    mds.batch(records.stream().map(MetadataRecord::getEntity).map(Drop::new).collect(Collectors.toList()), options);
  }

  @Test
  public void testBatchWithTwoVersionsOfSameApp() throws IOException {
    MetadataStorage mds = getMetadataStorage();
    MetadataEntity v42 = MetadataEntity.builder()
        .append(MetadataEntity.NAMESPACE, "ns")
        .appendAsType(MetadataEntity.APPLICATION, "dupapp")
        .append(MetadataEntity.VERSION, "42")
        .build();
    MetadataEntity snapshot = MetadataEntity.builder()
        .append(MetadataEntity.NAMESPACE, "ns")
        .appendAsType(MetadataEntity.APPLICATION, "dupapp")
        .append(MetadataEntity.VERSION, "-SNAPSHOT")
        .build();
    Metadata m1 = new Metadata(MetadataScope.USER, tags("t1"), props("a", "1"));
    Metadata m2 = new Metadata(MetadataScope.USER, tags("t2"), props("b", "2"));
    Metadata merged = new Metadata(MetadataScope.USER, tags("t1", "t2"), props("a", "1", "b", "2"));

    try {
      // Both versions are the same document, so the two writes must not be in the same bulk request
      List<MetadataChange> changes = mds.batch(
          Arrays.asList(new Update(v42, m1), new Update(snapshot, m2)), MutationOptions.DEFAULT);
      Assert.assertEquals(2, changes.size());
      Assert.assertEquals(m1, changes.get(0).getAfter());
      Assert.assertEquals(m1, changes.get(1).getBefore());
      Assert.assertEquals(merged, changes.get(1).getAfter());
      Assert.assertEquals(merged, mds.read(new Read(v42)));
    } finally {
      mds.apply(new Drop(v42), MutationOptions.DEFAULT);
    }
  }

  @Test
  public void testBatchDropOfMissingEntities() throws IOException {
    MetadataStorage mds = getMetadataStorage();
    MetadataEntity e1 = MetadataEntity.ofDataset("ns", "missing1");
    MetadataEntity e2 = MetadataEntity.ofDataset("ns", "missing2");
    // A delete of a missing document is not an error, so the batch succeeds
    List<MetadataChange> changes = mds.batch(
        Arrays.asList(new Drop(e1), new Drop(e2)), MutationOptions.DEFAULT);
    Assert.assertEquals(Arrays.asList(
        new MetadataChange(e1, Metadata.EMPTY, Metadata.EMPTY),
        new MetadataChange(e2, Metadata.EMPTY, Metadata.EMPTY)), changes);
  }

  @Test
  public void testReadFromMissingIndexFails() throws IOException {
    String indexName = "idxmissing" + new Random().nextInt(Integer.MAX_VALUE);
    try (ElasticsearchMetadataStorage store = newStore(indexName)) {
      // A missing index must not be mistaken for a missing document
      store.read(new Read(MetadataEntity.ofDataset("ns", "ds")));
      Assert.fail("Expected reading from a missing index to fail");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("index_not_found_exception"));
      Assert.assertTrue(String.valueOf(e.getCause()), e.getCause() instanceof OpenSearchException);
    }
  }

  @Test
  public void testBatchOnMissingIndexFails() throws IOException {
    String indexName = "idxmissingbatch" + new Random().nextInt(Integer.MAX_VALUE);
    try (ElasticsearchMetadataStorage store = newStore(indexName)) {
      // A batch of several entities reads them with one multi-get request
      store.batch(Arrays.asList(
          new Update(MetadataEntity.ofDataset("ns", "ds1"), Metadata.EMPTY),
          new Update(MetadataEntity.ofDataset("ns", "ds2"), Metadata.EMPTY)),
          MutationOptions.DEFAULT);
      Assert.fail("Expected a batch on a missing index to fail");
    } catch (IOException e) {
      // The multi-get reports the missing index for each document
      Assert.assertTrue(e.getMessage(), e.getMessage().startsWith(
          "Failed to read from index for entity " + MetadataEntity.ofDataset("ns", "ds1") + ": "));
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("index_not_found_exception"));
    }
  }

  @Test
  public void testSearchOnMissingIndexFails() throws IOException {
    String indexName = "idxmissingsearch" + new Random().nextInt(Integer.MAX_VALUE);
    try (ElasticsearchMetadataStorage store = newStore(indexName)) {
      store.search(SearchRequest.of("*").build());
      Assert.fail("Expected a search on a missing index to fail");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("index_not_found_exception"));
      Assert.assertTrue(String.valueOf(e.getCause()), e.getCause() instanceof OpenSearchException);
    }
  }

  @Test
  public void testReadUnparsableDocumentFails() throws IOException {
    // The document has metadata that is not an object
    assertReadFails(JsonData.of(ImmutableMap.of("metadata", 42, "hidden", false)));
  }

  @Test
  public void testReadDocumentWithoutMetadataFails() throws IOException {
    assertReadFails(JsonData.of(Collections.singletonMap("hidden", false)));
  }

  @Test
  public void testReadDocumentWithEmptyMetadataFails() throws IOException {
    assertReadFails(
        JsonData.of(ImmutableMap.of("metadata", Collections.emptyMap(), "hidden", false)));
  }

  /**
   * Indexes a document with the given source and asserts that reading it fails. The source is not
   * hidden, so that a search finds it.
   */
  private static void assertReadFails(JsonData source) throws IOException {
    String indexName = "idxunparsable" + new Random().nextInt(Integer.MAX_VALUE);
    try (ElasticsearchMetadataStorage store = newStore(indexName);
         ApacheHttpClient5Transport transport = ApacheHttpClient5TransportBuilder
             .builder(new HttpHost("http", "localhost", Integer.parseInt(getElasticPort())))
             .setMapper(new JsonbJsonpMapper()).build()) {
      store.createIndex();
      MetadataEntity entity = MetadataEntity.ofDataset("ns", "ds");
      try {
        // refresh, so that a search finds the document
        new OpenSearchClient(transport).index(i -> i.index(indexName)
            .id(ElasticsearchMetadataStorage.toDocumentId(entity)).document(source)
            .refresh(Refresh.True));
        try {
          store.read(new Read(entity));
          Assert.fail("Expected reading an unparsable document to fail");
        } catch (IOException e) {
          Assert.assertTrue(e.getMessage(),
              e.getMessage().startsWith("Failed to read from index for entity"));
          Assert.assertTrue(String.valueOf(e.getCause()),
              e.getCause() instanceof JsonParseException);
        }
        try {
          // A batch of several entities reads them with one multi-get request
          store.batch(Arrays.asList(new Update(entity, Metadata.EMPTY),
              new Update(MetadataEntity.ofDataset("ns", "other"), Metadata.EMPTY)),
              MutationOptions.DEFAULT);
          Assert.fail("Expected a batch with an unparsable document to fail");
        } catch (IOException e) {
          Assert.assertTrue(String.valueOf(e.getCause()),
              e.getCause() instanceof JsonParseException);
        }
        try {
          store.search(SearchRequest.of("*").build());
          Assert.fail("Expected a search that finds an unparsable document to fail");
        } catch (IOException e) {
          Assert.assertTrue(String.valueOf(e.getCause()),
              e.getCause() instanceof JsonParseException);
        }
      } finally {
        store.dropIndex();
      }
    }
  }

  @Test
  public void testCreateIndexFailureIsReported() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Config.CONF_ELASTIC_INDEX_NAME, "idxbad" + new Random().nextInt(Integer.MAX_VALUE));
    cConf.set(Config.CONF_ELASTIC_HOSTS, "localhost:" + getElasticPort());
    // An index must have at least one shard, so the request to create it fails
    cConf.setInt(Config.CONF_ELASTIC_NUM_SHARDS, 0);
    try (ElasticsearchMetadataStorage store =
        new ElasticsearchMetadataStorage(cConf, SConfiguration.create())) {
      try {
        store.createIndex();
        Assert.fail("Expected creating the index to fail");
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().startsWith("Unable to create index"));
        Assert.assertTrue(String.valueOf(e.getCause()),
            e.getCause() instanceof OpenSearchException);
      } finally {
        store.dropIndex();
      }
    }
  }

  @Test
  public void testCreateIndexFailsOnForeignIndex() throws IOException {
    String indexName = "idxforeign" + new Random().nextInt(Integer.MAX_VALUE);
    try (ElasticsearchMetadataStorage store = newStore(indexName);
         ApacheHttpClient5Transport transport = ApacheHttpClient5TransportBuilder
             .builder(new HttpHost("http", "localhost", Integer.parseInt(getElasticPort())))
             .setMapper(new JsonbJsonpMapper()).build()) {
      OpenSearchClient client = new OpenSearchClient(transport);
      // An index that CDAP did not create has no CDAP version in its mapping
      client.indices().create(c -> c.index(indexName));
      try {
        store.createIndex();
        Assert.fail("Expected creating the index to fail on an index that CDAP did not create");
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage(),
            e.getMessage().contains("was not created by this version"));
      } finally {
        client.indices().delete(d -> d.index(indexName));
      }
    }
  }

  @Test
  public void testBulkWithMissingDeleteAndConflict() throws IOException {
    String indexName = "idxbulk" + new Random().nextInt(Integer.MAX_VALUE);
    MetadataEntity missing = MetadataEntity.ofDataset("ns", "missing");
    MetadataEntity app = MetadataEntity.builder()
        .append(MetadataEntity.NAMESPACE, "ns")
        .appendAsType(MetadataEntity.APPLICATION, "bulkapp")
        .append(MetadataEntity.VERSION, "42")
        .build();
    try (ElasticsearchMetadataStorage store = newStore(indexName)) {
      store.createIndex();
      try {
        store.apply(new Update(app, new Metadata(MetadataScope.USER, tags("t1"), props())),
            MutationOptions.DEFAULT);
        // The delete has no document, and the write expects another version of the document
        List<BulkOperation> operations = Arrays.asList(
            BulkOperation.of(b -> b.delete(d -> d.index(indexName)
                .id(ElasticsearchMetadataStorage.toDocumentId(missing)))),
            BulkOperation.of(b -> b.index(i -> i.index(indexName)
                .id(ElasticsearchMetadataStorage.toDocumentId(app))
                .document(JsonData.of(Collections.emptyMap())).ifSeqNo(1000L).ifPrimaryTerm(1L))));
        Set<MetadataEntity> entities = new HashSet<>(Arrays.asList(missing, app));
        try {
          store.executeBulk(operations, MutationOptions.DEFAULT, entities);
          Assert.fail("Expected the write of a changed document to conflict");
        } catch (MetadataConflictException e) {
          // The document id has no version, so this proves the entity is looked up by document id
          Assert.assertEquals(Collections.singletonList(app), e.getConflictingEntities());
        }
        // The delete of the missing document succeeded, so only the conflicting write remains
        Assert.assertEquals(Collections.singleton(app), entities);
      } finally {
        store.dropIndex();
      }
    }
  }

  @Test
  public void testBasicAuthOverTlsWithoutVerification() throws Exception {
    String password = "secret";
    KeyStore keyStore = KeyStores.generatedCertKeyStore(1, password);
    KeyManagerFactory keyManagerFactory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, password.toCharArray());
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(keyManagerFactory.getKeyManagers(), null, null);

    // A server with a self-signed certificate that records the credentials of each request
    HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.setHttpsConfigurator(new HttpsConfigurator(sslContext));
    List<String> authorizations = Collections.synchronizedList(new ArrayList<>());
    server.createContext("/", exchange -> {
      authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
      if ("HEAD".equals(exchange.getRequestMethod())) {
        // The index does not exist
        exchange.sendResponseHeaders(404, -1);
      } else {
        // Creating the index fails
        byte[] body = ("{\"error\":{\"type\":\"illegal_argument_exception\",\"reason\":\"test\"},"
            + "\"status\":400}").getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(400, body.length);
        try (OutputStream output = exchange.getResponseBody()) {
          output.write(body);
        }
      }
      exchange.close();
    });
    server.start();
    try {
      CConfiguration cConf = CConfiguration.create();
      cConf.set(Config.CONF_ELASTIC_INDEX_NAME, "idxtls");
      cConf.set(Config.CONF_ELASTIC_HOSTS, "https://127.0.0.1:" + server.getAddress().getPort());
      cConf.setBoolean(Config.CONF_ELASTIC_TLS_VERIFY, false);
      SConfiguration sConf = SConfiguration.create();
      sConf.set(Config.CONF_ELASTIC_USERNAME, "user");
      sConf.set(Config.CONF_ELASTIC_PASSWORD, "pass");
      try (ElasticsearchMetadataStorage store = new ElasticsearchMetadataStorage(cConf, sConf)) {
        store.createIndex();
        Assert.fail("Expected creating the index to fail");
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().startsWith("Unable to create index"));
      }
      // The requests reached the server over TLS, with the credentials
      String expected = "Basic "
          + Base64.getEncoder().encodeToString("user:pass".getBytes(StandardCharsets.UTF_8));
      Assert.assertFalse(authorizations.isEmpty());
      Assert.assertEquals(expected, authorizations.get(0));

      // With verification, the self-signed certificate is rejected before any request is sent
      authorizations.clear();
      cConf.setBoolean(Config.CONF_ELASTIC_TLS_VERIFY, true);
      try (ElasticsearchMetadataStorage store = new ElasticsearchMetadataStorage(cConf, sConf)) {
        store.createIndex();
        Assert.fail("Expected the certificate to be rejected");
      } catch (IOException e) {
        // The transport can wrap the failure of the handshake
        Throwable cause = e;
        while (cause != null && !(cause instanceof SSLException)) {
          cause = cause.getCause();
        }
        Assert.assertNotNull("Expected an SSLException, but got " + e, cause);
      }
      Assert.assertTrue(authorizations.isEmpty());
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testConcurrentCreateIndex() throws Exception {
    String indexName = "idxconcurrent" + new Random().nextInt(Integer.MAX_VALUE);
    int numStores = 4;
    List<ElasticsearchMetadataStorage> stores = new ArrayList<>();
    for (int i = 0; i < numStores; i++) {
      stores.add(newStore(indexName));
    }
    ExecutorService executor = Executors.newFixedThreadPool(numStores);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (ElasticsearchMetadataStorage store : stores) {
        futures.add(executor.submit(() -> {
          start.await();
          store.createIndex();
          return null;
        }));
      }
      start.countDown();
      // If another store created the index after the check, creating it must still succeed
      for (Future<?> future : futures) {
        future.get(1, TimeUnit.MINUTES);
      }
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(1, TimeUnit.MINUTES);
      try {
        stores.get(0).dropIndex();
      } finally {
        stores.forEach(Closeables::closeQuietly);
      }
    }
  }

  private static String getElasticPort() {
    return System.getProperty("elastic.http.port", "9200");
  }

  private static ElasticsearchMetadataStorage newStore(String indexName) {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Config.CONF_ELASTIC_INDEX_NAME, indexName);
    cConf.set(Config.CONF_ELASTIC_HOSTS, "localhost:" + getElasticPort());
    return new ElasticsearchMetadataStorage(cConf, SConfiguration.create());
  }

  @Override
  protected void validateCursor(String cursor, int expectedOffset, int expectedPageSize) {
    Cursor c = Cursor.fromString(cursor);
    Assert.assertEquals(expectedOffset, c.getOffset());
    Assert.assertEquals(expectedPageSize, c.getLimit());
  }
}
