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
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.metadata.Cursor;
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
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchMetadataStorageTest extends MetadataStorageTest {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchMetadataStorageTest.class);

  // The index name for the tests that use a fake search cluster
  private static final String FAKE_INDEX = "fakeidx";

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
      LOG.info("Elasticsearch port is {}", elasticPort);
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
  public void testBatchPartialConflictWithNonSnapshotVersionedEntity() throws IOException {
    String elasticPort = System.getProperty("elastic.http.port", "9200");
    String indexName = "idxconflict" + Math.abs(new Random().nextInt());
    MetadataEntity versionedApp = MetadataEntity.builder()
        .append(MetadataEntity.NAMESPACE, "ns")
        .appendAsType(MetadataEntity.APPLICATION, "verapp")
        .append(MetadataEntity.VERSION, "42")
        .build();
    MetadataEntity dataset = MetadataEntity.ofDataset("ns", "conflict_ds");
    Metadata appInitial = new Metadata(MetadataScope.USER, tags("app-init"), props("k", "v0"));
    Metadata appUpdate = new Metadata(MetadataScope.USER, tags("app-upd"), props("k", "v1"));
    Metadata dsInitial = new Metadata(MetadataScope.USER, tags("ds-init"), props("dk", "dv0"));
    Metadata dsConcurrent = new Metadata(MetadataScope.USER, tags("ds-conc"), props("c", "1"));
    Metadata dsUpdate = new Metadata(MetadataScope.USER, tags("ds-upd"), props("dk", "dv1"));

    AtomicBoolean injectedConflict = new AtomicBoolean(false);
    AtomicInteger appBulkWrites = new AtomicInteger();

    try (ElasticsearchMetadataStorage directStore = newStore(indexName, "localhost:" + elasticPort);
         CloseableHttpClient forwardClient = HttpClients.createDefault()) {
      directStore.createIndex();
      try {
        directStore.batch(Arrays.asList(
            new Update(versionedApp, appInitial),
            new Update(dataset, dsInitial)), MutationOptions.DEFAULT);

        HttpServer proxy = startProxy(elasticPort, forwardClient, (method, path, body) -> {
          if (path.equals("/_bulk")) {
            if (body.contains("application:namespace=ns,application=verapp")) {
              appBulkWrites.incrementAndGet();
            }
            if (injectedConflict.compareAndSet(false, true)) {
              // Advance the seq_no of the dataset after it was read, so that it conflicts once
              directStore.apply(new Update(dataset, dsConcurrent), MutationOptions.DEFAULT);
            }
          }
        });
        try (ElasticsearchMetadataStorage proxyStore =
                 newStore(indexName, "localhost:" + proxy.getAddress().getPort())) {
          proxyStore.createIndex();
          List<MetadataChange> changes = proxyStore.batch(Arrays.asList(
              new Update(versionedApp, appUpdate),
              new Update(dataset, dsUpdate)), MutationOptions.DEFAULT);

          Assert.assertTrue(injectedConflict.get());
          Assert.assertEquals(1, appBulkWrites.get());
          Assert.assertEquals(2, changes.size());
          Assert.assertEquals(versionedApp, changes.get(0).getEntity());
          Assert.assertEquals(appInitial, changes.get(0).getBefore());
          Assert.assertEquals(
              new Metadata(MetadataScope.USER, tags("app-init", "app-upd"), props("k", "v1")),
              changes.get(0).getAfter());
          Assert.assertEquals(dataset, changes.get(1).getEntity());
          Assert.assertEquals(
              new Metadata(MetadataScope.USER, tags("ds-init", "ds-conc"),
                  props("dk", "dv0", "c", "1")),
              changes.get(1).getBefore());
          Assert.assertEquals(
              new Metadata(MetadataScope.USER, tags("ds-init", "ds-conc", "ds-upd"),
                  props("dk", "dv1", "c", "1")),
              changes.get(1).getAfter());
        } finally {
          proxy.stop(0);
        }
      } finally {
        directStore.dropIndex();
      }
    }
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

    // Both versions are the same document, so the two writes must not be in the same bulk request
    List<MetadataChange> changes = mds.batch(
        Arrays.asList(new Update(v42, m1), new Update(snapshot, m2)), MutationOptions.DEFAULT);
    Assert.assertEquals(2, changes.size());
    Assert.assertEquals(m1, changes.get(0).getAfter());
    Assert.assertEquals(m1, changes.get(1).getBefore());
    Assert.assertEquals(merged, changes.get(1).getAfter());
    Assert.assertEquals(merged, mds.read(new Read(v42)));

    mds.apply(new Drop(v42), MutationOptions.DEFAULT);
  }

  @Test
  public void testReadFromMissingIndexFails() throws IOException {
    String indexName = "idxmissing" + Math.abs(new Random().nextInt());
    try (ElasticsearchMetadataStorage store =
             newStore(indexName, "localhost:" + System.getProperty("elastic.http.port", "9200"))) {
      // A missing index must not be mistaken for a missing document
      store.read(new Read(MetadataEntity.ofDataset("ns", "ds")));
      Assert.fail("Expected reading from a missing index to fail");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("index_not_found_exception"));
    }
  }

  @Test
  public void testDeleteFromDroppedIndexFails() throws IOException {
    assertDeleteFromDroppedIndexFails(DeleteMode.SINGLE);
  }

  @Test
  public void testBatchDeleteFromDroppedIndexFails() throws IOException {
    assertDeleteFromDroppedIndexFails(DeleteMode.BATCH);
  }

  /** How the entities are deleted in {@link #assertDeleteFromDroppedIndexFails(DeleteMode)}. */
  private enum DeleteMode {
    /** Deletes one entity in a single request. */
    SINGLE,
    /** Deletes two entities in a bulk request. */
    BATCH
  }

  /**
   * Drops the index after the documents were read and before they are deleted, and verifies that
   * the delete fails instead of treating the missing index like missing documents.
   *
   * @param mode whether to delete in a single request or in a bulk request
   */
  private static void assertDeleteFromDroppedIndexFails(DeleteMode mode) throws IOException {
    boolean batch = mode == DeleteMode.BATCH;
    String elasticPort = System.getProperty("elastic.http.port", "9200");
    String indexName = "idxdropped" + Math.abs(new Random().nextInt());
    MetadataEntity ds1 = MetadataEntity.ofDataset("ns", "dropped_ds1");
    MetadataEntity ds2 = MetadataEntity.ofDataset("ns", "dropped_ds2");
    Metadata metadata = new Metadata(MetadataScope.USER, tags("t"), props());

    try (ElasticsearchMetadataStorage directStore = newStore(indexName, "localhost:" + elasticPort);
         CloseableHttpClient forwardClient = HttpClients.createDefault()) {
      directStore.createIndex();
      try {
        directStore.batch(Arrays.asList(new Update(ds1, metadata), new Update(ds2, metadata)),
            MutationOptions.DEFAULT);

        // Forward all requests to the cluster, but drop the index right before the delete
        String deleteMethod = batch ? "POST" : "DELETE";
        String deletePath = batch ? "/_bulk" : "/" + indexName + "/_doc/";
        HttpServer proxy = startProxy(elasticPort, forwardClient, (method, path, body) -> {
          if (deleteMethod.equals(method) && path.startsWith(deletePath)) {
            directStore.dropIndex();
          }
        });
        try (ElasticsearchMetadataStorage proxyStore =
                 newStore(indexName, "localhost:" + proxy.getAddress().getPort())) {
          if (batch) {
            proxyStore.batch(Arrays.asList(new Drop(ds1), new Drop(ds2)), MutationOptions.DEFAULT);
          } else {
            proxyStore.apply(new Drop(ds1), MutationOptions.DEFAULT);
          }
          Assert.fail("Expected deleting from a dropped index to fail");
        } catch (IOException e) {
          String messages = e.getMessage() + Arrays.stream(e.getSuppressed())
              .map(Throwable::getMessage).collect(Collectors.joining(", ", " [", "]"));
          Assert.assertTrue(messages, messages.contains("index_not_found_exception"));
        } finally {
          proxy.stop(0);
        }
      } finally {
        directStore.dropIndex();
      }
    }
  }

  @Test
  public void testCreateIndexCreatedByAnotherService() throws IOException {
    String openSearch = "{\"version\":{\"distribution\":\"opensearch\",\"number\":\"3.10.0\"}}";
    // Another service created the index after the existence check
    assertCreateIndex(openSearch, 400,
        "{\"error\":{\"type\":\"resource_already_exists_exception\"},\"status\":400}", null);
    assertCreateIndex(openSearch, 400,
        "{\"error\":{\"type\":\"illegal_argument_exception\"},\"status\":400}",
        "Unable to create index");
    assertCreateIndex(openSearch, 200, "{\"acknowledged\":false}", "not acknowledged");
  }

  @Test
  public void testCreateIndexChecksClusterVersion() throws IOException {
    assertCreateIndex("{\"version\":{\"number\":\"6.8.23\"}}", 200, "{\"acknowledged\":true}",
        "Elasticsearch 6.8.23");
    assertCreateIndex("{\"version\":{\"number\":\"7.10.2\"}}", 200, "{\"acknowledged\":true}",
        null);
    assertCreateIndex("{\"tagline\":\"unknown\"}", 200, "{\"acknowledged\":true}",
        "Unable to determine the version");

    // The user is not allowed to read the version of the cluster
    HttpServer server = startFakeCluster((method, path, body) -> new FakeResponse(403, "{}"));
    try (ElasticsearchMetadataStorage store = newStore(FAKE_INDEX, server)) {
      store.createIndex();
      Assert.fail("Expected creating the index to fail");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("cluster:monitor/main"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testReadUnparsableDocumentFails() throws IOException {
    // The document has a source that is not an object
    HttpServer server = startFakeCluster((method, path, body) -> new FakeResponse(200,
        "{\"found\":true,\"_seq_no\":1,\"_primary_term\":1,\"_source\":42}"));
    try (ElasticsearchMetadataStorage store = newStore(FAKE_INDEX, server)) {
      store.read(new Read(MetadataEntity.ofDataset("ns", "ds")));
      Assert.fail("Expected reading an unparsable document to fail");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage(),
          e.getMessage().startsWith("Failed to read from index for entity"));
      Assert.assertTrue(String.valueOf(e.getCause()), e.getCause() instanceof RuntimeException);
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testBatchFailures() throws IOException {
    // One write fails for a reason other than a conflict. The response also has a document that
    // was not in the request, which is ignored.
    AtomicInteger bulkRequests = new AtomicInteger();
    IOException e = batchAgainstFakeCluster(
        "{\"docs\":[{\"found\":false},{\"found\":false}]}", bulkRequests);
    Assert.assertEquals("Bulk request unsuccessful", e.getMessage());
    // The retry logic adds its own suppressed exception, so only look at the failed writes
    List<String> failures = Arrays.stream(e.getSuppressed()).map(Throwable::getMessage)
        .filter(message -> message.contains("request unsuccessful for entity"))
        .collect(Collectors.toList());
    Assert.assertEquals(failures.toString(), 1, failures.size());
    String failure = failures.get(0);
    Assert.assertTrue(failure, failure.contains("mapper_parsing_exception"));
    Assert.assertTrue(failure, failure.contains("fake_ds2"));
    // A failure that is not a conflict is not retried
    Assert.assertEquals(1, bulkRequests.get());

    // The multi-get returns fewer documents than requested, so nothing is written
    bulkRequests.set(0);
    e = batchAgainstFakeCluster("{\"docs\":[{\"found\":false}]}", bulkRequests);
    Assert.assertTrue(e.getMessage(),
        e.getMessage().startsWith("Failed to read from index for entities"));
    Assert.assertEquals(0, bulkRequests.get());

    // The multi-get response has no documents, so nothing is written
    e = batchAgainstFakeCluster("{}", bulkRequests);
    Assert.assertTrue(e.getMessage(),
        e.getMessage().startsWith("Failed to read from index for entities"));
    Assert.assertEquals(0, bulkRequests.get());

    // The multi-get fails for one document, so nothing is written
    e = batchAgainstFakeCluster(
        "{\"docs\":[{\"found\":false},{\"error\":{\"type\":\"shard_failure\"}}]}",
        bulkRequests);
    Assert.assertTrue(e.getMessage(), e.getMessage().contains("shard_failure"));
    Assert.assertEquals(0, bulkRequests.get());
  }

  /**
   * Writes two datasets in one batch against a fake cluster, and returns the exception that the
   * batch throws. The fake cluster answers the multi-get with the given body, and it answers a
   * bulk request with {@link #toFakeBulkResponse(String)}.
   */
  private static IOException batchAgainstFakeCluster(String multiGetBody,
      AtomicInteger bulkRequests) throws IOException {
    HttpServer server = startFakeCluster((method, path, body) -> {
      if (path.equals("/" + FAKE_INDEX + "/_mget")) {
        return new FakeResponse(200, multiGetBody);
      }
      if (path.equals("/_bulk")) {
        bulkRequests.incrementAndGet();
        return new FakeResponse(200, toFakeBulkResponse(body));
      }
      return new FakeResponse(400, "{\"error\":{\"type\":\"unexpected_request\"}}");
    });
    Metadata metadata = new Metadata(MetadataScope.USER, tags("t"), props());
    try (ElasticsearchMetadataStorage store = newStore(FAKE_INDEX, server)) {
      store.batch(Arrays.asList(
          new Update(MetadataEntity.ofDataset("ns", "fake_ds1"), metadata),
          new Update(MetadataEntity.ofDataset("ns", "fake_ds2"), metadata)),
          MutationOptions.DEFAULT);
      throw new AssertionError("Expected the batch to fail");
    } catch (IOException e) {
      return e;
    } finally {
      server.stop(0);
    }
  }

  /**
   * Returns a bulk response for the given bulk request body: the first write succeeds, the other
   * writes fail with a mapping error, and one more failed item has a document id that was not in
   * the request.
   */
  private static String toFakeBulkResponse(String bulkBody) {
    JsonArray items = new JsonArray();
    for (String line : bulkBody.split("\n")) {
      Map.Entry<String, JsonElement> action =
          new JsonParser().parse(line).getAsJsonObject().entrySet().iterator().next();
      boolean isActionLine = Arrays.asList("create", "index", "delete").contains(action.getKey());
      if (!isActionLine) {
        continue;
      }
      boolean isFirstWrite = items.size() == 0;
      JsonObject result = new JsonObject();
      result.add("_id", action.getValue().getAsJsonObject().get("_id"));
      if (isFirstWrite) {
        result.addProperty("status", 201);
      } else {
        JsonObject error = new JsonObject();
        error.addProperty("type", "mapper_parsing_exception");
        result.addProperty("status", 400);
        result.add("error", error);
      }
      JsonObject item = new JsonObject();
      item.add(action.getKey(), result);
      items.add(item);
    }
    JsonObject unknown = new JsonObject();
    unknown.addProperty("_id", "dataset:namespace=ns,dataset=unknown");
    unknown.addProperty("status", 400);
    JsonObject unknownError = new JsonObject();
    unknownError.addProperty("type", "mapper_parsing_exception");
    unknown.add("error", unknownError);
    JsonObject unknownItem = new JsonObject();
    unknownItem.add("create", unknown);
    items.add(unknownItem);

    JsonObject response = new JsonObject();
    response.addProperty("errors", true);
    response.add("items", items);
    return response.toString();
  }

  /**
   * Calls {@link ElasticsearchMetadataStorage#createIndex()} against a fake cluster that has no
   * index, and that answers the root endpoint and the request to create the index as given.
   *
   * @param expectedError a part of the expected error message, or null if no error is expected
   */
  private static void assertCreateIndex(String rootBody, int createStatus, String createBody,
      @Nullable String expectedError) throws IOException {
    HttpServer server = startFakeCluster((method, path, body) -> {
      if ("/".equals(path)) {
        return new FakeResponse(200, rootBody);
      }
      if ("HEAD".equals(method)) {
        return new FakeResponse(404, "");
      }
      if ("PUT".equals(method)) {
        return new FakeResponse(createStatus, createBody);
      }
      if (path.endsWith("/_settings")) {
        return new FakeResponse(200, "{\"" + FAKE_INDEX + "\":{\"settings\":{\"index\":{}}}}");
      }
      return new FakeResponse(404, "{\"found\":false}");
    });
    try (ElasticsearchMetadataStorage store = newStore(FAKE_INDEX, server)) {
      store.createIndex();
      if (expectedError != null) {
        Assert.fail("Expected an error that contains: " + expectedError);
      }
    } catch (IOException e) {
      if (expectedError == null || !e.getMessage().contains(expectedError)) {
        throw e;
      }
    } finally {
      server.stop(0);
    }
  }

  private static ElasticsearchMetadataStorage newStore(String indexName, String hosts) {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Config.CONF_ELASTIC_INDEX_NAME, indexName);
    cConf.set(Config.CONF_ELASTIC_HOSTS, hosts);
    return new ElasticsearchMetadataStorage(cConf, SConfiguration.create());
  }

  private static ElasticsearchMetadataStorage newStore(String indexName, HttpServer server) {
    return newStore(indexName, "localhost:" + server.getAddress().getPort());
  }

  /** A response that a fake cluster sends. */
  private static final class FakeResponse {
    private final int status;
    private final String body;

    FakeResponse(int status, String body) {
      this.status = status;
      this.body = body;
    }
  }

  /** Decides how a fake cluster answers a request. */
  private interface FakeCluster {
    FakeResponse respond(String method, String path, String body);
  }

  /** Starts a fake search cluster that answers every request as the given function decides. */
  private static HttpServer startFakeCluster(FakeCluster cluster) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/", exchange -> {
      String body = new String(ByteStreams.toByteArray(exchange.getRequestBody()),
          StandardCharsets.UTF_8);
      FakeResponse response = cluster.respond(exchange.getRequestMethod(),
          exchange.getRequestURI().getPath(), body);
      sendJson(exchange, response.status, response.body);
    });
    server.start();
    return server;
  }

  /** Called by a proxy before it forwards a request to the search cluster. */
  private interface BeforeForward {
    void accept(String method, String path, String body) throws IOException;
  }

  /**
   * Starts a proxy that forwards every request to the search cluster, and the response back to the
   * client. The proxy calls the given function before it forwards a request.
   */
  private static HttpServer startProxy(String elasticPort, CloseableHttpClient client,
      BeforeForward beforeForward) throws IOException {
    HttpServer proxy = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    proxy.createContext("/", exchange -> {
      String method = exchange.getRequestMethod();
      byte[] body = ByteStreams.toByteArray(exchange.getRequestBody());
      beforeForward.accept(method, exchange.getRequestURI().getPath(),
          new String(body, StandardCharsets.UTF_8));
      ForwardRequest request = new ForwardRequest(method,
          URI.create("http://localhost:" + elasticPort + exchange.getRequestURI()));
      String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
      if (contentType != null) {
        request.setHeader("Content-Type", contentType);
      }
      if (body.length > 0) {
        request.setEntity(new ByteArrayEntity(body));
      }
      try (CloseableHttpResponse response = client.execute(request)) {
        HttpEntity entity = response.getEntity();
        byte[] responseBody = entity == null || "HEAD".equals(method)
            ? new byte[0] : EntityUtils.toByteArray(entity);
        sendJson(exchange, response.getStatusLine().getStatusCode(),
            new String(responseBody, StandardCharsets.UTF_8));
      }
    });
    proxy.start();
    return proxy;
  }

  private static void sendJson(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
    if (bytes.length > 0) {
      exchange.getResponseBody().write(bytes);
    }
    exchange.close();
  }

  @Override
  protected void validateCursor(String cursor, int expectedOffset, int expectedPageSize) {
    Cursor c = Cursor.fromString(cursor);
    Assert.assertEquals(expectedOffset, c.getOffset());
    Assert.assertEquals(expectedPageSize, c.getLimit());
  }

  private static final class ForwardRequest extends HttpEntityEnclosingRequestBase {

    private final String method;

    ForwardRequest(String method, URI uri) {
      this.method = method;
      setURI(uri);
    }

    @Override
    public String getMethod() {
      return method;
    }
  }
}
