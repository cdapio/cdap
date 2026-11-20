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

    CConfiguration directConf = CConfiguration.create();
    directConf.set(Config.CONF_ELASTIC_INDEX_NAME, indexName);
    directConf.set(Config.CONF_ELASTIC_HOSTS, "localhost:" + elasticPort);
    SConfiguration sConf = SConfiguration.create();

    AtomicBoolean injectedConflict = new AtomicBoolean(false);
    AtomicInteger appBulkWrites = new AtomicInteger();

    try (ElasticsearchMetadataStorage directStore =
             new ElasticsearchMetadataStorage(directConf, sConf);
         CloseableHttpClient forwardClient = HttpClients.createDefault()) {
      directStore.createIndex();
      try {
        directStore.batch(Arrays.asList(
            new Update(versionedApp, appInitial),
            new Update(dataset, dsInitial)), MutationOptions.DEFAULT);

        HttpServer proxy = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        proxy.createContext("/", exchange -> {
          byte[] reqBody = ByteStreams.toByteArray(exchange.getRequestBody());
          String rawUri = exchange.getRequestURI().toString();
          if (rawUri.startsWith("/_bulk")) {
            String bulkText = new String(reqBody, StandardCharsets.UTF_8);
            if (bulkText.contains("application:namespace=ns,application=verapp")) {
              appBulkWrites.incrementAndGet();
            }
          }
          ForwardRequest req = new ForwardRequest(
              exchange.getRequestMethod(),
              URI.create("http://localhost:" + elasticPort + rawUri));
          String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
          if (contentType != null) {
            req.setHeader("Content-Type", contentType);
          }
          if (reqBody.length > 0) {
            req.setEntity(new ByteArrayEntity(reqBody));
          }
          int status;
          byte[] respBody;
          try (CloseableHttpResponse resp = forwardClient.execute(req)) {
            status = resp.getStatusLine().getStatusCode();
            HttpEntity entity = resp.getEntity();
            respBody = entity == null ? new byte[0] : EntityUtils.toByteArray(entity);
          }
          if (rawUri.endsWith("/_mget") && injectedConflict.compareAndSet(false, true)) {
            // Advance dataset seq_no between _mget and _bulk so dataset conflicts with 409 once.
            directStore.apply(new Update(dataset, dsConcurrent), MutationOptions.DEFAULT);
          }
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          boolean noBody = "HEAD".equals(exchange.getRequestMethod()) || respBody.length == 0;
          exchange.sendResponseHeaders(status, noBody ? -1 : respBody.length);
          if (!noBody) {
            exchange.getResponseBody().write(respBody);
          }
          exchange.close();
        });
        proxy.start();
        try {
          CConfiguration proxyConf = CConfiguration.create();
          proxyConf.set(Config.CONF_ELASTIC_INDEX_NAME, indexName);
          proxyConf.set(Config.CONF_ELASTIC_HOSTS, "localhost:" + proxy.getAddress().getPort());
          try (ElasticsearchMetadataStorage proxyStore =
                   new ElasticsearchMetadataStorage(proxyConf, sConf)) {
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
          }
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
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Config.CONF_ELASTIC_INDEX_NAME, "idxmissing" + Math.abs(new Random().nextInt()));
    cConf.set(Config.CONF_ELASTIC_HOSTS,
        "localhost:" + System.getProperty("elastic.http.port", "9200"));
    try (ElasticsearchMetadataStorage store =
             new ElasticsearchMetadataStorage(cConf, SConfiguration.create())) {
      // A missing index must not be mistaken for a missing document
      store.read(new Read(MetadataEntity.ofDataset("ns", "ds")));
      Assert.fail("Expected reading from a missing index to fail");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("index_not_found_exception"));
    }
  }

  @Test
  public void testDeleteFromDroppedIndexFails() throws IOException {
    String elasticPort = System.getProperty("elastic.http.port", "9200");
    String indexName = "idxdropped" + Math.abs(new Random().nextInt());
    MetadataEntity dataset = MetadataEntity.ofDataset("ns", "dropped_ds");
    SConfiguration sConf = SConfiguration.create();

    CConfiguration directConf = CConfiguration.create();
    directConf.set(Config.CONF_ELASTIC_INDEX_NAME, indexName);
    directConf.set(Config.CONF_ELASTIC_HOSTS, "localhost:" + elasticPort);
    try (ElasticsearchMetadataStorage directStore =
             new ElasticsearchMetadataStorage(directConf, sConf);
         CloseableHttpClient forwardClient = HttpClients.createDefault()) {
      directStore.createIndex();
      directStore.apply(new Update(dataset, new Metadata(MetadataScope.USER, tags("t"), props())),
          MutationOptions.DEFAULT);

      // Forward all requests to the cluster, but drop the index right before the delete
      HttpServer proxy = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
      proxy.createContext("/", exchange -> {
        if ("DELETE".equals(exchange.getRequestMethod())) {
          directStore.dropIndex();
        }
        forward(exchange, forwardClient, elasticPort);
      });
      proxy.start();
      CConfiguration proxyConf = CConfiguration.create();
      proxyConf.set(Config.CONF_ELASTIC_INDEX_NAME, indexName);
      proxyConf.set(Config.CONF_ELASTIC_HOSTS, "localhost:" + proxy.getAddress().getPort());
      try (ElasticsearchMetadataStorage proxyStore =
               new ElasticsearchMetadataStorage(proxyConf, sConf)) {
        proxyStore.apply(new Drop(dataset), MutationOptions.DEFAULT);
        Assert.fail("Expected deleting from a dropped index to fail");
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("index_not_found_exception"));
      } finally {
        proxy.stop(0);
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
  }

  /**
   * Calls {@link ElasticsearchMetadataStorage#createIndex()} against a fake cluster that has no
   * index, and that answers the root endpoint and the request to create the index as given.
   *
   * @param expectedError a part of the expected error message, or null if no error is expected
   */
  private static void assertCreateIndex(String rootBody, int createStatus, String createBody,
      @Nullable String expectedError) throws IOException {
    String indexName = "fakeidx";
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/", exchange -> {
      ByteStreams.toByteArray(exchange.getRequestBody());
      String method = exchange.getRequestMethod();
      String path = exchange.getRequestURI().getPath();
      if ("/".equals(path)) {
        sendJson(exchange, 200, rootBody);
      } else if ("HEAD".equals(method)) {
        sendJson(exchange, 404, "");
      } else if ("PUT".equals(method)) {
        sendJson(exchange, createStatus, createBody);
      } else if (path.endsWith("/_settings")) {
        sendJson(exchange, 200, "{\"" + indexName + "\":{\"settings\":{\"index\":{}}}}");
      } else {
        sendJson(exchange, 404, "{\"found\":false}");
      }
    });
    server.start();
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Config.CONF_ELASTIC_INDEX_NAME, indexName);
    cConf.set(Config.CONF_ELASTIC_HOSTS, "localhost:" + server.getAddress().getPort());
    try (ElasticsearchMetadataStorage store =
             new ElasticsearchMetadataStorage(cConf, SConfiguration.create())) {
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

  private static void sendJson(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
    if (bytes.length > 0) {
      exchange.getResponseBody().write(bytes);
    }
    exchange.close();
  }

  /**
   * Forwards a request to the search cluster, and its response back to the client.
   */
  private static void forward(HttpExchange exchange, CloseableHttpClient client,
      String elasticPort) throws IOException {
    byte[] reqBody = ByteStreams.toByteArray(exchange.getRequestBody());
    ForwardRequest req = new ForwardRequest(exchange.getRequestMethod(),
        URI.create("http://localhost:" + elasticPort + exchange.getRequestURI()));
    String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
    if (contentType != null) {
      req.setHeader("Content-Type", contentType);
    }
    if (reqBody.length > 0) {
      req.setEntity(new ByteArrayEntity(reqBody));
    }
    try (CloseableHttpResponse resp = client.execute(req)) {
      HttpEntity entity = resp.getEntity();
      byte[] respBody = entity == null || "HEAD".equals(exchange.getRequestMethod())
          ? new byte[0] : EntityUtils.toByteArray(entity);
      sendJson(exchange, resp.getStatusLine().getStatusCode(),
          new String(respBody, StandardCharsets.UTF_8));
    }
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
