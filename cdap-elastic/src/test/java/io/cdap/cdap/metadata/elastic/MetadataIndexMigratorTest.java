/*
 * Copyright © 2026 Cask Data, Inc.
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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.metadata.elastic.MetadataIndexMigrator.Manifest;
import io.cdap.cdap.metadata.elastic.MetadataIndexMigrator.Status;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.cdap.spi.metadata.ScopedName;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport;

/**
 * Tests for {@link MetadataIndexMigrator}. Most tests use a fake cluster, so that they can make
 * the cluster fail. One test moves an index of the search cluster of the integration tests.
 */
public class MetadataIndexMigratorTest {

  private static final String INDEX = "cdap.metadata";
  private static final long WAIT_MILLIS = 300;
  private static final long RETRY_DELAY_MILLIS = 5;
  private static final int TEST_SOCKET_TIMEOUT_MS = 10_000;

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

  private FakeCluster cluster;
  private ApacheHttpClient5Transport transport;
  private OpenSearchClient client;
  private Path dir;

  @Before
  public void setUp() throws IOException {
    cluster = new FakeCluster();
    transport = MetadataIndexMigrator.createTransport(
        "localhost:" + cluster.getPort(), TEST_SOCKET_TIMEOUT_MS);
    client = new OpenSearchClient(transport);
    dir = tmpFolder.newFolder().toPath().resolve("migration");
  }

  @After
  public void tearDown() throws IOException {
    transport.close();
    cluster.stop();
    if (cluster.assertionError != null) {
      throw cluster.assertionError;
    }
  }

  private MetadataIndexMigrator migrator() {
    return new MetadataIndexMigrator(client, INDEX, dir, WAIT_MILLIS, RETRY_DELAY_MILLIS);
  }

  private Manifest readManifest() throws IOException {
    return new Gson().fromJson(
        new String(Files.readAllBytes(dir.resolve(MetadataIndexMigrator.MANIFEST_FILE)),
            StandardCharsets.UTF_8), Manifest.class);
  }

  private JsonObject readImportSummary() throws IOException {
    return new JsonParser().parse(new String(
        Files.readAllBytes(dir.resolve(MetadataIndexMigrator.IMPORT_COMPLETE_FILE)),
        StandardCharsets.UTF_8)).getAsJsonObject();
  }

  @Test
  public void testExportWritesAllDocumentsOnce() throws IOException {
    // more documents than one scroll page
    cluster.addDocs(450);

    migrator().exportIndex();

    Manifest manifest = readManifest();
    Assert.assertEquals(Status.OK, manifest.getStatus());
    Assert.assertEquals(450, manifest.getDocCount());
    List<String> lines = Files.readAllLines(dir.resolve(MetadataIndexMigrator.DOCS_FILE));
    Assert.assertEquals(450, lines.size());
    JsonObject first = new JsonParser().parse(lines.get(0)).getAsJsonObject();
    Assert.assertEquals("doc-0", first.get("_id").getAsString());
    Assert.assertEquals(cluster.docs.get("doc-0"), first.get("_source"));
    Assert.assertTrue("The export must stop writes to the old index", cluster.writesBlocked);
    Assert.assertTrue("The export must release the scroll", cluster.scrollCleared);

    // an export that completed does not read the cluster again
    int requests = cluster.requests.get();
    migrator().exportIndex();
    Assert.assertEquals(requests, cluster.requests.get());
  }

  @Test
  public void testExportRunsAgainIfDocumentsFileIsDamaged() throws IOException {
    cluster.addDocs(3);
    migrator().exportIndex();
    Files.write(dir.resolve(MetadataIndexMigrator.DOCS_FILE), new byte[]{'x'});

    migrator().exportIndex();

    Assert.assertEquals(3, Files.readAllLines(dir.resolve(MetadataIndexMigrator.DOCS_FILE)).size());
    Assert.assertEquals(Status.OK, readManifest().getStatus());
  }

  @Test
  public void testExportOfMissingIndexIsEmpty() throws IOException {
    cluster.indexExists = false;

    migrator().exportIndex();

    Manifest manifest = readManifest();
    Assert.assertEquals(Status.EMPTY, manifest.getStatus());
    Assert.assertEquals(0, manifest.getDocCount());
    Assert.assertFalse(Files.exists(dir.resolve(MetadataIndexMigrator.DOCS_FILE)));
  }

  @Test
  public void testExportGivesUpOnUnavailableCluster() throws IOException {
    cluster.addDocs(3);
    cluster.failRequests = Integer.MAX_VALUE;

    migrator().exportIndex();

    Manifest manifest = readManifest();
    Assert.assertEquals(Status.UNAVAILABLE, manifest.getStatus());
    Assert.assertEquals(0, manifest.getDocCount());
  }

  @Test
  public void testExportGivesUpOnUnreachableCluster() throws IOException {
    cluster.stop();

    migrator().exportIndex();

    Assert.assertEquals(Status.UNAVAILABLE, readManifest().getStatus());
  }

  @Test
  public void testExportWaitsForCluster() throws IOException {
    cluster.addDocs(3);
    // more failures than one request() tolerates, so exportWhenUsable() retries
    cluster.failRequests = 6;

    migrator().exportIndex();

    Manifest manifest = readManifest();
    Assert.assertEquals(Status.OK, manifest.getStatus());
    Assert.assertEquals(3, manifest.getDocCount());
  }

  @Test
  public void testExportPartialWhenSearchResponseMissesScrollId() throws IOException {
    cluster.addDocs(3);
    cluster.omitScrollId = true;

    migrator().exportIndex();

    Manifest manifest = readManifest();
    Assert.assertEquals(Status.PARTIAL, manifest.getStatus());
    Assert.assertEquals(3, manifest.getDocCount());
  }

  @Test
  public void testExportWithFailedShardsIsPartial() throws IOException {
    cluster.addDocs(7);
    cluster.failedShards = 2;

    migrator().exportIndex();

    Manifest manifest = readManifest();
    Assert.assertEquals(Status.PARTIAL, manifest.getStatus());
    Assert.assertEquals(7, manifest.getDocCount());
    Assert.assertEquals(7, Files.readAllLines(dir.resolve(MetadataIndexMigrator.DOCS_FILE)).size());
  }

  @Test
  public void testExportWithUnassignedPrimaryShardIsPartial() throws IOException {
    cluster.addDocs(450);
    cluster.unassignedShards = 1;

    migrator().exportIndex();

    Manifest manifest = readManifest();
    Assert.assertEquals(Status.PARTIAL, manifest.getStatus());
    Assert.assertEquals(450, manifest.getDocCount());
    Assert.assertEquals(1, cluster.searchCalls.get());
    Map<String, JsonObject> exported = new LinkedHashMap<>(cluster.docs);

    cluster.reset();
    migrator().importIndex();
    Assert.assertEquals(exported, cluster.docs);
    Assert.assertEquals("PARTIAL", readImportSummary().get("export_status").getAsString());
  }

  @Test
  public void testExportWithFailedShardsOnLaterPageRunsOneScan() throws IOException {
    cluster.addDocs(450);
    cluster.failedShards = 1;
    cluster.failedShardsFromPage = 2;

    migrator().exportIndex();

    Manifest manifest = readManifest();
    Assert.assertEquals(Status.PARTIAL, manifest.getStatus());
    Assert.assertEquals(450, manifest.getDocCount());
    Assert.assertEquals(1, cluster.searchCalls.get());
  }

  @Test
  public void testExportRescansWhenScrollFailsOnce() throws IOException {
    cluster.addDocs(450);
    cluster.failScrollOnPage = 2;
    cluster.failScrollScans = 1;

    migrator().exportIndex();

    Manifest manifest = readManifest();
    Assert.assertEquals(Status.OK, manifest.getStatus());
    Assert.assertEquals(450, manifest.getDocCount());
    Assert.assertEquals(2, cluster.searchCalls.get());
    Assert.assertEquals(450,
        Files.readAllLines(dir.resolve(MetadataIndexMigrator.DOCS_FILE)).size());
  }

  @Test
  public void testExportKeepsFirstPagesWhenScrollFailsEveryScan() throws Exception {
    cluster.addDocs(450);
    cluster.failScrollOnPage = 3;
    cluster.failScrollScans = Integer.MAX_VALUE;

    migrator().exportIndex();

    Manifest manifest = readManifest();
    Assert.assertEquals(Status.PARTIAL, manifest.getStatus());
    Assert.assertEquals(400, manifest.getDocCount());
    Path docsFile = dir.resolve(MetadataIndexMigrator.DOCS_FILE);
    Assert.assertEquals(400, Files.readAllLines(docsFile).size());
    Assert.assertEquals(sha256Hex(Files.readAllBytes(docsFile)), manifest.getSha256());

    cluster.reset();
    migrator().importIndex();
    Assert.assertEquals(400, cluster.docs.size());
    Assert.assertEquals("PARTIAL", readImportSummary().get("export_status").getAsString());
  }

  @Test
  public void testExportUnavailableWhenFirstSearchFailsEveryScan() throws IOException {
    cluster.addDocs(10);
    cluster.failSearch = true;

    migrator().exportIndex();

    Manifest manifest = readManifest();
    Assert.assertEquals(Status.UNAVAILABLE, manifest.getStatus());
    Assert.assertEquals(0, manifest.getDocCount());
    Assert.assertFalse(Files.exists(dir.resolve(MetadataIndexMigrator.DOCS_FILE)));
  }

  @Test
  public void testExportUnblocksWritesWhenLocalWriteFailsBeforeManifest() throws IOException {
    cluster.addDocs(3);
    // Make docs.ndjson.tmp a directory so opening it fails after index.blocks.write=true is sent.
    Files.createDirectories(dir.resolve(MetadataIndexMigrator.DOCS_FILE + ".tmp"));

    try {
      migrator().exportIndex();
      Assert.fail("Expected export to fail when documents file is not writable");
    } catch (IOException e) {
      // expected
    }

    Assert.assertFalse(Files.exists(dir.resolve(MetadataIndexMigrator.MANIFEST_FILE)));
    Assert.assertEquals(Arrays.asList(true, false), cluster.writeBlockUpdates);
    Assert.assertFalse(cluster.writesBlocked);
  }

  @Test
  public void testExportAfterImportDoesNothing() throws IOException {
    cluster.addDocs(3);
    Files.createDirectories(dir);
    Files.write(dir.resolve(MetadataIndexMigrator.IMPORT_COMPLETE_FILE), new byte[0]);

    migrator().exportIndex();

    Assert.assertEquals(0, cluster.requests.get());
    Assert.assertFalse(Files.exists(dir.resolve(MetadataIndexMigrator.MANIFEST_FILE)));
  }

  @Test
  public void testImportLoadsAllDocuments() throws IOException {
    cluster.addDocs(450);
    migrator().exportIndex();
    Map<String, JsonObject> exported = new LinkedHashMap<>(cluster.docs);
    cluster.reset();

    migrator().importIndex();

    Assert.assertEquals(exported, cluster.docs);
    // the number of shards and replicas are the defaults of the cluster
    Assert.assertEquals(
        new JsonParser().parse(
            ElasticsearchMetadataStorage.createSettings(null, null, null).toJsonString()),
        cluster.createRequest.getAsJsonObject("settings"));
    Assert.assertEquals(
        new JsonParser().parse(ElasticsearchMetadataStorage.createMappings().toJsonString()),
        cluster.createRequest.getAsJsonObject("mappings"));
    Assert.assertEquals("true", cluster.meta.get(Config.MAPPING_REBUILD_REQUIRED).getAsString());
    // the other entries of the index meta stay
    Assert.assertTrue(cluster.meta.has(Config.MAPPING_CDAP_VERSION));
    JsonObject summary = readImportSummary();
    Assert.assertEquals("OK", summary.get("export_status").getAsString());
    Assert.assertEquals(450, summary.get("indexed").getAsLong());
    Assert.assertEquals(0, summary.get("rejected").getAsLong());
    Assert.assertFalse("The import must delete the exported documents",
        Files.exists(dir.resolve(MetadataIndexMigrator.DOCS_FILE)));

    // an import that completed does not use the cluster again
    int requests = cluster.requests.get();
    migrator().importIndex();
    Assert.assertEquals(requests, cluster.requests.get());
  }

  @Test
  public void testImportOfEmptyExportCreatesIndex() throws IOException {
    cluster.indexExists = false;
    migrator().exportIndex();

    migrator().importIndex();

    Assert.assertNotNull(cluster.createRequest);
    Assert.assertTrue(cluster.docs.isEmpty());
    Assert.assertEquals("EMPTY", readImportSummary().get("export_status").getAsString());
  }

  @Test
  public void testImportOfPartialExportLoadsReadableDocuments() throws IOException {
    cluster.addDocs(7);
    cluster.failedShards = 1;
    migrator().exportIndex();
    cluster.reset();

    migrator().importIndex();

    Assert.assertEquals(7, cluster.docs.size());
    Assert.assertEquals("PARTIAL", readImportSummary().get("export_status").getAsString());
  }

  @Test
  public void testImportOfUnavailableExportCreatesIndex() throws IOException {
    cluster.failRequests = Integer.MAX_VALUE;
    migrator().exportIndex();
    cluster.reset();

    migrator().importIndex();

    Assert.assertNotNull(cluster.createRequest);
    Assert.assertTrue(cluster.docs.isEmpty());
    Assert.assertEquals("true", cluster.meta.get(Config.MAPPING_REBUILD_REQUIRED).getAsString());
    Assert.assertEquals("UNAVAILABLE", readImportSummary().get("export_status").getAsString());
  }

  @Test
  public void testImportToleratesAlreadyExistsOnCreate() throws IOException {
    cluster.addDocs(3);
    migrator().exportIndex();
    cluster.reset();
    cluster.createAlreadyExists = true;

    migrator().importIndex();

    Assert.assertEquals(3, cluster.docs.size());
    Assert.assertEquals("true", cluster.meta.get(Config.MAPPING_REBUILD_REQUIRED).getAsString());
  }

  @Test
  public void testImportContinuesAfterRejectedDocument() throws IOException {
    cluster.addDocs(5);
    migrator().exportIndex();
    cluster.reset();
    cluster.rejectedIds.add("doc-2");

    migrator().importIndex();

    Assert.assertEquals(4, cluster.docs.size());
    Assert.assertFalse(cluster.docs.containsKey("doc-2"));
    List<String> rejected =
        Files.readAllLines(dir.resolve(MetadataIndexMigrator.REJECTED_DOCS_FILE));
    Assert.assertEquals(1, rejected.size());
    Assert.assertEquals("doc-2", new JsonParser().parse(rejected.get(0)).getAsJsonObject()
        .getAsJsonObject("doc").get("_id").getAsString());
    Assert.assertEquals(1, readImportSummary().get("rejected").getAsLong());
  }

  @Test
  public void testImportContinuesAfterMalformedDocumentLine() throws Exception {
    Files.createDirectories(dir);
    String content = "{\"_id\":\"doc-0\",\"_source\":{\"name\":\"a\"}}\n"
        + "{corrupted json\n"
        + "{\"_id\":\"doc-missing-source\",\"_source\":null}\n"
        + "{\"_id\":\"doc-1\",\"_source\":{\"name\":\"b\"}}\n";
    byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    Files.write(dir.resolve(MetadataIndexMigrator.DOCS_FILE), bytes);
    Manifest manifest = new Manifest(Status.OK, INDEX, 4, sha256Hex(bytes), null);
    Files.write(dir.resolve(MetadataIndexMigrator.MANIFEST_FILE),
        manifest.toString().getBytes(StandardCharsets.UTF_8));
    cluster.reset();

    migrator().importIndex();

    Assert.assertEquals(2, cluster.docs.size());
    Assert.assertTrue(cluster.docs.containsKey("doc-0"));
    Assert.assertTrue(cluster.docs.containsKey("doc-1"));
    List<String> rejected =
        Files.readAllLines(dir.resolve(MetadataIndexMigrator.REJECTED_DOCS_FILE));
    Assert.assertEquals(2, rejected.size());
    Assert.assertEquals("{corrupted json", new JsonParser().parse(rejected.get(0))
        .getAsJsonObject().get("raw_line").getAsString());
    Assert.assertEquals("{\"_id\":\"doc-missing-source\",\"_source\":null}",
        new JsonParser().parse(rejected.get(1)).getAsJsonObject().get("raw_line").getAsString());
    JsonObject summary = readImportSummary();
    Assert.assertEquals(2, summary.get("indexed").getAsLong());
    Assert.assertEquals(2, summary.get("rejected").getAsLong());
  }

  @Test
  public void testImportFailsOnCountMismatch() throws IOException {
    cluster.addDocs(5);
    migrator().exportIndex();
    cluster.reset();
    cluster.countOverride = 4L;

    try {
      migrator().importIndex();
      Assert.fail("Expected import to fail when indexed count is lower than expected");
    } catch (IOException e) {
      // expected
    }
    Assert.assertFalse(Files.exists(dir.resolve(MetadataIndexMigrator.IMPORT_COMPLETE_FILE)));
    Assert.assertTrue(Files.exists(dir.resolve(MetadataIndexMigrator.DOCS_FILE)));
  }

  @Test
  public void testImportFailsWhenThrottlingExhaustsRetries() throws IOException {
    cluster.addDocs(3);
    migrator().exportIndex();
    cluster.reset();
    cluster.alwaysThrottledIds.add("doc-1");

    try {
      migrator().importIndex();
      Assert.fail("Expected import to fail when throttled document exhausts retries");
    } catch (IOException e) {
      // expected
    }
    Assert.assertFalse(Files.exists(dir.resolve(MetadataIndexMigrator.IMPORT_COMPLETE_FILE)));
    Assert.assertTrue(Files.exists(dir.resolve(MetadataIndexMigrator.DOCS_FILE)));
  }

  @Test
  public void testImportSendsThrottledDocumentsAgain() throws IOException {
    cluster.addDocs(5);
    migrator().exportIndex();
    cluster.reset();
    cluster.throttledOnce.add("doc-1");
    cluster.throttledOnce.add("doc-4");

    migrator().importIndex();

    Assert.assertEquals(5, cluster.docs.size());
  }

  @Test
  public void testImportMixedRejectedAndThrottledInSameBatch() throws IOException {
    cluster.addDocs(5);
    migrator().exportIndex();
    cluster.reset();
    cluster.rejectedIds.add("doc-1");
    cluster.throttledOnce.add("doc-2");
    cluster.throttledOnce.add("doc-4");

    migrator().importIndex();

    Assert.assertEquals(4, cluster.docs.size());
    Assert.assertFalse(cluster.docs.containsKey("doc-1"));
    Assert.assertTrue(cluster.docs.containsKey("doc-2"));
    Assert.assertTrue(cluster.docs.containsKey("doc-4"));
    List<String> rejected =
        Files.readAllLines(dir.resolve(MetadataIndexMigrator.REJECTED_DOCS_FILE));
    Assert.assertEquals(1, rejected.size());
    JsonObject summary = readImportSummary();
    Assert.assertEquals(4, summary.get("indexed").getAsLong());
    Assert.assertEquals(1, summary.get("rejected").getAsLong());
  }

  @Test
  public void testImportFlushesPartialBatchBeforeOversizedDocument() throws IOException {
    cluster.addDocs(1);
    JsonObject largeSource = new JsonObject();
    largeSource.addProperty("payload", Strings.repeat("x", 5 * 1024 * 1024));
    cluster.docs.put("doc-large", largeSource);
    JsonObject trailingSource = new JsonObject();
    trailingSource.addProperty("name", "trailing");
    cluster.docs.put("doc-trailing", trailingSource);

    migrator().exportIndex();
    Map<String, JsonObject> exported = new LinkedHashMap<>(cluster.docs);
    cluster.reset();

    migrator().importIndex();

    Assert.assertEquals(exported, cluster.docs);
    Assert.assertEquals(Arrays.asList(
        Collections.singletonList("doc-0"),
        Collections.singletonList("doc-large"),
        Collections.singletonList("doc-trailing")), cluster.bulkBatches);
  }

  @Test
  public void testImportRunsAgainAfterFailure() throws IOException {
    cluster.addDocs(5);
    migrator().exportIndex();
    cluster.reset();
    cluster.failBulk = true;

    try {
      migrator().importIndex();
      Assert.fail("Expected the import to fail");
    } catch (IOException e) {
      // expected
    }
    Assert.assertFalse(Files.exists(dir.resolve(MetadataIndexMigrator.IMPORT_COMPLETE_FILE)));
    Assert.assertTrue(Files.exists(dir.resolve(MetadataIndexMigrator.DOCS_FILE)));

    cluster.failBulk = false;
    migrator().importIndex();
    Assert.assertEquals(5, cluster.docs.size());
  }

  @Test(expected = IOException.class)
  public void testImportFailsOnDamagedDocumentsFile() throws IOException {
    cluster.addDocs(5);
    migrator().exportIndex();
    Files.write(dir.resolve(MetadataIndexMigrator.DOCS_FILE), new byte[]{'x'});

    migrator().importIndex();
  }

  @Test(expected = IOException.class)
  public void testImportFailsWithoutExport() throws IOException {
    migrator().importIndex();
  }

  @Test(expected = IOException.class)
  public void testImportFailsIfClusterIsNotReady() throws IOException {
    cluster.indexExists = false;
    migrator().exportIndex();
    cluster.failRequests = Integer.MAX_VALUE;

    migrator().importIndex();
  }

  @Test
  public void testCommandLine() throws IOException {
    cluster.addDocs(3);
    String urlWithTrailingSlash = "http://localhost:" + cluster.getPort() + "/";

    MetadataIndexMigrator.run(new String[]{
        "export", "--url", urlWithTrailingSlash, "--dir", dir.toString(), "--wait-seconds", "1"});
    Assert.assertEquals(Status.OK, readManifest().getStatus());

    cluster.reset();
    MetadataIndexMigrator.run(new String[]{
        "import", "--url", urlWithTrailingSlash, "--dir", dir.toString(), "--index", INDEX});
    Assert.assertEquals(3, cluster.docs.size());

    for (String[] invalid : new String[][]{
        {}, {"export"}, {"copy", "--url", urlWithTrailingSlash, "--dir", dir.toString()},
        {"export", "--url", urlWithTrailingSlash},
        {"export", "--url", urlWithTrailingSlash, "--dir"},
        {"export", "--url", urlWithTrailingSlash, "--dir", dir.toString(), "--unknown", "x"}}) {
      try {
        MetadataIndexMigrator.run(invalid);
        Assert.fail("Expected arguments to be invalid: " + String.join(" ", invalid));
      } catch (IllegalArgumentException e) {
        // expected
      }
    }
  }

  private static String sha256Hex(byte[] bytes) throws Exception {
    byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
    StringBuilder hex = new StringBuilder(digest.length * 2);
    for (byte b : digest) {
      hex.append(String.format("%02x", b));
    }
    return hex.toString();
  }

  /**
   * Moves an index of the search cluster of the integration tests to another index, and reads
   * the metadata through the storage.
   */
  @Test
  public void testMigrateRealIndex() throws IOException {
    String port = System.getProperty("elastic.http.port", "9200");
    int suffix = new Random().nextInt(Integer.MAX_VALUE);
    String sourceIndex = "source" + suffix;
    String targetIndex = "target" + suffix;
    MetadataEntity entity = MetadataEntity.ofDataset("ns", "ds");
    Metadata metadata = new Metadata(ImmutableSet.of(
        new ScopedName(MetadataScope.USER, "tag one")),
        ImmutableMap.of(new ScopedName(MetadataScope.SYSTEM, "key"),
            "value with \"quotes\" and \n newline"));

    try (ElasticsearchMetadataStorage source = createStorage(port, sourceIndex);
        ElasticsearchMetadataStorage target = createStorage(port, targetIndex);
        ApacheHttpClient5Transport realTransport =
            MetadataIndexMigrator.createTransport("localhost:" + port, TEST_SOCKET_TIMEOUT_MS)) {
      OpenSearchClient realClient = new OpenSearchClient(realTransport);
      try {
        source.createIndex();
        source.apply(new MetadataMutation.Update(entity, metadata),
            MutationOptions.DEFAULT);

        new MetadataIndexMigrator(realClient, sourceIndex, dir, 10000, 100).exportIndex();
        Assert.assertEquals(Status.OK, readManifest().getStatus());
        Assert.assertEquals(1, readManifest().getDocCount());
        new MetadataIndexMigrator(realClient, targetIndex, dir, 10000, 100).importIndex();

        Assert.assertEquals(metadata, target.read(new Read(entity)));
        Assert.assertEquals(source.getVersionInfo().getMappingsChecksum(),
            target.getVersionInfo().getMappingsChecksum());
        Assert.assertFalse(source.isRebuildRequired());
        Assert.assertTrue(target.isRebuildRequired());
        target.markRebuilt();
        Assert.assertFalse(target.isRebuildRequired());
        // the version info is in the same object as the rebuild marker and must stay
        Assert.assertEquals(VersionInfo.METADATA_VERSION,
            target.getVersionInfo().getMetadataVersion());
        Assert.assertEquals(metadata, target.read(new Read(entity)));
      } finally {
        source.dropIndex();
        target.dropIndex();
      }
    }
  }

  private static ElasticsearchMetadataStorage createStorage(String port, String indexName) {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Config.CONF_ELASTIC_HOSTS, "localhost:" + port);
    cConf.set(Config.CONF_ELASTIC_INDEX_NAME, indexName);
    return new ElasticsearchMetadataStorage(cConf, SConfiguration.create());
  }

  /**
   * A fake of the few REST calls that the migration tool sends, with switches to make them fail.
   */
  private static final class FakeCluster {

    private static final int PAGE_SIZE = 200;

    private final HttpServer server;
    final AtomicInteger requests = new AtomicInteger();
    final AtomicInteger searchCalls = new AtomicInteger();
    final Map<String, JsonObject> docs = Collections.synchronizedMap(new LinkedHashMap<>());
    final List<String> rejectedIds = new ArrayList<>();
    final List<String> throttledOnce = new ArrayList<>();
    final List<String> alwaysThrottledIds = new ArrayList<>();
    final List<Boolean> writeBlockUpdates = Collections.synchronizedList(new ArrayList<>());
    final List<List<String>> bulkBatches = Collections.synchronizedList(new ArrayList<>());

    volatile boolean indexExists = true;
    volatile boolean createAlreadyExists;
    volatile boolean omitScrollId;
    volatile Long countOverride;
    volatile AssertionError assertionError;
    volatile int failRequests;
    volatile int failedShards;
    volatile int failedShardsFromPage = 1;
    volatile int unassignedShards;
    volatile boolean failSearch;
    volatile int failScrollOnPage;
    volatile int failScrollScans;
    volatile boolean failBulk;
    volatile boolean writesBlocked;
    volatile boolean scrollCleared;
    volatile JsonObject createRequest;
    volatile JsonObject meta = new JsonObject();
    private volatile int scrollOffset;

    FakeCluster() throws IOException {
      server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
      server.createContext("/", this::handle);
      server.start();
    }

    int getPort() {
      return server.getAddress().getPort();
    }

    void stop() {
      server.stop(0);
    }

    void addDocs(int count) {
      for (int i = 0; i < count; i++) {
        JsonObject source = new JsonObject();
        source.addProperty("name", "entity " + i);
        source.addProperty("created", 1700000000000L + i);
        docs.put("doc-" + i, source);
      }
    }

    /**
     * Turns this fake into a new cluster without an index.
     */
    void reset() {
      docs.clear();
      bulkBatches.clear();
      writeBlockUpdates.clear();
      indexExists = false;
      createAlreadyExists = false;
      omitScrollId = false;
      countOverride = null;
      alwaysThrottledIds.clear();
      failedShards = 0;
      failedShardsFromPage = 1;
      unassignedShards = 0;
      failSearch = false;
      failScrollOnPage = 0;
      failScrollScans = 0;
      failRequests = 0;
      createRequest = null;
      meta = new JsonObject();
    }

    private void handle(HttpExchange exchange) throws IOException {
      requests.incrementAndGet();
      String method = exchange.getRequestMethod();
      String path = exchange.getRequestURI().getPath();
      String body = readBody(exchange);
      try {
        if (failRequests > 0) {
          failRequests--;
          respond(exchange, 503, "{\"error\":\"not ready\"}");
          return;
        }
        route(exchange, method, path, body);
      } catch (AssertionError e) {
        assertionError = e;
        respond(exchange, 400, "{\"error\":\"" + e + "\"}");
      } catch (RuntimeException e) {
        respond(exchange, 500, "{\"error\":\"" + e + "\"}");
      }
    }

    private void route(HttpExchange exchange, String method, String path, String body)
        throws IOException {
      String index = "/" + INDEX;
      if (path.equals("/_cluster/health")) {
        respond(exchange, 200, "{\"status\":\"green\",\"timed_out\":false}");
      } else if (path.equals(index) && method.equals("HEAD")) {
        respond(exchange, indexExists ? 200 : 404, "");
      } else if (path.equals(index) && method.equals("PUT")) {
        createRequest = new JsonParser().parse(body).getAsJsonObject();
        meta = createRequest.getAsJsonObject("mappings").getAsJsonObject("_meta");
        indexExists = true;
        if (createAlreadyExists) {
          respond(exchange, 400,
              "{\"error\":{\"type\":\"resource_already_exists_exception\"}}");
        } else {
          respond(exchange, 200, "{\"acknowledged\":true}");
        }
      } else if (!indexExists && path.startsWith(index + "/")) {
        respond(exchange, 404, "{\"error\":\"index_not_found_exception\"}");
      } else if (path.equals(index + "/_refresh")) {
        respond(exchange, 200, "{}");
      } else if (path.equals(index + "/_settings") && method.equals("PUT")) {
        boolean blocked = new JsonParser().parse(body).getAsJsonObject()
            .get("index.blocks.write").getAsBoolean();
        writesBlocked = blocked;
        writeBlockUpdates.add(blocked);
        respond(exchange, 200, "{\"acknowledged\":true}");
      } else if (path.equals(index + "/_count")) {
        long count = countOverride != null ? countOverride : docs.size();
        respond(exchange, 200, "{\"count\":" + count + "}");
      } else if (path.equals(index + "/_mapping") && method.equals("GET")) {
        JsonObject mappings = new JsonObject();
        mappings.add("_meta", meta);
        JsonObject indexJson = new JsonObject();
        indexJson.add("mappings", mappings);
        JsonObject response = new JsonObject();
        response.add(INDEX, indexJson);
        respond(exchange, 200, response.toString());
      } else if (path.equals(index + "/_mapping") && method.equals("PUT")) {
        meta = new JsonParser().parse(body).getAsJsonObject().getAsJsonObject("_meta");
        respond(exchange, 200, "{\"acknowledged\":true}");
      } else if (path.equals(index + "/_search")) {
        Assert.assertEquals("scroll=5m", exchange.getRequestURI().getQuery());
        searchCalls.incrementAndGet();
        if (failSearch) {
          respond(exchange, 503, "{\"error\":\"search unavailable\"}");
          return;
        }
        scrollOffset = 0;
        respond(exchange, 200, nextPage());
      } else if (path.equals("/_search/scroll") && method.equals("POST")) {
        int currentPage = scrollOffset / PAGE_SIZE + 1;
        if (currentPage == failScrollOnPage && searchCalls.get() <= failScrollScans) {
          respond(exchange, 503, "{\"error\":\"scroll failed\"}");
          return;
        }
        respond(exchange, 200, nextPage());
      } else if (path.equals("/_search/scroll") && method.equals("DELETE")) {
        scrollCleared = true;
        respond(exchange, 200, "{}");
      } else if (path.equals("/_bulk")) {
        respond(exchange, failBulk ? 400 : 200, bulk(body));
      } else {
        respond(exchange, 400, "{\"error\":\"unexpected request " + method + " " + path + "\"}");
      }
    }

    /**
     * Returns the next page of a scroll, in the format of Elasticsearch 6.
     */
    private String nextPage() {
      int currentPage = scrollOffset / PAGE_SIZE + 1;
      List<Map.Entry<String, JsonObject>> all = new ArrayList<>(docs.entrySet());
      JsonArray hits = new JsonArray();
      int end = Math.min(all.size(), scrollOffset + PAGE_SIZE);
      for (int i = scrollOffset; i < end; i++) {
        JsonObject hit = new JsonObject();
        hit.addProperty("_index", INDEX);
        hit.addProperty("_type", "meta");
        hit.addProperty("_id", all.get(i).getKey());
        hit.add("_source", all.get(i).getValue());
        hits.add(hit);
      }
      scrollOffset = end;
      int pageFailedShards = currentPage >= failedShardsFromPage ? failedShards : 0;
      JsonObject shards = new JsonObject();
      shards.addProperty("total", 5);
      shards.addProperty("successful", 5 - pageFailedShards - unassignedShards);
      shards.addProperty("skipped", 0);
      shards.addProperty("failed", pageFailedShards);
      JsonObject hitsJson = new JsonObject();
      hitsJson.addProperty("total", all.size());
      hitsJson.add("hits", hits);
      JsonObject page = new JsonObject();
      if (!omitScrollId) {
        page.addProperty("_scroll_id", "scroll-1");
      }
      page.add("_shards", shards);
      page.add("hits", hitsJson);
      return page.toString();
    }

    private String bulk(String body) {
      String[] lines = body.split("\n");
      JsonArray items = new JsonArray();
      List<String> batchIds = new ArrayList<>();
      boolean errors = false;
      for (int i = 0; i < lines.length; i += 2) {
        JsonObject action = new JsonParser().parse(lines[i]).getAsJsonObject()
            .getAsJsonObject("index");
        Assert.assertEquals(INDEX, action.get("_index").getAsString());
        Assert.assertFalse("OpenSearch has no document types", action.has("_type"));
        String id = action.get("_id").getAsString();
        batchIds.add(id);
        int status = 201;
        if (rejectedIds.contains(id)) {
          status = 400;
        } else if (alwaysThrottledIds.contains(id) || throttledOnce.remove(id)) {
          status = 429;
        } else {
          docs.put(id, new JsonParser().parse(lines[i + 1]).getAsJsonObject());
        }
        errors |= status != 201;
        JsonObject result = new JsonObject();
        result.addProperty("_id", id);
        result.addProperty("status", status);
        if (status != 201) {
          result.addProperty("error", "status " + status);
        }
        JsonObject item = new JsonObject();
        item.add("index", result);
        items.add(item);
      }
      bulkBatches.add(batchIds);
      JsonObject response = new JsonObject();
      response.addProperty("errors", errors);
      response.add("items", items);
      return response.toString();
    }

    private static String readBody(HttpExchange exchange) throws IOException {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      try (InputStream in = exchange.getRequestBody()) {
        int read;
        while ((read = in.read(buffer)) > 0) {
          bytes.write(buffer, 0, read);
        }
      }
      return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
    }

    private static void respond(HttpExchange exchange, int status, String body)
        throws IOException {
      byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      boolean noBody = "HEAD".equals(exchange.getRequestMethod());
      exchange.sendResponseHeaders(status, noBody || bytes.length == 0 ? -1 : bytes.length);
      if (!noBody && bytes.length > 0) {
        exchange.getResponseBody().write(bytes);
      }
      exchange.close();
    }
  }
}
