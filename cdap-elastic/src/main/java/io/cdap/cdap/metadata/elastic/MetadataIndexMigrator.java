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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Moves the metadata index from an Elasticsearch 6.8 cluster to an OpenSearch cluster.
 *
 * <p>OpenSearch 3 cannot open the index files that Elasticsearch 6.8 wrote, and the two clusters
 * never run at the same time. Therefore the migration has two steps that share a directory:
 * <ol>
 *   <li>{@code export} reads all documents of the index from the old cluster and writes them to a
 *       file in the directory;</li>
 *   <li>{@code import} creates the index in the new cluster and loads the documents from that
 *       file.</li>
 * </ol>
 *
 * <p>Both steps can run again after a failure. A step that completed before does nothing.
 *
 * <p>The migration must not stop the upgrade of an instance for a problem of the old index. If
 * the old cluster cannot return all documents, the export keeps what it can read.
 *
 * <p>The export stops writes to the old index, so that the exported documents are complete. From
 * then on, the old version of CDAP cannot store metadata. The import records in the new index
 * that system metadata can be missing. The metadata service then writes the system metadata
 * again from the application, artifact and dataset stores
 * (see {@link ElasticsearchMetadataStorage#isRebuildRequired()}).
 */
public final class MetadataIndexMigrator {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataIndexMigrator.class);
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  @VisibleForTesting
  static final String MANIFEST_FILE = "manifest.json";
  @VisibleForTesting
  static final String DOCS_FILE = "docs.ndjson";
  @VisibleForTesting
  static final String REJECTED_DOCS_FILE = "rejected_docs.ndjson";
  @VisibleForTesting
  static final String IMPORT_COMPLETE_FILE = "IMPORT_COMPLETE";

  private static final String SCROLL_KEEP_ALIVE = "5m";
  private static final int SCROLL_PAGE_SIZE = 200;
  private static final int BULK_MAX_DOCS = 200;
  private static final int BULK_MAX_CHARS = 5 * 1024 * 1024;
  private static final int REQUEST_ATTEMPTS = 5;
  private static final int EXPORT_ATTEMPTS = 3;
  private static final int HTTP_BAD_REQUEST = 400;
  private static final int HTTP_NOT_FOUND = 404;
  private static final int SOCKET_TIMEOUT_MS = 120_000;
  private static final long DEFAULT_WAIT_SECONDS = 300;
  private static final long DEFAULT_RETRY_DELAY_MS = 2000;

  /**
   * The result of an export.
   */
  enum Status {
    /** All documents of the index were exported. */
    OK,
    /** The old cluster has no metadata index. */
    EMPTY,
    /** Only a part of the documents could be read, for example because a shard is corrupt. */
    PARTIAL,
    /** No documents could be read from the old cluster. */
    UNAVAILABLE
  }

  /**
   * Describes the exported documents. The export writes this file last, so its presence means
   * that the export completed.
   */
  static final class Manifest {

    private final Status status;
    private final String index;
    private final long docCount;
    @Nullable
    private final String sha256;
    @Nullable
    private final String reason;

    Manifest(Status status, String index, long docCount, @Nullable String sha256,
        @Nullable String reason) {
      this.status = status;
      this.index = index;
      this.docCount = docCount;
      this.sha256 = sha256;
      this.reason = reason;
    }

    Status getStatus() {
      return status;
    }

    long getDocCount() {
      return docCount;
    }

    @Nullable
    String getSha256() {
      return sha256;
    }

    @Override
    public String toString() {
      return GSON.toJson(this);
    }
  }

  private final IndexRestClient client;
  private final String indexName;
  private final Path dir;
  private final long waitMillis;
  private final long retryDelayMillis;

  /**
   * Creates a migrator.
   *
   * @param client the client for the cluster: the old cluster for an export, the new cluster for
   *     an import
   * @param indexName the name of the metadata index
   * @param dir the directory that holds the exported documents
   * @param waitMillis how long to wait for the cluster to become usable
   * @param retryDelayMillis how long to wait before a request is sent again
   */
  MetadataIndexMigrator(IndexRestClient client, String indexName, Path dir, long waitMillis,
      long retryDelayMillis) {
    this.client = client;
    this.indexName = indexName;
    this.dir = dir;
    this.waitMillis = waitMillis;
    this.retryDelayMillis = retryDelayMillis;
  }

  /**
   * Runs the migration tool.
   *
   * @param args {@code export|import --url <url> --dir <dir> [--index <name>]
   *     [--wait-seconds <seconds>]}
   */
  public static void main(String[] args) {
    try {
      run(args);
    } catch (Exception e) {
      LOG.error("Metadata index migration failed", e);
      System.err.println("Metadata index migration failed: " + e);
      System.exit(1);
    }
  }

  @VisibleForTesting
  static void run(String[] args) throws IOException {
    if (args.length < 1 || args.length % 2 != 1) {
      throw new IllegalArgumentException(usage());
    }
    String command = args[0];
    Map<String, String> options = new HashMap<>();
    for (int i = 1; i < args.length; i += 2) {
      options.put(args[i], args[i + 1]);
    }
    String url = options.remove("--url");
    String directory = options.remove("--dir");
    String index = options.containsKey("--index") ? options.remove("--index")
        : Config.DEFAULT_INDEX_NAME;
    long waitSeconds = options.containsKey("--wait-seconds")
        ? Long.parseLong(options.remove("--wait-seconds")) : DEFAULT_WAIT_SECONDS;
    if (url == null || directory == null || !options.isEmpty()) {
      throw new IllegalArgumentException(usage());
    }
    if (url.endsWith("/")) {
      url = url.substring(0, url.length() - 1);
    }

    try (IndexRestClient client = new IndexRestClient(url, null, null, true, SOCKET_TIMEOUT_MS)) {
      MetadataIndexMigrator migrator = new MetadataIndexMigrator(
          client, index, Paths.get(directory), waitSeconds * 1000, DEFAULT_RETRY_DELAY_MS);
      switch (command) {
        case "export":
          migrator.exportIndex();
          break;
        case "import":
          migrator.importIndex();
          break;
        default:
          throw new IllegalArgumentException(usage());
      }
    }
  }

  private static String usage() {
    return "Usage: " + MetadataIndexMigrator.class.getName()
        + " export|import --url <url> --dir <dir> [--index <name>] [--wait-seconds <seconds>]";
  }

  /**
   * Writes all documents of the index to the directory.
   *
   * <p>This method does not fail if the cluster or the index is not usable. It then writes a
   * manifest that tells the import that documents are missing.
   *
   * @throws IOException if the directory cannot be written
   */
  void exportIndex() throws IOException {
    Files.createDirectories(dir);
    if (Files.exists(dir.resolve(IMPORT_COMPLETE_FILE))) {
      LOG.info("The import completed before. Nothing to export.");
      return;
    }
    Manifest existing = readManifest();
    if (existing != null && isExportIntact(existing)) {
      LOG.info("The export completed before: {}", existing);
      return;
    }

    Manifest manifest;
    try {
      manifest = exportWhenUsable();
      writeAtomically(dir.resolve(MANIFEST_FILE), manifest.toString());
    } catch (IOException | RuntimeException e) {
      // Without a manifest the migration does not continue, and the old cluster stays in use.
      setWriteBlock(false);
      throw e;
    }
    LOG.info("Export finished: {}", manifest);
  }

  /**
   * Loads the exported documents into the index of the new cluster.
   *
   * @throws IOException if the import did not complete. The caller can run it again.
   */
  void importIndex() throws IOException {
    Path completeFile = dir.resolve(IMPORT_COMPLETE_FILE);
    if (Files.exists(completeFile)) {
      LOG.info("The import completed before. Nothing to import.");
      return;
    }
    Manifest manifest = readManifest();
    if (manifest == null) {
      throw new IOException("No " + MANIFEST_FILE + " in " + dir + ". Run the export first.");
    }
    if (manifest.getDocCount() > 0 && !isExportIntact(manifest)) {
      throw new IOException("The exported documents in " + dir + " do not match " + manifest);
    }

    waitForCluster();
    createIndexIfAbsent();
    long rejected = manifest.getDocCount() > 0 ? loadDocuments() : 0;

    request("POST", Arrays.asList(indexName, "_refresh")).ensureSuccess();
    long indexed = request("GET", Arrays.asList(indexName, "_count")).ensureSuccess()
        .getJson().get("count").getAsLong();
    if (indexed < manifest.getDocCount() - rejected) {
      throw new IOException(String.format(
          "Index %s has %d documents after the import, expected %d exported minus %d rejected",
          indexName, indexed, manifest.getDocCount(), rejected));
    }

    // CDAP was in use while the index moved, and its writes of system metadata failed without
    // notice in that time. Also, the export can be incomplete. In both cases the metadata
    // service must write the system metadata again.
    ElasticsearchMetadataStorage.setRebuildRequired(client, indexName, true);
    JsonObject summary = new JsonObject();
    summary.addProperty("export_status", manifest.getStatus().name());
    summary.addProperty("indexed", indexed);
    summary.addProperty("rejected", rejected);
    writeAtomically(completeFile, GSON.toJson(summary));
    Files.deleteIfExists(dir.resolve(DOCS_FILE));
    LOG.info("Import finished: {}", summary);
  }

  /**
   * Exports the index when the old cluster can tell whether it has the index. Waits for that
   * until the wait time is over. The time of the export itself does not count.
   */
  private Manifest exportWhenUsable() throws IOException {
    long deadline = System.currentTimeMillis() + waitMillis;
    while (true) {
      String problem;
      try {
        IndexRestClient.Response exists = request("HEAD", Collections.singletonList(indexName));
        if (exists.getStatus() == HTTP_NOT_FOUND) {
          return new Manifest(Status.EMPTY, indexName, 0, null, null);
        }
        if (exists.isSuccess()) {
          return exportDocuments();
        }
        problem = exists.toString();
      } catch (ClusterException e) {
        problem = e.getMessage();
      }
      if (System.currentTimeMillis() >= deadline) {
        LOG.warn("Giving up on the export of index {}: {}", indexName, problem);
        return new Manifest(Status.UNAVAILABLE, indexName, 0, null, problem);
      }
      LOG.warn("Index {} is not usable yet, trying again: {}", indexName, problem);
      sleep(retryDelayMillis);
    }
  }

  /**
   * Writes the documents of the index to the documents file.
   *
   * <p>If the cluster stops to answer during the scan, this method scans again. If no scan
   * completes, it keeps the scan that returned the most documents.
   *
   * @throws IOException if the file cannot be written
   */
  private Manifest exportDocuments() throws IOException {
    // Make all documents visible to the search, and stop writes, so that the old index does not
    // get documents that the export does not see. If this fails, then the index has a problem
    // that the export cannot repair, and the export still reads what it can.
    try {
      logIfFailed(request("POST", Arrays.asList(indexName, "_refresh")));
    } catch (ClusterException e) {
      LOG.warn("Unable to refresh index {}: {}", indexName, e.getMessage());
    }
    setWriteBlock(true);

    Path docsFile = dir.resolve(DOCS_FILE);
    Path tmpFile = dir.resolve(DOCS_FILE + ".tmp");
    Files.deleteIfExists(docsFile);
    Scan best = null;
    for (int attempt = 1; attempt <= EXPORT_ATTEMPTS; attempt++) {
      Scan scan = scanToFile(tmpFile);
      if (best == null || scan.incompleteReason == null || scan.exported > best.exported) {
        Files.move(tmpFile, docsFile, StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.ATOMIC_MOVE);
        best = scan;
      }
      // Scan again only if a request failed. Failed shards do not get better with a new scan.
      if (!scan.requestFailed) {
        break;
      }
      LOG.warn("Scan {} of index {} stopped after {} documents: {}", attempt, indexName,
          scan.exported, scan.incompleteReason);
      if (attempt < EXPORT_ATTEMPTS) {
        sleep(retryDelayMillis);
      }
    }
    Files.deleteIfExists(tmpFile);

    if (best.incompleteReason == null) {
      return new Manifest(Status.OK, indexName, best.exported, best.sha256, null);
    }
    if (best.exported == 0) {
      Files.deleteIfExists(docsFile);
      return new Manifest(Status.UNAVAILABLE, indexName, 0, null, best.incompleteReason);
    }
    return new Manifest(Status.PARTIAL, indexName, best.exported, best.sha256,
        best.incompleteReason);
  }

  /**
   * Scrolls over all documents of the index and writes one line for each document. Stops at the
   * first request that the cluster does not answer.
   *
   * @throws IOException if the file cannot be written
   */
  private Scan scanToFile(Path file) throws IOException {
    JsonObject search = new JsonObject();
    search.addProperty("size", SCROLL_PAGE_SIZE);
    JsonArray sort = new JsonArray();
    sort.add(new JsonPrimitive("_doc"));
    search.add("sort", sort);

    MessageDigest digest = newSha256();
    long exported = 0;
    long total = -1;
    String incompleteReason = null;
    boolean requestFailed = false;
    String scrollId = null;
    try (Writer writer = new BufferedWriter(new OutputStreamWriter(
        new DigestOutputStream(Files.newOutputStream(file), digest), StandardCharsets.UTF_8))) {
      JsonObject page = successJson(request("POST", Arrays.asList(indexName, "_search"),
          ImmutableMap.of("scroll", SCROLL_KEEP_ALIVE), search.toString(),
          ContentType.APPLICATION_JSON));
      total = getTotalHits(page);
      while (true) {
        scrollId = page.get("_scroll_id").getAsString();
        // A shard that has no copy to search counts neither as successful nor as failed.
        JsonObject shards = page.getAsJsonObject("_shards");
        if (shards.get("successful").getAsInt() < shards.get("total").getAsInt()) {
          incompleteReason = "Not all shards answered: " + shards;
        }
        JsonArray hits = page.getAsJsonObject("hits").getAsJsonArray("hits");
        if (hits.size() == 0) {
          break;
        }
        for (JsonElement hit : hits) {
          JsonObject line = new JsonObject();
          line.add("_id", hit.getAsJsonObject().get("_id"));
          line.add("_source", hit.getAsJsonObject().get("_source"));
          writer.write(line.toString());
          writer.write('\n');
          exported++;
        }
        JsonObject next = new JsonObject();
        next.addProperty("scroll", SCROLL_KEEP_ALIVE);
        next.addProperty("scroll_id", scrollId);
        page = successJson(request("POST", Arrays.asList("_search", "scroll"), next));
      }
    } catch (ClusterException e) {
      // Keep the documents that the scan wrote before the failure.
      requestFailed = true;
      incompleteReason = e.getMessage();
    } finally {
      clearScroll(scrollId);
    }
    if (incompleteReason == null && exported != total) {
      incompleteReason = String.format("Read %d of %d documents", exported, total);
    }
    return new Scan(exported, toHex(digest.digest()), incompleteReason, requestFailed);
  }

  /**
   * Stops or allows writes to the index. Does not fail: the export does not depend on it.
   */
  private void setWriteBlock(boolean blocked) {
    JsonObject settings = new JsonObject();
    settings.addProperty("index.blocks.write", blocked);
    try {
      logIfFailed(request("PUT", Arrays.asList(indexName, "_settings"), settings));
    } catch (ClusterException e) {
      LOG.warn("Unable to set the write block of index {} to {}: {}", indexName, blocked,
          e.getMessage());
    }
  }

  private void clearScroll(@Nullable String scrollId) {
    if (scrollId == null) {
      return;
    }
    JsonObject clear = new JsonObject();
    clear.addProperty("scroll_id", scrollId);
    try {
      client.execute("DELETE", Arrays.asList("_search", "scroll"), Collections.emptyMap(), clear);
    } catch (IOException e) {
      LOG.debug("Unable to clear the scroll. It expires by itself.", e);
    }
  }

  /**
   * Returns the total number of hits. Elasticsearch 6 returns a number, later versions and
   * OpenSearch return an object.
   */
  private static long getTotalHits(JsonObject searchResponse) {
    JsonElement total = searchResponse.getAsJsonObject("hits").get("total");
    return total.isJsonObject() ? total.getAsJsonObject().get("value").getAsLong()
        : total.getAsLong();
  }

  private void waitForCluster() throws IOException {
    long deadline = System.currentTimeMillis() + waitMillis;
    while (true) {
      String problem;
      try {
        IndexRestClient.Response health = client.execute("GET", Arrays.asList("_cluster", "health"),
            ImmutableMap.of("wait_for_status", "yellow", "timeout", "5s"), (JsonElement) null);
        if (health.isSuccess() && !health.getJson().get("timed_out").getAsBoolean()) {
          return;
        }
        problem = health.toString();
      } catch (IOException e) {
        problem = e.toString();
      }
      if (System.currentTimeMillis() >= deadline) {
        throw new IOException("The cluster is not ready for the import: " + problem);
      }
      LOG.warn("The cluster is not ready yet, trying again: {}", problem);
      sleep(retryDelayMillis);
    }
  }

  /**
   * Creates the index in the same way as {@link ElasticsearchMetadataStorage#createIndex()} does
   * without configuration: the number of shards and replicas are the defaults of the cluster.
   */
  private void createIndexIfAbsent() throws IOException {
    IndexRestClient.Response exists = request("HEAD", Collections.singletonList(indexName));
    if (exists.getStatus() != HTTP_NOT_FOUND) {
      exists.ensureSuccess();
      return;
    }
    JsonObject index = new JsonObject();
    index.add("settings", ElasticsearchMetadataStorage.createSettings(null, null, null));
    index.add("mappings", ElasticsearchMetadataStorage.createMappings());
    request("PUT", Collections.singletonList(indexName), index).ensureSuccess();
    LOG.info("Created index {}", indexName);
  }

  /**
   * Sends the exported documents to the index.
   *
   * @return the number of documents that the cluster rejected
   */
  private long loadDocuments() throws IOException {
    Files.deleteIfExists(dir.resolve(REJECTED_DOCS_FILE));
    long rejected = 0;
    long sent = 0;
    List<JsonObject> batch = new ArrayList<>();
    int batchChars = 0;
    try (BufferedReader reader = Files.newBufferedReader(dir.resolve(DOCS_FILE))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!batch.isEmpty()
            && (batch.size() >= BULK_MAX_DOCS || batchChars + line.length() > BULK_MAX_CHARS)) {
          rejected += sendBatch(batch);
          sent += batch.size();
          LOG.info("Imported {} documents", sent);
          batch.clear();
          batchChars = 0;
        }
        batch.add(new JsonParser().parse(line).getAsJsonObject());
        batchChars += line.length();
      }
    }
    if (!batch.isEmpty()) {
      rejected += sendBatch(batch);
    }
    return rejected;
  }

  /**
   * Indexes one batch of documents. Sends the documents again that failed for a reason that can
   * go away. Writes the documents that the cluster rejected as invalid to a file and continues,
   * because one invalid document must not stop the migration.
   *
   * @return the number of rejected documents
   */
  private long sendBatch(List<JsonObject> batch) throws IOException {
    long rejected = 0;
    List<JsonObject> pending = batch;
    for (int attempt = 1; !pending.isEmpty(); attempt++) {
      StringBuilder body = new StringBuilder();
      for (JsonObject doc : pending) {
        JsonObject target = new JsonObject();
        target.addProperty("_index", indexName);
        target.add("_id", doc.get("_id"));
        JsonObject action = new JsonObject();
        action.add("index", target);
        body.append(action).append('\n').append(doc.get("_source")).append('\n');
      }
      JsonObject response = successJson(request("POST", Collections.singletonList("_bulk"),
          Collections.emptyMap(), body.toString(), IndexRestClient.NDJSON));
      if (!response.get("errors").getAsBoolean()) {
        return rejected;
      }

      JsonArray items = response.getAsJsonArray("items");
      List<JsonObject> failed = new ArrayList<>();
      for (int i = 0; i < pending.size(); i++) {
        JsonObject result = items.get(i).getAsJsonObject().getAsJsonObject("index");
        int status = result.get("status").getAsInt();
        if (status == HTTP_BAD_REQUEST) {
          rejected++;
          reject(pending.get(i), result.get("error"));
        } else if (isRetryable(status)) {
          failed.add(pending.get(i));
        } else if (status < 200 || status >= 300) {
          throw new IOException("Unable to index document " + pending.get(i).get("_id") + ": "
              + result);
        }
      }
      if (!failed.isEmpty() && attempt >= REQUEST_ATTEMPTS) {
        throw new IOException(String.format("%d documents were not indexed after %d attempts",
            failed.size(), attempt));
      }
      pending = failed;
      if (!pending.isEmpty()) {
        sleep(retryDelayMillis);
      }
    }
    return rejected;
  }

  private void reject(JsonObject doc, JsonElement error) throws IOException {
    LOG.error("The cluster rejected document {}: {}", doc.get("_id"), error);
    JsonObject line = new JsonObject();
    line.add("doc", doc);
    line.add("error", error);
    Files.write(dir.resolve(REJECTED_DOCS_FILE),
        (line + "\n").getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }

  private IndexRestClient.Response request(String method, List<String> path)
      throws ClusterException {
    return request(method, path, Collections.emptyMap(), null, ContentType.APPLICATION_JSON);
  }

  private IndexRestClient.Response request(String method, List<String> path, JsonObject body)
      throws ClusterException {
    return request(method, path, Collections.emptyMap(), body.toString(),
        ContentType.APPLICATION_JSON);
  }

  /**
   * Sends a request. Sends it again if the cluster did not answer or answered with a status that
   * can go away. All requests of this tool can be repeated.
   *
   * @return the response, which can have a status that is not 2xx
   * @throws ClusterException if the cluster did not give a final answer
   */
  private IndexRestClient.Response request(String method, List<String> path,
      Map<String, String> parameters, @Nullable String body, ContentType contentType)
      throws ClusterException {
    String problem = null;
    for (int attempt = 1; attempt <= REQUEST_ATTEMPTS; attempt++) {
      if (attempt > 1) {
        sleep(retryDelayMillis);
      }
      try {
        IndexRestClient.Response response =
            client.execute(method, path, parameters, body, contentType);
        if (!isRetryable(response.getStatus())) {
          return response;
        }
        problem = response.toString();
      } catch (IOException e) {
        problem = e.toString();
      }
      LOG.warn("Request {} {} failed in attempt {}: {}", method, path, attempt, problem);
    }
    throw new ClusterException(problem);
  }

  private static JsonObject successJson(IndexRestClient.Response response)
      throws ClusterException {
    try {
      return response.ensureSuccess().getJson();
    } catch (IOException e) {
      throw new ClusterException(e.getMessage());
    }
  }

  private static boolean isRetryable(int status) {
    return status == 408 || status == 429 || status >= 500;
  }

  private static void logIfFailed(IndexRestClient.Response response) {
    if (!response.isSuccess()) {
      LOG.warn("Continuing after a request that is not required failed: {}", response);
    }
  }

  @Nullable
  private Manifest readManifest() throws IOException {
    Path file = dir.resolve(MANIFEST_FILE);
    if (!Files.exists(file)) {
      return null;
    }
    try {
      Manifest manifest = GSON.fromJson(
          new String(Files.readAllBytes(file), StandardCharsets.UTF_8), Manifest.class);
      return manifest == null || manifest.getStatus() == null ? null : manifest;
    } catch (JsonSyntaxException e) {
      LOG.warn("Ignoring invalid manifest {}", file, e);
      return null;
    }
  }

  /**
   * Returns whether the file of exported documents is the one that the manifest describes.
   */
  private boolean isExportIntact(Manifest manifest) throws IOException {
    if (manifest.getDocCount() == 0) {
      return true;
    }
    Path docs = dir.resolve(DOCS_FILE);
    if (!Files.exists(docs)) {
      return false;
    }
    MessageDigest digest = newSha256();
    byte[] buffer = new byte[64 * 1024];
    try (InputStream in = Files.newInputStream(docs)) {
      int read;
      while ((read = in.read(buffer)) > 0) {
        digest.update(buffer, 0, read);
      }
    }
    return toHex(digest.digest()).equals(manifest.getSha256());
  }

  /**
   * Writes a file in such a way that it is either complete or absent.
   */
  private static void writeAtomically(Path file, String content) throws IOException {
    Path tmpFile = file.resolveSibling(file.getFileName() + ".tmp");
    try (OutputStream out = Files.newOutputStream(tmpFile)) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }
    Files.move(tmpFile, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
  }

  private static MessageDigest newSha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      // Every Java platform has SHA-256.
      throw new IllegalStateException(e);
    }
  }

  private static String toHex(byte[] bytes) {
    StringBuilder hex = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      hex.append(String.format("%02x", b));
    }
    return hex.toString();
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted during the metadata index migration", e);
    }
  }

  /**
   * The result of one scan over the index.
   */
  private static final class Scan {

    private final long exported;
    private final String sha256;
    /** Why the scan did not return all documents, or null if it did. */
    @Nullable
    private final String incompleteReason;
    /** Whether the scan stopped because the cluster did not answer a request. */
    private final boolean requestFailed;

    Scan(long exported, String sha256, @Nullable String incompleteReason, boolean requestFailed) {
      this.exported = exported;
      this.sha256 = sha256;
      this.incompleteReason = incompleteReason;
      this.requestFailed = requestFailed;
    }
  }

  /**
   * The cluster did not give a usable answer. The export handles this state, hence it has its
   * own type, different from a failure to write the export files.
   */
  private static final class ClusterException extends IOException {

    ClusterException(String message) {
      super(message);
    }
  }
}
