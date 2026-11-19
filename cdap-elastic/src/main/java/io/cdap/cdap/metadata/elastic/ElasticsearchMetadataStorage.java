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

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.inject.Inject;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.metadata.Cursor;
import io.cdap.cdap.common.metadata.MetadataConflictException;
import io.cdap.cdap.common.metadata.MetadataUtil;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.utils.Checksums;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataChange;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import io.cdap.cdap.spi.metadata.MetadataDirective;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataRecord;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
import io.cdap.cdap.spi.metadata.ScopedNameTypeAdapter;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.SearchResponse;
import io.cdap.cdap.spi.metadata.Sorting;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A metadata storage provider that delegates to an OpenSearch cluster.
 *
 * <p>The class, its package, and its configuration keys still carry the name Elasticsearch,
 * because {@code metadata.storage.implementation: elastic} selects this provider in existing
 * deployments.
 */
public class ElasticsearchMetadataStorage implements MetadataStorage {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchMetadataStorage.class);

  private static final String SETTING_NUMBER_OF_SHARDS = "number_of_shards";
  private static final String SETTING_NUMBER_OF_REPLICAS = "number_of_replicas";
  private static final String SETTING_MAX_RESULT_WINDOW = "max_result_window";

  @VisibleForTesting
  static final String MAPPING_RESOURCE = "index.mapping.json";

  // ScopedName needs a type adapter because it is used as a key in a map,
  // which is not supported by default in Gson:
  // - It would serialize as a string "scope:name"
  // - but then it would not know how to deserialize it.
  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(ScopedName.class, new ScopedNameTypeAdapter()).create();

  @VisibleForTesting
  static final boolean KEEP = true;
  @VisibleForTesting
  static final boolean DISCARD = false;

  // used to tokenize the query string, same as the MetadataDataset
  private static final Pattern SPACE_SEPARATOR_PATTERN = Pattern.compile("\\s+");

  // Various fields in a metadata document (indexed in the search cluster). Beware that these
  // directly correspond to field name in the index settings (index.mapping.json). Any change here
  // must be reflected there, and vice versa.
  private static final String CREATED_FIELD = "created"; // contains the creation time
  private static final String HIDDEN_FIELD = "hidden"; // contains the _ from the entity name (if present)
  private static final String NAME_FIELD = "name"; // contains the entity name
  private static final String NAMESPACE_FIELD = "namespace"; // contains the type of the entity
  private static final String NESTED_NAME_FIELD = "props.name"; // contains the property name in nested props
  private static final String NESTED_SCOPE_FIELD = "props.scope"; // contains the scope in nested props
  private static final String NESTED_VALUE_FIELD = "props.value"; // contains the value in nested props
  private static final String PROPS_FIELD = "props"; // contains all properties
  private static final String TEXT_FIELD = "text"; // contains all plain text
  private static final String TYPE_FIELD = "type"; // contains the type of the entity

  // these are the only fields that are supported for sorting
  private static final Map<String, String> SORT_KEY_MAP = ImmutableMap.of(
      "entity-name", NAME_FIELD,
      "creation-time", CREATED_FIELD
  );
  private static final String SUPPORTED_SORT_KEYS = String.join(", ", SORT_KEY_MAP.keySet());

  // Names of REST endpoints and of fields in their requests and responses.
  private static final String DOC_ENDPOINT = "_doc";
  private static final String CREATE_ENDPOINT = "_create";
  private static final String SEARCH_ENDPOINT = "_search";
  private static final String SCROLL_ENDPOINT = "scroll";
  private static final String SOURCE = "_source";
  private static final String ID = "_id";
  private static final String SEQ_NO = "_seq_no";
  private static final String PRIMARY_TERM = "_primary_term";
  private static final String SCROLL_ID = "_scroll_id";
  private static final String ERROR = "error";
  private static final String FOUND = "found";
  private static final String HITS = "hits";
  private static final String RESOURCE_ALREADY_EXISTS = "resource_already_exists_exception";
  private static final String OPENSEARCH_DISTRIBUTION = "opensearch";
  private static final Map<String, String> NO_PARAMETERS = Collections.emptyMap();

  private final CConfiguration cConf;
  private final String elasticHosts;
  private final String indexName;
  private final String scrollTimeout;

  private final String credentialsUsername;
  private final String credentialsPassword;

  private final boolean verifyTLS;

  private volatile IndexRestClient client;

  private volatile boolean created;
  private int maxWindowSize = Config.DEFAULT_MAX_RESULT_WINDOW;

  // sleep 100 ms for at most 50 times
  private final RetryStrategy retryStrategyOnConflict;

  @Inject
  public ElasticsearchMetadataStorage(CConfiguration cConf, SConfiguration sConf) {
    this.cConf = cConf;
    this.indexName = cConf.get(Config.CONF_ELASTIC_INDEX_NAME, Config.DEFAULT_INDEX_NAME);
    this.scrollTimeout = cConf.get(Config.CONF_ELASTIC_SCROLL_TIMEOUT,
        Config.DEFAULT_SCROLL_TIMEOUT);
    this.elasticHosts = cConf.get(Config.CONF_ELASTIC_HOSTS, Config.DEFAULT_ELASTIC_HOSTS);
    this.credentialsUsername = sConf.get(Config.CONF_ELASTIC_USERNAME);
    this.credentialsPassword = sConf.get(Config.CONF_ELASTIC_PASSWORD);
    this.verifyTLS = cConf.getBoolean(Config.CONF_ELASTIC_TLS_VERIFY,
        Config.DEFAULT_ELASTIC_TLS_VERIFY);
    int numRetries = cConf.getInt(Config.CONF_ELASTIC_CONFLICT_NUM_RETRIES,
        Config.DEFAULT_ELASTIC_CONFLICT_NUM_RETRIES);
    int retrySleepMs = cConf.getInt(Config.CONF_ELASTIC_CONFLICT_RETRY_SLEEP_MS,
        Config.DEFAULT_ELASTIC_CONFLICT_RETRY_SLEEP_MS);
    this.retryStrategyOnConflict = RetryStrategies.limit(numRetries,
        RetryStrategies.fixDelay(retrySleepMs, TimeUnit.MILLISECONDS));
  }

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public void close() {
    Closeables.closeQuietly(client);
  }

  @Override
  public void createIndex() throws IOException {
    if (created) {
      return;
    }
    synchronized (this) {
      if (created) {
        return;
      }
      IndexRestClient client = getClient();
      checkClusterVersion(client);
      if (!indexExists(client)) {
        JsonObject index = new JsonObject();
        index.add("settings", createSettings());
        index.add("mappings", createMappings());
        LOG.info("Creating index '{}' with settings: {}", indexName, index.get("settings"));
        IndexRestClient.Response response =
            client.execute("PUT", Collections.singletonList(indexName), NO_PARAMETERS, index);
        if (!response.isSuccess()) {
          // Every CDAP pod runs the storage setup (StorageMain) when it starts, so several pods
          // can create the index at the same time. If another pod created it after the check
          // above, the index is there and this request is not needed.
          if (!response.getBody().contains(RESOURCE_ALREADY_EXISTS)) {
            throw new IOException("Unable to create index: " + response);
          }
        } else if (!isAcknowledged(response)) {
          throw new IOException("Create index request was not acknowledged: " + response);
        }
      }
      JsonObject settings = getObject(
          client.execute("GET", indexName, "_settings").ensureSuccess().getJson(),
          indexName, "settings", "index");
      if (settings == null) {
        throw new IOException("Unable to obtain settings for index " + indexName);
      }
      JsonElement maxWindow = settings.get(SETTING_MAX_RESULT_WINDOW);
      if (maxWindow != null) {
        maxWindowSize = Integer.parseInt(maxWindow.getAsString());
      }
      LOG.debug("Using index {} with 'max_result_window' of {}", indexName, maxWindowSize);

      // For some unknown reason, multi-get fails immediately after creating an index
      // However, performing one regular read appears to fix that, perhaps it waits until the index is ready?
      // Hence, perform one read to ensure the index is ready to use.
      readFromIndex(MetadataEntity.ofNamespace("system"));
      created = true;
    }
  }

  /**
   * The only way to store metadata for an index is to create a mapping for "_meta". We do this to
   * store the version of CDAP that creates the index.
   */
  VersionInfo getVersionInfo() throws IOException {
    JsonObject meta = getIndexMeta(getClient(), indexName);
    LOG.trace("Index mapping for _meta: {}", meta);
    try {
      return new VersionInfo(
          new ProjectInfo.Version(getMetaValue(meta, Config.MAPPING_CDAP_VERSION)),
          Integer.parseInt(getMetaValue(meta, Config.MAPPING_METADATA_VERSION)),
          Long.parseLong(getMetaValue(meta, Config.MAPPING_MAPPING_CHECKSUM)));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unable to extract version info from index mappings: " + meta, e);
    }
  }

  /**
   * Returns the value of a key in the "_meta" object of the index mappings.
   *
   * @throws IllegalArgumentException if the key is missing
   */
  private static String getMetaValue(JsonObject meta, String key) {
    JsonElement value = meta.get(key);
    if (value == null || value.isJsonNull()) {
      throw new IllegalArgumentException("Missing '" + key + "' in _meta");
    }
    return value.getAsString();
  }

  /**
   * Returns the "_meta" object of the index mappings.
   */
  private static JsonObject getIndexMeta(IndexRestClient client, String indexName)
      throws IOException {
    JsonObject mappings = client.execute("GET", indexName, "_mapping").ensureSuccess().getJson();
    JsonObject meta = getObject(mappings, indexName, "mappings", "_meta");
    if (meta == null) {
      throw new IOException("Index " + indexName + " has no _meta mapping: " + mappings);
    }
    return meta;
  }

  /**
   * Returns the client for talking to the search cluster.
   */
  private IndexRestClient getClient() {
    IndexRestClient client = this.client;
    if (client != null) {
      return client;
    }

    synchronized (this) {
      client = this.client;
      if (client != null) {
        return client;
      }

      LOG.info("Create new REST client for cluster {}", elasticHosts);
      this.client = client = new IndexRestClient(elasticHosts, credentialsUsername,
          credentialsPassword, verifyTLS);
      return client;
    }
  }

  /**
   * Fails if the search cluster is Elasticsearch before 7.0, which does not support the typeless
   * REST API and the response formats that this class uses.
   */
  private void checkClusterVersion(IndexRestClient client) throws IOException {
    IndexRestClient.Response response = client.execute("GET");
    if (response.getStatus() == HTTP_UNAUTHORIZED || response.getStatus() == HTTP_FORBIDDEN) {
      throw new IOException(String.format(
          "Unable to check the version of the search cluster at %s: the user needs the "
              + "cluster:monitor/main permission: %s", elasticHosts, response));
    }
    response.ensureSuccess();
    JsonObject version = getObject(response.getJson(), "version");
    if (version == null) {
      throw new IOException("Unable to determine the version of the search cluster: " + response);
    }
    JsonElement distribution = version.get("distribution");
    if (distribution != null && OPENSEARCH_DISTRIBUTION.equals(distribution.getAsString())) {
      return;
    }
    JsonElement number = version.get("number");
    String versionNumber = number == null ? "" : number.getAsString();
    int dot = versionNumber.indexOf('.');
    String major = dot > 0 ? versionNumber.substring(0, dot) : "";
    if (major.isEmpty() || !major.chars().allMatch(Character::isDigit)) {
      throw new IOException("Unable to determine the version of the search cluster: " + response);
    }
    if (Integer.parseInt(major) < 7) {
      throw new IOException(String.format(
          "The search cluster at %s runs Elasticsearch %s. The metadata storage requires "
              + "OpenSearch.", elasticHosts, versionNumber));
    }
  }

  private boolean indexExists(IndexRestClient client) throws IOException {
    IndexRestClient.Response response = client.execute("HEAD", indexName);
    if (response.getStatus() == HTTP_NOT_FOUND) {
      return false;
    }
    response.ensureSuccess();
    return true;
  }

  /**
   * Returns the mappings to create the index with. Also used by the migration tool, so that an
   * index that the tool creates is the same as an index that this class creates.
   */
  static JsonObject createMappings() throws IOException {
    URL url = ElasticsearchMetadataStorage.class.getClassLoader().getResource(MAPPING_RESOURCE);
    if (url == null) {
      throw new IllegalStateException(
          "Index mapping file '" + MAPPING_RESOURCE + "'not found in classpath");
    }
    // read the mappings file, replace the CDAP_VERSION placeholder with current version
    String mappings = Resources.toString(url, Charsets.UTF_8);
    @SuppressWarnings("ConstantConditions")
    long checksum = Checksums.fingerprint64(Bytes.toBytes(mappings));
    return new JsonParser().parse(mappings
        .replace(Config.MAPPING_CDAP_VERSION_PLACEHOLDER, ProjectInfo.getVersion().toString())
        .replace(Config.MAPPING_METADATA_VERSION_PLACEHOLDER,
            Integer.toString(VersionInfo.METADATA_VERSION))
        .replace(Config.MAPPING_MAPPING_CHECKSUM_PLACEHOLDER, Long.toString(checksum)))
        .getAsJsonObject();
  }

  // Index settings, used when creating the index
  private JsonObject createSettings() {
    return createSettings(cConf.get(Config.CONF_ELASTIC_NUM_SHARDS),
        cConf.get(Config.CONF_ELASTIC_NUM_REPLICAS),
        cConf.get(Config.CONF_ELASTIC_WINDOW_SIZE));
  }

  /**
   * Returns the settings to create the index with. Each argument can be null, to use the default
   * of the search cluster. Also used by the migration tool.
   */
  static JsonObject createSettings(@Nullable String numShards, @Nullable String numReplicas,
      @Nullable String maxResultWindow) {
    JsonObject indexSettings = new JsonObject();
    if (numShards != null) {
      indexSettings.addProperty(SETTING_NUMBER_OF_SHARDS, Integer.parseInt(numShards));
    }
    if (numReplicas != null) {
      indexSettings.addProperty(SETTING_NUMBER_OF_REPLICAS, Integer.parseInt(numReplicas));
    }
    if (maxResultWindow != null) {
      indexSettings.addProperty(SETTING_MAX_RESULT_WINDOW, Integer.parseInt(maxResultWindow));
    }
    JsonObject textAnalyzer = new JsonObject();
    textAnalyzer.addProperty("type", "pattern");
    // this reflects the tokenization performed by MetadataDataset
    textAnalyzer.addProperty("pattern", "[-_,;.\\s]+");
    textAnalyzer.addProperty("lowercase", true);
    JsonObject analyzers = new JsonObject();
    analyzers.add("text_analyzer", textAnalyzer);
    JsonObject analysis = new JsonObject();
    analysis.add("analyzer", analyzers);

    JsonObject settings = new JsonObject();
    if (!indexSettings.entrySet().isEmpty()) {
      settings.add("index", indexSettings);
    }
    settings.add("analysis", analysis);
    return settings;
  }

  @Override
  public void dropIndex() throws IOException {
    IndexRestClient client = getClient();
    synchronized (this) {
      if (!indexExists(client)) {
        created = false;
        return;
      }
      IndexRestClient.Response response = client.execute("DELETE", indexName).ensureSuccess();
      if (!isAcknowledged(response)) {
        LOG.warn("Delete index request was not acknowledged: {}", response);
      }
      created = false;
    }
  }

  @Override
  public MetadataChange apply(MetadataMutation mutation, MutationOptions options)
      throws IOException {
    MetadataEntity entity = mutation.getEntity();
    try {
      // repeatedly try to read current metadata, apply the mutation and reindex, until there is no conflict
      return Retries.callWithRetries(
          () -> {
            IndexedMetadata before = readFromIndex(entity);
            RequestAndChange intermediary = applyMutation(before, mutation);
            executeMutation(mutation.getEntity(), intermediary.getOperation(), options);
            return intermediary.getChange();
          },
          retryStrategyOnConflict,
          e -> e instanceof MetadataConflictException);
    } catch (MetadataConflictException e) {
      throw new MetadataConflictException("After retries: " + e.getRawMessage(),
          e.getConflictingEntities());
    }
  }

  @Override
  public List<MetadataChange> batch(List<? extends MetadataMutation> mutations,
      MutationOptions options) throws IOException {
    if (mutations.isEmpty()) {
      return Collections.emptyList();
    }
    if (mutations.size() == 1) {
      return Collections.singletonList(apply(mutations.get(0), options));
    }
    // first detect whether there are duplicate entity ids. If so, execute in sequence.
    // Entities that differ only in the version of an application share one document.
    Set<String> documentIds = new HashSet<>();
    LinkedHashMap<MetadataEntity, MetadataMutation> mutationMap = new LinkedHashMap<>(
        mutations.size());
    boolean duplicate = false;
    for (MetadataMutation mutation : mutations) {
      if (!documentIds.add(toDocumentId(mutation.getEntity()))) {
        duplicate = true;
        break;
      }
      mutationMap.put(mutation.getEntity(), mutation);
    }
    if (duplicate) {
      // if there are multiple mutations for the same entity, execute all in sequence
      List<MetadataChange> changes = new ArrayList<>(mutations.size());
      for (MetadataMutation mutation : mutations) {
        changes.add(apply(mutation, options));
      }
      return changes;
    }
    // collect all changes in an order-preserving map. The first time doBatch() is called, it will
    // enter all entities in the map. Every time it is retried, the change may get updated, but that
    // will not change the order of the map.
    LinkedHashMap<MetadataEntity, MetadataChange> changes = new LinkedHashMap<>(mutations.size());
    try {
      // repeatedly try to read current metadata, apply the mutations and reindex, until there is no conflict
      return Retries.callWithRetries(() -> doBatch(mutationMap, changes, options),
          RetryStrategies.limit(50, RetryStrategies.fixDelay(100, TimeUnit.MILLISECONDS)),
          e -> e instanceof MetadataConflictException);
    } catch (MetadataConflictException e) {
      throw new MetadataConflictException("After retries: " + e.getRawMessage(),
          e.getConflictingEntities());
    }
  }

  /**
   * For the given mutations, reads the current metadata for the involved entities, applies the
   * mutations, and attempts to execute the resulting index operations in bulk. When finished,
   * returns the list of metadata changes caused by the mutations.
   *
   * If a conflict occurs during any of these operations, the successful mutations are removed from
   * the mutations map, and the changes map is updated with the corresponding change, before the
   * {@link MetadataConflictException} is thrown. That is, calling this repeatedly will gradually
   * shrink the mutations map to an empty map and fill the changes map with changes performed.
   *
   * @param mutations the mutations to apply. Every mutation that is successfully executed is
   *     removed from this map, even if an exception is thrown.
   * @param changes the changes caused by the mutations. For every mutation that is successfully
   *     executed, this map is updated with the corresponding change, even if an exception is
   *     thrown.
   * @return the list of all changes performed by the mutations, if all mutations complete
   *     successfully
   * @throws MetadataConflictException if a conflict occurs for any of the mutations
   * @throws IOException for any other problem encountered
   */
  private List<MetadataChange> doBatch(LinkedHashMap<MetadataEntity, MetadataMutation> mutations,
      LinkedHashMap<MetadataEntity, MetadataChange> changes,
      MutationOptions options)
      throws IOException {
    JsonArray ids = new JsonArray();
    for (MetadataEntity entity : mutations.keySet()) {
      ids.add(new JsonPrimitive(toDocumentId(entity)));
    }
    JsonObject multiGet = new JsonObject();
    multiGet.add("ids", ids);
    JsonArray docs = getClient()
        .execute("POST", Arrays.asList(indexName, "_mget"), NO_PARAMETERS, multiGet)
        .ensureSuccess().getJson().getAsJsonArray("docs");
    if (docs == null || docs.size() != mutations.size()) {
      throw new IOException("Failed to read from index for entities " + mutations.keySet());
    }
    // responses are in the same order as the original requests
    int index = 0;
    List<WriteOperation> operations = new ArrayList<>(mutations.size());
    for (Map.Entry<MetadataEntity, MetadataMutation> entry : mutations.entrySet()) {
      JsonObject doc = docs.get(index++).getAsJsonObject();
      if (doc.has(ERROR)) {
        throw new IOException("Failed to read from index for entity " + entry.getKey() + ": "
            + doc.get(ERROR));
      }
      RequestAndChange intermediary = applyMutation(toIndexedMetadata(doc), entry.getValue());
      operations.add(intermediary.getOperation());
      changes.put(entry.getKey(), intermediary.getChange());
    }
    executeBulk(operations, options, mutations);
    return new ArrayList<>(changes.values());
  }

  @Override
  public Metadata read(Read read) throws IOException {
    Metadata metadata = readFromIndex(read.getEntity()).getMetadata();
    return filterMetadata(metadata, KEEP, read.getKinds(), read.getScopes(), read.getSelection());
  }

  @Override
  public SearchResponse search(SearchRequest request)
      throws IOException {
    return request.getCursor() != null && !request.getCursor().isEmpty()
        ? doScroll(request) : doSearch(request);
  }

  /**
   * Creates a write to the index that corresponds to the given mutation, along with the change
   * effected by this mutation. The write must be executed by the caller.
   *
   * @param before the metadata for the mutation's entity before the change
   * @param mutation the mutation to apply
   * @return a write to be executed, and the change caused by the mutation.
   */
  private RequestAndChange applyMutation(IndexedMetadata before,
      MetadataMutation mutation) {
    LOG.trace("Applying mutation {} to entity {} with metadata {}",
        mutation, mutation.getEntity(), before.getMetadata());
    switch (mutation.getType()) {
      case CREATE:
        return create(before, (MetadataMutation.Create) mutation);
      case DROP:
        return drop(mutation.getEntity(), before);
      case UPDATE:
        return update(mutation.getEntity(), before,
            ((MetadataMutation.Update) mutation).getUpdates());
      case REMOVE:
        return remove(before, (MetadataMutation.Remove) mutation);
      default:
        throw new IllegalStateException(
            String.format("Unknown mutation type '%s' for %s", mutation.getType(), mutation));
    }
  }

  /**
   * Creates the write for an entity creation or update. See {@link MetadataMutation.Create} for
   * detailed semantics.
   *
   * @param before the metadata for the mutation's entity before the change
   * @param create the mutation to apply
   * @return a write to be executed, and the change caused by the mutation
   */
  private RequestAndChange create(IndexedMetadata before, MetadataMutation.Create create) {
    // if the entity did not exist before, none of the directives apply and this is equivalent to update()
    if (!before.hasDocument()) {
      return update(create.getEntity(), before, create.getMetadata());
    }
    Metadata meta = create.getMetadata();
    Map<ScopedNameOfKind, MetadataDirective> directives = create.getDirectives();
    // determine the scopes that this mutation applies to (scopes that do not occur in the metadata are no changed)
    Set<MetadataScope> scopes = Stream.concat(meta.getTags().stream(),
            meta.getProperties().keySet().stream())
        .map(ScopedName::getScope).collect(Collectors.toSet());
    // compute what previously existing tags and properties have to be preserved (all others are replaced)
    Set<ScopedName> existingTagsToKeep = new HashSet<>();
    Map<ScopedName, String> existingPropertiesToKeep = new HashMap<>();
    // all tags and properties that are in a scope not affected by this mutation
    Sets.difference(MetadataScope.ALL, scopes).forEach(
        scope -> {
          before.getMetadata().getTags().stream()
              .filter(tag -> tag.getScope().equals(scope))
              .forEach(existingTagsToKeep::add);
          before.getMetadata().getProperties().entrySet().stream()
              .filter(entry -> entry.getKey().getScope().equals(scope))
              .forEach(entry -> existingPropertiesToKeep.put(entry.getKey(), entry.getValue()));
        });
    // tags and properties in affected scopes that must be kept or preserved
    directives.entrySet().stream()
        .filter(entry -> scopes.contains(entry.getKey().getScope()))
        .forEach(entry -> {
          ScopedNameOfKind key = entry.getKey();
          if (key.getKind() == MetadataKind.TAG
              && (entry.getValue() == MetadataDirective.PRESERVE
              || entry.getValue() == MetadataDirective.KEEP)) {
            ScopedName tag = new ScopedName(key.getScope(), key.getName());
            if (!meta.getTags().contains(tag) && before.getMetadata().getTags().contains(tag)) {
              existingTagsToKeep.add(tag);
            }
          } else if (key.getKind() == MetadataKind.PROPERTY) {
            ScopedName property = new ScopedName(key.getScope(), key.getName());
            String existingValue = before.getMetadata().getProperties().get(property);
            String newValue = meta.getProperties().get(property);
            if (existingValue != null
                && (
                entry.getValue() == MetadataDirective.PRESERVE && !existingValue.equals(newValue)
                    || entry.getValue() == MetadataDirective.KEEP && newValue == null)) {
              existingPropertiesToKeep.put(property, existingValue);
            }
          }
        });
    // compute the new tags and properties
    Set<ScopedName> newTags =
        existingTagsToKeep.isEmpty() ? meta.getTags()
            : Sets.union(meta.getTags(), existingTagsToKeep);
    Map<ScopedName, String> newProperties = meta.getProperties();
    if (!existingPropertiesToKeep.isEmpty()) {
      newProperties = new HashMap<>(newProperties);
      newProperties.putAll(existingPropertiesToKeep);
    }
    Metadata after = new Metadata(newTags, newProperties);
    return new RequestAndChange(writeToIndex(create.getEntity(), before, after),
        new MetadataChange(create.getEntity(), before.getMetadata(), after));
  }

  /**
   * Creates the delete for an entity deletion. This drops the corresponding metadata document
   * from the index.
   *
   * @param before the metadata for the mutation's entity before the change
   * @return a write to be executed, and the change caused by the mutation
   */
  private RequestAndChange drop(MetadataEntity entity, IndexedMetadata before) {
    return new RequestAndChange(deleteFromIndex(entity, before),
        new MetadataChange(entity, before.getMetadata(), Metadata.EMPTY));
  }

  /**
   * Creates the write for updating the metadata of an entity. This updates or adds the new
   * metadata to the corresponding metadata document in the index.
   *
   * @param before the metadata for the mutation's entity before the change, and its version
   * @return a write to be executed, and the change caused by the mutation
   */
  private RequestAndChange update(MetadataEntity entity,
      IndexedMetadata before,
      Metadata updates) {
    Set<ScopedName> tags = new HashSet<>(before.getMetadata().getTags());
    tags.addAll(updates.getTags());
    Map<ScopedName, String> properties = new HashMap<>(before.getMetadata().getProperties());
    properties.putAll(updates.getProperties());
    Metadata after = new Metadata(tags, properties);
    return new RequestAndChange(writeToIndex(entity, before, after),
        new MetadataChange(entity, before.getMetadata(), after));
  }

  /**
   * Creates the write for removing some metadata from an entity. This removed the specified
   * metadata from the corresponding metadata document in the index.
   *
   * Note that even if all tags and properties are removed, the document will remain in the index
   * and it still searchable by its type, name, or a match-all query. Use {@link
   * MetadataMutation.Drop} to completely remove the entity from the index.
   *
   * @param before the metadata for the mutation's entity before the change
   * @return a write to be executed, and the change caused by the mutation
   */
  private RequestAndChange remove(IndexedMetadata before, MetadataMutation.Remove remove) {
    Metadata after = filterMetadata(before.getMetadata(), DISCARD,
        remove.getKinds(), remove.getScopes(), remove.getRemovals());
    return new RequestAndChange(writeToIndex(remove.getEntity(), before, after),
        new MetadataChange(remove.getEntity(), before.getMetadata(), after));
  }

  /**
   * Reads the existing metadata for an entity from the index.
   *
   * @return existing metadata along with the version of its document in the index, or an empty
   *     metadata without a version.
   */
  private IndexedMetadata readFromIndex(MetadataEntity entity) throws IOException {
    String id = toDocumentId(entity);
    IndexRestClient.Response response;
    try {
      response = getClient().execute("GET", indexName, DOC_ENDPOINT, id);
    } catch (IOException e) {
      throw new IOException("Failed to read from index for entity " + entity, e);
    }
    if (!response.isSuccess() && response.getStatus() != HTTP_NOT_FOUND) {
      throw new IOException("Failed to read from index for entity " + entity + ": " + response);
    }
    JsonObject doc = response.getJson();
    // A missing document and a missing index both return 404. Only the latter has an error.
    if (doc.has(ERROR)) {
      throw new IOException("Failed to read from index for entity " + entity + ": " + response);
    }
    try {
      return toIndexedMetadata(doc);
    } catch (RuntimeException e) {
      // Callers only expect an IOException, for example to turn it into a MetadataException. A
      // document that cannot be parsed is reported the same way.
      throw new IOException("Failed to read from index for entity " + entity + ": " + response, e);
    }
  }

  /**
   * Converts a document, as the get and multi-get endpoints return it, into metadata.
   */
  private static IndexedMetadata toIndexedMetadata(JsonObject doc) {
    JsonElement found = doc.get(FOUND);
    if (found == null || !found.getAsBoolean()) {
      return IndexedMetadata.NONE;
    }
    Metadata metadata = GSON.fromJson(doc.get(SOURCE), MetadataDocument.class).getMetadata();
    return IndexedMetadata.of(metadata, doc.get(SEQ_NO).getAsLong(),
        doc.get(PRIMARY_TERM).getAsLong());
  }

  /**
   * Creates a write for adding or updating the metadata for an entity in the index. The write must
   * be executed by the caller.
   */
  private WriteOperation writeToIndex(MetadataEntity entity, IndexedMetadata before,
      Metadata metadata) {
    MetadataDocument doc = MetadataDocument.of(entity, metadata);
    LOG.trace("Indexing document: {}", doc);
    return WriteOperation.store(toDocumentId(entity), GSON.toJsonTree(doc), before);
  }

  /**
   * Creates a write for removing an entity in the index. The write must be executed by the caller.
   */
  private WriteOperation deleteFromIndex(MetadataEntity entity, IndexedMetadata before) {
    String id = toDocumentId(entity);
    LOG.trace("Deleting document with id: {}", id);
    return WriteOperation.delete(id, before);
  }

  /**
   * Executes a write of a document (index or delete), and handles possible failure.
   *
   * @throws MetadataConflictException if a conflict occurs
   * @throws IOException for any other problem encountered
   */
  private void executeMutation(MetadataEntity entity, WriteOperation operation,
      MutationOptions options) throws IOException {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("refresh", getRefreshPolicy(options));
    if (operation.getIfSeqNo() != null && operation.getIfPrimaryTerm() != null) {
      parameters.put("if_seq_no", Long.toString(operation.getIfSeqNo()));
      parameters.put("if_primary_term", Long.toString(operation.getIfPrimaryTerm()));
    }
    boolean isDelete = operation.getType() == WriteOperation.Type.DELETE;
    String requestType = isDelete ? "Delete" : "Index";
    String endpoint =
        operation.getType() == WriteOperation.Type.CREATE ? CREATE_ENDPOINT : DOC_ENDPOINT;
    IndexRestClient.Response response = getClient().execute(isDelete ? "DELETE" : "PUT",
        Arrays.asList(indexName, endpoint, operation.getDocumentId()), parameters,
        operation.getSource());
    if (isDelete && response.getStatus() == HTTP_NOT_FOUND && !response.getJson().has(ERROR)) {
      // ignore entities that do not exist - only happens for deletes. A missing index also
      // returns 404, but with an error.
      return;
    }
    if (response.getStatus() == HTTP_CONFLICT) {
      LOG.debug("Encountered conflict in {} request for entity {}", requestType, entity);
      throw new MetadataConflictException(requestType + " conflict for ${conflicting}", entity);
    }
    if (!response.isSuccess()) {
      throw new IOException(String.format("%s request unsuccessful for entity %s: %s",
          requestType, entity, response));
    }
  }

  /**
   * Executes a bulk request and handles the responses for possible failures, and removes all
   * successful mutations from the mutations map.
   *
   * @param mutations the mutations represented by this bulk. Every mutation that is
   *     successfully executed is removed from this map, even if an exception is thrown.
   * @throws MetadataConflictException if a conflict occurs for any of the operations in the
   *     bulk
   * @throws IOException for any other problem encountered
   */
  private void executeBulk(List<WriteOperation> operations, MutationOptions options,
      LinkedHashMap<MetadataEntity, MetadataMutation> mutations) throws IOException {
    StringBuilder body = new StringBuilder();
    for (WriteOperation operation : operations) {
      body.append(operation.toBulkAction(indexName)).append('\n');
      if (operation.getSource() != null) {
        body.append(operation.getSource()).append('\n');
      }
    }
    JsonObject response = getClient().execute("POST", Collections.singletonList("_bulk"),
            Collections.singletonMap("refresh", getRefreshPolicy(options)), body.toString(),
            IndexRestClient.NDJSON)
        .ensureSuccess().getJson();
    if (!response.get("errors").getAsBoolean()) {
      return;
    }
    // The document id does not have the version of an application, so the entity cannot be
    // derived from it: look up the entity that the caller used as the key.
    Map<String, MetadataEntity> entitiesByDocumentId = new HashMap<>();
    for (MetadataEntity entity : mutations.keySet()) {
      entitiesByDocumentId.put(toDocumentId(entity), entity);
    }
    IOException ioe = null;
    List<MetadataEntity> conflictEntities = new ArrayList<>();
    for (JsonElement element : response.getAsJsonArray("items")) {
      // each item is an object with a single member, named after the kind of write
      Map.Entry<String, JsonElement> item =
          element.getAsJsonObject().entrySet().iterator().next();
      JsonObject result = item.getValue().getAsJsonObject();
      String documentId = result.get(ID).getAsString();
      MetadataEntity entityId = entitiesByDocumentId.get(documentId);
      if (entityId == null) {
        LOG.warn("Bulk response has document id {} that was not in the request", documentId);
        continue;
      }
      if (!result.has(ERROR)) {
        // remove this mutation - it was successful
        mutations.remove(entityId);
        continue;
      }
      // A delete of a missing document has no error, so it was handled above. A 404 with an error
      // means that the index is missing, which is a failure, as in executeMutation().
      int status = result.get("status").getAsInt();
      if (status == HTTP_CONFLICT) {
        conflictEntities.add(entityId);
        continue;
      }
      // not a conflict -> true failure
      if (ioe == null) {
        ioe = new IOException("Bulk request unsuccessful");
      }
      ioe.addSuppressed(new IOException(String.format("%s request unsuccessful for entity %s: %s",
          item.getKey(), entityId, result.get(ERROR))));
    }
    if (ioe != null) {
      throw ioe;
    }
    if (!conflictEntities.isEmpty()) {
      LOG.debug("Encountered conflicts in batch mutation for entities {}", conflictEntities);
      throw new MetadataConflictException("Bulk request conflicts for entities ${conflicting}",
          conflictEntities);
    }
  }

  /**
   * Sets the refresh policy for a write request. If {@link MutationOptions#isAsynchronous()} is
   * true, write requests will return immediately after the request is acknowledged; otherwise, they
   * will only return after they are confirmed to be applied to the index.
   */
  private static String getRefreshPolicy(MutationOptions options) {
    return options.isAsynchronous() ? "true" : "wait_for";
  }

  /**
   * Perform a search that continues a previous search using a cursor. Such a search is always
   * relative to the offset of the cursor (with no additional offset allowed) and uses the the same
   * page size as the original search.
   *
   * If the cursor provided in the request had expired, this performs a new search, beginning at the
   * offset given by the cursor.
   *
   * @return the search response containing the next page of results.
   */
  private SearchResponse doScroll(SearchRequest request)
      throws IOException {

    Cursor cursor = Cursor.fromString(request.getCursor());
    JsonObject scroll = new JsonObject();
    scroll.addProperty("scroll_id", cursor.getActualCursor());
    if (request.isCursorRequested()) {
      scroll.addProperty("scroll", scrollTimeout);
    }
    IndexRestClient.Response response = getClient().execute("POST",
        Arrays.asList(SEARCH_ENDPOINT, SCROLL_ENDPOINT), NO_PARAMETERS, scroll);
    if (!response.isSuccess()) {
      // scroll invalid or timed out
      return doSearch(createRequestFromCursor(request, cursor));
    }
    JsonObject searchResponse = response.getJson();
    JsonElement timedOut = searchResponse.get("timed_out");
    if (timedOut != null && timedOut.getAsBoolean()) {
      // scroll had expired, we have to search again
      return doSearch(createRequestFromCursor(request, cursor));
    }
    return createSearchResponse(request, searchResponse, computeCursor(searchResponse, cursor),
        cursor.getOffset(), cursor.getLimit());
  }

  private SearchResponse createSearchResponse(SearchRequest request,
      JsonObject response,
      String cursor, int offset, int limit) {
    List<MetadataRecord> results = fromHits(getHits(response));
    return new SearchResponse(request, cursor, offset, limit,
        (int) getTotalHits(response), results);
  }

  private static SearchRequest createRequestFromCursor(SearchRequest request, Cursor cursor) {
    SearchRequest.Builder builder = SearchRequest.of(cursor.getQuery())
        .setOffset(cursor.getOffset())
        .setLimit(cursor.getLimit())
        .setShowHidden(cursor.isShowHidden())
        .setScope(cursor.getScope())
        .setCursorRequested(request.isCursorRequested());
    if (cursor.getSorting() != null) {
      builder.setSorting(Sorting.of(cursor.getSorting()));
    }
    if (cursor.getNamespaces() != null) {
      cursor.getNamespaces().forEach(builder::addNamespace);
    }
    if (cursor.getTypes() != null) {
      cursor.getTypes().forEach(builder::addType);
    }
    return builder.build();
  }

  /**
   * Perform a search that does continue a previous search using a cursor.
   *
   * @param request the search request
   */
  private SearchResponse doSearch(SearchRequest request)
      throws IOException {

    JsonObject searchSource = createSearchSource(request);
    // only request a scroll if the offset is 0. The search cluster will throw otherwise
    Map<String, String> parameters =
        request.isCursorRequested() && searchSource.get("from").getAsInt() == 0
            ? Collections.singletonMap("scroll", scrollTimeout) : NO_PARAMETERS;
    LOG.trace("Executing search request {}", searchSource);
    JsonObject searchResponse = getClient()
        .execute("POST", Arrays.asList(indexName, SEARCH_ENDPOINT), parameters, searchSource)
        .ensureSuccess().getJson();
    return createSearchResponse(request, searchResponse, computeCursor(searchResponse, request),
        request.getOffset(), request.getLimit());
  }

  /**
   * Generates the cursor to return as part of a search response:
   * <ul><li>
   *   If the response does not have a scroll id, returns null;
   * </li><li>
   *   If the number of results in this response exceeds the total number of results,
   *   cancels the scroll returned by the search cluster, then returns null.
   * </li><li>
   *   Otherwise returns a cursor representing the scroll id,
   *   offset and page size for a subsequent search.
   * </li></ul>
   */
  @Nullable
  private String computeCursor(JsonObject searchResponse, Cursor cursor) {
    String scrollId = getScrollId(searchResponse);
    if (scrollId != null) {
      int newOffset = cursor.getOffset() + getHits(searchResponse).size();
      if (newOffset < getTotalHits(searchResponse)) {
        return new Cursor(cursor, newOffset, scrollId).toString();
      }
      // we have seen all results, there are no more to fetch: clear the scroll
      cancelScroll(scrollId);
    }
    return null;
  }

  @Nullable
  private String computeCursor(JsonObject searchResponse, SearchRequest request) {
    String scrollId = getScrollId(searchResponse);
    if (scrollId != null) {
      int newOffset = request.getOffset() + getHits(searchResponse).size();
      if (newOffset < getTotalHits(searchResponse)) {
        return new Cursor(newOffset, request.getLimit(), request.isShowHidden(), request.getScope(),
            request.getNamespaces(), request.getTypes(),
            request.getSorting() == null ? null : request.getSorting().toString(),
            scrollId, request.getQuery()).toString();
      }
      // we have seen all results, there are no more to fetch: clear the scroll
      cancelScroll(scrollId);
    }
    return null;
  }

  private void cancelScroll(String scrollId) {
    JsonArray scrollIds = new JsonArray();
    scrollIds.add(new JsonPrimitive(scrollId));
    JsonObject clearScroll = new JsonObject();
    clearScroll.add("scroll_id", scrollIds);
    // We don't really care about the response: a scroll that is not cleared expires on its own.
    try {
      getClient().execute("DELETE", Arrays.asList(SEARCH_ENDPOINT, SCROLL_ENDPOINT),
          NO_PARAMETERS, clearScroll);
    } catch (IOException | RuntimeException e) {
      LOG.debug("Unable to clear scroll", e);
    }
  }

  @Nullable
  private static String getScrollId(JsonObject searchResponse) {
    JsonElement scrollId = searchResponse.get(SCROLL_ID);
    return scrollId == null || scrollId.isJsonNull() ? null : scrollId.getAsString();
  }

  private static JsonArray getHits(JsonObject searchResponse) {
    return searchResponse.getAsJsonObject(HITS).getAsJsonArray(HITS);
  }

  private static long getTotalHits(JsonObject searchResponse) {
    return searchResponse.getAsJsonObject(HITS).getAsJsonObject("total").get("value").getAsLong();
  }

  private JsonObject createSearchSource(SearchRequest request) {
    int offset = request.getOffset();
    int limit = request.getLimit();
    // clients cannot know what the index's max window size is, but any request where offset + limit
    // exceeds that size will fail in the search cluster. Hence we need to adjust the requested
    // number of results to not exceed that window size.
    if (maxWindowSize < offset) {
      throw new IllegalArgumentException(
          String.format("Offset %d is greater than the index's '%s' settings of %d",
              offset, SETTING_MAX_RESULT_WINDOW, maxWindowSize));
    }
    limit = Integer.min(maxWindowSize - offset, limit);

    JsonObject searchSource = new JsonObject();
    searchSource.addProperty("from", offset);
    searchSource.addProperty("size", limit);
    // Without this, the total number of results is only exact up to 10000, but the cursor and
    // the pagination of the callers need the exact number.
    searchSource.addProperty("track_total_hits", true);
    if (request.getSorting() != null) {
      JsonObject sort = new JsonObject();
      sort.addProperty(mapSortKey(request.getSorting().getKey().toLowerCase(Locale.ROOT)),
          request.getSorting().getOrder().name().toLowerCase(Locale.ROOT));
      searchSource.add("sort", sort);
    }
    searchSource.add("query", createQuery(request));
    return searchSource;
  }

  private static String mapSortKey(String key) {
    String newKey = SORT_KEY_MAP.get(key);
    if (newKey != null) {
      return newKey;
    }
    throw new IllegalArgumentException(String.format(
        "Field '%s' cannot be used as a sort key. Only the following are supported: %s.",
        key, SUPPORTED_SORT_KEYS));
  }

  /**
   * Creates a query from a search request. In essence, this returns
   *
   * <pre>
   *   (field1:term1 OR ... OR NESTED(props):name/scope/term ...)
   * [ AND (namespace:ns1 OR ... OR namespace:nsK) ]
   * [ AND (type:type1 OR ... OR type:typeM) ]
   * [ AND hidden:false ]
   * </pre>
   *
   * The field for each field:term is as selected by the request's scope (user, system, or text).
   * The NESTED subqueries expect the term to occur in a property with the name field as name, and
   * optionally the scope of the search request as its scope.
   *
   * See {@link MetadataDocument} for details about the index mapping.
   */
  private JsonObject createQuery(SearchRequest request) {
    // first create a query from the query terms
    JsonObject mainQuery = createMainQuery(request);

    List<JsonObject> conditions = new ArrayList<>();
    // if the request asks only for a subset of entity types, add a boolean clause for that
    if (request.getTypes() != null && !request.getTypes().isEmpty()) {
      addCondition(conditions, request.getTypes().stream().map(
              type -> Queries.filterTerm(TYPE_FIELD, type.toLowerCase(Locale.ROOT)))
          .collect(Collectors.toList()));
    }
    if (request.getNamespaces() != null) {
      addCondition(conditions, request.getNamespaces().stream().map(
              ns -> Queries.filterTerm(NAMESPACE_FIELD, ns.toLowerCase(Locale.ROOT)))
          .collect(Collectors.toList()));
    }
    if (!request.isShowHidden()) {
      conditions.add(Queries.filterTerm(HIDDEN_FIELD, false));
    }
    if (!conditions.isEmpty()) {
      List<JsonObject> must = new ArrayList<>(conditions);
      must.add(mainQuery);
      mainQuery = Queries.allOf(must);
    }
    return mainQuery;
  }

  /**
   * For a list of alternative sub-queries, create a query that requires at least one of them to be
   * fulfilled, without contributing to the query scoring, and add it to a list of queries.
   *
   * Depending on whether the number of sub-queries is 1, adds the single query or a boolean OR over
   * all sub-queries.
   */
  private void addCondition(List<JsonObject> conditions, List<JsonObject> subQueries) {
    if (subQueries.size() == 1) {
      conditions.add(subQueries.get(0));
      return;
    }
    conditions.add(Queries.anyOf(subQueries));
  }

  /**
   * Creates the (sub-)query from the terms in the query string.
   */
  private JsonObject createMainQuery(SearchRequest request) {
    if (request.getQuery().equals("*")) {
      return Queries.matchAll();
    }
    // the indexed document contains three text fields: one for each scope and for all scopes combined.
    // all terms must occur in the text field as selected by the scope in the search request.
    String textField =
        request.getScope() == null ? TEXT_FIELD : request.getScope().name().toLowerCase();

    // split the query into its terms and iterate over all terms
    Iterable<String> terms = Splitter.on(SPACE_SEPARATOR_PATTERN)
        .omitEmptyStrings().trimResults().split(request.getQuery());
    List<JsonObject> termQueries = new ArrayList<>();
    for (String term : terms) {
      termQueries.add(createTermQuery(term, textField, request));
    }
    if (termQueries.isEmpty()) {
      return Queries.matchAll();
    }
    if (termQueries.size() == 1) {
      return termQueries.get(0);
    }
    return Queries.anyOf(termQueries);
  }

  /**
   * Create a sub-query for a single term in the query string.
   *
   * @param term the term as it appears in the query, possibly with a field qualifier
   * @param textField the default text field to search if the term does not have a field
   */
  private JsonObject createTermQuery(String term, String textField, SearchRequest request) {
    // determine if the term has a field qualifier
    String field = null;
    term = term.trim().toLowerCase();
    // Create a term query on the term as is. This would include a field: prefix if the term hs one.
    // This is important for the case of schema search: If the schema contains a field f of type t,
    // then we index "f:t" in the plain text as well as in the "schema" property. If the query is
    // just "f:t", we must search the plain text field for that.
    JsonObject plainQuery = createTermQuery(textField, term);
    if (term.contains(MetadataConstants.KEYVALUE_SEPARATOR)) {
      // split the search term in two parts on first occurrence of KEYVALUE_SEPARATOR and trim the key and value
      String[] split = term.split(MetadataConstants.KEYVALUE_SEPARATOR, 2);
      field = split[0].trim();
      term = split[1].trim();
    }
    if (field == null) {
      return plainQuery;
    }
    if (MetadataConstants.TTL_KEY.equals(field)
        && (request.getScope() == null || MetadataScope.SYSTEM == request.getScope())) {
      try {
        // since TTL is indexed as a long, the search cluster can handle any numeric value.
        // But it would fail on "*" or "?". Hence only create a term query for TTL if
        // we can parse this as a double (that covers pretty much all numeric formats)
        Double.parseDouble(term);
        return Queries.term(field, term);
      } catch (NumberFormatException e) {
        // ignore - the follow-on code will create a regular term query for this
      }
    }
    List<JsonObject> propertyConditions = new ArrayList<>();
    propertyConditions.add(Queries.filterTerm(NESTED_NAME_FIELD, field));
    if (request.getScope() != null) {
      propertyConditions.add(Queries.filterTerm(NESTED_SCOPE_FIELD, request.getScope().name()));
    }
    propertyConditions.add(createTermQuery(NESTED_VALUE_FIELD, term));
    JsonObject propertyQuery = Queries.nested(PROPS_FIELD, Queries.allOf(propertyConditions));

    // match either a plain term of the form "f:t" or the word "t" in property "f"
    return Queries.anyOf(Arrays.asList(plainQuery, propertyQuery));
  }

  /**
   * Create a query for a single term in a given field.
   *
   * @return a wildcard query is the term contains * or ?, or a match query otherwise
   */
  private JsonObject createTermQuery(String field, String term) {
    return term.contains("*") || term.contains("?")
        ? Queries.wildcard(field, term)
        // the term should not get split in to multiple words, but in case it does, let's require all words
        : Queries.matchAllWords(field, term);
  }

  /**
   * Transforms a set of Elasticsearch search hits into metadata records.
   */
  private static List<MetadataRecord> fromHits(JsonArray hits) {
    return StreamSupport.stream(hits.spliterator(), false)
        .map(JsonElement::getAsJsonObject)
        .map(hit -> new MetadataRecord(
            toMetadataEntity(hit.get(ID).getAsString()),
            GSON.fromJson(hit.get(SOURCE), MetadataDocument.class).getMetadata()))
        .collect(Collectors.toList());
  }

  /**
   * Filter the metadata based on the given scopes, kinds, and selection. Based on the value of
   * {@param keep}, this can be used to keep or to discard the matching tags and properties.
   *
   * @param keep if true, only matching metadata elements are kept; otherwise only non-matching
   *     elements are kept.
   */
  @VisibleForTesting
  @SuppressWarnings("ConstantConditions")
  static Metadata filterMetadata(Metadata metadata, boolean keep, Set<MetadataKind> kinds,
      Set<MetadataScope> scopes, Set<ScopedNameOfKind> selection) {
    if (selection != null) {
      return new Metadata(
          Sets.filter(metadata.getTags(), tag ->
              keep == selection.contains(
                  new ScopedNameOfKind(MetadataKind.TAG, tag.getScope(), tag.getName()))),
          Maps.filterKeys(metadata.getProperties(), key ->
              keep == selection.contains(
                  new ScopedNameOfKind(MetadataKind.PROPERTY, key.getScope(), key.getName())))
      );
    }
    return new Metadata(
        Sets.filter(metadata.getTags(), tag ->
            keep == (kinds.contains(MetadataKind.TAG) && scopes.contains(tag.getScope()))),
        Maps.filterKeys(metadata.getProperties(), key ->
            keep == (kinds.contains(MetadataKind.PROPERTY) && scopes.contains(key.getScope()))));
  }

  /**
   * Translate a metadata entity into a documemt id in the index.
   */
  private static String toDocumentId(MetadataEntity entity) {
    StringBuilder builder = new StringBuilder(entity.getType());
    char sep = ':';
    for (MetadataEntity.KeyValue kv : entity) {
      // TODO (CDAP-13597): Handle versioning of metadata entities in a better way
      // if it is a versioned entity then ignore the version
      if (MetadataUtil.isVersionedEntityType(entity.getType())
          && MetadataEntity.VERSION.equalsIgnoreCase(kv.getKey())) {
        continue;
      }
      builder.append(sep).append(kv.getKey()).append('=').append(kv.getValue());
      sep = ',';
    }
    return builder.toString();
  }

  /**
   * Translate a document id in the index into a metadata entity.
   */
  private static MetadataEntity toMetadataEntity(String documentId) {
    int index = documentId.indexOf(':');
    if (index < 0) {
      throw new IllegalArgumentException(
          "Document Id must be of the form 'type:k=v,...' but is " + documentId);
    }
    String type = documentId.substring(0, index);
    MetadataEntity.Builder builder = MetadataEntity.builder();
    for (String part : documentId.substring(index + 1).split(",")) {
      String[] parts = part.split("=", 2);
      if (parts[0].equals(type)) {
        builder.appendAsType(parts[0], parts[1]);
      } else {
        builder.append(parts[0], parts[1]);
      }
    }
    // TODO (CDAP-13597): Handle versioning of metadata entities in a better way
    // if it is a versioned entity then add the default version
    return MetadataUtil.addVersionIfNeeded(builder.build());
  }

  private static boolean isAcknowledged(IndexRestClient.Response response) throws IOException {
    JsonElement acknowledged = response.getJson().get("acknowledged");
    return acknowledged != null && acknowledged.getAsBoolean();
  }

  /**
   * Follows a path of member names from a JSON object to a nested JSON object.
   *
   * @return the nested object, or null if a member on the path is missing or is not an object
   */
  @Nullable
  private static JsonObject getObject(JsonObject json, String... path) {
    JsonObject current = json;
    for (String name : path) {
      JsonElement next = current.get(name);
      if (next == null || !next.isJsonObject()) {
        return null;
      }
      current = next.getAsJsonObject();
    }
    return current;
  }
}
