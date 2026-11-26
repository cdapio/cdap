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

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.inject.Inject;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.io.Closeables;
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
import io.cdap.cdap.spi.metadata.Sorting;
import jakarta.json.JsonException;
import jakarta.json.JsonReader;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonParser;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.security.GeneralSecurityException;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.HostnameVerificationPolicy;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.Timeout;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.jsonb.JsonbJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.HealthStatus;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch._types.query_dsl.ChildScoreMode;
import org.opensearch.client.opensearch._types.query_dsl.Operator;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.opensearch.core.MgetResponse;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.get.GetResult;
import org.opensearch.client.opensearch.core.mget.MultiGetResponseItem;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.client.opensearch.core.search.TotalHits;
import org.opensearch.client.opensearch.indices.CreateIndexResponse;
import org.opensearch.client.opensearch.indices.DeleteIndexResponse;
import org.opensearch.client.opensearch.indices.IndexSettings;
import org.opensearch.client.opensearch.indices.IndexState;
import org.opensearch.client.opensearch.indices.PutMappingResponse;
import org.opensearch.client.opensearch.indices.get_mapping.IndexMappingRecord;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.opensearch.client.transport.httpclient5.ResponseException;
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

  private static final String SETTING_MAX_RESULT_WINDOW = "max_result_window";

  @VisibleForTesting
  static final String MAPPING_RESOURCE = "index.mapping.json";

  // ScopedName needs a type adapter because it is used as a key in a map,
  // which is not supported by default in Gson:
  // - It would serialize as a string "scope:name"
  // - but then it would not know how to deserialize it.
  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(ScopedName.class, new ScopedNameTypeAdapter()).create();

  static final JsonpMapper MAPPER = new JsonbJsonpMapper();

  // When TLS verification is disabled, getClients() replaces the connection manager of the
  // transport. The replacement keeps the default timeouts of the transport.
  private static final Timeout CONNECT_TIMEOUT =
      Timeout.ofMilliseconds(ApacheHttpClient5TransportBuilder.DEFAULT_CONNECT_TIMEOUT_MILLIS);
  private static final Timeout SOCKET_TIMEOUT =
      Timeout.ofMilliseconds(ApacheHttpClient5TransportBuilder.DEFAULT_RESPONSE_TIMEOUT_MILLIS);

  @VisibleForTesting
  static final boolean KEEP = true;
  @VisibleForTesting
  static final boolean DISCARD = false;

  // used to tokenize the query string, same as the MetadataDataset
  private static final Pattern SPACE_SEPARATOR_PATTERN = Pattern.compile("\\s+");

  // Various fields in a metadata document (indexed in elasticsearch). Beware that these directly
  // correspond to field name in the index settings (index.mapping.json). Any change here must be
  // reflected there, and vice versa.
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

  private final CConfiguration cConf;
  private final String elasticHosts;
  private final String indexName;
  private final String scrollTimeout;

  private final String credentialsUsername;
  private final String credentialsPassword;

  private final boolean verifyTLS;

  private final AtomicReference<Clients> clients = new AtomicReference<>();
  private volatile boolean closed;

  private volatile boolean created;
  private volatile int maxWindowSize = Config.DEFAULT_MAX_RESULT_WINDOW;

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
  public synchronized void close() {
    closed = true;
    Clients current = clients.getAndSet(null);
    if (current != null) {
      Closeables.closeQuietly(current.transport);
    }
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
      OpenSearchClient client = getClients().client;
      if (!indexExists(client)) {
        IndexSettings settings = createSettings();
        TypeMapping mappings = createMappings();
        LOG.info("Creating index '{}' with settings: {}", indexName, settings.toJsonString());
        try {
          CreateIndexResponse response = client.indices().create(c -> c
              .index(indexName).settings(settings).mappings(mappings));
          if (!response.acknowledged()) {
            throw new IOException(
                "Create index request was not acknowledged: " + response.toJsonString());
          }
        } catch (OpenSearchException e) {
          // Several processes can try to create the index at the same time. If another one created
          // it after the check above, the index already exists and this request is not needed.
          if (!"resource_already_exists_exception".equals(e.error().type())) {
            throw new IOException("Unable to create index " + indexName, e);
          }
        }
      }
      // Another process can have created the index just now, so wait until it can be used. A new
      // index is yellow while its primary shards still initialize, so wait for those too. The wait
      // is best effort: if it times out or is not permitted, the read below still checks the index.
      try {
        client.cluster().health(h -> h.index(indexName).waitForStatus(HealthStatus.Yellow)
            .waitForNoInitializingShards(true).timeout(t -> t.time("25s")));
      } catch (OpenSearchException | ResponseException e) {
        LOG.debug("Unable to wait for index {} to become available", indexName, e);
      }
      IndexState state;
      try {
        state = client.indices().getSettings(g -> g.index(indexName)).get(indexName);
      } catch (OpenSearchException e) {
        throw new IOException("Unable to obtain settings for index " + indexName, e);
      }
      if (state == null || state.settings() == null) {
        throw new IOException("Unable to obtain settings for index " + indexName);
      }
      // The response nests the index settings under "index". If it does not, they are at the top
      // level.
      IndexSettings topLevel = state.settings();
      IndexSettings actualSettings = topLevel.index() == null ? topLevel : topLevel.index();
      if (actualSettings.maxResultWindow() != null) {
        maxWindowSize = actualSettings.maxResultWindow();
      }
      LOG.debug("Using index {} with 'max_result_window' of {}", indexName, maxWindowSize);

      // An index that was created for Elasticsearch 6.x keeps its mapping under a type, so the
      // mapping has no CDAP version at the top level. Fail instead of serving empty metadata.
      try {
        getVersionInfo();
      } catch (IllegalArgumentException | JsonException e) {
        throw new IOException("Index " + indexName + " was not created by this version of CDAP,"
            + " for example it was created by Elasticsearch 6.x", e);
      }

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
    // In our index.mapping.json, _meta is a map from String to String
    Map<String, JsonData> meta = getIndexMeta(getClients().client, indexName);
    LOG.trace("Index mapping for _meta: {}", meta);
    try {
      return new VersionInfo(new ProjectInfo.Version(getMetaValue(meta, Config.MAPPING_CDAP_VERSION)),
          Integer.parseInt(getMetaValue(meta, Config.MAPPING_METADATA_VERSION)),
          Long.parseLong(getMetaValue(meta, Config.MAPPING_MAPPING_CHECKSUM)));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unable to extract version info from index mappings: " + meta, e);
    }
  }

  static Map<String, JsonData> getIndexMeta(OpenSearchClient client, String indexName)
      throws IOException {
    IndexMappingRecord record;
    try {
      record = client.indices().getMapping(g -> g.index(indexName)).get(indexName);
    } catch (OpenSearchException e) {
      throw new IOException("Unable to obtain mappings for index " + indexName, e);
    }
    if (record == null) {
      throw new IOException("Unable to obtain mappings for index " + indexName);
    }
    return record.mappings().meta();
  }

  /**
   * Records in the "_meta" object of the index whether the index misses metadata that must be
   * written again.
   */
  static void setRebuildRequired(OpenSearchClient client, String indexName, boolean required)
      throws IOException {
    Map<String, JsonData> meta = new LinkedHashMap<>(getIndexMeta(client, indexName));
    if (required) {
      meta.put(Config.MAPPING_REBUILD_REQUIRED, JsonData.of("true"));
    } else {
      meta.remove(Config.MAPPING_REBUILD_REQUIRED);
    }
    // A mapping update replaces the complete "_meta" object.
    PutMappingResponse response;
    try {
      response = client.indices().putMapping(p -> p.index(indexName).meta(meta));
    } catch (OpenSearchException e) {
      throw new IOException("Unable to update mappings for index " + indexName, e);
    }
    if (!response.acknowledged()) {
      throw new IOException("Mapping update was not acknowledged: " + response.toJsonString());
    }
  }

  private static String getMetaValue(Map<String, JsonData> meta, String key) {
    JsonData value = meta.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Missing '" + key + "' in _meta");
    }
    JsonValue json = value.toJson();
    if (json.getValueType() != JsonValue.ValueType.STRING) {
      throw new IllegalArgumentException("'" + key + "' in _meta is not a string: " + json);
    }
    return ((JsonString) json).getString();
  }

  /**
   * Returns the clients for talking to the search cluster.
   *
   * @throws IOException if this storage is closed, or the clients cannot be created
   */
  private Clients getClients() throws IOException {
    Clients current = clients.get();
    if (current != null) {
      return current;
    }

    synchronized (this) {
      if (closed) {
        throw new IOException("The metadata storage is closed");
      }
      current = clients.get();
      if (current != null) {
        return current;
      }

      LOG.info("Create new OpenSearch client for cluster {}", elasticHosts);
      HttpHost[] hosts = Arrays.stream(elasticHosts.split(",")).map(hostAndPort -> {
        String scheme = "http";
        String host = hostAndPort;

        int schemeIdx = host.indexOf("://");
        if (schemeIdx > 0) {
          scheme = host.substring(0, schemeIdx);
          host = host.substring(schemeIdx + 3);
        }

        int port;
        int portIdx = host.lastIndexOf(":");
        if (portIdx > 0) {
          try {
            port = Integer.parseInt(host.substring(portIdx + 1));
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid HTTP host: " + host, e);
          }

          host = host.substring(0, portIdx);
        } else {
          port = scheme.equals("https") ? 9243 : 9200;
        }

        return new HttpHost(scheme, host, port);
      }).toArray(HttpHost[]::new);

      // Fail instead of silently verifying certificates when verification is disabled
      SSLContext trustAllContext = null;
      if (!verifyTLS) {
        try {
          trustAllContext = SSLContextBuilder.create()
              .loadTrustMaterial(TrustAllStrategy.INSTANCE).build();
        } catch (GeneralSecurityException e) {
          throw new IOException(e);
        }
      }
      SSLContext sslContext = trustAllContext;

      ApacheHttpClient5TransportBuilder builder = ApacheHttpClient5TransportBuilder.builder(hosts)
          .setMapper(MAPPER);

      builder.setHttpClientConfigCallback(httpClientConfigCallback -> {
        if (credentialsUsername != null && credentialsPassword != null) {
          LOG.info("Creating REST client with authentication, username: {}", credentialsUsername);
          BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
          for (HttpHost host : hosts) {
            credentialsProvider.setCredentials(new AuthScope(host),
                new UsernamePasswordCredentials(credentialsUsername,
                    credentialsPassword.toCharArray()));
            if (!"https".equals(host.getSchemeName())) {
              LOG.warn("Sending credentials to {} without TLS", host);
            }
          }
          httpClientConfigCallback.setDefaultCredentialsProvider(credentialsProvider);
        }

        if (!verifyTLS) {
          LOG.warn("Creating REST client with TLS verification DISABLED");
          httpClientConfigCallback.setConnectionManager(
              PoolingAsyncClientConnectionManagerBuilder.create()
                  .setMaxConnPerRoute(ApacheHttpClient5TransportBuilder.DEFAULT_MAX_CONN_PER_ROUTE)
                  .setMaxConnTotal(ApacheHttpClient5TransportBuilder.DEFAULT_MAX_CONN_TOTAL)
                  .setDefaultConnectionConfig(ConnectionConfig.custom()
                      .setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT)
                      .build())
                  .setTlsStrategy(ClientTlsStrategyBuilder.create().setSslContext(sslContext)
                      // Only the verifier set here checks the host name, not also the JDK
                      .setHostVerificationPolicy(HostnameVerificationPolicy.CLIENT)
                      .setHostnameVerifier(NoopHostnameVerifier.INSTANCE).buildAsync())
                  .build());
        }

        return httpClientConfigCallback;
      });

      current = new Clients(builder.build());
      clients.set(current);
      return current;
    }
  }

  static TypeMapping createMappings() throws IOException {
    URL url = ElasticsearchMetadataStorage.class.getClassLoader().getResource(MAPPING_RESOURCE);
    if (url == null) {
      throw new IllegalStateException(
          "Index mapping file '" + MAPPING_RESOURCE + "'not found in classpath");
    }
    // read the mappings file, replace the CDAP_VERSION placeholder with current version
    String mappings = Resources.toString(url, Charsets.UTF_8);
    @SuppressWarnings("ConstantConditions")
    long checksum = Checksums.fingerprint64(Bytes.toBytes(mappings));
    String resolved = mappings
        .replace(Config.MAPPING_CDAP_VERSION_PLACEHOLDER, ProjectInfo.getVersion().toString())
        .replace(Config.MAPPING_METADATA_VERSION_PLACEHOLDER,
            Integer.toString(VersionInfo.METADATA_VERSION))
        .replace(Config.MAPPING_MAPPING_CHECKSUM_PLACEHOLDER, Long.toString(checksum));
    // The file has comments, which Gson tolerates but the JSON-P parser does not
    String json = GSON.fromJson(resolved, JsonElement.class).toString();
    try (JsonParser parser = MAPPER.jsonProvider().createParser(new StringReader(json))) {
      return new TypeMapping.Builder().withJson(parser, MAPPER).build();
    }
  }

  // Index settings, used when creating the index
  private IndexSettings createSettings() {
    return createSettings(cConf.get(Config.CONF_ELASTIC_NUM_SHARDS),
        cConf.get(Config.CONF_ELASTIC_NUM_REPLICAS),
        cConf.get(Config.CONF_ELASTIC_WINDOW_SIZE));
  }

  static IndexSettings createSettings(@Nullable String numShards,
      @Nullable String numReplicas, @Nullable String maxResultWindow) {
    return IndexSettings.of(s -> s
        .numberOfShards(numShards == null ? null : Integer.parseInt(numShards))
        .numberOfReplicas(numReplicas == null ? null : Integer.parseInt(numReplicas))
        .maxResultWindow(maxResultWindow == null ? null : Integer.parseInt(maxResultWindow))
        .analysis(a -> a.analyzer("text_analyzer", an -> an.pattern(p -> p
            // this reflects the tokenization performed by MetadataDataset
            .pattern("[-_,;.\\s]+")
            .lowercase(true)))));
  }

  @Override
  public void dropIndex() throws IOException {
    OpenSearchClient client = getClients().client;
    synchronized (this) {
      if (!indexExists(client)) {
        created = false;
        return;
      }
      try {
        DeleteIndexResponse response = client.indices().delete(d -> d.index(indexName));
        if (!response.acknowledged()) {
          LOG.warn("Delete index request was not acknowledged: {}", response.toJsonString());
        }
      } catch (OpenSearchException e) {
        // Another process can delete the index after the check above
        if (!"index_not_found_exception".equals(e.error().type())) {
          throw new IOException("Unable to drop index " + indexName, e);
        }
      }
      created = false;
    }
  }

  private boolean indexExists(OpenSearchClient client) throws IOException {
    try {
      return client.indices().exists(e -> e.index(indexName)).value();
    } catch (OpenSearchException e) {
      throw new IOException("Unable to check whether index " + indexName + " exists", e);
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
            executeBulk(Collections.singletonList(intermediary.getOperation()), options,
                new HashSet<>(Collections.singleton(entity)));
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
    List<String> ids = mutations.keySet().stream()
        .map(ElasticsearchMetadataStorage::toDocumentId).collect(Collectors.toList());
    MgetResponse<JsonData> multiGetResponse;
    try {
      multiGetResponse = getClients().client.mget(m -> m.index(indexName).ids(ids), JsonData.class);
    } catch (OpenSearchException e) {
      throw new IOException("Failed to read from index for entities " + mutations.keySet() + ": "
          + e.getMessage(), e);
    }
    if (multiGetResponse.docs().size() != mutations.size()) {
      throw new IOException("Failed to read from index for entities " + mutations.keySet());
    }
    // responses are in the same order as the original requests
    int index = 0;
    List<BulkOperation> operations = new ArrayList<>(mutations.size());
    for (Map.Entry<MetadataEntity, MetadataMutation> entry : mutations.entrySet()) {
      MultiGetResponseItem<JsonData> itemResponse = multiGetResponse.docs().get(index++);
      if (itemResponse.isFailure()) {
        throw new IOException("Failed to read from index for entity " + entry.getKey() + ": "
            + describe(itemResponse.failure().error()));
      }
      IndexedMetadata before;
      try {
        before = toIndexedMetadata(itemResponse.result());
      } catch (RuntimeException e) {
        throw new IOException("Failed to read from index for entity " + entry.getKey() + ": "
            + e.getMessage(), e);
      }
      RequestAndChange intermediary = applyMutation(before, entry.getValue());
      operations.add(intermediary.getOperation());
      changes.put(entry.getKey(), intermediary.getChange());
    }
    executeBulk(operations, options, mutations.keySet());
    return new ArrayList<>(changes.values());
  }

  @Override
  public Metadata read(Read read) throws IOException {
    Metadata metadata = readFromIndex(read.getEntity()).getMetadata();
    return filterMetadata(metadata, KEEP, read.getKinds(), read.getScopes(), read.getSelection());
  }

  @Override
  public io.cdap.cdap.spi.metadata.SearchResponse search(SearchRequest request)
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
   * Creates the write for an entity creation or update. See {@link
   * MetadataMutation.Create} for detailed semantics.
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
   * Creates the delete for an entity deletion. This drops the corresponding
   * metadata document from the index.
   *
   * @param before the metadata for the mutation's entity before the change
   * @return a write to be executed, and the change caused by the mutation
   */
  private RequestAndChange drop(MetadataEntity entity, IndexedMetadata before) {
    return new RequestAndChange(deleteFromIndex(entity, before),
        new MetadataChange(entity, before.getMetadata(), Metadata.EMPTY));
  }

  /**
   * Creates the write for updating the metadata of an entity. This updates or
   * adds the new metadata to the corresponding metadata document in the index.
   *
   * @param before the metadata for the mutation's entity before the change
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
   * Creates the write for removing some metadata from an entity. This removed
   * the specified metadata from the corresponding metadata document in the index.
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
   * @return existing metadata along with the sequence number and primary term of its document, or
   *     {@link IndexedMetadata#NONE} if there is no document.
   */
  private IndexedMetadata readFromIndex(MetadataEntity entity) throws IOException {
    String id = toDocumentId(entity);
    try {
      // A missing document is a response with found=false, but a missing index fails
      GetResponse<JsonData> response =
          getClients().client.get(g -> g.index(indexName).id(id), JsonData.class);
      return toIndexedMetadata(response);
    } catch (Exception e) {
      if (e.getCause() instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IOException("Failed to read from index for entity " + entity + ": "
          + e.getMessage(), e);
    }
  }

  /**
   * Converts a document, as the get and multi-get endpoints return it, into metadata.
   */
  private static IndexedMetadata toIndexedMetadata(GetResult<JsonData> result) {
    if (!result.found()) {
      return IndexedMetadata.NONE;
    }
    if (result.seqNo() == null || result.primaryTerm() == null) {
      throw new IllegalStateException("Document " + result.id()
          + " has no sequence number or primary term");
    }
    return IndexedMetadata.of(toMetadata(result.source()), result.seqNo(), result.primaryTerm());
  }

  /**
   * Converts the source of a document in the index into metadata.
   *
   * @throws JsonParseException if the source is not a valid metadata document
   */
  private static Metadata toMetadata(@Nullable JsonData source) {
    MetadataDocument doc = source == null
        ? null : GSON.fromJson(source.toJson().toString(), MetadataDocument.class);
    if (doc == null || doc.getMetadata() == null || doc.getMetadata().getTags() == null
        || doc.getMetadata().getProperties() == null) {
      throw new JsonParseException("Document has no metadata: " + source);
    }
    return doc.getMetadata();
  }

  /**
   * Converts a metadata document into the source of a document in the index.
   */
  private static JsonData toSource(MetadataDocument doc) {
    try (JsonReader reader =
        MAPPER.jsonProvider().createReader(new StringReader(GSON.toJson(doc)))) {
      return JsonData.of(reader.readValue());
    }
  }

  /**
   * Creates a write for adding or updating the metadata for an entity in the index. If the
   * entity has no document yet, the write only succeeds if there is still none. Otherwise it only
   * succeeds if the document did not change since it was read. The write must be executed by the
   * caller.
   */
  private BulkOperation writeToIndex(MetadataEntity entity, IndexedMetadata before,
      Metadata metadata) {
    MetadataDocument doc = MetadataDocument.of(entity, metadata);
    LOG.trace("Indexing document: {}", doc);
    String id = toDocumentId(entity);
    JsonData source = toSource(doc);
    if (!before.hasDocument()) {
      return BulkOperation.of(b -> b.create(c -> c.index(indexName).id(id).document(source)));
    }
    return BulkOperation.of(b -> b.index(i -> i.index(indexName).id(id).document(source)
        .ifSeqNo(before.getSeqNo()).ifPrimaryTerm(before.getPrimaryTerm())));
  }

  /**
   * Creates a write for removing an entity in the index. If the entity has a document, the write
   * only succeeds if that document did not change since it was read. The write must be executed
   * by the caller.
   */
  private BulkOperation deleteFromIndex(MetadataEntity entity, IndexedMetadata before) {
    String id = toDocumentId(entity);
    LOG.trace("Deleting document with id: {}", id);
    return BulkOperation.of(b -> b.delete(d -> d.index(indexName).id(id)
        .ifSeqNo(before.getSeqNo()).ifPrimaryTerm(before.getPrimaryTerm())));
  }

  /**
   * Executes a bulk request and handles the responses for possible failures, and removes all
   * successful mutations from the mutations map.
   *
   * @param entities the entities written by this bulk. Every entity whose write succeeds is
   *     removed from this set, even if an exception is thrown.
   * @throws MetadataConflictException if a conflict occurs for any of the operations in the
   *     bulk
   * @throws IOException for any other problem encountered
   */
  @VisibleForTesting
  void executeBulk(List<BulkOperation> operations, MutationOptions options,
      Set<MetadataEntity> entities) throws IOException {
    BulkResponse response;
    try {
      response = getClients().client.bulk(BulkRequest.of(b -> b.operations(operations)
          .refresh(getRefreshPolicy(options))));
    } catch (OpenSearchException e) {
      throw new IOException("Bulk request unsuccessful: " + e.getMessage(), e);
    }
    if (!response.errors()) {
      return;
    }
    // The document id does not have the version of an application, so the entity cannot be
    // derived from it: look up the entity that the caller used as the key.
    Map<String, MetadataEntity> entitiesByDocumentId = new HashMap<>();
    for (MetadataEntity entity : entities) {
      entitiesByDocumentId.put(toDocumentId(entity), entity);
    }
    IOException ioe = null;
    List<MetadataEntity> conflictEntities = new ArrayList<>();
    for (BulkResponseItem itemResponse : response.items()) {
      MetadataEntity entityId = entitiesByDocumentId.get(itemResponse.id());
      if (entityId == null) {
        LOG.warn("Bulk response has document id {} that was not in the request",
            itemResponse.id());
        if (itemResponse.error() != null) {
          ioe = addFailure(ioe, String.format("%s request unsuccessful for document %s: %s",
              itemResponse.operationType().jsonValue(), itemResponse.id(),
              describe(itemResponse.error())));
        }
        continue;
      }
      // A delete of a missing document has no error, so it is successful. A missing index is an
      // error.
      if (itemResponse.error() == null) {
        // remove this mutation - it was successful
        entities.remove(entityId);
        continue;
      }
      // this mutation failed
      if (itemResponse.status() == HTTP_CONFLICT) {
        conflictEntities.add(entityId);
        continue;
      }
      // not a conflict -> true failure
      ioe = addFailure(ioe, String.format("%s request unsuccessful for entity %s: %s",
          itemResponse.operationType().jsonValue(), entityId, describe(itemResponse.error())));
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
   * Returns an exception for a failed write: the given one with the failure added, or a new one if
   * this is the first failure.
   */
  private static IOException addFailure(@Nullable IOException ioe, String message) {
    if (ioe == null) {
      return new IOException(message);
    }
    ioe.addSuppressed(new IOException(message));
    return ioe;
  }

  /**
   * Returns the type of an error, followed by its reason if it has one.
   */
  private static String describe(ErrorCause error) {
    return error.reason() == null ? error.type() : error.type() + ": " + error.reason();
  }

  /**
   * Returns the HTTP status of an error response, or 0 if it does not have one.
   */
  private static int getStatus(Exception e) {
    if (e instanceof ResponseException) {
      return ((ResponseException) e).status();
    }
    Integer status = e instanceof OpenSearchException
        ? ((OpenSearchException) e).response().status() : null;
    return status == null ? 0 : status;
  }

  /**
   * Returns the refresh policy for a write request. If {@link MutationOptions#isAsynchronous()} is
   * true, write requests will return immediately after the request is acknowledged; otherwise, they
   * will only return after they are confirmed to be applied to the index.
   */
  private static Refresh getRefreshPolicy(MutationOptions options) {
    return options.isAsynchronous() ? Refresh.True : Refresh.WaitFor;
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
  private io.cdap.cdap.spi.metadata.SearchResponse doScroll(SearchRequest request)
      throws IOException {

    Cursor cursor = Cursor.fromString(request.getCursor());
    SearchResponse<JsonData> searchResponse;
    try {
      searchResponse = getClients().client.scroll(s -> {
        s.scrollId(cursor.getActualCursor());
        if (request.isCursorRequested()) {
          s.scroll(t -> t.time(scrollTimeout));
        }
        return s;
      }, JsonData.class);
    } catch (OpenSearchException | ResponseException e) {
      int status = getStatus(e);
      if (status != HTTP_BAD_REQUEST && status != HTTP_NOT_FOUND) {
        throw new IOException("Scroll request unsuccessful: " + e.getMessage(), e);
      }
      // scroll invalid or timed out
      return doSearch(createRequestFromCursor(request, cursor));
    }
    if (searchResponse.timedOut()) {
      // scroll had expired, we have to search again
      return doSearch(createRequestFromCursor(request, cursor));
    }
    return createSearchResponse(request, searchResponse, computeCursor(searchResponse, cursor),
        cursor.getOffset(), cursor.getLimit());
  }

  private io.cdap.cdap.spi.metadata.SearchResponse createSearchResponse(SearchRequest request,
      SearchResponse<JsonData> response,
      String cursor, int offset, int limit) throws IOException {
    List<MetadataRecord> results;
    try {
      results = fromHits(response.hits().hits());
    } catch (RuntimeException e) {
      throw new IOException("Unable to read search results: " + e.getMessage(), e);
    }
    return new io.cdap.cdap.spi.metadata.SearchResponse(request, cursor, offset, limit,
        (int) getTotalHits(response), results);
  }

  /**
   * Returns the total number of results of a search. The search requests track the total, so it is
   * only missing if the cluster does not return it: then this is the number of results returned.
   */
  private static long getTotalHits(SearchResponse<JsonData> response) {
    TotalHits total = response.hits().total();
    return total == null ? response.hits().hits().size() : total.value();
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
  private io.cdap.cdap.spi.metadata.SearchResponse doSearch(SearchRequest request)
      throws IOException {

    org.opensearch.client.opensearch.core.SearchRequest searchRequest = createSearchRequest(request);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Executing search request {}", searchRequest.toJsonString());
    }
    SearchResponse<JsonData> searchResponse;
    try {
      searchResponse = getClients().client.search(searchRequest, JsonData.class);
    } catch (OpenSearchException e) {
      throw new IOException("Search request unsuccessful: " + e.getMessage(), e);
    }
    return createSearchResponse(request, searchResponse, computeCursor(searchResponse, request),
        request.getOffset(), request.getLimit());
  }

  /**
   * Generates the cursor to return as part of a search response.
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
  private String computeCursor(SearchResponse<JsonData> searchResponse, Cursor cursor) {
    if (searchResponse.scrollId() != null) {
      int newOffset = cursor.getOffset() + searchResponse.hits().hits().size();
      if (newOffset < getTotalHits(searchResponse)) {
        return new Cursor(cursor, newOffset, searchResponse.scrollId()).toString();
      }
      // we have seen all results, there are no more to fetch: clear the scroll
      cancelScroll(searchResponse.scrollId());
    }
    return null;
  }

  @Nullable
  private String computeCursor(SearchResponse<JsonData> searchResponse, SearchRequest request) {
    if (searchResponse.scrollId() != null) {
      int newOffset = request.getOffset() + searchResponse.hits().hits().size();
      if (newOffset < getTotalHits(searchResponse)) {
        return new Cursor(newOffset, request.getLimit(), request.isShowHidden(), request.getScope(),
            request.getNamespaces(), request.getTypes(),
            request.getSorting() == null ? null : request.getSorting().toString(),
            searchResponse.scrollId(), request.getQuery()).toString();
      }
      // we have seen all results, there are no more to fetch: clear the scroll
      cancelScroll(searchResponse.scrollId());
    }
    return null;
  }

  private void cancelScroll(String scrollId) {
    // Clear the scroll in the background, so that the search does not wait for it. We don't really
    // care about the response: a scroll that is not cleared expires on its own.
    try {
      getClients().asyncClient.clearScroll(c -> c.scrollId(scrollId))
          .whenComplete((response, e) -> {
            if (e != null) {
              LOG.debug("Unable to clear scroll", e);
            }
          });
    } catch (IOException | RuntimeException e) {
      LOG.debug("Unable to clear scroll", e);
    }
  }

  private org.opensearch.client.opensearch.core.SearchRequest createSearchRequest(
      SearchRequest request) {
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

    org.opensearch.client.opensearch.core.SearchRequest.Builder builder =
        new org.opensearch.client.opensearch.core.SearchRequest.Builder()
            .index(indexName)
            .from(offset)
            .size(limit)
            // Without this, the total number of results is only exact up to 10000, but the
            // cursor and the pagination of the callers need the exact number.
            .trackTotalHits(t -> t.enabled(true));
    if (request.getSorting() != null) {
      String field = mapSortKey(request.getSorting().getKey().toLowerCase(Locale.ROOT));
      SortOrder order = request.getSorting().getOrder() == Sorting.Order.ASC
          ? SortOrder.Asc : SortOrder.Desc;
      builder.sort(s -> s.field(f -> f.field(field).order(order)));
    }
    // only request a scroll if the offset is 0. The search cluster will throw otherwise
    if (request.isCursorRequested() && offset == 0) {
      builder.scroll(t -> t.time(scrollTimeout));
    }
    return builder.query(createQuery(request)).build();
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
  private Query createQuery(SearchRequest request) {
    // first create a query from the query terms
    Query mainQuery = createMainQuery(request);

    List<Query> conditions = new ArrayList<>();
    // if the request asks only for a subset of entity types, add a boolean clause for that
    if (request.getTypes() != null && !request.getTypes().isEmpty()) {
      addCondition(conditions, request.getTypes().stream().map(
              type -> filterTerm(TYPE_FIELD, FieldValue.of(type.toLowerCase(Locale.ROOT))))
          .collect(Collectors.toList()));
    }
    if (request.getNamespaces() != null) {
      addCondition(conditions, request.getNamespaces().stream().map(
              ns -> filterTerm(NAMESPACE_FIELD, FieldValue.of(ns.toLowerCase(Locale.ROOT))))
          .collect(Collectors.toList()));
    }
    if (!request.isShowHidden()) {
      conditions.add(filterTerm(HIDDEN_FIELD, FieldValue.of(false)));
    }
    if (!conditions.isEmpty()) {
      conditions.add(mainQuery);
      mainQuery = Query.of(q -> q.bool(b -> b.must(conditions)));
    }
    return mainQuery;
  }

  /**
   * Creates a query for documents whose field has exactly the given value. The query does not add
   * to the score of a document, so it only restricts the results of another query.
   */
  private static Query filterTerm(String field, FieldValue value) {
    return Query.of(q -> q.term(t -> t.field(field).value(value).boost(0.0F)));
  }

  /**
   * For a list of alternative sub-queries, create a query that requires at least one of them to be
   * fulfilled, without contributing to the query scoring, and add it to a list of queries.
   *
   * Depending on whether the number of sub-queries is 1, adds the single query or a boolean OR over
   * all sub-queries.
   */
  private void addCondition(List<Query> conditions, List<Query> subQueries) {
    if (subQueries.size() == 1) {
      conditions.add(subQueries.get(0));
      return;
    }
    conditions.add(anyOf(subQueries));
  }

  /**
   * Creates a query for documents that match at least one of the given queries.
   */
  private static Query anyOf(List<Query> queries) {
    return Query.of(q -> q.bool(b -> b.should(queries).minimumShouldMatch("1")));
  }

  /**
   * Creates the (sub-)query from the terms in the query string.
   */
  private Query createMainQuery(SearchRequest request) {
    if (request.getQuery().equals("*")) {
      return Query.of(q -> q.matchAll(m -> m));
    }
    // the indexed document contains three text fields: one for each scope and for all scopes combined.
    // all terms must occur in the text field as selected by the scope in the search request.
    String textField =
        request.getScope() == null ? TEXT_FIELD : request.getScope().name().toLowerCase();

    // split the query into its terms and iterate over all terms
    Iterable<String> terms = Splitter.on(SPACE_SEPARATOR_PATTERN)
        .omitEmptyStrings().trimResults().split(request.getQuery());
    List<Query> termQueries = new ArrayList<>();
    for (String term : terms) {
      termQueries.add(createTermQuery(term, textField, request));
    }
    if (termQueries.isEmpty()) {
      return Query.of(q -> q.matchAll(m -> m));
    }
    if (termQueries.size() == 1) {
      return termQueries.get(0);
    }
    return anyOf(termQueries);
  }

  /**
   * Create a sub-query for a single term in the query string.
   *
   * @param term the term as it appears in the query, possibly with a field qualifier
   * @param textField the default text field to search if the term does not have a field
   */
  private Query createTermQuery(String term, String textField, SearchRequest request) {
    // determine if the term has a field qualifier
    String field = null;
    term = term.trim().toLowerCase();
    // Create a term query on the term as is. This would include a field: prefix if the term hs one.
    // This is important for the case of schema search: If the schema contains a field f of type t,
    // then we index "f:t" in the plain text as well as in the "schema" property. If the query is
    // just "f:t", we must search the plain text field for that.
    Query plainQuery = createTermQuery(textField, term);
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
        // field and term are reassigned above, so the lambda needs copies of them
        String ttlField = field;
        String ttl = term;
        return Query.of(q -> q.term(t -> t.field(ttlField).value(FieldValue.of(ttl))));
      } catch (NumberFormatException e) {
        // ignore - the follow-on code will create a regular term query for this
      }
    }
    List<Query> propertyConditions = new ArrayList<>();
    propertyConditions.add(filterTerm(NESTED_NAME_FIELD, FieldValue.of(field)));
    if (request.getScope() != null) {
      propertyConditions.add(filterTerm(NESTED_SCOPE_FIELD, FieldValue.of(request.getScope().name())));
    }
    propertyConditions.add(createTermQuery(NESTED_VALUE_FIELD, term));
    Query propertyQuery = Query.of(q -> q.nested(n -> n.path(PROPS_FIELD)
        .query(p -> p.bool(b -> b.must(propertyConditions))).scoreMode(ChildScoreMode.Max)));

    // match either a plain term of the form "f:t" or the word "t" in property "f"
    return anyOf(Arrays.asList(plainQuery, propertyQuery));
  }

  /**
   * Create a query for a single term in a given field.
   *
   * @return a wildcard query is the term contains * or ?, or a match query otherwise
   */
  private Query createTermQuery(String field, String term) {
    return term.contains("*") || term.contains("?")
        ? Query.of(q -> q.wildcard(w -> w.field(field).value(term)))
        // the term should not get split in to multiple words, but in case it does, let's require all words
        : Query.of(q -> q.match(m -> m.field(field).query(FieldValue.of(term))
            .operator(Operator.And)));
  }

  /**
   * Transforms a list of search hits into metadata records.
   */
  private static List<MetadataRecord> fromHits(List<Hit<JsonData>> hits) {
    return hits.stream()
        .map(hit -> new MetadataRecord(
            toMetadataEntity(hit.id()),
            toMetadata(hit.source())))
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
  @VisibleForTesting
  static String toDocumentId(MetadataEntity entity) {
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

  /**
   * The clients for talking to the search cluster. They share one transport, so they are created
   * and closed together.
   */
  private static final class Clients {
    private final ApacheHttpClient5Transport transport;
    private final OpenSearchClient client;
    private final OpenSearchAsyncClient asyncClient;

    private Clients(ApacheHttpClient5Transport transport) {
      this.transport = transport;
      this.client = new OpenSearchClient(transport);
      this.asyncClient = new OpenSearchAsyncClient(transport);
    }
  }
}
