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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.metadata.Cursor;
import io.cdap.cdap.common.metadata.MetadataConflictException;
import io.cdap.cdap.common.metadata.MetadataUtil;
import io.cdap.cdap.common.metadata.QueryParser;
import io.cdap.cdap.common.metadata.QueryTerm;
import io.cdap.cdap.common.metadata.QueryTerm.Qualifier;
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
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.Sorting;
import org.apache.http.HttpHost;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * A metadata storage provider that delegates to Elasticsearch.
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

  // Various fields in a metadata document (indexed in elasticsearch). Beware that these directly
  // correspond to field name in the index settings (index.mapping.json). Any change here must be
  // reflected there, and vice versa.
  private static final String DOC_TYPE = "meta";
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
  private static final String SUPPORTED_SORT_KEYS = SORT_KEY_MAP.keySet().stream().collect(Collectors.joining(", "));

  private final CConfiguration cConf;
  private final RestHighLevelClient client;
  private final String indexName;
  private final String scrollTimeout;

  private volatile boolean created = false;
  private int maxWindowSize = Config.DEFAULT_MAX_RESULT_WINDOW;

  // sleep 100 ms for at most 50 times
  private final RetryStrategy retryStrategyOnConflict;

  @Inject
  public ElasticsearchMetadataStorage(CConfiguration cConf) {
    this.cConf = cConf;
    indexName = cConf.get(Config.CONF_ELASTIC_INDEX_NAME, Config.DEFAULT_INDEX_NAME);
    scrollTimeout = cConf.get(Config.CONF_ELASTIC_SCROLL_TIMEOUT, Config.DEFAULT_SCROLL_TIMEOUT);
    String elasticHosts = cConf.get(Config.CONF_ELASTIC_HOSTS, Config.DEFAULT_ELASTIC_HOSTS);
    int numRetries = cConf.getInt(Config.CONF_ELASTIC_CONFLICT_NUM_RETRIES,
                                  Config.DEFAULT_ELASTIC_CONFLICT_NUM_RETRIES);
    int retrySleepMs = cConf.getInt(Config.CONF_ELASTIC_CONFLICT_RETRY_SLEEP_MS,
                                    Config.DEFAULT_ELASTIC_CONFLICT_RETRY_SLEEP_MS);
    retryStrategyOnConflict = RetryStrategies.limit(numRetries,
                                                    RetryStrategies.fixDelay(retrySleepMs, TimeUnit.MILLISECONDS));

    LOG.info("Elasticsearch cluster is {}", elasticHosts);
    HttpHost[] hosts = Arrays.stream(elasticHosts.split(",")).map(hostAndPort -> {
      int pos = hostAndPort.indexOf(':');
      String host = pos < 0 ? hostAndPort : hostAndPort.substring(0, pos);
      int port = pos < 0 ? 9200 : Integer.parseInt(hostAndPort.substring(pos + 1));
      return new HttpHost(host, port);
    }).toArray(HttpHost[]::new);
    client = new RestHighLevelClient(RestClient.builder(hosts));
  }

  @Override
  public void close() {
    if (client != null) {
      Closeables.closeQuietly(client);
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
      GetIndexRequest request = new GetIndexRequest();
      request.indices(indexName);
      if (!client.indices().exists(request, RequestOptions.DEFAULT)) {
        String settings = createSettings();
        LOG.info("Creating index '{}' with settings: {}", indexName, settings);
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.settings(settings, XContentType.JSON);
        createIndexRequest.mapping(DOC_TYPE, createMappings(), XContentType.JSON);
        CreateIndexResponse response = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        if (!response.isAcknowledged()) {
          throw new IOException("Create index request was not acknowledged by EleasticSearch: " + response);
        }
      }
      request = new GetIndexRequest();
      request.indices(indexName);
      GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
      Settings settings = response.getSettings().get(indexName);
      if (settings == null) {
        throw new IOException("Unable to obtain Elasticsearch settings for index " + indexName);
      }
      String maxWindow = response.getSetting(indexName, "index.max_result_window");
      if (maxWindow != null) {
        maxWindowSize = Integer.parseInt(maxWindow);
      }
      LOG.debug("Using Elasticsearch index {} with 'max_result_window' of {}", indexName, maxWindowSize);

      // For some unknown reason, multi-get fails immediately after creating an index
      // However, performing one regular read appears to fix that, perhaps it waits until the index is ready?
      // Hence, perform one read to ensure the index is ready to use.
      readFromIndex(MetadataEntity.ofNamespace("system"));
      created = true;
    }
  }

  /**
   * The only way to store metadata for an index in Elasticsearch is to create a mapping
   * for "_meta". We do this to store the version of CDAP that creates the index.
   */
  public VersionInfo getVersionInfo() throws IOException {
    GetMappingsRequest request = new GetMappingsRequest().indices(indexName);
    GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);
    MappingMetaData mapping = response.getMappings().get(indexName).get(DOC_TYPE);
    // Elastic allows arbitrary structure as the mapping. In our index.mappings.json, it is
    // a map from String to String. That is why it expected to be safe to cast here.
    @SuppressWarnings("unchecked")
    Map<String, String> meta = (Map<String, String>) mapping.getSourceAsMap().get("_meta");
    LOG.trace("Index mapping for _meta: {}", meta);
    try {
      return new VersionInfo(new ProjectInfo.Version(meta.get(Config.MAPPING_CDAP_VERSION)),
                             Integer.parseInt(meta.get(Config.MAPPING_METADATA_VERSION)),
                             Long.parseLong(meta.get(Config.MAPPING_MAPPING_CHECKSUM)));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
        "Unable to extract version info from index mappings: " + mapping.getSourceAsMap(), e);
    }
  }

  private String createMappings() throws IOException {
    @SuppressWarnings("ConstantConditions")
    URL url = getClass().getClassLoader().getResource(MAPPING_RESOURCE);
    if (url == null) {
      throw new IllegalStateException("Index mapping file '" + MAPPING_RESOURCE + "'not found in classpath");
    }
    // read the mappings file, replace the CDAP_VERSION placeholder with current version
    String mappings = Resources.toString(url, Charsets.UTF_8);
    @SuppressWarnings("ConstantConditions")
    long checksum = Checksums.fingerprint64(Bytes.toBytes(mappings));
    return mappings
      .replace(Config.MAPPING_CDAP_VERSION_PLACEHOLDER, ProjectInfo.getVersion().toString())
      .replace(Config.MAPPING_METADATA_VERSION_PLACEHOLDER, Integer.toString(VersionInfo.METADATA_VERSION))
      .replace(Config.MAPPING_MAPPING_CHECKSUM_PLACEHOLDER, Long.toString(checksum));
  }

  // Elasticsearch index settings, used when creating the index
  private String createSettings() {
    String numShards = cConf.get(Config.CONF_ELASTIC_NUM_SHARDS);
    String numReplicas = cConf.get(Config.CONF_ELASTIC_NUM_REPLICAS);
    String maxResultWindow = cConf.get(Config.CONF_ELASTIC_WINDOW_SIZE);
    Map<String, Object> indexSettings = new HashMap<>();
    if (numShards != null) {
      indexSettings.put(SETTING_NUMBER_OF_SHARDS, Integer.parseInt(numShards));
    }
    if (numReplicas != null) {
      indexSettings.put(SETTING_NUMBER_OF_REPLICAS, Integer.parseInt(numReplicas));
    }
    if (maxResultWindow != null) {
      indexSettings.put(SETTING_MAX_RESULT_WINDOW, Integer.parseInt(maxResultWindow));
    }
    return ("{" +
      (indexSettings.isEmpty() ? "" : "'index': " + GSON.toJson(indexSettings) + ",") +
      "  'analysis': {" +
      "    'analyzer': {" +
      "      'text_analyzer': {" +
      "        'type': 'pattern'," +
      "        'pattern': '[-_,;.\\\\s]+'," + // this reflects the tokenization performed by MetadataDataset
      "        'lowercase': true" +
      "      }" +
      "    }" +
      "  }" +
      "}").replace('\'', '"');
  }

  @Override
  public void dropIndex() throws IOException {
    synchronized (this) {
      GetIndexRequest request = new GetIndexRequest();
      request.indices(indexName);
      if (!client.indices().exists(request, RequestOptions.DEFAULT)) {
        created = false;
        return;
      }
      DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
      AcknowledgedResponse response = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
      if (!response.isAcknowledged()) {
        LOG.warn("Delete index request was not acknowledged by ElasticSearch: {}", response);
      }
      created = false;
    }
  }

  @Override
  public MetadataChange apply(MetadataMutation mutation, MutationOptions options) throws IOException {
    MetadataEntity entity = mutation.getEntity();
    try {
      // repeatedly try to read current metadata, apply the mutation and reindex, until there is no conflict
      return Retries.callWithRetries(
        () -> {
          VersionedMetadata before = readFromIndex(entity);
          RequestAndChange intermediary = applyMutation(before, mutation);
          executeMutation(mutation.getEntity(), intermediary.getRequest(), options);
          return intermediary.getChange();
        },
        retryStrategyOnConflict,
        e -> e instanceof MetadataConflictException);
    } catch (MetadataConflictException e) {
      throw new MetadataConflictException("After retries: " + e.getRawMessage(), e.getConflictingEntities());
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
    // first detect whether there are duplicate entity ids. If so, execute in sequence
    Set<MetadataEntity> entities = new HashSet<>();
    LinkedHashMap<MetadataEntity, MetadataMutation> mutationMap = new LinkedHashMap<>(mutations.size());
    boolean duplicate = false;
    for (MetadataMutation mutation : mutations) {
      if (!entities.add(mutation.getEntity())) {
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
      throw new MetadataConflictException("After retries: " + e.getRawMessage(), e.getConflictingEntities());
    }
  }

  /**
   * For the given mutations, reads the current metadata for the involved entities, applies the mutations,
   * and attempts to execute the resulting index operations in bulk. When finished, returns the list of
   * metadata changes caused by the mutations.
   *
   * If a conflict occurs during any of these operations, the successful mutations are removed from the
   * mutations map, and the changes map is updated with the corresponding change, before the
   * {@link MetadataConflictException} is thrown. That is, calling this repeatedly will gradually
   * shrink the mutations map to an empty map and fill the changes map with changes performed.
   *
   * @param mutations the mutations to apply. Every mutation that is successfully executed is removed
   *                  from this map, even if an exception is thrown.
   * @param changes the changes caused by the mutations. For every mutation that is successfully executed,
   *                this map is updated with the corresponding change, even if an exception is thrown.
   *
   * @return the list of all changes performed by the mutations, if all mutations complete successfully
   *
   * @throws MetadataConflictException if a conflict occurs for any of the mutations
   * @throws IOException for any other problem encountered
   */
  private List<MetadataChange> doBatch(LinkedHashMap<MetadataEntity, MetadataMutation> mutations,
                                       LinkedHashMap<MetadataEntity, MetadataChange> changes,
                                       MutationOptions options)
    throws IOException {
    MultiGetRequest multiGet = new MultiGetRequest();
    for (Map.Entry<MetadataEntity, MetadataMutation> entry : mutations.entrySet()) {
      multiGet.add(indexName, DOC_TYPE, toDocumentId(entry.getKey()));
    }
    MultiGetResponse multiGetResponse = client.mget(multiGet, RequestOptions.DEFAULT);
    // responses are in the same order as the original requests
    int index = 0;
    BulkRequest bulkRequest = new BulkRequest();
    for (Map.Entry<MetadataEntity, MetadataMutation> entry : mutations.entrySet()) {
      MultiGetItemResponse itemResponse = multiGetResponse.getResponses()[index++];
      if (itemResponse.isFailed()) {
        throw new IOException("Failed to read from index for entity " + entry.getKey());
      }
      GetResponse getResponse = itemResponse.getResponse();
      VersionedMetadata before = getResponse.isExists()
        ? VersionedMetadata.of(GSON.fromJson(getResponse.getSourceAsString(), MetadataDocument.class).getMetadata(),
                               getResponse.getVersion())
        : VersionedMetadata.NONE;
      RequestAndChange intermediary = applyMutation(before, entry.getValue());
      bulkRequest.add((DocWriteRequest) intermediary.getRequest());
      changes.put(entry.getKey(), intermediary.getChange());
    }
    setRefreshPolicy(bulkRequest, options);
    executeBulk(bulkRequest, mutations);
    return changes.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());
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
   * Creates an ElasticSearch request that corresponds to the given mutation,
   * along with the change effected by this mutation. The request must be
   * executed by the caller.
   *
   * @param before the metadata for the mutation's entity before the change
   * @param mutation the mutation to apply
   *
   * @return an ElasticSearch request to be executed, and the change caused by the mutation.
   */
  private RequestAndChange applyMutation(VersionedMetadata before,
                                         MetadataMutation mutation) {
    LOG.trace("Applying mutation {} to entity {} with metadata", mutation, mutation.getEntity(), before.getMetadata());
    switch (mutation.getType()) {
      case CREATE:
        return create(before, (MetadataMutation.Create) mutation);
      case DROP:
        return drop(mutation.getEntity(), before);
      case UPDATE:
        return update(mutation.getEntity(), before, ((MetadataMutation.Update) mutation).getUpdates());
      case REMOVE:
        return remove(before, (MetadataMutation.Remove) mutation);
      default:
        throw new IllegalStateException(
          String.format("Unknown mutation type '%s' for %s", mutation.getType(), mutation));
    }
  }

  /**
   * Creates the Elasticsearch index request for an entity creation or update.
   * See {@link MetadataMutation.Create} for detailed semantics.
   *
   * @param before the metadata for the mutation's entity before the change
   * @param create the mutation to apply
   *
   * @return an ElasticSearch request to be executed, and the change caused by the mutation
   */
  private RequestAndChange create(VersionedMetadata before, MetadataMutation.Create create) {
    // if the entity did not exist before, none of the directives apply and this is equivalent to update()
    if (!before.existing()) {
      return update(create.getEntity(), before, create.getMetadata());
    }
    Metadata meta = create.getMetadata();
    Map<ScopedNameOfKind, MetadataDirective> directives = create.getDirectives();
    // determine the scopes that this mutation applies to (scopes that do not occur in the metadata are no changed)
    Set<MetadataScope> scopes = Stream.concat(meta.getTags().stream(), meta.getProperties().keySet().stream())
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
          && (entry.getValue() == MetadataDirective.PRESERVE || entry.getValue() == MetadataDirective.KEEP)) {
          ScopedName tag = new ScopedName(key.getScope(), key.getName());
          if (!meta.getTags().contains(tag) && before.getMetadata().getTags().contains(tag)) {
            existingTagsToKeep.add(tag);
          }
        } else if (key.getKind() == MetadataKind.PROPERTY) {
          ScopedName property = new ScopedName(key.getScope(), key.getName());
          String existingValue = before.getMetadata().getProperties().get(property);
          String newValue = meta.getProperties().get(property);
          if (existingValue != null
            && (entry.getValue() == MetadataDirective.PRESERVE && !existingValue.equals(newValue)
            || entry.getValue() == MetadataDirective.KEEP && newValue == null)) {
            existingPropertiesToKeep.put(property, existingValue);
          }
        }
      });
    // compute the new tags and properties
    Set<ScopedName> newTags =
      existingTagsToKeep.isEmpty() ? meta.getTags() : Sets.union(meta.getTags(), existingTagsToKeep);
    Map<ScopedName, String> newProperties = meta.getProperties();
    if (!existingPropertiesToKeep.isEmpty()) {
      newProperties = new HashMap<>(newProperties);
      newProperties.putAll(existingPropertiesToKeep);
    }
    Metadata after = new Metadata(newTags, newProperties);
    return new RequestAndChange(writeToIndex(create.getEntity(), before.getVersion(), after),
                                new MetadataChange(create.getEntity(), before.getMetadata(), after));
  }

  /**
   * Creates the Elasticsearch delete request for an entity deletion.
   * This drops the corresponding metadata document from the index.
   *
   * @param before the metadata for the mutation's entity before the change
   *
   * @return an ElasticSearch request to be executed, and the change caused by the mutation
   */
  private RequestAndChange drop(MetadataEntity entity, VersionedMetadata before) {
    return new RequestAndChange(deleteFromIndex(entity, before.getVersion()),
                                new MetadataChange(entity, before.getMetadata(), Metadata.EMPTY));
  }

  /**
   * Creates the Elasticsearch index request for updating the metadata of an entity.
   * This updates or adds the new metadata to the corresponding metadata document in the index.
   *
   * @param before the metadata for the mutation's entity before the change, and its version
   *
   * @return an ElasticSearch request to be executed, and the change caused by the mutation
   */
  private RequestAndChange update(MetadataEntity entity,
                                  VersionedMetadata before,
                                  Metadata updates) {
    Set<ScopedName> tags = new HashSet<>(before.getMetadata().getTags());
    tags.addAll(updates.getTags());
    Map<ScopedName, String> properties = new HashMap<>(before.getMetadata().getProperties());
    properties.putAll(updates.getProperties());
    Metadata after = new Metadata(tags, properties);
    return new RequestAndChange(writeToIndex(entity, before.getVersion(), after),
                                new MetadataChange(entity, before.getMetadata(), after));
  }

  /**
   * Creates the Elasticsearch index request for removing some metadata from an entity.
   * This removed the specified metadata from the corresponding metadata document in the index.
   *
   * Note that even if all tags and properties are removed, the document will remain in the index
   * and it still searchable by its type, name, or a match-all query. Use {@link MetadataMutation.Drop}
   * to completely remove the entity from the index.
   *
   * @param before the metadata for the mutation's entity before the change
   *
   * @return an ElasticSearch request to be executed, and the change caused by the mutation
   */
  private RequestAndChange remove(VersionedMetadata before, MetadataMutation.Remove remove) {
    Metadata after = filterMetadata(before.getMetadata(), DISCARD,
                                    remove.getKinds(), remove.getScopes(), remove.getRemovals());
    return new RequestAndChange(writeToIndex(remove.getEntity(), before.getVersion(), after),
                                new MetadataChange(remove.getEntity(), before.getMetadata(), after));
  }

  /**
   * Reads the existing metadata for an entity from the index.
   *
   * @return existing metadata along with its version in the index, or an empty metadata with null version.
   */
  private VersionedMetadata readFromIndex(MetadataEntity entity)
    throws IOException {
    String id = toDocumentId(entity);
    try {
      GetRequest getRequest = new GetRequest(indexName).type(DOC_TYPE).id(id);
      GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
      if (!response.isExists()) {
        return VersionedMetadata.NONE;
      }
      Metadata metadata = GSON.fromJson(response.getSourceAsString(), MetadataDocument.class).getMetadata();
      return VersionedMetadata.of(metadata, response.getVersion());
    } catch (Exception e) {
      throw new IOException("Failed to read from index for entity " + entity);
    }
  }

  /**
   * Create an Elasticsearch index request for adding or updating the metadata for an entity in the index.
   * The request must be executed by the caller.
   */
  private IndexRequest writeToIndex(MetadataEntity entity, Long expectVersion, Metadata metadata) {
    MetadataDocument doc = MetadataDocument.of(entity, metadata);
    LOG.trace("Indexing document: {}", doc);
    IndexRequest request = new IndexRequest(indexName)
      .type(DOC_TYPE)
      .id(toDocumentId(entity))
      .source(GSON.toJson(doc), XContentType.JSON);
    if (expectVersion == null) {
      request.opType("create");
    } else {
      request.version(expectVersion);
    }
    return request;
  }

  /**
   * Create an Elasticsearch delete request for removing an entity in the index.
   * The request must be executed by the caller.
   */
  private DeleteRequest deleteFromIndex(MetadataEntity entity, Long existingVersion) {
    String id = toDocumentId(entity);
    LOG.trace("Deleting document with id: {}", id);
    DeleteRequest deleteRequest = new DeleteRequest(indexName).type(DOC_TYPE).id(id);
    if (existingVersion != null) {
      deleteRequest.version(existingVersion);
    }
    return deleteRequest;
  }

  /**
   * Executes an ElasticSearch request to modify a document (index or delete), and handles possible failure.
   *
   * @throws MetadataConflictException if a conflict occurs
   * @throws IOException for any other problem encountered
   */
  private void executeMutation(MetadataEntity entity, WriteRequest<?> writeRequest,
                               MutationOptions options) throws IOException {
    String requestType = null;
    DocWriteResponse response;
    setRefreshPolicy(writeRequest, options);
    try {
      if (writeRequest instanceof DeleteRequest) {
        requestType = "Delete";
        response = client.delete((DeleteRequest) writeRequest, RequestOptions.DEFAULT);
      } else if (writeRequest instanceof IndexRequest) {
        requestType = "Index";
        response = client.index((IndexRequest) writeRequest, RequestOptions.DEFAULT);
      } else {
        throw new IllegalStateException("Unexpected WriteRequest of type " + writeRequest.getClass().getName());
      }
    } catch (ElasticsearchStatusException e) {
      if (isConflict(e.status())) {
        LOG.debug("Encountered conflict in {} request for entity {}", requestType, entity);
        throw new MetadataConflictException(requestType + " conflict for ${conflicting}", entity);
      }
      throw e;
    }
    if (isNotFound(response.status())) {
      // ignore entities that do not exist - only happens for deletes
      return;
    }
    if (isConflict(response.status())) {
      LOG.debug("Encountered conflict in {} request for entity {}", requestType, entity);
      throw new MetadataConflictException(requestType + " conflict for ${conflicting}", entity);
    }
    if (isFailure(response)) {
      throw new IOException(String.format(requestType + " request unsuccessful for entity %s: %s", entity, response));
    }
  }

  /**
   * Executes a bulk request and handles the responses for possible failures, and removes all successful
   * mutations from the mutations map.
   *
   * @param mutations the mutations represented by this bulk. Every mutation that is successfully executed
   *                  is removed from this map, even if an exception is thrown.
   *
   * @throws MetadataConflictException if a conflict occurs for any of the operations in the bulk
   * @throws IOException for any other problem encountered
   */
  private void executeBulk(BulkRequest bulkRequest, LinkedHashMap<MetadataEntity, MetadataMutation> mutations)
    throws IOException {
    BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
    if (response.hasFailures()) {
      IOException ioe = null;
      List<MetadataEntity> conflictEntities = new ArrayList<>();
      for (BulkItemResponse itemResponse : response) {
        MetadataEntity entityId;
        try {
          entityId = toMetadataEntity(itemResponse.getId());
        } catch (Exception e) {
          LOG.warn("Cannot parse entity id from document id {} in bulk response", itemResponse.getId());
          continue;
        }
        if (!itemResponse.isFailed()) {
          // remove this mutation - it was successful
          mutations.remove(entityId);
          continue;
        }
        if (isNotFound(itemResponse.status())) {
          // only happens for deletes - we consider this successful
          mutations.remove(entityId);
          continue;
        }
        // this mutation failed
        BulkItemResponse.Failure failure = itemResponse.getFailure();
        if (isConflict(failure.getStatus())) {
          conflictEntities.add(entityId);
          continue;
        }
        // not a conflict -> true failure
        if (ioe == null) {
          ioe = new IOException("Bulk request unsuccessful");
        }
        ioe.addSuppressed(new IOException(String.format("%s request unsuccessful for entity %s: %s",
                                                        itemResponse.getOpType(), entityId, failure.getMessage())));
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
  }

  /**
   * Sets the refresh policy for a write request.
   * If {@link MutationOptions#isAsynchronous()} is true, write requests will return
   * immediately after the request is acknowledged; otherwise,
   * they will only return after they are confirmed to be applied to the index.
   */
  private void setRefreshPolicy(WriteRequest<?> request, MutationOptions options) {
    request.setRefreshPolicy(
      options.isAsynchronous() ? WriteRequest.RefreshPolicy.IMMEDIATE : WriteRequest.RefreshPolicy.WAIT_UNTIL);
  }

  /**
   * Perform a search that continues a previous search using a cursor. Such a search
   * is always relative to the offset of the cursor (with no additional offset allowed)
   * and uses the the same page size as the original search.
   *
   * If the cursor provided in the request had expired, this performs a new search,
   * beginning at the offset given by the cursor.
   *
   * @return the search response containing the next page of results.
   */
  private io.cdap.cdap.spi.metadata.SearchResponse doScroll(SearchRequest request)
    throws IOException {

    Cursor cursor = Cursor.fromString(request.getCursor());
    SearchScrollRequest scrollRequest = new SearchScrollRequest(cursor.getActualCursor());
    if (request.isCursorRequested()) {
      scrollRequest.scroll(scrollTimeout);
    }
    SearchResponse searchResponse;
    try {
      searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
    } catch (ElasticsearchStatusException e) {
      // scroll invalid or timed out
      return doSearch(createRequestFromCursor(request, cursor));
    }
    if (searchResponse.isTimedOut()) {
      // scroll had expired, we have to search again
      return doSearch(createRequestFromCursor(request, cursor));
    }
    SearchHits hits = searchResponse.getHits();
    List<MetadataRecord> results = fromHits(hits);
    String newCursor = computeCursor(searchResponse, cursor);
    return new io.cdap.cdap.spi.metadata.SearchResponse(request, newCursor, cursor.getOffset(), cursor.getLimit(),
                                                        (int) hits.getTotalHits(), results);
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

    org.elasticsearch.action.search.SearchRequest searchRequest =
      new org.elasticsearch.action.search.SearchRequest(indexName);
    SearchSourceBuilder searchSource = createSearchSource(request);
    searchRequest.source(searchSource);
    // only request a scroll if the offset is 0. Elastic will throw otherwise
    if (request.isCursorRequested() && searchSource.from() == 0) {
      searchRequest.scroll(scrollTimeout);
    }
    LOG.debug("Executing search request {}", searchRequest);
    SearchResponse searchResponse =
      client.search(searchRequest, RequestOptions.DEFAULT);
    SearchHits hits = searchResponse.getHits();
    List<MetadataRecord> results = fromHits(hits);
    String newCursor = computeCursor(searchResponse, request);
    return new io.cdap.cdap.spi.metadata.SearchResponse(request, newCursor, request.getOffset(), request.getLimit(),
                                                        (int) hits.getTotalHits(), results);
  }

  /**
   * Generates the cursor to return as part of a search response:
   * <ul><li>
   *   If the Elasticsearch response does not have a scroll id, returns null;
   * </li><li>
   *   If the number of results in this response exceeds the total number of results,
   *   cancels the scroll returned by Elasticsearch, then returns null.
   * </li><li>
   *   Otherwise returns a cursor representing the Elasticsearch scroll id,
   *   offset and page size for a subsequent search.
   * </li></ul>
   */
  @Nullable
  private String computeCursor(SearchResponse searchResponse, Cursor cursor) {
    if (searchResponse.getScrollId() != null) {
      SearchHits hits = searchResponse.getHits();
      int newOffset = cursor.getOffset() + hits.getHits().length;
      if (newOffset < hits.getTotalHits()) {
        return new Cursor(cursor, newOffset, searchResponse.getScrollId()).toString();
      }
      // we have seen all results, there are no more to fetch: clear the scroll
      cancelSroll(searchResponse.getScrollId());
    }
    return null;
  }

  @Nullable
  private String computeCursor(SearchResponse searchResponse, SearchRequest request) {
    if (searchResponse.getScrollId() != null) {
      SearchHits hits = searchResponse.getHits();
      int newOffset = request.getOffset() + hits.getHits().length;
      if (newOffset < hits.getTotalHits()) {
        return new Cursor(newOffset, request.getLimit(), request.isShowHidden(), request.getScope(),
                          request.getNamespaces(), request.getTypes(),
                          request.getSorting() == null ? null : request.getSorting().toString(),
                          searchResponse.getScrollId(), request.getQuery()).toString();
      }
      // we have seen all results, there are no more to fetch: clear the scroll
      cancelSroll(searchResponse.getScrollId());
    }
    return null;
  }

  private void cancelSroll(String scrollId) {
    ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
    clearScrollRequest.addScrollId(scrollId);
    // clear the scroll request asynchronously. We don't really care about the response
    client.clearScrollAsync(clearScrollRequest, RequestOptions.DEFAULT, ActionListener.wrap(x -> { }, x -> { }));
  }

  private SearchSourceBuilder createSearchSource(SearchRequest request) {
    int offset = request.getOffset();
    int limit = request.getLimit();
    // clients cannot know what the index's max window size is, but any request where offset + limit
    // exceeds that size will fail in ES. Hence we need to adjust the requested number of results to
    // not exceed that window size.
    if (maxWindowSize < offset) {
      throw new IllegalArgumentException(String.format("Offset %d is greater than the index's '%s' settings of %d",
                                                       offset, SETTING_MAX_RESULT_WINDOW, maxWindowSize));
    }
    limit = Integer.min(maxWindowSize - offset, limit);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.from(offset);
    searchSourceBuilder.size(limit);
    if (request.getSorting() != null) {
      searchSourceBuilder.sort(mapSortKey(request.getSorting().getKey().toLowerCase()),
                               SortOrder.valueOf(request.getSorting().getOrder().name()));
    }
    searchSourceBuilder.query(createQuery(request));
    return searchSourceBuilder;
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
   * Creates an Elasticsearch query from a search request. In essence, this returns
   *
   * <pre>
   *   (field1:term1 OR ... OR NESTED(props):name/scope/term ...)
   * [ AND (namespace:ns1 OR ... OR namespace:nsK) ]
   * [ AND (type:type1 OR ... OR type:typeM) ]
   * [ AND hidden:false ]
   * </pre>
   *
   * The field for each field:term is as selected by the request's scope (user, system, or text).
   * The NESTED subqueries expect the term to occur in a property with the name field as name,
   * and optionally the scope of the search request as its scope.
   *
   * See {@link MetadataDocument} for details about the index mapping.
   */
  private QueryBuilder createQuery(SearchRequest request) {
    // first create a query from the query terms
    QueryBuilder mainQuery = createMainQuery(request);

    List<QueryBuilder> conditions = new ArrayList<>();
    // if the request asks only for a subset of entity types, add a boolean clause for that
    if (request.getTypes() != null && !request.getTypes().isEmpty()) {
      addCondition(conditions, request.getTypes().stream().map(
        type -> QueryBuilders.termQuery(TYPE_FIELD, type.toLowerCase()).boost(0.0F)).collect(Collectors.toList()));
    }
    if (request.getNamespaces() != null) {
      addCondition(conditions, request.getNamespaces().stream().map(
        ns -> QueryBuilders.termQuery(NAMESPACE_FIELD, ns.toLowerCase()).boost(0.0F)).collect(Collectors.toList()));
    }
    if (!request.isShowHidden()) {
      conditions.add(new TermQueryBuilder(HIDDEN_FIELD, false).boost((0.0F)));
    }
    if (!conditions.isEmpty()) {
      BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
      conditions.forEach(boolQuery::must);
      boolQuery.must(mainQuery);
      mainQuery = boolQuery;
    }
    return mainQuery;
  }

  /**
   * For a list of alternative sub-queries, create a query that requires at least one of them to
   * be fulfilled, without contributing to the query scoring, and add it to a list of queries.
   *
   * Depending on whether the number of sub-queries is 1, adds the single query or a boolean
   * OR over all sub-queries.
   */
  private void addCondition(List<QueryBuilder> conditions, List<TermQueryBuilder> subQueries) {
    if (subQueries.size() == 1) {
      conditions.add(subQueries.get(0));
      return;
    }
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    subQueries.forEach(boolQuery::should);
    boolQuery.minimumShouldMatch(1);
    conditions.add(boolQuery);
  }

  /**
   * Creates the (sub-)query for Elasticsearch from the terms in the query string.
   */
  private QueryBuilder createMainQuery(SearchRequest request) {
    if (request.getQuery().equals("*")) {
      return QueryBuilders.matchAllQuery();
    }
    // the indexed document contains three text fields: one for each scope and for all scopes combined.
    // all terms must occur in the text field as selected by the scope in the search request.
    String textField = request.getScope() == null ? TEXT_FIELD : request.getScope().name().toLowerCase();

    // split the query into its parsed terms and create corresponding QueryBuilders for each
    List<QueryTerm> queryTerms = QueryParser.parse(request.getQuery());
    if (queryTerms.isEmpty()) {
      return QueryBuilders.matchAllQuery();
    }
    if (queryTerms.size() == 1) {
      return createTermQuery(queryTerms.get(0).getTerm(), textField, request);
    }

    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    for (QueryTerm queryTerm : queryTerms) {
      QueryBuilder builder = createTermQuery(queryTerm.getTerm(), textField, request);
      if (queryTerm.getQualifier() == Qualifier.REQUIRED) {
        boolQuery.must(builder);
      } else {
        boolQuery.should(builder);
      }
    }

    // if optional terms are the only terms available, require at least one optional term
    if (boolQuery.must().isEmpty() && !boolQuery.should().isEmpty()) {
      boolQuery.minimumShouldMatch(1);
    }
    return boolQuery;
  }

  /**
   * Create a sub-query for a single term in the query string.
   *
   * @param term the term as it appears in the query, possibly with a field qualifier
   * @param textField the default text field to search if the term does not have a field
   */
  private QueryBuilder createTermQuery(String term, String textField, SearchRequest request) {
    // determine if the term has a field qualifier
    String field = null;
    term = term.trim().toLowerCase();
    // Create a term query on the term as is. This would include a field: prefix if the term hs one.
    // This is important for the case of schema search: If the schema contains a field f of type t,
    // then we index "f:t" in the plain text as well as in the "schema" property. If the query is
    // just "f:t", we must search the plain text field for that.
    QueryBuilder plainQuery = createTermQuery(textField, term);
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
        // since TTL is indexed as a long, Elasticsearch can handle any numeric value.
        // But it would fail on "*" or "?". Hence only create a term query for TTL if
        // we can parse this as a double (that covers pretty much all numeric formats)
        Double.parseDouble(term);
        return QueryBuilders.termQuery(field, term);
      } catch (NumberFormatException e) {
        // ignore - the follow-on code will create a regular term query for this
      }
    }
    BoolQueryBuilder boolQuery = new BoolQueryBuilder();
    boolQuery.must(new TermQueryBuilder(NESTED_NAME_FIELD, field).boost(0.0F));
    if (request.getScope() != null) {
      boolQuery.must(new TermQueryBuilder(NESTED_SCOPE_FIELD, request.getScope().name()).boost(0.0F));
    }
    boolQuery.must(createTermQuery(NESTED_VALUE_FIELD, term));
    QueryBuilder propertyQuery = QueryBuilders.nestedQuery(PROPS_FIELD, boolQuery, ScoreMode.Max);

    // match either a plain term of the form "f:t" or the word "t" in property "f"
    return new BoolQueryBuilder().should(plainQuery).should(propertyQuery).minimumShouldMatch(1);
  }

  /**
   * Create a query for a single term in a given field.
   *
   * @return a wildcard query is the term contains * or ?, or a match query otherwise
   */
  private QueryBuilder createTermQuery(String field, String term) {
    return term.contains("*") || term.contains("?")
      ? new WildcardQueryBuilder(field, term)
      // the term should not get split in to multiple words, but in case it does, let's require all words
      : new MatchQueryBuilder(field, term).operator(Operator.AND);
  }

  /**
   * Transforms a set of Elasticsearch search hits into metadata records.
   */
  private static List<MetadataRecord> fromHits(SearchHits hits) {
    return Arrays.stream(hits.getHits())
      .map((SearchHit hit) -> new MetadataRecord(
        toMetadataEntity(hit.getId()),
        GSON.fromJson(hit.getSourceAsString(), MetadataDocument.class).getMetadata()))
      .collect(Collectors.toList());
  }

  /**
   * Filter the metadata based on the given scopes, kinds, and selection.
   * Based on the value of {@param keep}, this can be used to keep or to
   * discard the matching tags and properties.
   *
   * @param keep if true, only matching metadata elements are kept; otherwise only non-matching elements are kept.
   */
  @VisibleForTesting
  @SuppressWarnings("ConstantConditions")
  static Metadata filterMetadata(Metadata metadata, boolean keep, Set<MetadataKind> kinds,
                                 Set<MetadataScope> scopes, Set<ScopedNameOfKind> selection) {
    if (selection != null) {
      return new Metadata(
        Sets.filter(metadata.getTags(), tag ->
          keep == selection.contains(new ScopedNameOfKind(MetadataKind.TAG, tag.getScope(), tag.getName()))),
        Maps.filterKeys(metadata.getProperties(), key ->
          keep == selection.contains(new ScopedNameOfKind(MetadataKind.PROPERTY, key.getScope(), key.getName())))
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
      if (MetadataUtil.isVersionedEntityType(entity.getType()) &&
        MetadataEntity.VERSION.equalsIgnoreCase(kv.getKey())) {
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
      throw new IllegalArgumentException("Document Id must be of the form 'type:k=v,...' but is " + documentId);
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

  private static boolean isFailure(DocWriteResponse response) {
    int httpStatus = response.status().getStatus();
    return httpStatus != 200 && httpStatus != 201;
  }

  private static boolean isNotFound(RestStatus status) {
    return status.getStatus() == 404;
  }

  private static boolean isConflict(RestStatus status) {
    return status.getStatus() == 409;
  }

}
