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

package co.cask.cdap.metadata.elastic;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.metadata.MetadataConstants;
import co.cask.cdap.spi.metadata.Metadata;
import co.cask.cdap.spi.metadata.MetadataChange;
import co.cask.cdap.spi.metadata.MetadataDirective;
import co.cask.cdap.spi.metadata.MetadataKind;
import co.cask.cdap.spi.metadata.MetadataMutation;
import co.cask.cdap.spi.metadata.MetadataRecord;
import co.cask.cdap.spi.metadata.MetadataStorage;
import co.cask.cdap.spi.metadata.Read;
import co.cask.cdap.spi.metadata.ScopedName;
import co.cask.cdap.spi.metadata.ScopedNameOfKind;
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
import com.google.inject.Inject;
import org.apache.http.HttpHost;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * A metadata storage provider that delegates to Elasticsearch.
 */
public class ElasticsearchMetadataStorage implements MetadataStorage {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchMetadataStorage.class);

  // Elasticsearch-specific settings
  @VisibleForTesting
  static final String CONF_ELASTIC_HOSTS = "metadata.elasticsearch.cluster.hosts";
  @VisibleForTesting
  static final String CONF_ELASTIC_INDEX_NAME = "metadata.elasticsearch.index.name";
  @VisibleForTesting
  static final String CONF_ELASTIC_SCROLL_TIMEOUT = "metadata.elasticsearch.scroll.timeout";
  @VisibleForTesting
  static final String CONF_ELASTIC_WAIT_FOR_MUTATIONS = "metadata.elasticsearch.wait.for.mutations";
  private static final String CONF_ELASTIC_NUM_SHARDS = "metadata.elasticsearch.num.shards";
  private static final String CONF_ELASTIC_NUM_REPLICAS = "metadata.elasticsearch.num.replicas";

  private static final String DEFAULT_ELASTIC_HOSTS = "localhost:9200";
  private static final String DEFAULT_INDEX_NAME = "cdap.metadata";
  private static final String DEFAULT_SCROLL_TIMEOUT = "60s";
  private static final int DEFAULT_NUM_SHARDS = 1;
  private static final int DEFAULT_NUM_REPLICAS = 1;
  private static final boolean DEFAULT_WAIT_FOR_MUTATIONS = false;

  private static final String MAPPING_RESOURCE = "index.mapping.json";

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

  // various fields in a metadata document (indexed in elasticsearch)
  private static final String DOC_TYPE = "meta";
  private static final String CREATED_FIELD = "name"; // contains the creation time
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
  private final WriteRequest.RefreshPolicy refreshPolicy;

  private volatile boolean created = false;

  @Inject
  public ElasticsearchMetadataStorage(CConfiguration cConf) {
    this.cConf = cConf;
    indexName = cConf.get(CONF_ELASTIC_INDEX_NAME, DEFAULT_INDEX_NAME);
    scrollTimeout = cConf.get(CONF_ELASTIC_SCROLL_TIMEOUT, DEFAULT_SCROLL_TIMEOUT);
    refreshPolicy = cConf.getBoolean(CONF_ELASTIC_WAIT_FOR_MUTATIONS, DEFAULT_WAIT_FOR_MUTATIONS)
      ? WriteRequest.RefreshPolicy.WAIT_UNTIL : WriteRequest.RefreshPolicy.IMMEDIATE;
    String elasticHosts = cConf.get(CONF_ELASTIC_HOSTS, DEFAULT_ELASTIC_HOSTS);
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

  private void ensureIndexCreated() throws IOException {
    if (created) {
      return;
    }
    synchronized (this) {
      if (created) {
        return;
      }
      GetIndexRequest request = new GetIndexRequest();
      request.indices(indexName);
      if (client.indices().exists(request, RequestOptions.DEFAULT)) {
        created = true;
        return;
      }
      LOG.info("Creating index " + indexName);
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
      createIndexRequest.settings(createSettings(), XContentType.JSON);
      createIndexRequest.mapping(DOC_TYPE, getMapping(), XContentType.JSON);
      CreateIndexResponse response = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
      if (!response.isAcknowledged()) {
        throw new IOException("Create index request was not acknowledged by EleasticSearch: " + response);
      }
      created = true;
    }
  }

  private String getMapping() throws IOException {
    @SuppressWarnings("ConstantConditions")
    URL url = getClass().getClassLoader().getResource(MAPPING_RESOURCE);
    if (url == null) {
      throw new IllegalStateException("Index mapping file '" + MAPPING_RESOURCE + "'not found in classpath");
    }
    return Resources.toString(url, Charsets.UTF_8);
  }

  // Elasticsearch index settings, used when creating the index
  private String createSettings() {
    int numShards = cConf.getInt(CONF_ELASTIC_NUM_SHARDS, DEFAULT_NUM_SHARDS);
    int numReplicas = cConf.getInt(CONF_ELASTIC_NUM_REPLICAS, DEFAULT_NUM_REPLICAS);
    return ("{" +
      "  'index': {" +
      "    'number_of_shards': " + numShards + "," +
      "    'number_of_replicas': " + numReplicas +
      "  }," +
      "  'analysis': {" +
      "    'analyzer': {" +
      "      'text_analyzer': {" +
      "        'type': 'pattern'," +
      "        'pattern': '\\\\W|_'," + // this reflects the tokenization performed by MetadataDataset
      "        'lowercase': true" +
      "      }" +
      "    }" +
      "  }" +
      "}").replace('\'', '"');
  }

  @VisibleForTesting
  void deleteIndex() throws IOException {
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
  public MetadataChange apply(MetadataMutation mutation) throws IOException {
    MetadataEntity entity = mutation.getEntity();
    Metadata before = readFromIndex(entity);
    Tuple<? extends DocWriteRequest, MetadataChange> intermediary = applyMutation(before, mutation);
    executeMutation(mutation.getEntity(), intermediary.v1());
    return intermediary.v2();
  }

  @Override
  public List<MetadataChange> batch(List<? extends MetadataMutation> mutations) throws IOException {
    MultiGetRequest multiGet = new MultiGetRequest();
    for (MetadataMutation mutation : mutations) {
      multiGet.add(indexName, DOC_TYPE, toDocumentId(mutation.getEntity()));
    }
    MultiGetResponse multiGetResponse = client.mget(multiGet, RequestOptions.DEFAULT);
    // responses are in the same order as the original requests
    int index = 0;
    List<MetadataChange> changes = new ArrayList<>(mutations.size());
    BulkRequest bulkRequest = new BulkRequest();
    for (MetadataMutation mutation : mutations) {
      MultiGetItemResponse itemResponse = multiGetResponse.getResponses()[index++];
      if (itemResponse.isFailed()) {
        throw new IOException("Failed to read from index for entity " + mutation.getEntity());
      }
      GetResponse getResponse = itemResponse.getResponse();
      Metadata before = getResponse.isExists()
        ? GSON.fromJson(getResponse.getSourceAsString(), MetadataDocument.class).getMetadata() : Metadata.EMPTY;
      Tuple<? extends DocWriteRequest, MetadataChange> intermediary = applyMutation(before, mutation);
      bulkRequest.add(intermediary.v1());
      changes.add(intermediary.v2());
    }
    setRefreshPolicy(bulkRequest);
    executeBulk(bulkRequest);
    return changes;
  }

  @Override
  public Metadata read(Read read) throws IOException {
    Metadata metadata = readFromIndex(read.getEntity());
    return filterMetadata(metadata, KEEP, read.getKinds(), read.getScopes(), read.getSelection());
  }

  @Override
  public co.cask.cdap.spi.metadata.SearchResponse search(co.cask.cdap.spi.metadata.SearchRequest request)
    throws IOException {
    ensureIndexCreated();
    return request.getCursor() != null ? doScroll(request) : doSearch(request, request.getOffset());
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
  private Tuple<? extends DocWriteRequest, MetadataChange> applyMutation(Metadata before, MetadataMutation mutation)
    throws IOException {
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
  private Tuple<IndexRequest, MetadataChange> create(Metadata before, MetadataMutation.Create create)
    throws IOException {
    // if the entity did not exist before, none of the directives apply and this is equivalent to update()
    if (before.isEmpty()) {
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
        before.getTags().stream()
          .filter(tag -> tag.getScope().equals(scope))
          .forEach(existingTagsToKeep::add);
        before.getProperties().entrySet().stream()
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
          if (!meta.getTags().contains(tag) && before.getTags().contains(tag)) {
            existingTagsToKeep.add(tag);
          }
        } else if (key.getKind() == MetadataKind.PROPERTY) {
          ScopedName property = new ScopedName(key.getScope(), key.getName());
          String existingValue = before.getProperties().get(property);
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
    return Tuple.tuple(writeToIndex(create.getEntity(), after), new MetadataChange(create.getEntity(), before, after));
  }

  /**
   * Creates the Elasticsearch delete request for an entity deletion.
   * This drops the corresponding metadata document from the index.
   *
   * @param before the metadata for the mutation's entity before the change
   *
   * @return an ElasticSearch request to be executed, and the change caused by the mutation
   */
  private Tuple<DeleteRequest, MetadataChange> drop(MetadataEntity entity, Metadata before) {
    return Tuple.tuple(deleteFromIndex(entity), new MetadataChange(entity, before, Metadata.EMPTY));
  }

  /**
   * Creates the Elasticsearch index request for updating the metadata of an entity.
   * This updates or adds the new metadata to the corresponding metadata document in the index.
   *
   * @param before the metadata for the mutation's entity before the change
   *
   * @return an ElasticSearch request to be executed, and the change caused by the mutation
   */
  private Tuple<IndexRequest, MetadataChange> update(MetadataEntity entity, Metadata before, Metadata updates)
    throws IOException {
    Set<ScopedName> tags = new HashSet<>(before.getTags());
    tags.addAll(updates.getTags());
    Map<ScopedName, String> properties = new HashMap<>(before.getProperties());
    properties.putAll(updates.getProperties());
    Metadata after = new Metadata(tags, properties);
    return Tuple.tuple(writeToIndex(entity, after), new MetadataChange(entity, before, after));
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
  private Tuple<IndexRequest, MetadataChange> remove(Metadata before, MetadataMutation.Remove remove)
    throws IOException {
    Metadata after = filterMetadata(before, DISCARD, remove.getKinds(), remove.getScopes(), remove.getRemovals());
    return Tuple.tuple(writeToIndex(remove.getEntity(), after), new MetadataChange(remove.getEntity(), before, after));
  }

  /**
   * Reads the existing metadata for an entity from the index.
   *
   * @return the metadata from the index if found, or empty metadata if the entity was not found in the index;
   *         never null.
   */
  private Metadata readFromIndex(MetadataEntity metadataEntity) throws IOException {
    return readFromIndexIfExists(metadataEntity)
      .map(MetadataDocument::getMetadata)
      .orElse(Metadata.EMPTY);
  }

  /**
   * Reads the existing metadata for an entity from the index.
   *
   * @return an optional metadata, depending on whether the entity was found in the index.
   */
  private Optional<MetadataDocument> readFromIndexIfExists(MetadataEntity entity) throws IOException {
    String id = toDocumentId(entity);
    try {
      ensureIndexCreated();
      GetRequest getRequest = new GetRequest(indexName).type(DOC_TYPE).id(id);
      GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
      if (!response.isExists()) {
        return Optional.empty();
      }
      return Optional.of(GSON.fromJson(response.getSourceAsString(), MetadataDocument.class));
    } catch (Exception e) {
      throw new IOException("Failed to read from index for entity " + entity);
    }
  }

  /**
   * Create an Elasticsearch index request for adding or updating the metadata for an entity in the index.
   * The request must be executed by the caller.
   */
  private IndexRequest writeToIndex(MetadataEntity entity, Metadata metadata) throws IOException {
    ensureIndexCreated();
    MetadataDocument doc = MetadataDocument.of(entity, metadata);
    LOG.info("Indexing document: {}", doc);
    return new IndexRequest(indexName)
      .type(DOC_TYPE)
      .id(toDocumentId(entity))
      .source(GSON.toJson(doc), XContentType.JSON);
  }

  /**
   * Create an Elasticsearch delete request for removing an entity in the index.
   * The request must be executed by the caller.
   */
  private DeleteRequest deleteFromIndex(MetadataEntity entity) {
    String id = toDocumentId(entity);
    return new DeleteRequest(indexName).type(DOC_TYPE).id(id);
  }

  /**
   * Executes an ElasticSearch request to modify a document (index or delete), and handles possible failure.
   */
  private void executeMutation(MetadataEntity entity, DocWriteRequest<?> writeRequest) throws IOException {
    if (writeRequest instanceof DeleteRequest) {
      DeleteRequest deleteRequest = (DeleteRequest) writeRequest;
      setRefreshPolicy(deleteRequest);
      DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
      if (isFailure(response.status().getStatus()) && !isNotFound(response.status().getStatus())) {
        throw new IOException(String.format("Delete request unsuccessful for entity %s: %s", entity, response));
      }
    } else if (writeRequest instanceof IndexRequest) {
      IndexRequest indexRequest = (IndexRequest) writeRequest;
      setRefreshPolicy(indexRequest);
      IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
      if (isFailure(response.status().getStatus())) {
        throw new IOException(String.format("Index request unsuccessful for entity %s: %s", entity, response));
      }
    } else {
      throw new IllegalStateException("Unexpected DocWriteRequest of class " + writeRequest.getClass().getName());
    }
  }

  /**
   * Executes a bulk request and handles the responses for possible failures.
   */
  private void executeBulk(BulkRequest bulkRequest) throws IOException {
    BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
    if (response.hasFailures()) {
      IOException ioe = new IOException("Bulk request unsuccessful");
      for (BulkItemResponse itemResponse : response) {
        if (itemResponse.isFailed()) {
          BulkItemResponse.Failure failure = itemResponse.getFailure();
          String entityId;
          try {
            entityId = toMetadataEntity(failure.getId()).toString();
          } catch (Exception e) {
            LOG.warn("Cannot parse entity id from document id {} in bulk response", failure.getId());
            entityId = "unknown entity";
          }
          ioe.addSuppressed(new IOException(String.format("%s request unsuccessful for entity %s: %s",
                                                          itemResponse.getOpType(), entityId, failure.getMessage())));
        }
      }
      throw ioe;
    }
  }

  /**
   * Sets the refresh policy for a write request. Depending on configuration
   * {@link #CONF_ELASTIC_WAIT_FOR_MUTATIONS}, write requests will only return after they are
   * confirmed to be applied to the index, or return immediately after the request is acknowledged.
   */
  private void setRefreshPolicy(WriteRequest<?> request) {
    request.setRefreshPolicy(refreshPolicy);
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
  private co.cask.cdap.spi.metadata.SearchResponse doScroll(co.cask.cdap.spi.metadata.SearchRequest request)
    throws IOException {

    Cursor cursor = Cursor.fromString(request.getCursor());
    cursor.validate(request.getOffset(), request.getLimit());
    SearchScrollRequest scrollRequest = new SearchScrollRequest(cursor.getScrollId());
    if (request.isCursorRequested()) {
      scrollRequest.scroll(scrollTimeout);
    }
    SearchResponse searchResponse =
      client.scroll(scrollRequest, RequestOptions.DEFAULT);
    if (searchResponse.isTimedOut()) {
      // scroll had expired, we have to search again
      return doSearch(request, cursor.getOffset());
    }
    SearchHits hits = searchResponse.getHits();
    List<MetadataRecord> results = fromHits(hits);
    String newCursor = computeCursor(searchResponse, cursor.getOffset(), cursor.getPageSize());
    return new co.cask.cdap.spi.metadata.SearchResponse(request, newCursor, (int) hits.getTotalHits(), results);
  }

  /**
   * Perform a search that does continue a previous search using a cursor.
   *
   * @param request the search request
   * @param offset the offset for the first result to return. This could be either the offset
   *               from the request, or the offset from a cursor that had expired.
   */
  private co.cask.cdap.spi.metadata.SearchResponse doSearch(co.cask.cdap.spi.metadata.SearchRequest request, int offset)
    throws IOException {

    SearchRequest searchRequest = new SearchRequest(indexName);
    searchRequest.source(createSearchSource(request, offset));
    if (request.isCursorRequested()) {
      searchRequest.scroll(scrollTimeout);
    }
    LOG.debug("Executing search request {}", searchRequest);
    SearchResponse searchResponse =
      client.search(searchRequest, RequestOptions.DEFAULT);
    SearchHits hits = searchResponse.getHits();
    List<MetadataRecord> results = fromHits(hits);
    String newCursor = computeCursor(searchResponse, request.getOffset(), request.getLimit());
    return new co.cask.cdap.spi.metadata.SearchResponse(request, newCursor, (int) hits.getTotalHits(), results);
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
  private String computeCursor(SearchResponse searchResponse, int offset, int pageSize) {
    if (searchResponse.getScrollId() != null) {
      SearchHits hits = searchResponse.getHits();
      int newOffset = offset + hits.getHits().length;
      if (newOffset < hits.getTotalHits()) {
        return new Cursor(newOffset, pageSize, searchResponse.getScrollId()).toString();
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

  private SearchSourceBuilder createSearchSource(co.cask.cdap.spi.metadata.SearchRequest request, int offset) {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.from(offset);
    searchSourceBuilder.size(Integer.min(10000, request.getLimit()));
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
  private QueryBuilder createQuery(co.cask.cdap.spi.metadata.SearchRequest request) {
    // first create a query from the query terms
    QueryBuilder mainQuery = createMainQuery(request);

    List<QueryBuilder> conditions = new ArrayList<>();
    // if the request asks only for a subset of entity types, add a boolean clause for that
    if (request.getTypes() != null) {
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
  private QueryBuilder createMainQuery(co.cask.cdap.spi.metadata.SearchRequest request) {
    if (request.getQuery().equals("*")) {
      return QueryBuilders.matchAllQuery();
    }
    // the indexed document contains three text fields: one for each scope and for all scopes combined.
    // all terms must occur in the text field as selected by the scope in the search request.
    String textField = request.getScope() == null ? TEXT_FIELD : request.getScope().name().toLowerCase();

    // split the query into its terms and iterate over all terms
    Iterable<String> terms = Splitter.on(SPACE_SEPARATOR_PATTERN)
      .omitEmptyStrings().trimResults().split(request.getQuery());
    List<QueryBuilder> termQueries = new ArrayList<>();
    for (String term : terms) {
      termQueries.add(createTermQuery(term, textField, request));
    }
    if (termQueries.isEmpty()) {
      return QueryBuilders.matchAllQuery();
    }
    if (termQueries.size() == 1) {
      return termQueries.get(0);
    }
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    termQueries.forEach(boolQuery::should);
    return boolQuery;
  }

  /**
   * Create a sub-query for a single term in the query string.
   *
   * @param term the term as it appears in the query, possibly with a field qualifier
   * @param textField the default text field to search if the term does not have a field
   */
  private QueryBuilder createTermQuery(String term, String textField, co.cask.cdap.spi.metadata.SearchRequest request) {
    // determine if the term has a field qualifier
    String field = null;
    term = term.trim().toLowerCase();
    if (term.contains(MetadataConstants.KEYVALUE_SEPARATOR)) {
      // split the search term in two parts on first occurrence of KEYVALUE_SEPARATOR and trim the key and value
      String[] split = term.split(MetadataConstants.KEYVALUE_SEPARATOR, 2);
      field = split[0].trim();
      term = split[1].trim();
    }
    if (field == null) {
      return createTermQuery(textField, term);
    }
    BoolQueryBuilder boolQuery = new BoolQueryBuilder();
    boolQuery.must(new TermQueryBuilder(NESTED_NAME_FIELD, field).boost(0.0F));
    if (request.getScope() != null) {
      boolQuery.must(new TermQueryBuilder(NESTED_SCOPE_FIELD, request.getScope().name()).boost(0.0F));
    }
    boolQuery.must(createTermQuery(NESTED_VALUE_FIELD, term));
    return QueryBuilders.nestedQuery(PROPS_FIELD, boolQuery, ScoreMode.Max);
  }

  /**
   * Create a query for a single term in a given field.
   *
   * @return a wildcard query is the term contains * or ?, or a match query otherwise
   */
  private QueryBuilder createTermQuery(String field, String term) {
    return term.contains("*") || term.contains("?")
      ? new WildcardQueryBuilder(field, term)
      : new MatchQueryBuilder(field, term);
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
    return builder.build();
  }

  private static boolean isFailure(int httpStatus) {
    return httpStatus != 200 && httpStatus != 201;
  }
  private static boolean isNotFound(int httpStatus) {
    return httpStatus == 404;
  }

}
