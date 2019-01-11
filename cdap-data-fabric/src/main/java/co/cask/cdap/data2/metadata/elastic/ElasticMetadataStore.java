/*
 * Copyright 2019 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.elastic;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.metadata.MetadataRecord;
import co.cask.cdap.data2.metadata.dataset.SearchRequest;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.metadata.store.NoOpMetadataStore;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * ElasticSearch implementation.
 */
public class ElasticMetadataStore extends NoOpMetadataStore implements MetadataStore, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticMetadataStore.class);

  static final String CONF_INDEX_NAME = "elastic.metadata.index";
  private static final String DEFAULT_INDEX_NAME = "cdap.metadata";

  private static final Gson GSON = new Gson();
  private static final Set<String> NO_TAGS = Collections.emptySet();
  private static final Map<String, String> NO_PROPERTIES = Collections.emptyMap();

  private final RestHighLevelClient client;
  private final String indexName;

  private final AtomicBoolean created = new AtomicBoolean(false);

  @Inject
  public ElasticMetadataStore(CConfiguration cConf) {
    indexName = cConf.get(CONF_INDEX_NAME, DEFAULT_INDEX_NAME);
    String elasticHosts = cConf.get("elastic.cluster.hosts", "localhost:9200");
    HttpHost[] hosts = Arrays.stream(elasticHosts.split(",")).map(hostAndPort -> {
      int pos = hostAndPort.indexOf(':');
      String host = pos < 0 ? hostAndPort : hostAndPort.substring(0, pos);
      int port = pos < 0 ? 9200 : Integer.parseInt(hostAndPort.substring(pos + 1));
      return new HttpHost(host, port);
    }).toArray(HttpHost[]::new);
    client = new RestHighLevelClient(RestClient.builder(hosts));
  }

  @VisibleForTesting
  void ensureIndexCreated() throws IOException {
    if (created.get()) {
      return;
    }
    synchronized (created) {
      if (created.get()) {
        return;
      }
      GetIndexRequest request = new GetIndexRequest();
      request.indices(indexName);
      if (client.indices().exists(request, RequestOptions.DEFAULT)) {
        created.set(true);
        return;
      }
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
      createIndexRequest.settings(Settings.builder()
                                    .put("index.number_of_shards", 1)
                                    .put("index.number_of_replicas", 1));
      CreateIndexResponse response = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
      if (!response.isAcknowledged()) {
        throw new IOException("Create index request was not acknowledged by EleasticSearch");
      }
      created.set(true);
    }
  }

  @VisibleForTesting
  void deleteIndex() throws IOException {
    synchronized (created) {
      GetIndexRequest request = new GetIndexRequest();
      request.indices(indexName);
      if (!client.indices().exists(request, RequestOptions.DEFAULT)) {
        created.set(false);
        return;
      }
      DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
      AcknowledgedResponse response = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
      if (!response.isAcknowledged()) {
        LOG.warn("Delete index request was not acknowledged by ElasticSearch");
      }
      created.set(false);
    }
  }

  @Override
  public void close() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  @Override
  public void setProperty(MetadataScope scope, MetadataEntity metadataEntity, String key, String value) {
    setProperties(scope, metadataEntity, Collections.singletonMap(key, value));
  }

  @Override
  public void setProperties(MetadataScope scope, MetadataEntity metadataEntity, Map<String, String> properties) {
    MetadataDocument metadata = toMetadataDocument(scope, metadataEntity, NO_TAGS, properties);
    addMetadata(metadataEntity, metadata);
  }

  @Override
  public void setProperties(MetadataScope scope, Map<MetadataEntity, Map<String, String>> toUpdate) {
    toUpdate.forEach((key, value) -> setProperties(scope, key, value));
  }

  @Override
  public void addTags(MetadataScope scope, MetadataEntity metadataEntity, Set<String> tagsToAdd) {
    MetadataDocument metadata = toMetadataDocument(scope, metadataEntity, tagsToAdd, NO_PROPERTIES);
    addMetadata(metadataEntity, metadata);
  }

  private void addMetadata(MetadataEntity metadataEntity, MetadataDocument toAdd) {
      MetadataDocument existing = readMetadata(metadataEntity);
      MetadataDocument metadata = existing.merge(toAdd);
      updateMetadata(metadataEntity, metadata);
  }

  private void updateMetadata(MetadataEntity entity, MetadataDocument metadata) {
    try {
      ensureIndexCreated();
      IndexRequest indexRequest = new IndexRequest(indexName)
        .type("doc")
        .id(toDocumentId(entity))
        .source(GSON.toJson(metadata), XContentType.JSON)
        .opType(DocWriteRequest.OpType.INDEX);
      IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
      if (!isSuccess(response.status().getStatus())) {
        throw new IOException(String.format("Index request unsuccessful for entity %s: %s", entity, response));
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void removeMetadata(MetadataEntity metadataEntity) {
    String id = toDocumentId(metadataEntity);
    DeleteRequest deleteRequest = new DeleteRequest(indexName).type("doc").id(id);
    try {
      DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
      if (!isSuccess(response.status().getStatus())) {
        // TODO handle not found
        throw new IOException(String.format("Delete request unsuccessful for entity %s: %s", metadataEntity, response));
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void removeMetadata(MetadataScope scope, MetadataEntity metadataEntity) {
    //noinspection ConstantConditions
    removeMetadata(metadataEntity,
                   tag -> !scope.name().equals(tag.getScope()),
                   property -> !scope.name().equals(property.getScope()));
  }

  @Override
  public void removeProperties(MetadataScope scope, MetadataEntity metadataEntity) {
    //noinspection ConstantConditions
    removeMetadata(metadataEntity,
                   tag -> true,
                   property -> !scope.name().equals(property.getScope()));
  }

  @Override
  public void removeProperties(MetadataScope scope, MetadataEntity metadataEntity, Set<String> keys) {
    //noinspection ConstantConditions
    removeMetadata(metadataEntity,
                   tag -> true,
                   property -> !scope.name().equals(property.getScope()) || !keys.contains(property.getName()));
  }

  @Override
  public void removeProperties(MetadataScope scope, Map<MetadataEntity, Set<String>> toRemove) {
    toRemove.forEach((key, value) -> removeProperties(scope, key, value));
  }

  @Override
  public void removeTags(MetadataScope scope, MetadataEntity metadataEntity) {
    //noinspection ConstantConditions
    removeMetadata(metadataEntity,
                   tag -> !scope.name().equals(tag.getScope()),
                   property -> true);
  }

  @Override
  public void removeTags(MetadataScope scope, MetadataEntity metadataEntity, Set<String> tagsToRemove) {
    //noinspection ConstantConditions
    removeMetadata(metadataEntity,
                   tag -> !scope.name().equals(tag.getScope()) || !tagsToRemove.contains(tag.getName()),
                   property -> true);
  }

  private void removeMetadata(MetadataEntity entity,
                              Predicate<MetadataDocument.Tag> keepTag,
                              Predicate<MetadataDocument.Property> keepProperty) {
    Optional<MetadataDocument> read = readMetadataIfExists(entity);
    if (!read.isPresent()) {
      return;
    }
    MetadataDocument existing = read.get();
    MetadataDocument metadata = existing.remove(keepTag, keepProperty);
    updateMetadata(entity, metadata);
  }

  @Override
  public Map<String, String> getProperties(MetadataScope scope, MetadataEntity metadataEntity) {
    MetadataDocument doc = readMetadata(metadataEntity);
    return extractProperties(scope, doc);
  }

  @Override
  public Map<String, String> getProperties(MetadataEntity metadataEntity) {
    MetadataDocument doc = readMetadata(metadataEntity);
    return extractProperties(null, doc);
  }

  @Override
  public Set<String> getTags(MetadataScope scope, MetadataEntity metadataEntity) {
    MetadataDocument doc = readMetadata(metadataEntity);
    return extractTags(scope, doc);
  }

  @Override
  public Set<String> getTags(MetadataEntity metadataEntity) {
    MetadataDocument doc = readMetadata(metadataEntity);
    return extractTags(null, doc);
  }

  @Override
  public Set<MetadataRecord> getMetadata(MetadataEntity metadataEntity) {
    MetadataDocument doc = readMetadata(metadataEntity);
    return ImmutableSet.of(
      toMetadataRecord(MetadataScope.SYSTEM, metadataEntity, doc),
      toMetadataRecord(MetadataScope.USER, metadataEntity, doc)
    );
  }

  @Override
  public MetadataRecord getMetadata(MetadataScope scope, MetadataEntity metadataEntity) {
    MetadataDocument doc = readMetadata(metadataEntity);
    return toMetadataRecord(scope, metadataEntity, doc);
  }

  @Override
  public Set<MetadataRecord> getMetadata(MetadataScope scope, Set<MetadataEntity> metadataEntities) {
    return metadataEntities.stream().map(entity -> getMetadata(scope, entity)).collect(Collectors.toSet());
  }

  private MetadataDocument readMetadata(MetadataEntity metadataEntity) {
    return readMetadataIfExists(metadataEntity).orElse(MetadataDocument.of(metadataEntity).build());
  }

  private Optional<MetadataDocument> readMetadataIfExists(MetadataEntity metadataEntity) {
    String id = toDocumentId(metadataEntity);
    try {
      ensureIndexCreated();
      GetRequest getRequest = new GetRequest(indexName).type("doc").id(id);
      GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
      if (!response.isExists()) {
        return Optional.empty();
      }
      String source = response.getSourceAsString();
      return Optional.of(GSON.fromJson(source, MetadataDocument.class));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public MetadataSearchResponse search(SearchRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<MetadataRecord> getSnapshotBeforeTime(MetadataScope scope, Set<MetadataEntity> metadataEntities,
                                                   long timeMillis) {
    // TODO implement history
    return getMetadata(scope, metadataEntities);
  }

  private static MetadataDocument toMetadataDocument(MetadataScope scope, MetadataEntity entity,
                                                     Set<String> tags, Map<String, String> properties) {
    MetadataDocument.Builder builder = MetadataDocument.of(entity);
    tags.forEach(name -> builder.addTag(scope, name));
    properties.forEach((key, value) -> builder.addProperty(scope, key, value));
    return builder.build();
  }

  private static MetadataRecord toMetadataRecord(MetadataScope scope, MetadataEntity entity, MetadataDocument doc) {
    return new MetadataRecord(entity, scope, extractProperties(scope, doc), extractTags(scope, doc));
  }

  private static Map<String, String> extractProperties(@Nullable MetadataScope scope, MetadataDocument doc) {
    return doc.getProperties().stream()
      .filter(property -> scope == null || scope.name().equals(property.getScope()))
      .collect(Collectors.toMap(MetadataDocument.Property::getName, MetadataDocument.Property::getValue));
  }

  private static Set<String> extractTags(@Nullable MetadataScope scope, MetadataDocument doc) {
    return doc.getTags().stream()
      .filter(tag -> scope == null || scope.name().equals(tag.getScope()))
      .map(MetadataDocument.Tag::getName)
      .collect(Collectors.toSet());
  }

  private static boolean isSuccess(int httpStatus) {
    return httpStatus == 200 || httpStatus == 201;
  }

  private static String toDocumentId(MetadataEntity entity) {
    StringBuilder builder = new StringBuilder(entity.getType());
    char sep = ':';
    for (MetadataEntity.KeyValue kv : entity) {
      builder.append(sep).append(kv.getKey()).append('=').append(kv.getValue());
      sep = ',';
    }
    return builder.toString();
  }
}
