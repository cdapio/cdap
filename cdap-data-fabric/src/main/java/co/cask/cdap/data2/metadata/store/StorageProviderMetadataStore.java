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

package co.cask.cdap.data2.metadata.store;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.metadata.MetadataRecord;
import co.cask.cdap.data2.audit.AuditPublisher;
import co.cask.cdap.data2.audit.AuditPublishers;
import co.cask.cdap.data2.audit.payload.builder.MetadataPayloadBuilder;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.SearchRequest;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.metadata.MetadataPayload;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.spi.metadata.Metadata;
import co.cask.cdap.spi.metadata.MetadataChange;
import co.cask.cdap.spi.metadata.MetadataDirective;
import co.cask.cdap.spi.metadata.MetadataKind;
import co.cask.cdap.spi.metadata.MetadataMutation;
import co.cask.cdap.spi.metadata.MetadataStorage;
import co.cask.cdap.spi.metadata.Read;
import co.cask.cdap.spi.metadata.ScopedName;
import co.cask.cdap.spi.metadata.ScopedNameOfKind;
import co.cask.cdap.spi.metadata.SearchResponse;
import co.cask.cdap.spi.metadata.Sorting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Metadata store implementation that delegates to a storage provider.
 */
public class StorageProviderMetadataStore implements MetadataStore {

  private final MetadataStorage storage;
  private AuditPublisher auditPublisher;

  @Inject
  public StorageProviderMetadataStore(MetadataStorage storage) {
    this.storage = storage;
  }

  @SuppressWarnings("unused")
  @Inject(optional = true)
  public void setAuditPublisher(AuditPublisher auditPublisher) {
    this.auditPublisher = auditPublisher;
  }

  @Override
  public void createIndex() throws IOException {
    storage.createIndex();
  }

  @Override
  public void dropIndex() throws IOException {
    storage.dropIndex();
  }

  @Override
  public void replaceMetadata(MetadataScope scope,
                              MetadataDataset.Record metadata,
                              Set<String> propertiesToKeep,
                              Set<String> propertiesToPreserve) {
    Map<ScopedNameOfKind, MetadataDirective> directives = new HashMap<>();
    propertiesToKeep.forEach(name -> directives.put(new ScopedNameOfKind(MetadataKind.PROPERTY, scope, name),
                                                    MetadataDirective.KEEP));
    propertiesToPreserve.forEach(name -> directives.put(new ScopedNameOfKind(MetadataKind.PROPERTY, scope, name),
                                                        MetadataDirective.PRESERVE));
    MetadataMutation mutation = new MetadataMutation.Create(
      metadata.getMetadataEntity(), new Metadata(scope, metadata.getTags(), metadata.getProperties()), directives);
    applyMutation(mutation);
  }

  @Override
  public void addProperties(MetadataScope scope, MetadataEntity entity, Map<String, String> properties) {
    applyMutation(new MetadataMutation.Update(entity, new Metadata(scope, properties)));
  }

  @Override
  public void addProperties(MetadataScope scope, Map<MetadataEntity, Map<String, String>> toUpdate) {
    List<MetadataMutation> mutations = toUpdate.entrySet().stream()
      .map(entry -> new MetadataMutation.Update(entry.getKey(), new Metadata(scope, entry.getValue())))
      .collect(Collectors.toList());
    applyBatch(mutations);
  }

  @Override
  public void addProperty(MetadataScope scope, MetadataEntity entity, String key, String value) {
    addProperties(scope, entity, ImmutableMap.of(key, value));
  }

  @Override
  public void addTags(MetadataScope scope, MetadataEntity entity, Set<String> tagsToAdd) {
    applyMutation(new MetadataMutation.Update(entity, new Metadata(scope, tagsToAdd)));
  }

  @Override
  public void removeMetadata(MetadataEntity entity) {
    applyMutation(new MetadataMutation.Drop(entity));
  }

  @Override
  public void removeMetadata(MetadataScope scope, MetadataEntity entity) {
    applyMutation(new MetadataMutation.Remove(entity, scope));
  }

  @Override
  public void removeProperties(MetadataScope scope, MetadataEntity entity) {
    applyMutation(new MetadataMutation.Remove(entity, scope, MetadataKind.PROPERTY));
  }

  @Override
  public void removeProperties(MetadataScope scope, MetadataEntity entity, Set<String> keys) {
    applyMutation(new MetadataMutation.Remove(entity, keys.stream()
      .map(name -> new ScopedNameOfKind(MetadataKind.PROPERTY, scope, name)).collect(Collectors.toSet())));
  }

  @Override
  public void removeProperties(MetadataScope scope, Map<MetadataEntity, Set<String>> toRemove) {
    applyBatch(toRemove.entrySet().stream()
                 .map(entry -> new MetadataMutation.Remove(entry.getKey(),
                                                           entry.getValue().stream()
                                                             .map(name -> new ScopedNameOfKind(MetadataKind.PROPERTY,
                                                                                               scope, name))
                                                             .collect(Collectors.toSet())))
    .collect(Collectors.toList()));
  }

  @Override
  public void removeTags(MetadataScope scope, MetadataEntity entity) {
    applyMutation(new MetadataMutation.Remove(entity, scope, MetadataKind.TAG));
  }

  @Override
  public void removeTags(MetadataScope scope, MetadataEntity entity, Set<String> tagsToRemove) {
    applyMutation(new MetadataMutation.Remove(entity, tagsToRemove.stream()
      .map(name -> new ScopedNameOfKind(MetadataKind.TAG, scope, name)).collect(Collectors.toSet())));
  }


  @Override
  public Set<MetadataRecord> getMetadata(MetadataEntity metadataEntity) {
    Metadata metadata = performRead(new Read(metadataEntity));
    return MetadataScope.ALL.stream()
      .map(scope -> new MetadataRecord(metadataEntity, scope, metadata.getProperties(scope), metadata.getTags(scope)))
      .collect(Collectors.toSet());
  }

  @Override
  public MetadataRecord getMetadata(MetadataScope scope, MetadataEntity metadataEntity) {
    Metadata metadata = performRead(new Read(metadataEntity, scope));
    return new MetadataRecord(metadataEntity, scope, metadata.getProperties(scope), metadata.getTags(scope));
  }

  @Override
  public Set<MetadataRecord> getMetadata(MetadataScope scope, Set<MetadataEntity> entities) {
    return entities.stream().map(entity -> getMetadata(scope, entity)).collect(Collectors.toSet());
  }

  @Override
  public Map<String, String> getProperties(MetadataEntity entity) {
    Metadata metadata = performRead(new Read(entity, MetadataKind.PROPERTY));
    return metadata.getProperties().entrySet().stream()
      .collect(Collectors.toMap(entry -> entry.getKey().getName(), Map.Entry::getValue));
  }

  @Override
  public Map<String, String> getProperties(MetadataScope scope, MetadataEntity entity) {
    Metadata metadata = performRead(new Read(entity, scope, MetadataKind.PROPERTY));
    return metadata.getProperties(scope);
  }

  @Override
  public Set<String> getTags(MetadataEntity entity) {
    Metadata metadata = performRead(new Read(entity, MetadataKind.TAG));
    return metadata.getTags().stream().map(ScopedName::getName).collect(Collectors.toSet());
  }

  @Override
  public Set<String> getTags(MetadataScope scope, MetadataEntity entity) {
    Metadata metadata = performRead(new Read(entity, scope, MetadataKind.TAG));
    return metadata.getTags(scope);
  }

  @Override
  public MetadataSearchResponse search(SearchRequest request) {
    String sorting = "";

    co.cask.cdap.spi.metadata.SearchRequest.Builder req = co.cask.cdap.spi.metadata.SearchRequest
      .of(request.getQuery())
      .setCursor(request.getCursor())
      .setCursorRequested(request.getNumCursors() > 0)
      .setOffset(request.getOffset())
      .setLimit(request.getLimit())
      .setShowHidden(request.shouldShowHidden());
    if (request.getNamespaceId().isPresent()) {
      NamespaceId namespaceId = request.getNamespaceId().get();
      if (NamespaceId.SYSTEM.equals(namespaceId)) {
        // system namespace given -> search only system
        req.addSystemNamespace();
      } else {
        // user namespace given -> search depending on what entity scopes are requested
        if (request.getEntityScopes().contains(EntityScope.SYSTEM)) {
          req.addSystemNamespace();
        }
        if (request.getEntityScopes().contains(EntityScope.USER)) {
          req.addNamespace(namespaceId.getNamespace());
        }
      }
    } else {
      // no namespace given
      if (!request.getEntityScopes().contains(EntityScope.USER)) {
        // only system entities requested -> restrict to that
        req.addSystemNamespace();
      }
      // else search all namespaces
    }
    if (request.getTypes() != null && !request.getTypes().contains(EntityTypeSimpleName.ALL)) {
      request.getTypes().forEach(type -> req.addType(type.name()));
    }
    if (!request.getSortInfo().equals(SortInfo.DEFAULT)) {
      req.setSorting(new Sorting(request.getSortInfo().getSortBy(),
                                 Sorting.Order.valueOf(request.getSortInfo().getSortOrder().name())));
      sorting = request.getSortInfo().getSortBy() + " " + request.getSortInfo().getSortOrder().name();
    }
    SearchResponse result;
    try {
      result = storage.search(req.build());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    Set<MetadataSearchResultRecord> resultRecords = result.getResults().stream()
      .map(record -> {
        Map<MetadataScope, co.cask.cdap.api.metadata.Metadata> metaMap = new HashMap<>();
        MetadataScope.ALL.forEach(scope -> {
          Map<String, String> props = record.getMetadata().getProperties(scope);
          Set<String> tags = record.getMetadata().getTags(scope);
          if (!props.isEmpty() || !tags.isEmpty()) {
            metaMap.put(scope, new co.cask.cdap.api.metadata.Metadata(props, tags));
          }
        });
        return new MetadataSearchResultRecord(record.getEntity(), metaMap);
      })
      .collect(toLinkedSet());
    return new MetadataSearchResponse(sorting, request.getOffset(), request.getLimit(),
                                      Math.abs(request.getNumCursors()), result.getTotalResults(), resultRecords,
                                      result.getCursor() == null ? null : ImmutableList.of(result.getCursor()),
                                      request.shouldShowHidden(), request.getEntityScopes());
  }

  @Override
  public Set<MetadataRecord> getSnapshotBeforeTime(MetadataScope scope,
                                                   Set<MetadataEntity> entities,
                                                   long timeMillis) {
    return getMetadata(scope, entities);
  }

  private static <T> Collector<T, ?, Set<T>> toLinkedSet() {
    return new Collector<T, Set<T>, Set<T>>() {
      @Override
      public Supplier<Set<T>> supplier() {
        return LinkedHashSet::new;
      }

      @Override
      public BiConsumer<Set<T>, T> accumulator() {
        return Set::add;
      }

      @Override
      public BinaryOperator<Set<T>> combiner() {
        return (a, b) -> {
          a.addAll(b);
          return a;
        };
      }

      @Override
      public Function<Set<T>, Set<T>> finisher() {
        return a -> a;
      }

      @Override
      public Set<Characteristics> characteristics() {
        return Collections.singleton(Characteristics.IDENTITY_FINISH);
      }
    };
  }

  private Metadata performRead(Read read) {
    try {
      return storage.read(read);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private void applyMutation(MetadataMutation mutation) {
    try {
      MetadataChange change = storage.apply(mutation);
      publishAudit(change);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private void applyBatch(List<MetadataMutation> mutations) {
    try {
      Collection<MetadataChange> changes = storage.batch(mutations);
      for (MetadataChange change : changes) {
        publishAudit(change);
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private void publishAudit(MetadataChange change) {
    publishAudit(change, MetadataScope.SYSTEM);
    publishAudit(change, MetadataScope.USER);
  }

  private void publishAudit(MetadataChange change, MetadataScope scope) {
    Map<String, String> propsBefore = change.getBefore().getProperties(scope);
    Map<String, String> propsAfter = change.getAfter().getProperties(scope);
    Set<String> tagsBefore = change.getBefore().getTags(scope);
    Set<String> tagsAfter = change.getAfter().getTags(scope);

    boolean propsChanged = !propsBefore.equals(propsAfter);
    boolean tagsChanged = !tagsBefore.equals(tagsAfter);
    if (!propsChanged && !tagsChanged) {
      return; // no change to log
    }

    // previous state is already given
    MetadataRecord previous = new MetadataRecord(change.getEntity(), scope, propsBefore, tagsBefore);

    // compute what was added
    @SuppressWarnings("ConstantConditions")
    Map<String, String> propsAdded = Maps.filterEntries(
      propsAfter, entry -> !entry.getValue().equals(propsBefore.get(entry.getKey())));
    Set<String> tagsAdded = Sets.difference(tagsAfter, tagsBefore);
    MetadataRecord additions = new MetadataRecord(change.getEntity(), scope, propsAdded, tagsAdded);

    // compute what was deleted
    @SuppressWarnings("ConstantConditions")
    Map<String, String> propsDeleted = Maps.filterEntries(
      propsBefore, entry -> !entry.getValue().equals(propsAfter.get(entry.getKey())));
    Set<String> tagsDeleted = Sets.difference(tagsBefore, tagsAfter);
    MetadataRecord deletions = new MetadataRecord(change.getEntity(), scope, propsDeleted, tagsDeleted);

    // and publish
    MetadataPayload payload = new MetadataPayloadBuilder()
      .addPrevious(previous)
      .addAdditions(additions)
      .addDeletions(deletions)
      .build();
    AuditPublishers.publishAudit(auditPublisher, previous.getMetadataEntity(), AuditType.METADATA_CHANGE, payload);
  }
}
