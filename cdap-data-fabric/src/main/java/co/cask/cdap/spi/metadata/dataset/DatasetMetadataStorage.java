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

package co.cask.cdap.spi.metadata.dataset;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
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
import co.cask.cdap.spi.metadata.SearchRequest;
import co.cask.cdap.spi.metadata.SearchResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.tephra.TransactionExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static co.cask.cdap.api.metadata.MetadataScope.SYSTEM;
import static co.cask.cdap.api.metadata.MetadataScope.USER;
import static co.cask.cdap.spi.metadata.MetadataKind.PROPERTY;
import static co.cask.cdap.spi.metadata.MetadataKind.TAG;

/**
 * A dataset-based implementation of the Metadata SPI.
 */
public class DatasetMetadataStorage implements MetadataStorage {

  private final SearchHelper searchHelper;

  @Inject
  DatasetMetadataStorage(SearchHelper searchHelper) {
    this.searchHelper = searchHelper;
  }

  private <T> T execute(TransactionExecutor.Function<MetadataDatasetContext, T> func) {
    return searchHelper.execute(func);
  }

  @Override
  public MetadataChange apply(MetadataMutation mutation) {
    return execute(context -> apply(context, mutation));
  }

  private MetadataChange apply(MetadataDatasetContext context, MetadataMutation mutation) {
    switch (mutation.getType()) {
      case CREATE:
        MetadataMutation.Create create = (MetadataMutation.Create) mutation;
        return create(context, create.getEntity(), create.getMetadata(), create.getDirectives());
      case DROP:
        MetadataMutation.Drop drop = (MetadataMutation.Drop) mutation;
        return drop(context, drop.getEntity());
      case UPDATE:
        MetadataMutation.Update update = (MetadataMutation.Update) mutation;
        return update(context, update.getEntity(), update.getUpdates());
      case REMOVE:
        MetadataMutation.Remove remove = (MetadataMutation.Remove) mutation;
        return remove(context, remove);
      default:
        throw new IllegalStateException(
          String.format("Unknown MetadataMutation type %s for %s", mutation.getType(), mutation.getEntity()));
    }
  }

  @Override
  public List<MetadataChange> batch(List<? extends MetadataMutation> mutations) {
    return execute(context -> mutations.stream()
      .map(mutation -> apply(context, mutation)).collect(Collectors.toList()));
  }

  private MetadataChange remove(MetadataDatasetContext context, MetadataMutation.Remove remove) {
    MetadataEntity entity = remove.getEntity();
    MetadataDataset.Change userChange, systemChange;
    if (remove.getRemovals() != null) {
      Set<String> userTagsToRemove = new HashSet<>();
      Set<String> systemTagsToRemove = new HashSet<>();
      Set<String> userPropertiesToRemove = new HashSet<>();
      Set<String> systemPropertiesToRemove = new HashSet<>();
      remove.getRemovals().forEach(removal -> (TAG == removal.getKind()
        ? USER == removal.getScope() ? userTagsToRemove : systemTagsToRemove
        : USER == removal.getScope() ? userPropertiesToRemove : systemPropertiesToRemove)
        .add(removal.getName()));
      userChange = removeInScope(context, USER, entity, userTagsToRemove, userPropertiesToRemove);
      systemChange = removeInScope(context, SYSTEM, entity, systemTagsToRemove, systemPropertiesToRemove);
    } else {
      Set<MetadataScope> scopes = remove.getScopes();
      Set<MetadataKind> kinds = remove.getKinds();
      userChange = removeScope(context, USER, entity, scopes, kinds);
      systemChange = removeScope(context, SYSTEM, entity, scopes, kinds);
    }
    return combineChanges(entity, userChange, systemChange);

  }

  private MetadataDataset.Change removeScope(MetadataDatasetContext context,
                                             MetadataScope scope, MetadataEntity entity,
                                             Set<MetadataScope> scopesToRemoves, Set<MetadataKind> kindsToRemove) {
    MetadataDataset dataset = context.getDataset(scope);
    if (scopesToRemoves.contains(scope)) {
      if (MetadataKind.ALL.equals(kindsToRemove)) {
        return dataset.removeMetadata(entity);
      }
      if (kindsToRemove.contains(PROPERTY)) {
        return dataset.removeProperties(entity);
      }
      if (kindsToRemove.contains(TAG)) {
        return dataset.removeTags(entity);
      }
    }
    // nothing to remove - return identity change
    MetadataDataset.Record existing = dataset.getMetadata(entity);
    return new MetadataDataset.Change(existing, existing);
  }

  private MetadataDataset.Change removeInScope(MetadataDatasetContext context,
                                               MetadataScope scope, MetadataEntity entity,
                                               Set<String> tagsToRemove, Set<String> propertiesToRemove) {
    MetadataDataset dataset = context.getDataset(scope);
    MetadataDataset.Record before = null;
    MetadataDataset.Record after = null;
    if (tagsToRemove.isEmpty() && propertiesToRemove.isEmpty()) {
      before = dataset.getMetadata(entity);
      after = before;
    } else {
      if (!tagsToRemove.isEmpty()) {
        MetadataDataset.Change change = dataset.removeTags(entity, tagsToRemove);
        before = change.getExisting();
        after = change.getLatest();
      }
      if (!propertiesToRemove.isEmpty()) {
        MetadataDataset.Change change = dataset.removeProperties(entity, propertiesToRemove);
        before = before != null ? before : change.getExisting();
        after = change.getLatest();
      }
    }
    return new MetadataDataset.Change(before, after);
  }

  private MetadataChange update(MetadataDatasetContext context,
                                MetadataEntity entity, Metadata updates) {
    Set<String> userTagsToAdd = new HashSet<>();
    Set<String> systemTagsToAdd = new HashSet<>();
    Map<String, String> userPropertiesToAdd = new HashMap<>();
    Map<String, String> systemPropertiesToAdd = new HashMap<>();
    updates.getTags().forEach(tag -> (USER == tag.getScope() ? userTagsToAdd : systemTagsToAdd).add(tag.getName()));
    updates.getProperties().forEach(
      (key, value) -> (USER == key.getScope() ? userPropertiesToAdd : systemPropertiesToAdd).put(key.getName(), value));
    MetadataDataset.Change userChange =
      addInScope(context, USER, entity, userTagsToAdd, userPropertiesToAdd);
    MetadataDataset.Change systemChange =
      addInScope(context, SYSTEM, entity, systemTagsToAdd, systemPropertiesToAdd);
    return combineChanges(entity, userChange, systemChange);
  }

  private MetadataDataset.Change addInScope(MetadataDatasetContext context,
                                            MetadataScope scope, MetadataEntity entity,
                                            Set<String> tagsToAdd, Map<String, String> propertiesToAdd) {

    MetadataDataset dataset = context.getDataset(scope);
    MetadataDataset.Record before = null, after = null;
    if (tagsToAdd.isEmpty() && propertiesToAdd.isEmpty()) {
      before = dataset.getMetadata(entity);
      after = before;
    } else {
      if (!tagsToAdd.isEmpty()) {
        MetadataDataset.Change change = dataset.addTags(entity, tagsToAdd);
        before = change.getExisting();
        after = change.getLatest();
      }
      if (!propertiesToAdd.isEmpty()) {
        MetadataDataset.Change change = dataset.addProperties(entity, propertiesToAdd);
        before = before != null ? before : change.getExisting();
        after = change.getLatest();
      }
    }
    return new MetadataDataset.Change(before, after);
  }

  private MetadataChange drop(MetadataDatasetContext context, MetadataEntity entity) {
    MetadataDataset.Change userChange = context.getDataset(USER).removeMetadata(entity);
    MetadataDataset.Change systemChange = context.getDataset(SYSTEM).removeMetadata(entity);
    return combineChanges(entity, userChange, systemChange);
  }

  private MetadataChange create(MetadataDatasetContext context, MetadataEntity entity,
                                Metadata metadata, Map<ScopedNameOfKind, MetadataDirective> directives) {
    Set<String> newUserTags = new HashSet<>();
    Set<String> newSystemTags = new HashSet<>();
    Map<String, String> newUserProperties = new HashMap<>();
    Map<String, String> newSystemProperties = new HashMap<>();
    metadata.getTags().forEach(tag -> (USER == tag.getScope() ? newUserTags : newSystemTags).add(tag.getName()));
    metadata.getProperties().forEach(
      (key, value) -> (USER == key.getScope() ? newUserProperties : newSystemProperties).put(key.getName(), value));
    MetadataDataset.Change userChange =
      replaceInScope(context, USER, entity, newUserTags, newUserProperties, directives);
    MetadataDataset.Change systemChange =
      replaceInScope(context, SYSTEM, entity, newSystemTags, newSystemProperties, directives);
    return combineChanges(entity, userChange, systemChange);
  }

  private MetadataDataset.Change replaceInScope(MetadataDatasetContext context,
                                                MetadataScope scope, MetadataEntity entity,
                                                Set<String> newTags, Map<String, String> newProperties,
                                                Map<ScopedNameOfKind, MetadataDirective> directives) {
    MetadataDataset dataset = context.getDataset(scope);
    MetadataDataset.Record before = dataset.getMetadata(entity);
    if (newTags.isEmpty() && newProperties.isEmpty()) {
      // this scope remains unchanged
      return new MetadataDataset.Change(before, before);
    }
    Set<String> existingTags = before.getTags();
    Set<String> tagsToKeepOrPreserve = directives.entrySet().stream()
      .filter(entry -> entry.getKey().getScope() == scope && entry.getKey().getKind() == TAG
        && (entry.getValue() == MetadataDirective.KEEP || entry.getValue() == MetadataDirective.PRESERVE))
      .map(Map.Entry::getKey)
      .map(ScopedName::getName)
      .filter(existingTags::contains)
      .collect(Collectors.toSet());
    newTags = Sets.union(newTags, tagsToKeepOrPreserve);

    Map<String, String> existingProperties = before.getProperties();
    Map<String, String> propertiesToKeepOrPreserve = directives.entrySet().stream()
      .filter(entry -> entry.getKey().getScope() == scope && entry.getKey().getKind() == PROPERTY)
      .filter(entry -> existingProperties.containsKey(entry.getKey().getName()))
      .filter(entry -> entry.getValue() == MetadataDirective.PRESERVE
        || entry.getValue() == MetadataDirective.KEEP && !newProperties.containsKey(entry.getKey().getName()))
      .map(Map.Entry::getKey)
      .map(ScopedName::getName)
      .collect(Collectors.toMap(name -> name, existingProperties::get));
    newProperties.putAll(propertiesToKeepOrPreserve);

    Set<String> tagsToRemove = Sets.difference(before.getTags(), newTags);
    Set<String> tagsToAdd = Sets.difference(newTags, before.getTags());
    Set<String> propertiesToRemove = Sets.difference(before.getProperties().keySet(), newProperties.keySet());
    @SuppressWarnings("ConstantConditions")
    Map<String, String> propertiesToAdd = Maps.filterEntries(
      newProperties, entry -> !entry.getValue().equals(existingProperties.get(entry.getKey())));

    MetadataDataset.Record after = before;
    if (!tagsToRemove.isEmpty()) {
      after = dataset.removeTags(entity, tagsToRemove).getLatest();
    }
    if (!tagsToAdd.isEmpty()) {
      after = dataset.addTags(entity, tagsToAdd).getLatest();
    }
    if (!propertiesToRemove.isEmpty()) {
      after = dataset.removeProperties(entity, propertiesToRemove).getLatest();
    }
    if (!propertiesToAdd.isEmpty()) {
      after = dataset.addProperties(entity, propertiesToAdd).getLatest();
    }
    return new MetadataDataset.Change(before, after);
  }

  @Override
  public Metadata read(Read read) {
    return execute(context -> read(context, read));
  }

  private Metadata read(MetadataDatasetContext context, Read read) {
    MetadataDataset.Record userMetadata = readScope(context, MetadataScope.USER, read);
    MetadataDataset.Record systemMetadata = readScope(context, MetadataScope.SYSTEM, read);
    return mergeDisjointMetadata(new Metadata(USER, userMetadata.getTags(), userMetadata.getProperties()),
                                 new Metadata(SYSTEM, systemMetadata.getTags(), systemMetadata.getProperties()));
  }

  private MetadataDataset.Record readScope(MetadataDatasetContext context,
                                           MetadataScope scope, Read read) {
    MetadataEntity entity = read.getEntity();
    if (read.getSelection() == null && (!read.getScopes().contains(scope) || read.getKinds().isEmpty())) {
      return new MetadataDataset.Record(entity);
    }
    Set<ScopedNameOfKind> selectionForScope = null;
    if (read.getSelection() != null) {
      //noinspection ConstantConditions
      selectionForScope = Sets.filter(read.getSelection(), entry -> entry.getScope() == scope);
      if (selectionForScope.isEmpty()) {
        return new MetadataDataset.Record(entity);
      }
    }
    // now we know we must read from the dataset
    MetadataDataset dataset = context.getDataset(scope);
    if (selectionForScope != null) {
      // request is for a specific set of tags and properties
      Set<String> tagsToRead = selectionForScope.stream()
        .filter(entry -> TAG == entry.getKind())
        .map(ScopedName::getName)
        .collect(Collectors.toSet());
      Set<String> propertiesToRead = selectionForScope.stream()
        .filter(entry -> PROPERTY == entry.getKind())
        .map(ScopedName::getName)
        .collect(Collectors.toSet());
      Set<String> tags = tagsToRead.isEmpty() ? Collections.emptySet()
        : Sets.intersection(tagsToRead, dataset.getTags(entity));
      Map<String, String> properties = propertiesToRead.isEmpty() ? Collections.emptyMap()
        : Maps.filterKeys(dataset.getProperties(entity), propertiesToRead::contains);
      return new MetadataDataset.Record(entity, properties, tags);
    }
    if (MetadataKind.ALL.equals(read.getKinds())) {
      // all metadata kinds requested
      return dataset.getMetadata(entity);
    }
    // exactly one kind is requested
    MetadataKind requestKind = read.getKinds().iterator().next();
    if (requestKind == TAG) {
      return new MetadataDataset.Record(entity, Collections.emptyMap(), dataset.getTags(entity));
    }
    if (requestKind == PROPERTY) {
      return new MetadataDataset.Record(entity, dataset.getProperties(entity), Collections.emptySet());
    }
    throw new IllegalStateException("Encountered metadata read request for unknown kind " + requestKind);
  }

  @Override
  public SearchResponse search(SearchRequest request) {
    NamespaceId namespace = null;
    Set<EntityScope> entityScopes;
    if (request.getNamespaces() == null || request.getNamespaces().isEmpty()) {
      entityScopes = EnumSet.allOf(EntityScope.class);
    } else {
      boolean hasSystem = false;
      for (String ns : request.getNamespaces()) {
        if (ns.equals(NamespaceId.SYSTEM.getNamespace())) {
          hasSystem = true;
        } else {
          if (namespace != null) {
            throw new UnsupportedOperationException(String.format(
              "This implementation supports at most one non-system namespace, but %s as well as %s were given",
              namespace.getNamespace(), ns));
          }
          namespace = new NamespaceId(ns);
        }
      }
      if (hasSystem) {
        if (namespace == null) {
          namespace = NamespaceId.SYSTEM;
          entityScopes = Collections.singleton(EntityScope.SYSTEM);
        } else {
          entityScopes = EnumSet.allOf(EntityScope.class);
        }
      } else {
        entityScopes = Collections.singleton(EntityScope.USER);
      }
    }
    MetadataSearchResponse response = searchHelper.search(new co.cask.cdap.data2.metadata.dataset.SearchRequest(
      namespace,
      request.getQuery(),
      request.getTypes() == null ? Collections.emptySet() :
        request.getTypes().stream().map(EntityTypeSimpleName::valueOf).collect(Collectors.toSet()),
      request.getSorting() == null ? SortInfo.DEFAULT :
        new SortInfo(request.getSorting().getKey(), SortInfo.SortOrder.valueOf(request.getSorting().getOrder().name())),
      request.getOffset(),
      request.getLimit(),
      request.isCursorRequested() ? 1 : 0,
      request.getCursor(),
      request.isShowHidden(),
      entityScopes
    ));
    // translate results back
    List<MetadataRecord> results = new ArrayList<>(response.getResults().size());
    response.getResults().forEach(record -> {
      Metadata metadata = null;
      for (Map.Entry<MetadataScope, co.cask.cdap.api.metadata.Metadata> entry : record.getMetadata().entrySet()) {
        Metadata toAdd = new Metadata(entry.getKey(), entry.getValue().getTags(), entry.getValue().getProperties());
        metadata = metadata == null ? toAdd : mergeDisjointMetadata(metadata, toAdd);
      }
      results.add(new MetadataRecord(record.getMetadataEntity(), metadata));
    });
    String cursor =
      response.getCursors() == null || response.getCursors().isEmpty() ? null : response.getCursors().get(0);
    return new SearchResponse(request, cursor, response.getTotal(), results);
  }

  private MetadataChange combineChanges(MetadataEntity entity,
                                        MetadataDataset.Change userChange,
                                        MetadataDataset.Change sysChange) {

    Metadata userBefore = new Metadata(USER, userChange.getExisting().getTags(),
                                       userChange.getExisting().getProperties());
    Metadata sysBefore = new Metadata(SYSTEM, sysChange.getExisting().getTags(),
                                      sysChange.getExisting().getProperties());
    Metadata before = mergeDisjointMetadata(userBefore, sysBefore);

    Metadata userAfter = new Metadata(USER, userChange.getLatest().getTags(), userChange.getLatest().getProperties());
    Metadata sysAfter = new Metadata(SYSTEM, sysChange.getLatest().getTags(), sysChange.getLatest().getProperties());
    Metadata after = mergeDisjointMetadata(userAfter, sysAfter);

    return new MetadataChange(entity, before, after);
  }

  /**
   * Helper method to merge disjoint metadata objects. This is used to merge the metadata fro (up to) two
   * metadata records returned by the datasets (one per scope) into a single Metadata object.
   */
  private static Metadata mergeDisjointMetadata(Metadata meta, Metadata other) {
    return new Metadata(Sets.union(meta.getTags(), other.getTags()),
                        // this will not conflict because the two metadata are mutually disjoint
                        ImmutableMap.<ScopedName, String>builder()
                          .putAll(meta.getProperties())
                          .putAll(other.getProperties()).build());
  }

  @Override
  public void close() {
    // nop-op
  }
}
