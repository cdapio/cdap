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

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ImmutablePair;
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
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
public class DatasetMetadataStorage extends SearchHelper implements MetadataStorage {

  @Inject
  DatasetMetadataStorage(TransactionSystemClient txClient,
                         @Named(Constants.Dataset.TABLE_TYPE) DatasetDefinition tableDefinition) {
    super(txClient, tableDefinition);
  }

  @Override
  public void createIndex() throws IOException {
    createDatasets();
  }

  @Override
  public void dropIndex() throws IOException {
    dropDatasets();
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
    ImmutablePair<NamespaceId, Set<EntityScope>> namespaceAndScopes =
      determineNamespaceAndScopes(request.getNamespaces());
    CursorAndOffsetInfo cursorOffsetAndLimits = determineCursorOffsetAndLimits(request);
    MetadataSearchResponse response = search(new co.cask.cdap.data2.metadata.dataset.SearchRequest(
      namespaceAndScopes.getFirst(),
      request.getQuery(),
      request.getTypes() == null ? Collections.emptySet() :
        request.getTypes().stream().map(EntityTypeSimpleName::valueOfSerializedForm).collect(Collectors.toSet()),
      request.getSorting() == null ? SortInfo.DEFAULT :
        new SortInfo(request.getSorting().getKey(), SortInfo.SortOrder.valueOf(request.getSorting().getOrder().name())),
      cursorOffsetAndLimits.getOffsetToRequest(),
      cursorOffsetAndLimits.getLimitToRequest(),
      request.isCursorRequested() ? 1 : 0,
      cursorOffsetAndLimits.getCursor(),
      request.isShowHidden(),
      namespaceAndScopes.getSecond()
    ), request.getScope());

    // translate results back and limit them to at most what was requested (see above where we add 1)
    int limitToRespond = cursorOffsetAndLimits.getLimitToRespond();
    int offsetToRespond = cursorOffsetAndLimits.getOffsetToRespond();
    List<MetadataRecord> results =
      response.getResults().stream().limit(limitToRespond).map(record -> {
        Metadata metadata = null;
        for (Map.Entry<MetadataScope, co.cask.cdap.api.metadata.Metadata> entry : record.getMetadata().entrySet()) {
          Metadata toAdd = new Metadata(entry.getKey(), entry.getValue().getTags(), entry.getValue().getProperties());
          metadata = metadata == null ? toAdd : mergeDisjointMetadata(metadata, toAdd);
        }
        return new MetadataRecord(record.getMetadataEntity(), metadata);
      }).collect(Collectors.toList());

    String cursorToReturn = null;
    if (response.getCursors() != null && !response.getCursors().isEmpty()) {
      // the new cursor's offset is the previous cursor's offset plus the number of results
      cursorToReturn = new Cursor(offsetToRespond + results.size(), limitToRespond,
                                  response.getCursors().get(0)).toString();
    }
    // adjust the total results by the difference of requested offset and the true offset that we respond back
    int totalResults = offsetToRespond - cursorOffsetAndLimits.getOffsetToRequest() + response.getTotal();
    return new SearchResponse(request, cursorToReturn, offsetToRespond, limitToRespond,
                              totalResults, results);
  }

  @VisibleForTesting
  static CursorAndOffsetInfo determineCursorOffsetAndLimits(SearchRequest request) {
    // limit and offset will be what we request from the datasets
    int limit = request.getLimit();
    int offset = request.getOffset();
    // limitToRespond and offsetToRespond will be what we return in the response
    int limitToRespond = limit;
    int offsetToRespond = offset;
    // if the request has a cursor, then that supersedes the offset and limit
    String cursor = null;
    if (request.getCursor() != null && !request.getCursor().isEmpty()) {
      // deserialize the cursor
      Cursor c = Cursor.fromString(request.getCursor());
      cursor = c.getCursor();
      // request as many as given by the cursor - and that is also returned in the response
      limit = c.getPageSize();
      limitToRespond = limit;
      // we must request offset zero, because the dataset interprets it relative to the cursor
      offset = 0;
      offsetToRespond = c.getOffset();
    } else if (!request.isCursorRequested() && limit < Integer.MAX_VALUE) {
      // if no cursor is requested, fetch one extra result to determine if there are more results following
      limit++;
    }
    return new CursorAndOffsetInfo(cursor, offset, offsetToRespond, limit, limitToRespond);
  }

  @VisibleForTesting
  static ImmutablePair<NamespaceId, Set<EntityScope>> determineNamespaceAndScopes(Set<String> namespaces) {
    // if the request does not specify namespaces at all, then it searches all, including system
    if (namespaces == null || namespaces.isEmpty()) {
      return ImmutablePair.of(null, EnumSet.allOf(EntityScope.class));
    }
    boolean hasSystem = false;
    Set<String> userNamespaces = namespaces;
    if (namespaces.contains(NamespaceId.SYSTEM.getNamespace())) {
      userNamespaces = new HashSet<>(namespaces);
      userNamespaces.remove(NamespaceId.SYSTEM.getNamespace());
      // if the request only specifies the system namespace, search that namespace and system scope
      if (userNamespaces.isEmpty()) {
        return ImmutablePair.of(NamespaceId.SYSTEM, EnumSet.of(EntityScope.SYSTEM));
      }
      hasSystem = true;
    }
    // we have at least one non-system namespace
    if (userNamespaces.size() > 1) {
      throw new UnsupportedOperationException(String.format(
        "This implementation supports at most one non-system namespace, but %s were requested", userNamespaces));
    }
    // we have exactly one non-system namespace
    NamespaceId namespace = new NamespaceId(userNamespaces.iterator().next());
    return hasSystem
      ? ImmutablePair.of(namespace, EnumSet.allOf(EntityScope.class))
      : ImmutablePair.of(namespace, EnumSet.of(EntityScope.USER));
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

  /**
   * Helper class to represent adjustments made to the search request parameters
   * before delegating to the MetadataDataset, based on whether the request has
   * a cursor and whether it requests a cursor, or not.
   */
  static class CursorAndOffsetInfo {
    private final String cursor;
    private final int offsetToRequest;
    private final int offsetToRespond;
    private final int limitToRequest;
    private final int limitToRespond;

    /**
     * @param cursor          the cursor to pass to the metadata dataset
     * @param offsetToRequest the offset to request from the metadata dataset
     * @param offsetToRespond the offset to return in the search response
     * @param limitToRequest  the result limit to request from the metadata dataset
     * @param limitToRespond  the result limit to return in the search response
     */
    CursorAndOffsetInfo(String cursor,
                        int offsetToRequest, int offsetToRespond,
                        int limitToRequest, int limitToRespond) {
      this.cursor = cursor;
      this.offsetToRequest = offsetToRequest;
      this.offsetToRespond = offsetToRespond;
      this.limitToRequest = limitToRequest;
      this.limitToRespond = limitToRespond;
    }

    String getCursor() {
      return cursor;
    }

    int getOffsetToRequest() {
      return offsetToRequest;
    }

    int getOffsetToRespond() {
      return offsetToRespond;
    }

    int getLimitToRequest() {
      return limitToRequest;
    }

    int getLimitToRespond() {
      return limitToRespond;
    }
  }
}
