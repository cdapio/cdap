/*
 * Copyright 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.data2.audit.AuditPublisher;
import co.cask.cdap.data2.audit.AuditPublishers;
import co.cask.cdap.data2.audit.payload.builder.MetadataPayloadBuilder;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.dataset.Metadata;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.MetadataDatasetDefinition;
import co.cask.cdap.data2.metadata.dataset.MetadataEntry;
import co.cask.cdap.data2.metadata.dataset.SearchResults;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of {@link MetadataStore} used in distributed mode.
 */
public class DefaultMetadataStore implements MetadataStore {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadataStore.class);
  private static final Map<String, String> EMPTY_PROPERTIES = ImmutableMap.of();
  private static final Set<String> EMPTY_TAGS = ImmutableSet.of();
  private static final int BATCH_SIZE = 1000;

  // TODO: Can be made private after CDAP-7835 is fixed
  public static final DatasetId BUSINESS_METADATA_INSTANCE_ID = NamespaceId.SYSTEM.dataset("business.metadata");
  public static final DatasetId SYSTEM_METADATA_INSTANCE_ID = NamespaceId.SYSTEM.dataset("system.metadata");

  private static final Comparator<Map.Entry<NamespacedEntityId, Integer>> SEARCH_RESULT_DESC_SCORE_COMPARATOR =
    new Comparator<Map.Entry<NamespacedEntityId, Integer>>() {
      @Override
      public int compare(Map.Entry<NamespacedEntityId, Integer> o1, Map.Entry<NamespacedEntityId, Integer> o2) {
        // sort in descending order
        return o2.getValue() - o1.getValue();
      }
    };

  private final TransactionExecutorFactory txExecutorFactory;
  private final DatasetFramework dsFramework;
  private AuditPublisher auditPublisher;

  @Inject
  DefaultMetadataStore(TransactionExecutorFactory txExecutorFactory, DatasetFramework dsFramework) {
    this.txExecutorFactory = txExecutorFactory;
    this.dsFramework = dsFramework;
  }


  @SuppressWarnings("unused")
  @Inject(optional = true)
  public void setAuditPublisher(AuditPublisher auditPublisher) {
    this.auditPublisher = auditPublisher;
  }

  /**
   * Adds/updates metadata for the specified {@link NamespacedEntityId}.
   */
  @Override
  public void setProperties(final MetadataScope scope, final NamespacedEntityId namespacedEntityId,
                            final Map<String, String> properties) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        Map<String, String> existingProperties = input.getProperties(namespacedEntityId);
        Set<String> existingTags = input.getTags(namespacedEntityId);
        previousRef.set(new MetadataRecord(namespacedEntityId, scope, existingProperties, existingTags));
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          input.setProperty(namespacedEntityId, entry.getKey(), entry.getValue());
        }
      }
    }, scope);
    final ImmutableMap.Builder<String, String> propAdditions = ImmutableMap.builder();
    final ImmutableMap.Builder<String, String> propDeletions = ImmutableMap.builder();
    MetadataRecord previousRecord = previousRef.get();
    // Iterating over properties all over again, because we want to move the diff calculation outside the transaction.
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String existingValue = previousRecord.getProperties().get(entry.getKey());
      if (existingValue != null && existingValue.equals(entry.getValue())) {
        // Value already exists and is the same as the value being passed. No update necessary.
        continue;
      }
      // At this point, its either an update of an existing property (1 addition + 1 deletion) or a new property.
      // If it is an update, then mark a single deletion.
      if (existingValue != null) {
        propDeletions.put(entry.getKey(), existingValue);
      }
      // In both update or new cases, mark a single addition.
      propAdditions.put(entry.getKey(), entry.getValue());
    }
    publishAudit(previousRecord, new MetadataRecord(namespacedEntityId, scope, propAdditions.build(), EMPTY_TAGS),
                 new MetadataRecord(namespacedEntityId, scope, propDeletions.build(), EMPTY_TAGS));
  }

  @Override
  public void setProperty(final MetadataScope scope, final NamespacedEntityId namespacedEntityId, final String key,
                          final String value) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        Map<String, String> existingProperties = input.getProperties(namespacedEntityId);
        Set<String> existingTags = input.getTags(namespacedEntityId);
        previousRef.set(new MetadataRecord(namespacedEntityId, scope, existingProperties, existingTags));
        input.setProperty(namespacedEntityId, key, value);
      }
    }, scope);
    publishAudit(previousRef.get(),
                 new MetadataRecord(namespacedEntityId, scope, ImmutableMap.of(key, value), EMPTY_TAGS),
                 new MetadataRecord(namespacedEntityId, scope));
  }

  /**
   * Adds tags for the specified {@link NamespacedEntityId}.
   */
  @Override
  public void addTags(final MetadataScope scope, final NamespacedEntityId namespacedEntityId,
                      final String... tagsToAdd) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        Map<String, String> existingProperties = input.getProperties(namespacedEntityId);
        Set<String> existingTags = input.getTags(namespacedEntityId);
        previousRef.set(new MetadataRecord(namespacedEntityId, scope, existingProperties, existingTags));
        input.addTags(namespacedEntityId, tagsToAdd);
      }
    }, scope);
    publishAudit(previousRef.get(),
                 new MetadataRecord(namespacedEntityId, scope, EMPTY_PROPERTIES, Sets.newHashSet(tagsToAdd)),
                 new MetadataRecord(namespacedEntityId, scope));
  }

  @Override
  public Set<MetadataRecord> getMetadata(NamespacedEntityId namespacedEntityId) {
    return ImmutableSet.of(getMetadata(MetadataScope.USER, namespacedEntityId), getMetadata(MetadataScope.SYSTEM,
                                                                                            namespacedEntityId));
  }

  @Override
  public MetadataRecord getMetadata(final MetadataScope scope, final NamespacedEntityId namespacedEntityId) {
    return execute(new TransactionExecutor.Function<MetadataDataset, MetadataRecord>() {
      @Override
      public MetadataRecord apply(MetadataDataset input) throws Exception {
        Map<String, String> properties = input.getProperties(namespacedEntityId);
        Set<String> tags = input.getTags(namespacedEntityId);
        return new MetadataRecord(namespacedEntityId, scope, properties, tags);
      }
    }, scope);
  }

  /**
   * @return a set of {@link MetadataRecord}s representing all the metadata (including properties and tags)
   * for the specified set of {@link NamespacedEntityId}s.
   */
  @Override
  public Set<MetadataRecord> getMetadata(final MetadataScope scope, final Set<NamespacedEntityId> namespacedEntityIds) {
    return execute(new TransactionExecutor.Function<MetadataDataset, Set<MetadataRecord>>() {
      @Override
      public Set<MetadataRecord> apply(MetadataDataset input) throws Exception {
        Set<MetadataRecord> metadataRecords = new HashSet<>(namespacedEntityIds.size());
        for (NamespacedEntityId namespacedEntityId : namespacedEntityIds) {
          Map<String, String> properties = input.getProperties(namespacedEntityId);
          Set<String> tags = input.getTags(namespacedEntityId);
          metadataRecords.add(new MetadataRecord(namespacedEntityId, scope, properties, tags));
        }
        return metadataRecords;
      }
    }, scope);
  }

  @Override
  public Map<String, String> getProperties(NamespacedEntityId namespacedEntityId) {
    return ImmutableMap.<String, String>builder()
      .putAll(getProperties(MetadataScope.USER, namespacedEntityId))
      .putAll(getProperties(MetadataScope.SYSTEM, namespacedEntityId))
      .build();
  }

  /**
   * @return the metadata for the specified {@link NamespacedEntityId}
   */
  @Override
  public Map<String, String> getProperties(MetadataScope scope, final NamespacedEntityId namespacedEntityId) {
    return execute(new TransactionExecutor.Function<MetadataDataset, Map<String, String>>() {
      @Override
      public Map<String, String> apply(MetadataDataset input) throws Exception {
        return input.getProperties(namespacedEntityId);
      }
    }, scope);
  }

  @Override
  public Set<String> getTags(NamespacedEntityId namespacedEntityId) {
    return ImmutableSet.<String>builder()
      .addAll(getTags(MetadataScope.USER, namespacedEntityId))
      .addAll(getTags(MetadataScope.SYSTEM, namespacedEntityId))
      .build();
  }

  /**
   * @return the tags for the specified {@link NamespacedEntityId}
   */
  @Override
  public Set<String> getTags(MetadataScope scope, final NamespacedEntityId namespacedEntityId) {
    return execute(new TransactionExecutor.Function<MetadataDataset, Set<String>>() {
      @Override
      public Set<String> apply(MetadataDataset input) throws Exception {
        return input.getTags(namespacedEntityId);
      }
    }, scope);
  }

  @Override
  public void removeMetadata(NamespacedEntityId namespacedEntityId) {
    removeMetadata(MetadataScope.USER, namespacedEntityId);
    removeMetadata(MetadataScope.SYSTEM, namespacedEntityId);
  }

  /**
   * Removes all metadata (including properties and tags) for the specified {@link NamespacedEntityId}.
   */
  @Override
  public void removeMetadata(final MetadataScope scope, final NamespacedEntityId namespacedEntityId) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(namespacedEntityId, scope, input.getProperties(namespacedEntityId),
                                           input.getTags(namespacedEntityId)));
        input.removeProperties(namespacedEntityId);
        input.removeTags(namespacedEntityId);
      }
    }, scope);
    MetadataRecord previous = previousRef.get();
    publishAudit(previous, new MetadataRecord(namespacedEntityId, scope), new MetadataRecord(previous));
  }

  /**
   * Removes all properties for the specified {@link NamespacedEntityId}.
   */
  @Override
  public void removeProperties(final MetadataScope scope, final NamespacedEntityId namespacedEntityId) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(namespacedEntityId, scope, input.getProperties(namespacedEntityId),
                                           input.getTags(namespacedEntityId)));
        input.removeProperties(namespacedEntityId);
      }
    }, scope);
    publishAudit(previousRef.get(), new MetadataRecord(namespacedEntityId, scope),
                 new MetadataRecord(namespacedEntityId, scope, previousRef.get().getProperties(), EMPTY_TAGS));
  }

  /**
   * Removes the specified properties of the {@link NamespacedEntityId}.
   */
  @Override
  public void removeProperties(final MetadataScope scope, final NamespacedEntityId namespacedEntityId,
                               final String... keys) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    final ImmutableMap.Builder<String, String> deletesBuilder = ImmutableMap.builder();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(namespacedEntityId, scope, input.getProperties(namespacedEntityId),
                                           input.getTags(namespacedEntityId)));
        for (String key : keys) {
          MetadataEntry record = input.getProperty(namespacedEntityId, key);
          if (record == null) {
            continue;
          }
          deletesBuilder.put(record.getKey(), record.getValue());
        }
        input.removeProperties(namespacedEntityId, keys);
      }
    }, scope);
    publishAudit(previousRef.get(), new MetadataRecord(namespacedEntityId, scope),
                 new MetadataRecord(namespacedEntityId, scope, deletesBuilder.build(), EMPTY_TAGS));
  }

  /**
   * Removes all the tags from the {@link NamespacedEntityId}
   */
  @Override
  public void removeTags(final MetadataScope scope, final NamespacedEntityId namespacedEntityId) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(namespacedEntityId, scope, input.getProperties(namespacedEntityId),
                                           input.getTags(namespacedEntityId)));
        input.removeTags(namespacedEntityId);
      }
    }, scope);
    MetadataRecord previous = previousRef.get();
    publishAudit(previous, new MetadataRecord(namespacedEntityId, scope),
                 new MetadataRecord(namespacedEntityId, scope, EMPTY_PROPERTIES, previous.getTags()));
  }

  /**
   * Removes the specified tags from the {@link NamespacedEntityId}
   */
  @Override
  public void removeTags(final MetadataScope scope, final NamespacedEntityId namespacedEntityId,
                         final String... tagsToRemove) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(namespacedEntityId, scope, input.getProperties(namespacedEntityId),
                                           input.getTags(namespacedEntityId)));
        input.removeTags(namespacedEntityId, tagsToRemove);
      }
    }, scope);
    publishAudit(previousRef.get(), new MetadataRecord(namespacedEntityId, scope),
                 new MetadataRecord(namespacedEntityId, scope, EMPTY_PROPERTIES, Sets.newHashSet(tagsToRemove)));
  }

  @Override
  public MetadataSearchResponse search(String namespaceId, String searchQuery,
                                       Set<EntityTypeSimpleName> types,
                                       SortInfo sortInfo, int offset, int limit,
                                       int numCursors, String cursor, boolean showHidden,
                                       Set<EntityScope> entityScope) throws BadRequestException {
    Set<MetadataScope> searchScopes = EnumSet.allOf(MetadataScope.class);
    if ("*".equals(searchQuery)) {
      if (SortInfo.DEFAULT.equals(sortInfo)) {
        // Can't disallow this completely, because it is required for upgrade, but log a warning to indicate that
        // a full index search should not be done in production.
        LOG.warn("Attempt to search through all indexes. This query can have an adverse effect on performance and is " +
                   "not recommended for production use. It is only meant to be used for administrative purposes " +
                   "such as upgrade. To improve the performance of such queries, please specify sort parameters " +
                   "as well.");
      } else {
        // when it is a known sort (stored sorted in the metadata dataset already), restrict it to system scope only
        searchScopes = EnumSet.of(MetadataScope.SYSTEM);
      }
    }
    return search(searchScopes, namespaceId, searchQuery, types, sortInfo, offset, limit, numCursors, cursor,
                  showHidden, entityScope);
  }

  private MetadataSearchResponse search(Set<MetadataScope> scopes, String namespaceId,
                                        String searchQuery, Set<EntityTypeSimpleName> types,
                                        SortInfo sortInfo, int offset, int limit,
                                        int numCursors, String cursor, boolean showHidden,
                                        Set<EntityScope> entityScope) throws BadRequestException {
    if (offset < 0) {
      throw new IllegalArgumentException("offset must not be negative");
    }

    if (limit < 0) {
      throw new IllegalArgumentException("limit must not be negative");
    }

    List<MetadataEntry> results = new LinkedList<>();
    List<String> cursors = new LinkedList<>();
    List<MetadataEntry> allResults = new LinkedList<>();
    for (MetadataScope scope : scopes) {
      SearchResults searchResults =
        getSearchResults(scope, namespaceId, searchQuery, types, sortInfo, offset, limit, numCursors, cursor,
                         showHidden, entityScope);
      results.addAll(searchResults.getResults());
      cursors.addAll(searchResults.getCursors());
      allResults.addAll(searchResults.getAllResults());
    }

    // sort if required
    Set<NamespacedEntityId> sortedEntities = getSortedEntities(results, sortInfo);
    int total = getSortedEntities(allResults, sortInfo).size();

    // pagination is not performed at the dataset level, because:
    // 1. scoring is needed for DEFAULT sort info. So perform it here for now.
    // 2. Even when using custom sorting, we still fetch extra results if numCursors > 1
    // TODO: Figure out how all of this can be done server (HBase) side
    if (SortInfo.DEFAULT.equals(sortInfo)) {
      int startIndex = Math.min(offset, sortedEntities.size());
      int endIndex = (int) Math.min(Integer.MAX_VALUE, (long) offset + limit); // Account for overflow
      endIndex = Math.min(endIndex, sortedEntities.size());

      // add 1 to maxIndex because end index is exclusive
      sortedEntities = new LinkedHashSet<>(
        ImmutableList.copyOf(sortedEntities).subList(startIndex, endIndex)
      );
    }

    // Fetch metadata for entities in the result list
    // Note: since the fetch is happening in a different transaction, the metadata for entities may have been
    // removed. It is okay not to have metadata for some results in case this happens.
    Map<NamespacedEntityId, Metadata> systemMetadata = fetchMetadata(sortedEntities, MetadataScope.SYSTEM);
    Map<NamespacedEntityId, Metadata> userMetadata = fetchMetadata(sortedEntities, MetadataScope.USER);

    return new MetadataSearchResponse(
      sortInfo.getSortBy() + " " + sortInfo.getSortOrder(), offset, limit, numCursors, total,
      addMetadataToEntities(sortedEntities, systemMetadata, userMetadata), cursors, showHidden,
      entityScope);
  }

  private SearchResults getSearchResults(final MetadataScope scope, final String namespaceId,
                                         final String searchQuery, final Set<EntityTypeSimpleName> types,
                                         final SortInfo sortInfo, final int offset,
                                         final int limit, final int numCursors,
                                         final String cursor, final boolean showHidden,
                                         final Set<EntityScope> entityScope) throws BadRequestException {
    return execute(
      new TransactionExecutor.Function<MetadataDataset, SearchResults>() {
        @Override
        public SearchResults apply(MetadataDataset input) throws Exception {
          return input.search(namespaceId, searchQuery, types, sortInfo, offset, limit, numCursors, cursor, showHidden,
                              entityScope);
        }
      }, scope);
  }

  private Set<NamespacedEntityId> getSortedEntities(List<MetadataEntry> results, SortInfo sortInfo) {
    // if sort order is not weighted, return entities in the order received.
    // in this case, the backing storage is expected to return results in the expected order.
    if (SortInfo.SortOrder.WEIGHTED != sortInfo.getSortOrder()) {
      Set<NamespacedEntityId> entities = new LinkedHashSet<>(results.size());
      for (MetadataEntry metadataEntry : results) {
        //TODO Remove this null check after CDAP-7228 resolved. Since previous CDAP version may have null value.
        if (metadataEntry != null) {
          entities.add(metadataEntry.getTargetId());
        }
      }
      return entities;
    }
    // if sort order is weighted, score results by weight, and return in descending order of weights
    // Score results
    final Map<NamespacedEntityId, Integer> weightedResults = new HashMap<>();
    for (MetadataEntry metadataEntry : results) {
      //TODO Remove this null check after CDAP-7228 resolved. Since previous CDAP version may have null value.
      if (metadataEntry != null) {
        Integer score = weightedResults.get(metadataEntry.getTargetId());
        score = (score == null) ? 0 : score;
        weightedResults.put(metadataEntry.getTargetId(), score + 1);
      }
    }

    // Sort the results by score
    List<Map.Entry<NamespacedEntityId, Integer>> resultList = new ArrayList<>(weightedResults.entrySet());
    Collections.sort(resultList, SEARCH_RESULT_DESC_SCORE_COMPARATOR);
    Set<NamespacedEntityId> result = new LinkedHashSet<>(resultList.size());
    for (Map.Entry<NamespacedEntityId, Integer> entry : resultList) {
      result.add(entry.getKey());
    }
    return result;
  }

  private Map<NamespacedEntityId, Metadata> fetchMetadata(final Set<NamespacedEntityId> namespacedEntityIds,
                                                          MetadataScope scope) {
    Set<Metadata> metadataSet =
      execute(new TransactionExecutor.Function<MetadataDataset, Set<Metadata>>() {
        @Override
        public Set<Metadata> apply(MetadataDataset input) throws Exception {
          return input.getMetadata(namespacedEntityIds);
        }
      }, scope);
    Map<NamespacedEntityId, Metadata> metadataMap = new HashMap<>();
    for (Metadata m : metadataSet) {
      metadataMap.put(m.getEntityId(), m);
    }
    return metadataMap;
  }

  private Set<MetadataSearchResultRecord> addMetadataToEntities(Set<NamespacedEntityId> entities,
                                                                Map<NamespacedEntityId, Metadata> systemMetadata,
                                                                Map<NamespacedEntityId, Metadata> userMetadata) {
    Set<MetadataSearchResultRecord> result = new LinkedHashSet<>();
    for (NamespacedEntityId entity : entities) {
      ImmutableMap.Builder<MetadataScope, co.cask.cdap.proto.metadata.Metadata> builder = ImmutableMap.builder();
      // Add system metadata
      Metadata metadata = systemMetadata.get(entity);
      if (metadata != null) {
        builder.put(MetadataScope.SYSTEM,
                    new co.cask.cdap.proto.metadata.Metadata(metadata.getProperties(), metadata.getTags()));
      }

      // Add user metadata
      metadata = userMetadata.get(entity);
      if (metadata != null) {
        builder.put(MetadataScope.USER,
                    new co.cask.cdap.proto.metadata.Metadata(metadata.getProperties(), metadata.getTags()));
      }

      // Create result
      result.add(new MetadataSearchResultRecord(entity, builder.build()));
    }
    return result;
  }

  @Override
  public Set<MetadataRecord> getSnapshotBeforeTime(final Set<NamespacedEntityId> namespacedEntityIds,
                                                   final long timeMillis) {
    return ImmutableSet.<MetadataRecord>builder()
      .addAll(getSnapshotBeforeTime(MetadataScope.USER, namespacedEntityIds, timeMillis))
      .addAll(getSnapshotBeforeTime(MetadataScope.SYSTEM, namespacedEntityIds, timeMillis))
      .build();
  }

  @Override
  public Set<MetadataRecord> getSnapshotBeforeTime(MetadataScope scope,
                                                   final Set<NamespacedEntityId> namespacedEntityIds,
                                                   final long timeMillis) {
    Set<Metadata> metadataHistoryEntries =
      execute(new TransactionExecutor.Function<MetadataDataset, Set<Metadata>>() {
        @Override
        public Set<Metadata> apply(MetadataDataset input) throws Exception {
          return input.getSnapshotBeforeTime(namespacedEntityIds, timeMillis);
        }
      }, scope);

    ImmutableSet.Builder<MetadataRecord> builder = ImmutableSet.builder();
    for (Metadata metadata : metadataHistoryEntries) {
      builder.add(new MetadataRecord(metadata.getEntityId(), scope,
                                     metadata.getProperties(), metadata.getTags()));
    }
    return builder.build();
  }

  @Override
  public void rebuildIndexes() {
    byte[] row = null;
    while ((row = rebuildIndex(row, MetadataScope.SYSTEM)) != null) {
      LOG.debug("Completed a batch for rebuilding system metadata indexes.");
    }
    while ((row = rebuildIndex(row, MetadataScope.USER)) != null) {
      LOG.debug("Completed a batch for rebuilding business metadata indexes.");
    }
  }

  @Override
  public void deleteAllIndexes() {
    while (deleteBatch(MetadataScope.SYSTEM) != 0) {
      LOG.debug("Deleted a batch of system metadata indexes.");
    }
    while (deleteBatch(MetadataScope.USER) != 0) {
      LOG.debug("Deleted a batch of business metadata indexes.");
    }
  }

  private void publishAudit(MetadataRecord previous, MetadataRecord additions, MetadataRecord deletions) {
    MetadataPayloadBuilder builder = new MetadataPayloadBuilder();
    builder.addPrevious(previous);
    builder.addAdditions(additions);
    builder.addDeletions(deletions);
    AuditPublishers.publishAudit(auditPublisher, previous.getEntityId(), AuditType.METADATA_CHANGE, builder.build());
  }

  private <T> T execute(TransactionExecutor.Function<MetadataDataset, T> func, MetadataScope scope) {
    MetadataDataset metadataDataset = newMetadataDataset(scope);
    TransactionExecutor txExecutor = Transactions.createTransactionExecutor(txExecutorFactory, metadataDataset);
    return txExecutor.executeUnchecked(func, metadataDataset);
  }

  private void execute(TransactionExecutor.Procedure<MetadataDataset> func, MetadataScope scope) {
    MetadataDataset metadataDataset = newMetadataDataset(scope);
    TransactionExecutor txExecutor = Transactions.createTransactionExecutor(txExecutorFactory, metadataDataset);
    txExecutor.executeUnchecked(func, metadataDataset);
  }

  private byte[] rebuildIndex(final byte[] startRowKey, MetadataScope scope) {
    return execute(new TransactionExecutor.Function<MetadataDataset, byte[]>() {
      @Override
      public byte[] apply(MetadataDataset input) throws Exception {
        return input.rebuildIndexes(startRowKey, BATCH_SIZE);
      }
    }, scope);
  }

  private int deleteBatch(MetadataScope scope) {
    return execute(new TransactionExecutor.Function<MetadataDataset, Integer>() {
      @Override
      public Integer apply(MetadataDataset input) throws Exception {
        return input.deleteAllIndexes(BATCH_SIZE);
      }
    }, scope);
  }

  private MetadataDataset newMetadataDataset(MetadataScope scope) {
    try {
      return DatasetsUtil.getOrCreateDataset(
        dsFramework, getMetadataDatasetInstance(scope), MetadataDataset.class.getName(),
        DatasetProperties.builder().add(MetadataDatasetDefinition.SCOPE_KEY, scope.name()).build(),
        DatasetDefinition.NO_ARGUMENTS, null);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private DatasetId getMetadataDatasetInstance(MetadataScope scope) {
    return MetadataScope.USER == scope ? BUSINESS_METADATA_INSTANCE_ID : SYSTEM_METADATA_INSTANCE_ID;
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework}. Used by the upgrade tool to upgrade Metadata
   * Datasets.
   *
   * @param framework Dataset framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework framework) throws IOException, DatasetManagementException {
    framework.addInstance(MetadataDataset.class.getName(), BUSINESS_METADATA_INSTANCE_ID, DatasetProperties.EMPTY);
    framework.addInstance(MetadataDataset.class.getName(), SYSTEM_METADATA_INSTANCE_ID, DatasetProperties.EMPTY);
  }
}
