/*
 * Copyright 2015-2017 Cask Data, Inc.
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
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.metadata.MetadataRecordV2;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.common.utils.ProjectInfo;
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

  private static final DatasetId BUSINESS_METADATA_INSTANCE_ID = NamespaceId.SYSTEM.dataset("business.metadata");
  private static final DatasetId SYSTEM_METADATA_INSTANCE_ID = NamespaceId.SYSTEM.dataset("system.metadata");

  //Special tag in the Metadata Dataset to mark Upgrade Status
  private static final String NEEDS_UPGRADE_TAG = "cdap.metadatadataset.needs_upgrade";
  private static final String VERSION_TAG_PREFIX = "cdap.version:";

  private static final Comparator<Map.Entry<MetadataEntity, Integer>> SEARCH_RESULT_DESC_SCORE_COMPARATOR =
    (o1, o2) -> {
      // sort in descending order
      return o2.getValue() - o1.getValue();
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
   * Adds/updates metadata for the specified {@link MetadataEntity}.
   */
  @Override
  public void setProperties(final MetadataScope scope, final MetadataEntity metadataEntity,
                            final Map<String, String> properties) {
    final AtomicReference<MetadataRecordV2> previousRef = new AtomicReference<>();
    execute(mds -> {
      Map<String, String> existingProperties = mds.getProperties(metadataEntity);
      Set<String> existingTags = mds.getTags(metadataEntity);
      previousRef.set(new MetadataRecordV2(metadataEntity, scope, existingProperties, existingTags));
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        mds.setProperty(metadataEntity, entry.getKey(), entry.getValue());
      }
    }, scope);
    final ImmutableMap.Builder<String, String> propAdditions = ImmutableMap.builder();
    final ImmutableMap.Builder<String, String> propDeletions = ImmutableMap.builder();
    MetadataRecordV2 previousRecord = previousRef.get();
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
    publishAudit(previousRecord, new MetadataRecordV2(metadataEntity, scope, propAdditions.build(), EMPTY_TAGS),
                 new MetadataRecordV2(metadataEntity, scope, propDeletions.build(), EMPTY_TAGS));
  }

  @Override
  public void setProperty(final MetadataScope scope, final MetadataEntity metadataEntity, final String key,
                          final String value) {
    final AtomicReference<MetadataRecordV2> previousRef = new AtomicReference<>();
    execute(mds -> {
      Map<String, String> existingProperties = mds.getProperties(metadataEntity);
      Set<String> existingTags = mds.getTags(metadataEntity);
      previousRef.set(new MetadataRecordV2(metadataEntity, scope, existingProperties, existingTags));
      mds.setProperty(metadataEntity, key, value);
    }, scope);
    publishAudit(previousRef.get(),
                 new MetadataRecordV2(metadataEntity, scope, ImmutableMap.of(key, value), EMPTY_TAGS),
                 new MetadataRecordV2(metadataEntity, scope));
  }

  /**
   * Adds tags for the specified {@link MetadataEntity}.
   */
  @Override
  public void addTags(final MetadataScope scope, final MetadataEntity metadataEntity,
                      final String... tagsToAdd) {
    final AtomicReference<MetadataRecordV2> previousRef = new AtomicReference<>();
    execute(mds -> {
      Map<String, String> existingProperties = mds.getProperties(metadataEntity);
      Set<String> existingTags = mds.getTags(metadataEntity);
      previousRef.set(new MetadataRecordV2(metadataEntity, scope, existingProperties, existingTags));
      mds.addTags(metadataEntity, tagsToAdd);
    }, scope);
    publishAudit(previousRef.get(),
                 new MetadataRecordV2(metadataEntity, scope, EMPTY_PROPERTIES, Sets.newHashSet(tagsToAdd)),
                 new MetadataRecordV2(metadataEntity, scope));
  }

  @Override
  public Set<MetadataRecordV2> getMetadata(MetadataEntity metadataEntity) {
    return ImmutableSet.of(getMetadata(MetadataScope.USER, metadataEntity), getMetadata(MetadataScope.SYSTEM,
                                                                                            metadataEntity));
  }

  @Override
  public MetadataRecordV2 getMetadata(final MetadataScope scope, final MetadataEntity metadataEntity) {
    return execute(mds -> {
      Map<String, String> properties = mds.getProperties(metadataEntity);
      Set<String> tags = mds.getTags(metadataEntity);
      return new MetadataRecordV2(metadataEntity, scope, properties, tags);
    }, scope);
  }

  /**
   * @return a set of {@link MetadataRecordV2}s representing all the metadata (including properties and tags)
   * for the specified set of {@link MetadataEntity}s.
   */
  @Override
  public Set<MetadataRecordV2> getMetadata(final MetadataScope scope, final Set<MetadataEntity> metadataEntities) {
    return execute(mds -> {
      Set<MetadataRecordV2> metadataRecordV2s = new HashSet<>(metadataEntities.size());
      for (MetadataEntity metadataEntity : metadataEntities) {
        Map<String, String> properties = mds.getProperties(metadataEntity);
        Set<String> tags = mds.getTags(metadataEntity);
        metadataRecordV2s.add(new MetadataRecordV2(metadataEntity, scope, properties, tags));
      }
      return metadataRecordV2s;
    }, scope);
  }

  @Override
  public Map<String, String> getProperties(MetadataEntity metadataEntity) {
    return ImmutableMap.<String, String>builder()
      .putAll(getProperties(MetadataScope.USER, metadataEntity))
      .putAll(getProperties(MetadataScope.SYSTEM, metadataEntity))
      .build();
  }

  /**
   * @return the metadata for the specified {@link MetadataEntity}
   */
  @Override
  public Map<String, String> getProperties(MetadataScope scope, final MetadataEntity metadataEntity) {
    return execute(mds -> {
      return mds.getProperties(metadataEntity);
    }, scope);
  }

  @Override
  public Set<String> getTags(MetadataEntity metadataEntity) {
    return ImmutableSet.<String>builder()
      .addAll(getTags(MetadataScope.USER, metadataEntity))
      .addAll(getTags(MetadataScope.SYSTEM, metadataEntity))
      .build();
  }

  /**
   * @return the tags for the specified {@link MetadataEntity}
   */
  @Override
  public Set<String> getTags(MetadataScope scope, final MetadataEntity metadataEntity) {
    return execute(mds -> {
      return mds.getTags(metadataEntity);
    }, scope);
  }

  @Override
  public void removeMetadata(MetadataEntity metadataEntity) {
    removeMetadata(MetadataScope.USER, metadataEntity);
    removeMetadata(MetadataScope.SYSTEM, metadataEntity);
  }

  /**
   * Removes all metadata (including properties and tags) for the specified {@link MetadataEntity}.
   */
  @Override
  public void removeMetadata(final MetadataScope scope, final MetadataEntity metadataEntity) {
    final AtomicReference<MetadataRecordV2> previousRef = new AtomicReference<>();
    execute(mds -> {
      previousRef.set(new MetadataRecordV2(metadataEntity, scope, mds.getProperties(metadataEntity),
                                           mds.getTags(metadataEntity)));
      mds.removeProperties(metadataEntity);
      mds.removeTags(metadataEntity);
    }, scope);
    MetadataRecordV2 previous = previousRef.get();
    publishAudit(previous, new MetadataRecordV2(metadataEntity, scope), new MetadataRecordV2(previous));
  }

  /**
   * Removes all properties for the specified {@link MetadataEntity}.
   */
  @Override
  public void removeProperties(final MetadataScope scope, final MetadataEntity metadataEntity) {
    final AtomicReference<MetadataRecordV2> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecordV2(metadataEntity, scope, input.getProperties(metadataEntity),
                                             input.getTags(metadataEntity)));
        input.removeProperties(metadataEntity);
      }
    }, scope);
    publishAudit(previousRef.get(), new MetadataRecordV2(metadataEntity, scope),
                 new MetadataRecordV2(metadataEntity, scope, previousRef.get().getProperties(), EMPTY_TAGS));
  }

  /**
   * Removes the specified properties of the {@link MetadataEntity}.
   */
  @Override
  public void removeProperties(final MetadataScope scope, final MetadataEntity metadataEntity,
                               final String... keys) {
    final AtomicReference<MetadataRecordV2> previousRef = new AtomicReference<>();
    final ImmutableMap.Builder<String, String> deletesBuilder = ImmutableMap.builder();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecordV2(metadataEntity, scope, input.getProperties(metadataEntity),
                                             input.getTags(metadataEntity)));
        for (String key : keys) {
          MetadataEntry record = input.getProperty(metadataEntity, key);
          if (record == null) {
            continue;
          }
          deletesBuilder.put(record.getKey(), record.getValue());
        }
        input.removeProperties(metadataEntity, keys);
      }
    }, scope);
    publishAudit(previousRef.get(), new MetadataRecordV2(metadataEntity, scope),
                 new MetadataRecordV2(metadataEntity, scope, deletesBuilder.build(), EMPTY_TAGS));
  }

  /**
   * Removes all the tags from the {@link MetadataEntity}
   */
  @Override
  public void removeTags(final MetadataScope scope, final MetadataEntity metadataEntity) {
    final AtomicReference<MetadataRecordV2> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecordV2(metadataEntity, scope, input.getProperties(metadataEntity),
                                             input.getTags(metadataEntity)));
        input.removeTags(metadataEntity);
      }
    }, scope);
    MetadataRecordV2 previous = previousRef.get();
    publishAudit(previous, new MetadataRecordV2(metadataEntity, scope),
                 new MetadataRecordV2(metadataEntity, scope, EMPTY_PROPERTIES, previous.getTags()));
  }

  /**
   * Removes the specified tags from the {@link MetadataEntity}
   */
  @Override
  public void removeTags(final MetadataScope scope, final MetadataEntity metadataEntity,
                         final String... tagsToRemove) {
    final AtomicReference<MetadataRecordV2> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecordV2(metadataEntity, scope, input.getProperties(metadataEntity),
                                             input.getTags(metadataEntity)));
        input.removeTags(metadataEntity, tagsToRemove);
      }
    }, scope);
    publishAudit(previousRef.get(), new MetadataRecordV2(metadataEntity, scope),
                 new MetadataRecordV2(metadataEntity, scope, EMPTY_PROPERTIES, Sets.newHashSet(tagsToRemove)));
  }

  @Override
  public MetadataSearchResponse search(String namespaceId, String searchQuery,
                                       Set<EntityTypeSimpleName> types,
                                       SortInfo sortInfo, int offset, int limit,
                                       int numCursors, String cursor, boolean showHidden,
                                       Set<EntityScope> entityScope) {
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
                                        Set<EntityScope> entityScope) {
    if (offset < 0) {
      throw new IllegalArgumentException("offset must not be negative");
    }

    if (limit < 0) {
      throw new IllegalArgumentException("limit must not be negative");
    }

    List<MetadataEntry> results = new LinkedList<>();
    List<String> cursors = new LinkedList<>();
    for (MetadataScope scope : scopes) {
      SearchResults searchResults =
        getSearchResults(scope, namespaceId, searchQuery, types, sortInfo, offset, limit, numCursors, cursor,
                         showHidden, entityScope);
      results.addAll(searchResults.getResults());
      cursors.addAll(searchResults.getCursors());
    }

    // sort if required
    Set<MetadataEntity> sortedEntities = getSortedEntities(results, sortInfo);
    int total = sortedEntities.size();

    // pagination is not performed at the dataset level, because:
    // 1. scoring is needed for DEFAULT sort info. So perform it here for now.
    // 2. Even when using custom sorting, we need to remove elements from the beginning to the offset and the cursors
    //    at the end
    // TODO: Figure out how all of this can be done server (HBase) side
    int startIndex = Math.min(offset, sortedEntities.size());
    int endIndex = (int) Math.min(Integer.MAX_VALUE, (long) offset + limit); // Account for overflow
    endIndex = Math.min(endIndex, sortedEntities.size());

    // add 1 to maxIndex because end index is exclusive
    sortedEntities = new LinkedHashSet<>(
      ImmutableList.copyOf(sortedEntities).subList(startIndex, endIndex)
    );

    // Fetch metadata for entities in the result list
    // Note: since the fetch is happening in a different transaction, the metadata for entities may have been
    // removed. It is okay not to have metadata for some results in case this happens.
    Map<MetadataEntity, Metadata> systemMetadata = fetchMetadata(sortedEntities, MetadataScope.SYSTEM);
    Map<MetadataEntity, Metadata> userMetadata = fetchMetadata(sortedEntities, MetadataScope.USER);

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
                                         final Set<EntityScope> entityScope) {
    return execute(
      mds -> {
        return mds.search(namespaceId, searchQuery, types, sortInfo, offset, limit, numCursors, cursor, showHidden,
                            entityScope);
      }, scope);
  }

  private Set<MetadataEntity> getSortedEntities(List<MetadataEntry> results, SortInfo sortInfo) {
    // if sort order is not weighted, return entities in the order received.
    // in this case, the backing storage is expected to return results in the expected order.
    if (SortInfo.SortOrder.WEIGHTED != sortInfo.getSortOrder()) {
      Set<MetadataEntity> entities = new LinkedHashSet<>(results.size());
      for (MetadataEntry metadataEntry : results) {
        entities.add(metadataEntry.getMetadataEntity());
      }
      return entities;
    }
    // if sort order is weighted, score results by weight, and return in descending order of weights
    // Score results
    final Map<MetadataEntity, Integer> weightedResults = new HashMap<>();
    for (MetadataEntry metadataEntry : results) {

      weightedResults.put(metadataEntry.getMetadataEntity(),
                          weightedResults.getOrDefault(metadataEntry.getTargetId().toMetadataEntity(), 0) + 1);
    }

    // Sort the results by score
    List<Map.Entry<MetadataEntity, Integer>> resultList = new ArrayList<>(weightedResults.entrySet());
    resultList.sort(SEARCH_RESULT_DESC_SCORE_COMPARATOR);
    Set<MetadataEntity> result = new LinkedHashSet<>(resultList.size());
    for (Map.Entry<MetadataEntity, Integer> entry : resultList) {
      result.add(entry.getKey());
    }
    return result;
  }

  private Map<MetadataEntity, Metadata> fetchMetadata(final Set<MetadataEntity> metadataEntities,
                                                          MetadataScope scope) {
    Set<Metadata> metadataSet =
      execute(new TransactionExecutor.Function<MetadataDataset, Set<Metadata>>() {
        @Override
        public Set<Metadata> apply(MetadataDataset input) throws Exception {
          return input.getMetadata(metadataEntities);
        }
      }, scope);
    Map<MetadataEntity, Metadata> metadataMap = new HashMap<>();
    for (Metadata m : metadataSet) {
      metadataMap.put(m.getMetadataEntity(), m);
    }
    return metadataMap;
  }

  private Set<MetadataSearchResultRecord> addMetadataToEntities(Set<MetadataEntity> entities,
                                                                Map<MetadataEntity, Metadata> systemMetadata,
                                                                Map<MetadataEntity, Metadata> userMetadata) {
    Set<MetadataSearchResultRecord> result = new LinkedHashSet<>();
    for (MetadataEntity entity : entities) {
      ImmutableMap.Builder<MetadataScope, co.cask.cdap.api.metadata.Metadata> builder = ImmutableMap.builder();
      // Add system metadata
      Metadata metadata = systemMetadata.get(entity);
      if (metadata != null) {
        builder.put(MetadataScope.SYSTEM,
                    new co.cask.cdap.api.metadata.Metadata(metadata.getProperties(), metadata.getTags()));
      }

      // Add user metadata
      metadata = userMetadata.get(entity);
      if (metadata != null) {
        builder.put(MetadataScope.USER,
                    new co.cask.cdap.api.metadata.Metadata(metadata.getProperties(), metadata.getTags()));
      }

      // Create result
      result.add(new MetadataSearchResultRecord(entity, builder.build()));
    }
    return result;
  }

  @Override
  public Set<MetadataRecordV2> getSnapshotBeforeTime(final Set<MetadataEntity> metadataEntities,
                                                     final long timeMillis) {
    return ImmutableSet.<MetadataRecordV2>builder()
      .addAll(getSnapshotBeforeTime(MetadataScope.USER, metadataEntities, timeMillis))
      .addAll(getSnapshotBeforeTime(MetadataScope.SYSTEM, metadataEntities, timeMillis))
      .build();
  }

  @Override
  public Set<MetadataRecordV2> getSnapshotBeforeTime(MetadataScope scope,
                                                     final Set<MetadataEntity> metadataEntities,
                                                     final long timeMillis) {
    Set<Metadata> metadataHistoryEntries =
      execute(new TransactionExecutor.Function<MetadataDataset, Set<Metadata>>() {
        @Override
        public Set<Metadata> apply(MetadataDataset input) throws Exception {
          return input.getSnapshotBeforeTime(metadataEntities, timeMillis);
        }
      }, scope);

    ImmutableSet.Builder<MetadataRecordV2> builder = ImmutableSet.builder();
    for (Metadata metadata : metadataHistoryEntries) {
      builder.add(new MetadataRecordV2(metadata.getEntityId(), scope,
                                       metadata.getProperties(), metadata.getTags()));
    }
    return builder.build();
  }

  @Override
  public void rebuildIndexes(MetadataScope scope,
                             RetryStrategy retryStrategy) {
    byte[] row = null;
    while ((row = rebuildIndexesWithRetries(scope, row, retryStrategy)) != null) {
      LOG.debug("Completed a batch for rebuilding {} metadata indexes.", scope);
    }
  }

  private byte[] rebuildIndexesWithRetries(final MetadataScope scope,
                             final byte[] row, RetryStrategy retryStrategy) {
    byte[] returnRow;
    try {
      returnRow = Retries.callWithRetries(new Retries.Callable<byte[], Exception>() {
        @Override
        public byte[] call() throws Exception {
          // Run data migration
          return rebuildIndex(row, scope);
        }
      }, retryStrategy);
    } catch (Exception e) {
      LOG.error("Failed to reIndex while Upgrading Metadata Dataset.", e);
      throw new RuntimeException(e);
    }
    return returnRow;
  }

  @Override
  public void deleteAllIndexes(MetadataScope scope) {
    while (deleteBatch(scope) != 0) {
      LOG.debug("Deleted a batch of {} metadata indexes.", scope);
    }
  }

  private void publishAudit(MetadataRecordV2 previous, MetadataRecordV2 additions, MetadataRecordV2 deletions) {
    MetadataPayloadBuilder builder = new MetadataPayloadBuilder();
    builder.addPrevious(previous);
    builder.addAdditions(additions);
    builder.addDeletions(deletions);
    AuditPublishers.publishAudit(auditPublisher, previous.getMetadataEntity(), AuditType.METADATA_CHANGE,
                                 builder.build());
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
        DatasetDefinition.NO_ARGUMENTS);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void removeNullOrEmptyTags(final DatasetId metadataDatasetInstance, final MetadataScope scope) {
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset dataset) throws Exception {
        dataset.removeNullOrEmptyTags(metadataDatasetInstance.toMetadataEntity());
      }
    }, scope);
  }

  @Override
  public void createOrUpgrade(MetadataScope scope) throws DatasetManagementException, IOException {
    DatasetId datasetId = getMetadataDatasetInstance(scope);
    if (dsFramework.hasInstance(datasetId)) {
      if (isUpgradeRequired(scope)) {
        dsFramework.updateInstance(
          datasetId,
          DatasetProperties.builder().add(MetadataDatasetDefinition.SCOPE_KEY, scope.name()).build()
        );
        removeNullOrEmptyTags(datasetId, scope);
      }
    } else {
      DatasetsUtil.createIfNotExists(
        dsFramework, datasetId, MetadataDataset.class.getName(),
        DatasetProperties.builder().add(MetadataDatasetDefinition.SCOPE_KEY, scope.name()).build());
      markUpgradeComplete(scope);
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

  @Override
  public void markUpgradeComplete(MetadataScope scope) throws DatasetManagementException, IOException {
    DatasetId datasetId = getMetadataDatasetInstance(scope);
    LOG.info("Add Upgrade tag with version {} to {}", ProjectInfo.getVersion().toString(), datasetId);
    addTags(scope, datasetId.toMetadataEntity(), getTagWithVersion(ProjectInfo.getVersion().toString()));
    removeTags(scope, datasetId.toMetadataEntity(), NEEDS_UPGRADE_TAG);
  }

  @Override
  public boolean isUpgradeRequired(MetadataScope scope) throws DatasetManagementException {
    DatasetId datasetId = getMetadataDatasetInstance(scope);
    Set<String> tags = getTags(scope, datasetId.toMetadataEntity());
    // Check if you are in the process of an Upgrade
    if (tags.contains(NEEDS_UPGRADE_TAG)) {
      LOG.debug("NEEDS_UPGRADE_TAG found on Metadata Dataset. Upgrade is required.");
      return true;
    }
    // If no tag was found or Version tag does not match current version
    boolean versionTagFound = false;
    for (String tag: tags) {
      if (tag.startsWith(VERSION_TAG_PREFIX)) {
        versionTagFound = true;
        String datasetVersion = getVersionFromVersionTag(tag);
        if (!datasetVersion.equals(ProjectInfo.getVersion().toString())) {
          LOG.debug("Metadata Dataset version mismatch. Needs Upgrade");
          removeTags(scope, datasetId.toMetadataEntity(), tag);
          addTags(scope, datasetId.toMetadataEntity(), NEEDS_UPGRADE_TAG);
          return true;
        }
      }
    }
    if (!versionTagFound) {
      addTags(scope, datasetId.toMetadataEntity(), NEEDS_UPGRADE_TAG);
      return true;
    }
    return false;
  }

  private String getVersionFromVersionTag(String tag) {
    return tag.substring(VERSION_TAG_PREFIX.length());
  }

  private String getTagWithVersion(String version) {
    return new String (VERSION_TAG_PREFIX + version);
  }
}
