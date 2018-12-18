/*
 * Copyright 2015-2018 Cask Data, Inc.
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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.InstanceNotFoundException;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.metadata.MetadataRecordV2;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.audit.AuditPublisher;
import co.cask.cdap.data2.audit.AuditPublishers;
import co.cask.cdap.data2.audit.payload.builder.MetadataPayloadBuilder;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.metadata.dataset.Metadata;
import co.cask.cdap.data2.metadata.dataset.MetadataChange;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.MetadataDatasetDefinition;
import co.cask.cdap.data2.metadata.dataset.MetadataEntry;
import co.cask.cdap.data2.metadata.dataset.SearchRequest;
import co.cask.cdap.data2.metadata.dataset.SearchResults;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.metadata.MetadataSearchResponseV2;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecordV2;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionSystemClient;
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
 * Implementation of {@link MetadataStore}.
 */
public class DefaultMetadataStore implements MetadataStore {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadataStore.class);
  private static final Map<String, String> EMPTY_PROPERTIES = ImmutableMap.of();
  private static final Set<String> EMPTY_TAGS = ImmutableSet.of();

  private static final DatasetId V2_BUSINESS_METADATA_INSTANCE_ID = NamespaceId.SYSTEM.dataset("v2.business");
  private static final DatasetId V2_SYSTEM_METADATA_INSTANCE_ID = NamespaceId.SYSTEM.dataset("v2.system");

  private static final Comparator<Map.Entry<MetadataEntity, Integer>> SEARCH_RESULT_DESC_SCORE_COMPARATOR =
    (o1, o2) -> {
      // sort in descending order
      return o2.getValue() - o1.getValue();
    };


  private final Transactional transactional;
  private final DatasetFramework dsFramework;
  private final DynamicDatasetCache datasetCache;
  private AuditPublisher auditPublisher;

  @Inject
  DefaultMetadataStore(TransactionSystemClient txClient, DatasetFramework dsFramework) {
    this.datasetCache = new MultiThreadDatasetCache(new SystemDatasetInstantiator(dsFramework),
                                                    new TransactionSystemClientAdapter(txClient),
                                                    NamespaceId.SYSTEM, Collections.emptyMap(), null, null);
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(datasetCache),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.dsFramework = dsFramework;
  }

  /**
   * Don't use this for anything other than unit tests. Deletes and re-creates the underlying datasets.
   *
   * @throws Exception if there was an error deleting and re-creating the underlying datasets
   */
  @SuppressWarnings("ResultOfMethodCallIgnored")
  @VisibleForTesting
  void deleteDatasets() throws Exception {
    datasetCache.invalidate();
    try {
      dsFramework.deleteInstance(V2_BUSINESS_METADATA_INSTANCE_ID);
    } catch (InstanceNotFoundException e) {
      // it's ok if it doesn't exist, we wanted to delete it anyway
    }
    try {
      dsFramework.deleteInstance(V2_SYSTEM_METADATA_INSTANCE_ID);
    } catch (InstanceNotFoundException e) {
      // it's ok if it doesn't exist, we wanted to delete it anyway
    }
  }

  @SuppressWarnings("unused")
  @Inject(optional = true)
  public void setAuditPublisher(AuditPublisher auditPublisher) {
    this.auditPublisher = auditPublisher;
  }

  @Override
  public void setProperties(final MetadataScope scope, final MetadataEntity metadataEntity,
                            final Map<String, String> properties) {
    MetadataChange metadataChange = execute(mds -> mds.setProperties(metadataEntity, properties), scope);
    MetadataRecordV2 previous = new MetadataRecordV2(metadataEntity, scope,
                                                     metadataChange.getExisting().getProperties(),
                                                     metadataChange.getExisting().getTags());
    final ImmutableMap.Builder<String, String> propAdditions = ImmutableMap.builder();
    final ImmutableMap.Builder<String, String> propDeletions = ImmutableMap.builder();
    // Iterating over properties all over again, because we want to move the diff calculation outside the transaction.
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String existingValue = previous.getProperties().get(entry.getKey());
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
    publishAudit(previous, new MetadataRecordV2(metadataEntity, scope, propAdditions.build(), EMPTY_TAGS),
                 new MetadataRecordV2(metadataEntity, scope, propDeletions.build(), EMPTY_TAGS));
  }

  @Override
  public void setProperty(final MetadataScope scope, final MetadataEntity metadataEntity, final String key,
                          final String value) {
    MetadataChange change = execute(mds -> mds.setProperty(metadataEntity, key, value), scope);
    publishAudit(new MetadataRecordV2(metadataEntity, scope, change.getExisting().getProperties(),
                                      change.getExisting().getTags()),
                 new MetadataRecordV2(metadataEntity, scope, ImmutableMap.of(key, value), EMPTY_TAGS),
                 new MetadataRecordV2(metadataEntity, scope));
  }

  /**
   * Adds tags for the specified {@link MetadataEntity}.
   */
  @Override
  public void addTags(final MetadataScope scope, final MetadataEntity metadataEntity,
                      final Set<String> tagsToAdd) {
    MetadataChange metadataChange = execute(mds -> mds.addTags(metadataEntity, tagsToAdd), scope);
    publishAudit(new MetadataRecordV2(metadataEntity, scope, metadataChange.getExisting().getProperties(),
                                      metadataChange.getExisting().getTags()),
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
      Metadata metadata = mds.getMetadata(metadataEntity);
      return new MetadataRecordV2(metadataEntity, scope, metadata.getProperties(), metadata.getTags());
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
      // do a batch get
      Set<Metadata> metadatas = mds.getMetadata(metadataEntities);
      for (Metadata metadata : metadatas) {
        metadataRecordV2s.add(new MetadataRecordV2(metadata.getMetadataEntity(), scope, metadata.getProperties(),
                                                   metadata.getTags()));
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
    return execute(mds -> mds.getProperties(metadataEntity), scope);
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
    return execute(mds -> mds.getTags(metadataEntity), scope);
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
    MetadataChange metadataChange = execute(mds -> mds.removeMetadata(metadataEntity), scope);
    MetadataRecordV2 previous = new MetadataRecordV2(metadataEntity, scope,
                                                     metadataChange.getExisting().getProperties(),
                                                     metadataChange.getExisting().getTags());
    publishAudit(previous, new MetadataRecordV2(metadataEntity, scope), new MetadataRecordV2(previous));
  }

  /**
   * Removes all properties for the specified {@link MetadataEntity}.
   */
  @Override
  public void removeProperties(final MetadataScope scope, final MetadataEntity metadataEntity) {
    MetadataChange metadataChange = execute(mds -> mds.removeProperties(metadataEntity), scope);
    MetadataRecordV2 previous = new MetadataRecordV2(metadataEntity, scope,
                                                     metadataChange.getExisting().getProperties(),
                                                     metadataChange.getExisting().getTags());
    publishAudit(previous, new MetadataRecordV2(metadataEntity, scope),
                 new MetadataRecordV2(metadataEntity, scope, previous.getProperties(), EMPTY_TAGS));
  }

  /**
   * Removes the specified properties of the {@link MetadataEntity}.
   */
  @Override
  public void removeProperties(final MetadataScope scope, final MetadataEntity metadataEntity,
                               final Set<String> keys) {
    final AtomicReference<Map<String, String>> deletes = new AtomicReference<>();
    MetadataChange metadataChange = execute(mds -> {
      MetadataChange change = mds.removeProperties(metadataEntity, keys);
      deletes.set(change.getDeletedProperties());
      return change;
    }, scope);
    publishAudit(new MetadataRecordV2(metadataEntity, scope, metadataChange.getExisting().getProperties(),
                                      metadataChange.getExisting().getTags()),
                 new MetadataRecordV2(metadataEntity, scope),
                 new MetadataRecordV2(metadataEntity, scope, deletes.get(), EMPTY_TAGS));
  }

  /**
   * Removes all the tags from the {@link MetadataEntity}
   */
  @Override
  public void removeTags(final MetadataScope scope, final MetadataEntity metadataEntity) {
    MetadataChange metadataChange = execute(mds -> mds.removeTags(metadataEntity), scope);
    MetadataRecordV2 previous = new MetadataRecordV2(metadataEntity, scope,
                                                     metadataChange.getExisting().getProperties(),
                                                     metadataChange.getExisting().getTags());
    publishAudit(previous, new MetadataRecordV2(metadataEntity, scope),
                 new MetadataRecordV2(metadataEntity, scope, EMPTY_PROPERTIES, previous.getTags()));
  }

  /**
   * Removes the specified tags from the {@link MetadataEntity}
   */
  @Override
  public void removeTags(final MetadataScope scope, final MetadataEntity metadataEntity,
                         final Set<String> tagsToRemove) {
    MetadataChange metadataChange = execute(mds -> mds.removeTags(metadataEntity, tagsToRemove), scope);
    publishAudit(new MetadataRecordV2(metadataEntity, scope, metadataChange.getExisting().getProperties(),
                                      metadataChange.getExisting().getTags()),
                 new MetadataRecordV2(metadataEntity, scope),
                 new MetadataRecordV2(metadataEntity, scope, EMPTY_PROPERTIES, Sets.newHashSet(tagsToRemove)));
  }

  @Override
  public MetadataSearchResponseV2 search(SearchRequest request) {
    Set<MetadataScope> searchScopes = EnumSet.allOf(MetadataScope.class);
    if ("*".equals(request.getQuery())) {
      if (SortInfo.DEFAULT.equals(request.getSortInfo())) {
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
    return search(searchScopes, request);
  }

  private MetadataSearchResponseV2 search(Set<MetadataScope> scopes, SearchRequest request) {
    List<MetadataEntry> results = new LinkedList<>();
    List<String> cursors = new LinkedList<>();
    for (MetadataScope scope : scopes) {
      SearchResults searchResults = execute(mds -> mds.search(request), scope);
      results.addAll(searchResults.getResults());
      cursors.addAll(searchResults.getCursors());
    }

    int offset = request.getOffset();
    int limit = request.getLimit();
    SortInfo sortInfo = request.getSortInfo();
    // sort if required
    Set<MetadataEntity> sortedEntities = getSortedEntities(results, sortInfo);
    int total = sortedEntities.size();

    // pagination is not performed at the dataset level, because:
    // 1. scoring is needed for DEFAULT sort info. So perform it here for now.
    // 2. Even when using custom sorting, we need to remove elements from the beginning to the offset and the cursors
    //    at the end
    // TODO: Figure out how all of this can be done server (HBase) side
    int startIndex = Math.min(request.getOffset(), sortedEntities.size());
    // Account for overflow
    int endIndex = (int) Math.min(Integer.MAX_VALUE, (long) offset + limit);
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

    return new MetadataSearchResponseV2(
      sortInfo.getSortBy() + " " + sortInfo.getSortOrder(), offset, limit, request.getNumCursors(), total,
      addMetadataToEntities(sortedEntities, systemMetadata, userMetadata), cursors, request.shouldShowHidden(),
      request.getEntityScopes());
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
                          weightedResults.getOrDefault(metadataEntry.getMetadataEntity(), 0) + 1);
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
    Set<Metadata> metadataSet = execute(mds -> mds.getMetadata(metadataEntities), scope);
    Map<MetadataEntity, Metadata> metadataMap = new HashMap<>();
    for (Metadata m : metadataSet) {
      metadataMap.put(m.getMetadataEntity(), m);
    }
    return metadataMap;
  }

  private Set<MetadataSearchResultRecordV2> addMetadataToEntities(Set<MetadataEntity> entities,
                                                                  Map<MetadataEntity, Metadata> systemMetadata,
                                                                  Map<MetadataEntity, Metadata> userMetadata) {
    Set<MetadataSearchResultRecordV2> result = new LinkedHashSet<>();
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
      result.add(new MetadataSearchResultRecordV2(entity, builder.build()));
    }
    return result;
  }

  @Override
  public Set<MetadataRecordV2> getSnapshotBeforeTime(MetadataScope scope,
                                                     final Set<MetadataEntity> metadataEntities,
                                                     final long timeMillis) {
    Set<Metadata> metadataHistoryEntries =
      execute(mds -> mds.getSnapshotBeforeTime(metadataEntities, timeMillis), scope);

    ImmutableSet.Builder<MetadataRecordV2> builder = ImmutableSet.builder();
    for (Metadata metadata : metadataHistoryEntries) {
      builder.add(new MetadataRecordV2(metadata.getEntityId(), scope,
                                       metadata.getProperties(), metadata.getTags()));
    }
    return builder.build();
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
    return Transactionals.execute(transactional, context -> {
      MetadataDataset metadataDataset = getMetadataDataset(context, dsFramework, scope);
      return func.apply(metadataDataset);
    });
  }

  // TODO (CDAP-14587): there should be no public accessor for the dataset
  public static MetadataDataset getMetadataDataset(DatasetContext context, DatasetFramework dsFramework,
                                                   MetadataScope scope) {
    try {
      return DatasetsUtil.getOrCreateDataset(context,
        dsFramework, getMetadataDatasetInstance(scope), MetadataDataset.class.getName(),
        DatasetProperties.builder().add(MetadataDatasetDefinition.SCOPE_KEY, scope.name()).build());
    } catch (DatasetManagementException | IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private static DatasetId getMetadataDatasetInstance(MetadataScope scope) {
    return MetadataScope.USER == scope ? V2_BUSINESS_METADATA_INSTANCE_ID : V2_SYSTEM_METADATA_INSTANCE_ID;
  }

  /**
   * Adds V2 datasets and types to the given {@link DatasetFramework}. Used by the upgrade tool.
   *
   * @param framework Dataset framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework framework) throws IOException, DatasetManagementException {
    framework.addInstance(MetadataDataset.class.getName(), V2_BUSINESS_METADATA_INSTANCE_ID, DatasetProperties.EMPTY);
    framework.addInstance(MetadataDataset.class.getName(), V2_SYSTEM_METADATA_INSTANCE_ID, DatasetProperties.EMPTY);
  }
}
