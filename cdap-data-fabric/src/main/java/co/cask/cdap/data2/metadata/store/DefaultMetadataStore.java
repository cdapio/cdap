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
import co.cask.cdap.data2.audit.AuditPublisher;
import co.cask.cdap.data2.audit.AuditPublishers;
import co.cask.cdap.data2.audit.payload.builder.MetadataPayloadBuilder;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.dataset.Metadata;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.MetadataEntry;
import co.cask.cdap.data2.metadata.indexer.Indexer;
import co.cask.cdap.data2.metadata.publisher.MetadataChangePublisher;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.id.NamespacedId;
import co.cask.cdap.proto.metadata.MetadataChangeRecord;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import com.google.common.base.Throwables;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Implementation of {@link MetadataStore} used in distributed mode.
 */
public class DefaultMetadataStore implements MetadataStore {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadataStore.class);
  private static final Id.DatasetInstance BUSINESS_METADATA_INSTANCE_ID =
    Id.DatasetInstance.from(Id.Namespace.SYSTEM, "business.metadata");
  private static final Id.DatasetInstance SYSTEM_METADATA_INSTANCE_ID =
    Id.DatasetInstance.from(Id.Namespace.SYSTEM, "system.metadata");
  private static final Map<String, String> EMPTY_PROPERTIES = ImmutableMap.of();
  private static final Set<String> EMPTY_TAGS = ImmutableSet.of();
  private static final int BATCH_SIZE = 1000;

  private static final Comparator<Map.Entry<NamespacedId, Integer>> SEARCH_RESULT_DESC_SCORE_COMPARATOR =
    new Comparator<Map.Entry<NamespacedId, Integer>>() {
      @Override
      public int compare(Map.Entry<NamespacedId, Integer> o1, Map.Entry<NamespacedId, Integer> o2) {
        // sort in descending order
        return o2.getValue() - o1.getValue();
      }
    };

  private final TransactionExecutorFactory txExecutorFactory;
  private final DatasetFramework dsFramework;
  private final MetadataChangePublisher changePublisher;
  private AuditPublisher auditPublisher;

  @Inject
  DefaultMetadataStore(TransactionExecutorFactory txExecutorFactory, DatasetFramework dsFramework,
                       MetadataChangePublisher changePublisher) {
    this.txExecutorFactory = txExecutorFactory;
    this.dsFramework = dsFramework;
    this.changePublisher = changePublisher;
  }


  @SuppressWarnings("unused")
  @Inject(optional = true)
  public void setAuditPublisher(AuditPublisher auditPublisher) {
    this.auditPublisher = auditPublisher;
  }

  @Override
  public void setProperties(MetadataScope scope, NamespacedId namespacedId, Map<String, String> properties) {
    setProperties(scope, namespacedId, properties, null);
  }

  /**
   * Adds/updates metadata for the specified {@link NamespacedId}.
   */
  @Override
  public void setProperties(final MetadataScope scope, final NamespacedId namespacedId,
                            final Map<String, String> properties, @Nullable final Indexer indexer) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        Map<String, String> existingProperties = input.getProperties(namespacedId);
        Set<String> existingTags = input.getTags(namespacedId);
        previousRef.set(new MetadataRecord(namespacedId, scope, existingProperties, existingTags));
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          input.setProperty(namespacedId, entry.getKey(), entry.getValue(), indexer);
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
    publish(previousRecord, new MetadataRecord(namespacedId, scope, propAdditions.build(), EMPTY_TAGS),
            new MetadataRecord(namespacedId, scope, propDeletions.build(), EMPTY_TAGS));
  }

  /**
   * Adds tags for the specified {@link NamespacedId}.
   */
  @Override
  public void addTags(final MetadataScope scope, final NamespacedId namespacedId, final String... tagsToAdd) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        Map<String, String> existingProperties = input.getProperties(namespacedId);
        Set<String> existingTags = input.getTags(namespacedId);
        previousRef.set(new MetadataRecord(namespacedId, scope, existingProperties, existingTags));
        input.addTags(namespacedId, tagsToAdd);
      }
    }, scope);
    publish(previousRef.get(), new MetadataRecord(namespacedId, scope, EMPTY_PROPERTIES, Sets.newHashSet(tagsToAdd)),
            new MetadataRecord(namespacedId, scope));
  }

  @Override
  public Set<MetadataRecord> getMetadata(NamespacedId namespacedId) {
    return ImmutableSet.of(getMetadata(MetadataScope.USER, namespacedId), getMetadata(MetadataScope.SYSTEM, namespacedId));
  }

  @Override
  public MetadataRecord getMetadata(final MetadataScope scope, final NamespacedId namespacedId) {
    return execute(new TransactionExecutor.Function<MetadataDataset, MetadataRecord>() {
      @Override
      public MetadataRecord apply(MetadataDataset input) throws Exception {
        Map<String, String> properties = input.getProperties(namespacedId);
        Set<String> tags = input.getTags(namespacedId);
        return new MetadataRecord(namespacedId, scope, properties, tags);
      }
    }, scope);
  }

  /**
   * @return a set of {@link MetadataRecord}s representing all the metadata (including properties and tags)
   * for the specified set of {@link NamespacedId}s.
   */
  @Override
  public Set<MetadataRecord> getMetadata(final MetadataScope scope, final Set<NamespacedId> namespacedIds) {
    return execute(new TransactionExecutor.Function<MetadataDataset, Set<MetadataRecord>>() {
      @Override
      public Set<MetadataRecord> apply(MetadataDataset input) throws Exception {
        Set<MetadataRecord> metadataRecords = new HashSet<>(namespacedIds.size());
        for (NamespacedId namespacedId : namespacedIds) {
          Map<String, String> properties = input.getProperties(namespacedId);
          Set<String> tags = input.getTags(namespacedId);
          metadataRecords.add(new MetadataRecord(namespacedId, scope, properties, tags));
        }
        return metadataRecords;
      }
    }, scope);
  }

  @Override
  public Map<String, String> getProperties(NamespacedId namespacedId) {
    return ImmutableMap.<String, String>builder()
      .putAll(getProperties(MetadataScope.USER, namespacedId))
      .putAll(getProperties(MetadataScope.SYSTEM, namespacedId))
      .build();
  }

  /**
   * @return the metadata for the specified {@link NamespacedId}
   */
  @Override
  public Map<String, String> getProperties(MetadataScope scope, final NamespacedId namespacedId) {
    return execute(new TransactionExecutor.Function<MetadataDataset, Map<String, String>>() {
      @Override
      public Map<String, String> apply(MetadataDataset input) throws Exception {
        return input.getProperties(namespacedId);
      }
    }, scope);
  }

  @Override
  public Set<String> getTags(NamespacedId namespacedId) {
    return ImmutableSet.<String>builder()
      .addAll(getTags(MetadataScope.USER, namespacedId))
      .addAll(getTags(MetadataScope.SYSTEM, namespacedId))
      .build();
  }

  /**
   * @return the tags for the specified {@link NamespacedId}
   */
  @Override
  public Set<String> getTags(MetadataScope scope, final NamespacedId namespacedId) {
    return execute(new TransactionExecutor.Function<MetadataDataset, Set<String>>() {
      @Override
      public Set<String> apply(MetadataDataset input) throws Exception {
        return input.getTags(namespacedId);
      }
    }, scope);
  }

  @Override
  public void removeMetadata(NamespacedId namespacedId) {
    removeMetadata(MetadataScope.USER, namespacedId);
    removeMetadata(MetadataScope.SYSTEM, namespacedId);
  }

  /**
   * Removes all metadata (including properties and tags) for the specified {@link NamespacedId}.
   */
  @Override
  public void removeMetadata(final MetadataScope scope, final NamespacedId namespacedId) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(namespacedId, scope, input.getProperties(namespacedId), input.getTags(namespacedId)));
        input.removeProperties(namespacedId);
        input.removeTags(namespacedId);
      }
    }, scope);
    MetadataRecord previous = previousRef.get();
    publish(previous, new MetadataRecord(namespacedId, scope), new MetadataRecord(previous));
  }

  /**
   * Removes all properties for the specified {@link NamespacedId}.
   */
  @Override
  public void removeProperties(final MetadataScope scope, final NamespacedId namespacedId) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(namespacedId, scope, input.getProperties(namespacedId), input.getTags(namespacedId)));
        input.removeProperties(namespacedId);
      }
    }, scope);
    publish(previousRef.get(), new MetadataRecord(namespacedId, scope),
            new MetadataRecord(namespacedId, scope, previousRef.get().getProperties(), EMPTY_TAGS));
  }

  /**
   * Removes the specified properties of the {@link NamespacedId}.
   */
  @Override
  public void removeProperties(final MetadataScope scope, final NamespacedId namespacedId, final String... keys) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    final ImmutableMap.Builder<String, String> deletesBuilder = ImmutableMap.builder();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(namespacedId, scope, input.getProperties(namespacedId), input.getTags(namespacedId)));
        for (String key : keys) {
          MetadataEntry record = input.getProperty(namespacedId, key);
          if (record == null) {
            continue;
          }
          deletesBuilder.put(record.getKey(), record.getValue());
        }
        input.removeProperties(namespacedId, keys);
      }
    }, scope);
    publish(previousRef.get(), new MetadataRecord(namespacedId, scope),
            new MetadataRecord(namespacedId, scope, deletesBuilder.build(), EMPTY_TAGS));
  }

  /**
   * Removes all the tags from the {@link NamespacedId}
   */
  @Override
  public void removeTags(final MetadataScope scope, final NamespacedId namespacedId) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(namespacedId, scope, input.getProperties(namespacedId), input.getTags(namespacedId)));
        input.removeTags(namespacedId);
      }
    }, scope);
    MetadataRecord previous = previousRef.get();
    publish(previous, new MetadataRecord(namespacedId, scope),
            new MetadataRecord(namespacedId, scope, EMPTY_PROPERTIES, previous.getTags()));
  }

  /**
   * Removes the specified tags from the {@link NamespacedId}
   */
  @Override
  public void removeTags(final MetadataScope scope, final NamespacedId namespacedId, final String ... tagsToRemove) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(namespacedId, scope, input.getProperties(namespacedId), input.getTags(namespacedId)));
        input.removeTags(namespacedId, tagsToRemove);
      }
    }, scope);
    publish(previousRef.get(), new MetadataRecord(namespacedId, scope),
            new MetadataRecord(namespacedId, scope, EMPTY_PROPERTIES, Sets.newHashSet(tagsToRemove)));
  }

  @Override
  public Set<MetadataSearchResultRecord> searchMetadata(String namespaceId, String searchQuery) {
    return ImmutableSet.<MetadataSearchResultRecord>builder()
      .addAll(searchMetadata(MetadataScope.USER, namespaceId, searchQuery))
      .addAll(searchMetadata(MetadataScope.SYSTEM, namespaceId, searchQuery))
      .build();
  }

  @Override
  public Set<MetadataSearchResultRecord> searchMetadata(MetadataScope scope, String namespaceId, String searchQuery) {
    return searchMetadataOnType(scope, namespaceId, searchQuery, ImmutableSet.of(MetadataSearchTargetType.ALL));
  }

  @Override
  public Set<MetadataSearchResultRecord> searchMetadataOnType(String namespaceId, String searchQuery,
                                                              Set<MetadataSearchTargetType> types) {
    return ImmutableSet.<MetadataSearchResultRecord>builder()
      .addAll(searchMetadataOnType(MetadataScope.USER, namespaceId, searchQuery, types))
      .addAll(searchMetadataOnType(MetadataScope.SYSTEM, namespaceId, searchQuery, types))
      .build();
  }

  @Override
  public Set<MetadataSearchResultRecord> searchMetadataOnType(final MetadataScope scope, final String namespaceId,
                                                              final String searchQuery,
                                                              final Set<MetadataSearchTargetType> types) {
    // Execute search query
    Iterable<MetadataEntry> results = execute(new TransactionExecutor.Function<MetadataDataset,
      Iterable<MetadataEntry>>() {
      @Override
      public Iterable<MetadataEntry> apply(MetadataDataset input) throws Exception {
        return input.search(namespaceId, searchQuery, types);
      }
    }, scope);

    // Score results
    final Map<NamespacedId, Integer> weightedResults = new HashMap<>();
    for (MetadataEntry metadataEntry : results) {
      Integer score = weightedResults.get(metadataEntry.getTargetId());
      score = score == null ? 0 : score;
      weightedResults.put(metadataEntry.getTargetId(), score + 1);
    }

    // Sort the results by score
    List<Map.Entry<NamespacedId, Integer>> resultList = new ArrayList<>(weightedResults.entrySet());
    Collections.sort(resultList, SEARCH_RESULT_DESC_SCORE_COMPARATOR);

    // Fetch metadata for entities in the result list
    // Note: since the fetch is happening in a different transaction, the metadata for entities may have been
    // removed. It is okay not to have metadata for some results in case this happens.
    Map<NamespacedId, Metadata> systemMetadata = fetchMetadata(weightedResults.keySet(), MetadataScope.SYSTEM);
    Map<NamespacedId, Metadata> userMetadata = fetchMetadata(weightedResults.keySet(), MetadataScope.USER);

    return addMetadataToResults(resultList, systemMetadata, userMetadata);
  }

  private Map<NamespacedId, Metadata> fetchMetadata(final Set<NamespacedId> namespacedIds, MetadataScope scope) {
    Set<Metadata> metadataSet =
      execute(new TransactionExecutor.Function<MetadataDataset, Set<Metadata>>() {
        @Override
        public Set<Metadata> apply(MetadataDataset input) throws Exception {
          return input.getMetadata(namespacedIds);
        }
      }, scope);
    Map<NamespacedId, Metadata> metadataMap = new HashMap<>();
    for (Metadata m : metadataSet) {
      metadataMap.put(m.getEntityId(), m);
    }
    return metadataMap;
  }

  Set<MetadataSearchResultRecord> addMetadataToResults(List<Map.Entry<NamespacedId, Integer>> results,
                                                       Map<NamespacedId, Metadata> systemMetadata,
                                                       Map<NamespacedId, Metadata> userMetadata) {
    Set<MetadataSearchResultRecord> result = new LinkedHashSet<>();
    for (Map.Entry<NamespacedId, Integer> entry : results) {
      ImmutableMap.Builder<MetadataScope, co.cask.cdap.proto.metadata.Metadata> builder = ImmutableMap.builder();
      // Add system metadata
      Metadata metadata = systemMetadata.get(entry.getKey());
      if (metadata != null) {
        builder.put(MetadataScope.SYSTEM,
                    new co.cask.cdap.proto.metadata.Metadata(metadata.getProperties(), metadata.getTags()));
      }

      // Add user metadata
      metadata = userMetadata.get(entry.getKey());
      if (metadata != null) {
        builder.put(MetadataScope.USER,
                    new co.cask.cdap.proto.metadata.Metadata(metadata.getProperties(), metadata.getTags()));
      }

      // Create result
      result.add(new MetadataSearchResultRecord(entry.getKey(), builder.build()));
    }
    return result;
  }

  @Override
  public Set<MetadataRecord> getSnapshotBeforeTime(final Set<NamespacedId> namespacedIds, final long timeMillis) {
    return ImmutableSet.<MetadataRecord>builder()
      .addAll(getSnapshotBeforeTime(MetadataScope.USER, namespacedIds, timeMillis))
      .addAll(getSnapshotBeforeTime(MetadataScope.SYSTEM, namespacedIds, timeMillis))
      .build();
  }

  @Override
  public Set<MetadataRecord> getSnapshotBeforeTime(MetadataScope scope, final Set<NamespacedId> namespacedIds,
                                                   final long timeMillis) {
    Set<Metadata> metadataHistoryEntries =
      execute(new TransactionExecutor.Function<MetadataDataset, Set<Metadata>>() {
        @Override
        public Set<Metadata> apply(MetadataDataset input) throws Exception {
          return input.getSnapshotBeforeTime(namespacedIds, timeMillis);
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

  private void publish(MetadataRecord previous, MetadataRecord additions, MetadataRecord deletions) {
    MetadataChangeRecord.MetadataDiffRecord diff = new MetadataChangeRecord.MetadataDiffRecord(additions, deletions);
    MetadataChangeRecord changeRecord = new MetadataChangeRecord(previous, diff, System.currentTimeMillis());
    changePublisher.publish(changeRecord);

    publishAudit(previous, additions, deletions);
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
        DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS, null);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private Id.DatasetInstance getMetadataDatasetInstance(MetadataScope scope) {
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
