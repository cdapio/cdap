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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.IndexedTableDefinition;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.metadata.dataset.Metadata;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.MetadataEntry;
import co.cask.cdap.data2.metadata.indexer.Indexer;
import co.cask.cdap.data2.metadata.publisher.MetadataChangePublisher;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataChangeRecord;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;

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

  // TODO: CDAP-4311 Needed only for Upgrade Tool for 3.3. Make private in 3.4.
  public static final Id.DatasetInstance BUSINESS_METADATA_INSTANCE_ID =
    Id.DatasetInstance.from(Id.Namespace.SYSTEM, "business.metadata");
  private static final Id.DatasetInstance SYSTEM_METADATA_INSTANCE_ID =
    Id.DatasetInstance.from(Id.Namespace.SYSTEM, "system.metadata");
  private static final Map<String, String> EMPTY_PROPERTIES = ImmutableMap.of();
  private static final Set<String> EMPTY_TAGS = ImmutableSet.of();

  private static final Comparator<Map.Entry<Id.NamespacedId, Integer>> SEARCH_RESULT_DESC_SCORE_COMPARATOR =
    new Comparator<Map.Entry<Id.NamespacedId, Integer>>() {
      @Override
      public int compare(Map.Entry<Id.NamespacedId, Integer> o1, Map.Entry<Id.NamespacedId, Integer> o2) {
        // sort in descending order
        return o2.getValue() - o1.getValue();
      }
    };

  private final TransactionExecutorFactory txExecutorFactory;
  private final DatasetFramework dsFramework;
  private final MetadataChangePublisher changePublisher;

  @Inject
  DefaultMetadataStore(TransactionExecutorFactory txExecutorFactory,
                       @Named(DataSetsModules.BASIC_DATASET_FRAMEWORK) DatasetFramework dsFramework,
                       MetadataChangePublisher changePublisher) {
    this.txExecutorFactory = txExecutorFactory;
    this.dsFramework = dsFramework;
    this.changePublisher = changePublisher;
  }

  @Override
  public void setProperties(MetadataScope scope, Id.NamespacedId entityId, Map<String, String> properties) {
    setProperties(scope, entityId, properties, null);
  }

  /**
   * Adds/updates metadata for the specified {@link Id.NamespacedId}.
   */
  @Override
  public void setProperties(final MetadataScope scope, final Id.NamespacedId entityId,
                            final Map<String, String> properties, @Nullable final Indexer indexer) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        Map<String, String> existingProperties = input.getProperties(entityId);
        Set<String> existingTags = input.getTags(entityId);
        previousRef.set(new MetadataRecord(entityId, scope, existingProperties, existingTags));
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          input.setProperty(entityId, entry.getKey(), entry.getValue(), indexer);
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
    publish(previousRecord, new MetadataRecord(entityId, scope, propAdditions.build(), EMPTY_TAGS),
            new MetadataRecord(entityId, scope, propDeletions.build(), EMPTY_TAGS));
  }

  /**
   * Adds tags for the specified {@link Id.NamespacedId}.
   */
  @Override
  public void addTags(final MetadataScope scope, final Id.NamespacedId entityId, final String... tagsToAdd) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        Map<String, String> existingProperties = input.getProperties(entityId);
        Set<String> existingTags = input.getTags(entityId);
        previousRef.set(new MetadataRecord(entityId, scope, existingProperties, existingTags));
        input.addTags(entityId, tagsToAdd);
      }
    }, scope);
    publish(previousRef.get(), new MetadataRecord(entityId, scope, EMPTY_PROPERTIES, Sets.newHashSet(tagsToAdd)),
            new MetadataRecord(entityId, scope));
  }

  @Override
  public Set<MetadataRecord> getMetadata(Id.NamespacedId entityId) {
    return ImmutableSet.of(getMetadata(MetadataScope.USER, entityId), getMetadata(MetadataScope.SYSTEM, entityId));
  }

  @Override
  public MetadataRecord getMetadata(final MetadataScope scope, final Id.NamespacedId entityId) {
    return execute(new TransactionExecutor.Function<MetadataDataset, MetadataRecord>() {
      @Override
      public MetadataRecord apply(MetadataDataset input) throws Exception {
        Map<String, String> properties = input.getProperties(entityId);
        Set<String> tags = input.getTags(entityId);
        return new MetadataRecord(entityId, scope, properties, tags);
      }
    }, scope);
  }

  /**
   * @return a set of {@link MetadataRecord}s representing all the metadata (including properties and tags)
   * for the specified set of {@link Id.NamespacedId}s.
   */
  @Override
  public Set<MetadataRecord> getMetadata(final MetadataScope scope, final Set<Id.NamespacedId> entityIds) {
    return execute(new TransactionExecutor.Function<MetadataDataset, Set<MetadataRecord>>() {
      @Override
      public Set<MetadataRecord> apply(MetadataDataset input) throws Exception {
        Set<MetadataRecord> metadataRecords = new HashSet<>(entityIds.size());
        for (Id.NamespacedId entityId : entityIds) {
          Map<String, String> properties = input.getProperties(entityId);
          Set<String> tags = input.getTags(entityId);
          metadataRecords.add(new MetadataRecord(entityId, scope, properties, tags));
        }
        return metadataRecords;
      }
    }, scope);
  }

  @Override
  public Map<String, String> getProperties(Id.NamespacedId entityId) {
    return ImmutableMap.<String, String>builder()
      .putAll(getProperties(MetadataScope.USER, entityId))
      .putAll(getProperties(MetadataScope.SYSTEM, entityId))
      .build();
  }

  /**
   * @return the metadata for the specified {@link Id.NamespacedId}
   */
  @Override
  public Map<String, String> getProperties(MetadataScope scope, final Id.NamespacedId entityId) {
    return execute(new TransactionExecutor.Function<MetadataDataset, Map<String, String>>() {
      @Override
      public Map<String, String> apply(MetadataDataset input) throws Exception {
        return input.getProperties(entityId);
      }
    }, scope);
  }

  @Override
  public Set<String> getTags(Id.NamespacedId entityId) {
    return ImmutableSet.<String>builder()
      .addAll(getTags(MetadataScope.USER, entityId))
      .addAll(getTags(MetadataScope.SYSTEM, entityId))
      .build();
  }

  /**
   * @return the tags for the specified {@link Id.NamespacedId}
   */
  @Override
  public Set<String> getTags(MetadataScope scope, final Id.NamespacedId entityId) {
    return execute(new TransactionExecutor.Function<MetadataDataset, Set<String>>() {
      @Override
      public Set<String> apply(MetadataDataset input) throws Exception {
        return input.getTags(entityId);
      }
    }, scope);
  }

  @Override
  public void removeMetadata(Id.NamespacedId entityId) {
    removeMetadata(MetadataScope.USER, entityId);
    removeMetadata(MetadataScope.SYSTEM, entityId);
  }

  /**
   * Removes all metadata (including properties and tags) for the specified {@link Id.NamespacedId}.
   */
  @Override
  public void removeMetadata(final MetadataScope scope, final Id.NamespacedId entityId) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, scope, input.getProperties(entityId), input.getTags(entityId)));
        input.removeProperties(entityId);
        input.removeTags(entityId);
      }
    }, scope);
    MetadataRecord previous = previousRef.get();
    publish(previous, new MetadataRecord(entityId, scope), new MetadataRecord(previous));
  }

  /**
   * Removes all properties for the specified {@link Id.NamespacedId}.
   */
  @Override
  public void removeProperties(final MetadataScope scope, final Id.NamespacedId entityId) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, scope, input.getProperties(entityId), input.getTags(entityId)));
        input.removeProperties(entityId);
      }
    }, scope);
    publish(previousRef.get(), new MetadataRecord(entityId, scope),
            new MetadataRecord(entityId, scope, previousRef.get().getProperties(), EMPTY_TAGS));
  }

  /**
   * Removes the specified properties of the {@link Id.NamespacedId}.
   */
  @Override
  public void removeProperties(final MetadataScope scope, final Id.NamespacedId entityId, final String... keys) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    final ImmutableMap.Builder<String, String> deletesBuilder = ImmutableMap.builder();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, scope, input.getProperties(entityId), input.getTags(entityId)));
        for (String key : keys) {
          MetadataEntry record = input.getProperty(entityId, key);
          if (record == null) {
            continue;
          }
          deletesBuilder.put(record.getKey(), record.getValue());
        }
        input.removeProperties(entityId, keys);
      }
    }, scope);
    publish(previousRef.get(), new MetadataRecord(entityId, scope),
            new MetadataRecord(entityId, scope, deletesBuilder.build(), EMPTY_TAGS));
  }

  /**
   * Removes all the tags from the {@link Id.NamespacedId}
   */
  @Override
  public void removeTags(final MetadataScope scope, final Id.NamespacedId entityId) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, scope, input.getProperties(entityId), input.getTags(entityId)));
        input.removeTags(entityId);
      }
    }, scope);
    MetadataRecord previous = previousRef.get();
    publish(previous, new MetadataRecord(entityId, scope),
            new MetadataRecord(entityId, scope, EMPTY_PROPERTIES, previous.getTags()));
  }

  /**
   * Removes the specified tags from the {@link Id.NamespacedId}
   */
  @Override
  public void removeTags(final MetadataScope scope, final Id.NamespacedId entityId, final String ... tagsToRemove) {
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, scope, input.getProperties(entityId), input.getTags(entityId)));
        input.removeTags(entityId, tagsToRemove);
      }
    }, scope);
    publish(previousRef.get(), new MetadataRecord(entityId, scope),
            new MetadataRecord(entityId, scope, EMPTY_PROPERTIES, Sets.newHashSet(tagsToRemove)));
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
    return searchMetadataOnType(scope, namespaceId, searchQuery, MetadataSearchTargetType.ALL);
  }

  @Override
  public Set<MetadataSearchResultRecord> searchMetadataOnType(String namespaceId, String searchQuery,
                                                              MetadataSearchTargetType type) {
    return ImmutableSet.<MetadataSearchResultRecord>builder()
      .addAll(searchMetadataOnType(MetadataScope.USER, namespaceId, searchQuery, type))
      .addAll(searchMetadataOnType(MetadataScope.SYSTEM, namespaceId, searchQuery, type))
      .build();
  }

  @Override
  public Set<MetadataSearchResultRecord> searchMetadataOnType(final MetadataScope scope, final String namespaceId,
                                                              final String searchQuery,
                                                              final MetadataSearchTargetType type) {
    // Execute search query
    Iterable<MetadataEntry> metadataEntries = execute(new TransactionExecutor.Function<MetadataDataset,
      Iterable<MetadataEntry>>() {
      @Override
      public Iterable<MetadataEntry> apply(MetadataDataset input) throws Exception {
        return input.search(namespaceId, searchQuery, type);
      }
    }, scope);

    // Score results
    Map<Id.NamespacedId, Integer> weightedResults = new HashMap<>();
    for (MetadataEntry metadataEntry : metadataEntries) {
      Integer score = weightedResults.get(metadataEntry.getTargetId());
      score = score == null ? 0 : score;
      weightedResults.put(metadataEntry.getTargetId(), score + 1);
    }

    // Sort the results by score
    List<Map.Entry<Id.NamespacedId, Integer>> resultList = new ArrayList<>(weightedResults.entrySet());
    Collections.sort(resultList, SEARCH_RESULT_DESC_SCORE_COMPARATOR);

    Set<MetadataSearchResultRecord> result = new LinkedHashSet<>();
    for (Map.Entry<Id.NamespacedId, Integer> entry : resultList) {
      result.add(new MetadataSearchResultRecord(entry.getKey()));
    }
    return result;
  }

  @Override
  public Set<MetadataRecord> getSnapshotBeforeTime(final Set<Id.NamespacedId> entityIds, final long timeMillis) {
    return ImmutableSet.<MetadataRecord>builder()
      .addAll(getSnapshotBeforeTime(MetadataScope.USER, entityIds, timeMillis))
      .addAll(getSnapshotBeforeTime(MetadataScope.SYSTEM, entityIds, timeMillis))
      .build();
  }

  @Override
  public Set<MetadataRecord> getSnapshotBeforeTime(MetadataScope scope, final Set<Id.NamespacedId> entityIds,
                                                   final long timeMillis) {
    Set<Metadata> metadataHistoryEntries =
      execute(new TransactionExecutor.Function<MetadataDataset, Set<Metadata>>() {
        @Override
        public Set<Metadata> apply(MetadataDataset input) throws Exception {
          return input.getSnapshotBeforeTime(entityIds, timeMillis);
        }
      }, scope);

    ImmutableSet.Builder<MetadataRecord> builder = ImmutableSet.builder();
    for (Metadata metadata : metadataHistoryEntries) {
      builder.add(new MetadataRecord(metadata.getEntityId(), scope,
                                     metadata.getProperties(), metadata.getTags()));
    }
    return builder.build();
  }

  private void publish(MetadataRecord previous, MetadataRecord additions, MetadataRecord deletions) {
    MetadataChangeRecord.MetadataDiffRecord diff = new MetadataChangeRecord.MetadataDiffRecord(additions, deletions);
    MetadataChangeRecord changeRecord = new MetadataChangeRecord(previous, diff, System.currentTimeMillis());
    changePublisher.publish(changeRecord);
  }

  private <T> T execute(TransactionExecutor.Function<MetadataDataset, T> func, MetadataScope scope) {
    MetadataDataset metadataDataset = newMetadataDataset(scope);
    TransactionExecutor txExecutor = Transactions.createTransactionExecutor(txExecutorFactory, metadataDataset);
    return txExecutor.executeUnchecked(func, metadataDataset);
  }

  private void execute(TransactionExecutor.Procedure<MetadataDataset> func, MetadataScope scope) {
    MetadataDataset metadataScope = newMetadataDataset(scope);
    TransactionExecutor txExecutor = Transactions.createTransactionExecutor(txExecutorFactory, metadataScope);
    txExecutor.executeUnchecked(func, metadataScope);
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
    // In 3.2 we indexed MetadataDataset.KEYVALUE_COLUMN (kv) and MetadataDataset.CASE_INSENSITIVE_VALUE_COLUMN (civ)
    // and in 3.3 we index new column MetadataDataset.INDEX_COLUMN (i). During upgrade we want to
    // have instance of BUSINESS_METADATA_INSTANCE_ID with columnToIndex with all these three column so that
    // when we do a delete while upgrading MetadataDataset#upgrade the delete operation also delete the records
    // for the old index column from index table.
    DatasetProperties dsProperties = getOldColumnToIndexAsProperties();
    //TODO: (UPG-3.3): Remove this after 3.3
    // the above is only needed for Business Metadata as in 3.2 we didn't have System Metadata table.
    framework.addInstance(MetadataDataset.class.getName(), BUSINESS_METADATA_INSTANCE_ID, dsProperties);
    framework.addInstance(MetadataDataset.class.getName(), SYSTEM_METADATA_INSTANCE_ID, DatasetProperties.EMPTY);
  }

  public void upgrade() {
    DatasetProperties dsProperties = getOldColumnToIndexAsProperties();
    MetadataDataset businessMetadataDataset;
    try {
      businessMetadataDataset = DatasetsUtil.getOrCreateDataset(
        dsFramework, BUSINESS_METADATA_INSTANCE_ID, MetadataDataset.class.getName(),
        dsProperties, DatasetDefinition.NO_ARGUMENTS, null);
    } catch (DatasetManagementException | IOException e) {
      throw Throwables.propagate(e);
    }
    Preconditions.checkNotNull(businessMetadataDataset, "Failed to get Business Metadata Dataset.");
    TransactionExecutor txExecutor = Transactions.createTransactionExecutor(txExecutorFactory, businessMetadataDataset);
    txExecutor.executeUnchecked(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        input.upgrade();
      }
    }, businessMetadataDataset);
  }

  /**
   * @return {@link DatasetProperties} where {@link IndexedTableDefinition#INDEX_COLUMNS_CONF_KEY} is set to
   * index columns from 3.2 and also 3.3
   */
  private static DatasetProperties getOldColumnToIndexAsProperties() {
    return DatasetProperties.builder()
        .add(IndexedTableDefinition.INDEX_COLUMNS_CONF_KEY,
             Joiner.on(",").join(MetadataDataset.KEYVALUE_COLUMN, MetadataDataset.CASE_INSENSITIVE_VALUE_COLUMN,
                                 MetadataDataset.INDEX_COLUMN)).build();
  }
}
