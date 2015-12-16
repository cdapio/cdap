/*
 * Copyright 2015 Cask Data, Inc.
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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.metadata.dataset.Metadata;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.MetadataEntry;
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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

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

  private final CConfiguration cConf;
  private final TransactionExecutorFactory txExecutorFactory;
  private final DatasetFramework dsFramework;
  private final MetadataChangePublisher changePublisher;

  @Inject
  DefaultMetadataStore(TransactionExecutorFactory txExecutorFactory,
                       @Named(DataSetsModules.BASIC_DATASET_FRAMEWORK) DatasetFramework dsFramework,
                       CConfiguration cConf, MetadataChangePublisher changePublisher) {
    this.txExecutorFactory = txExecutorFactory;
    this.dsFramework = dsFramework;
    this.cConf = cConf;
    this.changePublisher = changePublisher;
  }

  /**
   * Adds/updates metadata for the specified {@link Id.NamespacedId}.
   */
  @Override
  public void setProperties(MetadataScope scope, final Id.NamespacedId entityId, final Map<String, String> properties) {
    if (!cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED)) {
      setPropertiesNoPublish(scope, entityId, properties);
      return;
    }
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        Map<String, String> existingProperties = input.getProperties(entityId);
        Set<String> existingTags = input.getTags(entityId);
        previousRef.set(new MetadataRecord(entityId, existingProperties, existingTags));
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          input.setProperty(entityId, entry.getKey(), entry.getValue());
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
    publish(previousRecord, new MetadataRecord(entityId, propAdditions.build(), EMPTY_TAGS),
            new MetadataRecord(entityId, propDeletions.build(), EMPTY_TAGS));
  }

  private void setPropertiesNoPublish(MetadataScope scope, final Id.NamespacedId entityId,
                                      final Map<String, String> properties) {
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          input.setProperty(entityId, entry.getKey(), entry.getValue());
        }
      }
    }, scope);
  }

  /**
   * Adds tags for the specified {@link Id.NamespacedId}.
   */
  @Override
  public void addTags(MetadataScope scope, final Id.NamespacedId entityId, final String... tagsToAdd) {
    if (!cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED)) {
      addTagsNoPublish(scope, entityId, tagsToAdd);
      return;
    }
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        Map<String, String> existingProperties = input.getProperties(entityId);
        Set<String> existingTags = input.getTags(entityId);
        previousRef.set(new MetadataRecord(entityId, existingProperties, existingTags));
        input.addTags(entityId, tagsToAdd);
      }
    }, scope);
    publish(previousRef.get(), new MetadataRecord(entityId, EMPTY_PROPERTIES, Sets.newHashSet(tagsToAdd)),
            new MetadataRecord(entityId));
  }

  private void addTagsNoPublish(MetadataScope scope, final Id.NamespacedId entityId, final String... tagsToAdd) {
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        input.addTags(entityId, tagsToAdd);
      }
    }, scope);
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
        return new MetadataRecord(entityId, properties, tags);
      }
    }, scope);
  }

  /**
   * @return a set of {@link MetadataRecord}s representing all the metadata (including properties and tags)
   * for the specified set of {@link Id.NamespacedId}s.
   */
  @Override
  public Set<MetadataRecord> getMetadata(MetadataScope scope, final Set<Id.NamespacedId> entityIds) {
    return execute(new TransactionExecutor.Function<MetadataDataset, Set<MetadataRecord>>() {
      @Override
      public Set<MetadataRecord> apply(MetadataDataset input) throws Exception {
        Set<MetadataRecord> metadataRecords = new HashSet<>(entityIds.size());
        for (Id.NamespacedId entityId : entityIds) {
          Map<String, String> properties = input.getProperties(entityId);
          Set<String> tags = input.getTags(entityId);
          metadataRecords.add(new MetadataRecord(entityId, properties, tags));
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
  public void removeMetadata(MetadataScope scope, final Id.NamespacedId entityId) {
    if (!cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED)) {
      removeMetadataNoPublish(scope, entityId);
      return;
    }
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, input.getProperties(entityId), input.getTags(entityId)));
        input.removeProperties(entityId);
        input.removeTags(entityId);
      }
    }, scope);
    MetadataRecord previous = previousRef.get();
    publish(previous, new MetadataRecord(entityId), new MetadataRecord(previous));
  }

  private void removeMetadataNoPublish(MetadataScope scope, final Id.NamespacedId entityId) {
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        input.removeProperties(entityId);
        input.removeTags(entityId);
      }
    }, scope);
  }

  /**
   * Removes all properties for the specified {@link Id.NamespacedId}.
   */
  @Override
  public void removeProperties(MetadataScope scope, final Id.NamespacedId entityId) {
    if (!cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED)) {
      removePropertiesNoPublish(scope, entityId);
      return;
    }
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, input.getProperties(entityId), input.getTags(entityId)));
        input.removeProperties(entityId);
      }
    }, scope);
    publish(previousRef.get(), new MetadataRecord(entityId),
            new MetadataRecord(entityId, previousRef.get().getProperties(), EMPTY_TAGS));
  }

  private void removePropertiesNoPublish(MetadataScope scope, final Id.NamespacedId entityId) {
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        input.removeProperties(entityId);
      }
    }, scope);
  }

  /**
   * Removes the specified properties of the {@link Id.NamespacedId}.
   */
  @Override
  public void removeProperties(MetadataScope scope, final Id.NamespacedId entityId, final String... keys) {
    if (!cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED)) {
      removePropertiesNoPublish(scope, entityId, keys);
      return;
    }
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    final ImmutableMap.Builder<String, String> deletesBuilder = ImmutableMap.builder();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, input.getProperties(entityId), input.getTags(entityId)));
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
    publish(previousRef.get(), new MetadataRecord(entityId),
            new MetadataRecord(entityId, deletesBuilder.build(), EMPTY_TAGS));
  }

  private void removePropertiesNoPublish(MetadataScope scope, final Id.NamespacedId entityId, final String... keys) {
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        input.removeProperties(entityId, keys);
      }
    }, scope);
  }

  /**
   * Removes all the tags from the {@link Id.NamespacedId}
   */
  @Override
  public void removeTags(MetadataScope scope, final Id.NamespacedId entityId) {
    if (!cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED)) {
      removeTagsNoPublish(scope, entityId);
      return;
    }
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, input.getProperties(entityId), input.getTags(entityId)));
        input.removeTags(entityId);
      }
    }, scope);
    MetadataRecord previous = previousRef.get();
    publish(previous, new MetadataRecord(entityId), new MetadataRecord(entityId, EMPTY_PROPERTIES, previous.getTags()));
  }

  private void removeTagsNoPublish(MetadataScope scope, final Id.NamespacedId entityId) {
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        input.removeTags(entityId);
      }
    }, scope);
  }

  /**
   * Removes the specified tags from the {@link Id.NamespacedId}
   */
  @Override
  public void removeTags(MetadataScope scope, final Id.NamespacedId entityId, final String ... tagsToRemove) {
    if (!cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED)) {
      removeTagsNoPublish(scope, entityId, tagsToRemove);
      return;
    }
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, input.getProperties(entityId), input.getTags(entityId)));
        input.removeTags(entityId, tagsToRemove);
      }
    }, scope);
    publish(previousRef.get(), new MetadataRecord(entityId),
            new MetadataRecord(entityId, EMPTY_PROPERTIES, Sets.newHashSet(tagsToRemove)));
  }

  private void removeTagsNoPublish(MetadataScope scope, final Id.NamespacedId entityId, final String ... tagsToRemove) {
    execute(new TransactionExecutor.Procedure<MetadataDataset>() {
      @Override
      public void apply(MetadataDataset input) throws Exception {
        input.removeTags(entityId, tagsToRemove);
      }
    }, scope);
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
  public Set<MetadataSearchResultRecord> searchMetadataOnType(MetadataScope scope, final String namespaceId,
                                                              final String searchQuery,
                                                              final MetadataSearchTargetType type) {
    Iterable<MetadataEntry> metadataEntries = execute(new TransactionExecutor.Function<MetadataDataset,
      Iterable<MetadataEntry>>() {
      @Override
      public Iterable<MetadataEntry> apply(MetadataDataset input) throws Exception {
        // Currently we support two types of search formats: value and key:value.
        // Check for existence of separator char to make sure we did search in the right indexed column.
        if (searchQuery.contains(MetadataDataset.KEYVALUE_SEPARATOR)) {
          // key=value search
          return input.searchByKeyValue(namespaceId, searchQuery, type);
        }
        // value search
        return input.searchByValue(namespaceId, searchQuery, type);
      }
    }, scope);
    ImmutableSet.Builder<MetadataSearchResultRecord> builder = ImmutableSet.builder();
    for (MetadataEntry metadataEntry : metadataEntries) {
      builder.add(new MetadataSearchResultRecord(metadataEntry.getTargetId()));
    }
    return builder.build();
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
    framework.addInstance(MetadataDataset.class.getName(), BUSINESS_METADATA_INSTANCE_ID, DatasetProperties.EMPTY);
    framework.addInstance(MetadataDataset.class.getName(), SYSTEM_METADATA_INSTANCE_ID, DatasetProperties.EMPTY);
  }
}
