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
package co.cask.cdap.data2.metadata.service;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.dataset.BusinessMetadataDataset;
import co.cask.cdap.data2.metadata.dataset.BusinessMetadataRecord;
import co.cask.cdap.data2.metadata.publisher.MetadataChangePublisher;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataChangeRecord;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of {@link BusinessMetadataStore} used in distributed mode.
 */
public class DefaultBusinessMetadataStore implements BusinessMetadataStore {

  private static final Id.DatasetInstance BUSINESS_METADATA_INSTANCE_ID =
    Id.DatasetInstance.from(Id.Namespace.SYSTEM, "business.metadata");
  private static final Map<String, String> EMPTY_PROPERTIES = ImmutableMap.of();
  private static final Set<String> EMPTY_TAGS = ImmutableSet.of();

  private final CConfiguration cConf;
  private final TransactionExecutorFactory txExecutorFactory;
  private final DatasetFramework dsFramework;
  private final MetadataChangePublisher changePublisher;

  @Inject
  DefaultBusinessMetadataStore(TransactionExecutorFactory txExecutorFactory,
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
  public void setProperties(final Id.NamespacedId entityId, final Map<String, String> properties) {
    if (!cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED)) {
      setPropertiesNoPublish(entityId, properties);
      return;
    }
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<BusinessMetadataDataset>() {
      @Override
      public void apply(BusinessMetadataDataset input) throws Exception {
        Map<String, String> existingProperties = input.getProperties(entityId);
        Set<String> existingTags = input.getTags(entityId);
        previousRef.set(new MetadataRecord(entityId, existingProperties, existingTags));
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          input.setProperty(entityId, entry.getKey(), entry.getValue());
        }
      }
    });
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

  private void setPropertiesNoPublish(final Id.NamespacedId entityId, final Map<String, String> properties) {
    execute(new TransactionExecutor.Procedure<BusinessMetadataDataset>() {
      @Override
      public void apply(BusinessMetadataDataset input) throws Exception {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          input.setProperty(entityId, entry.getKey(), entry.getValue());
        }
      }
    });
  }

  /**
   * Adds tags for the specified {@link Id.NamespacedId}.
   */
  @Override
  public void addTags(final Id.NamespacedId entityId, final String ... tagsToAdd) {
    if (!cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED)) {
      addTagsNoPublish(entityId, tagsToAdd);
      return;
    }
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Function<BusinessMetadataDataset, Void>() {
      @Override
      public Void apply(BusinessMetadataDataset input) throws Exception {
        Map<String, String> existingProperties = input.getProperties(entityId);
        Set<String> existingTags = input.getTags(entityId);
        previousRef.set(new MetadataRecord(entityId, existingProperties, existingTags));
        input.addTags(entityId, tagsToAdd);
        return null;
      }
    });
    publish(previousRef.get(), new MetadataRecord(entityId, EMPTY_PROPERTIES, Sets.newHashSet(tagsToAdd)),
            new MetadataRecord(entityId));
  }

  private void addTagsNoPublish(final Id.NamespacedId entityId, final String... tagsToAdd) {
    execute(new TransactionExecutor.Function<BusinessMetadataDataset, Void>() {
      @Override
      public Void apply(BusinessMetadataDataset input) throws Exception {
        input.addTags(entityId, tagsToAdd);
        return null;
      }
    });
  }

  /**
   * @return a {@link MetadataRecord} representing all the metadata (including properties and tags) for the specified
   * {@link Id.NamespacedId}.
   */
  @Override
  public MetadataRecord getMetadata(final Id.NamespacedId entityId) {
    return execute(new TransactionExecutor.Function<BusinessMetadataDataset, MetadataRecord>() {
      @Override
      public MetadataRecord apply(BusinessMetadataDataset input) throws Exception {
        Map<String, String> properties = input.getProperties(entityId);
        Set<String> tags = input.getTags(entityId);
        return new MetadataRecord(entityId, properties, tags);
      }
    });
  }

  /**
   * @return a set of {@link MetadataRecord}s representing all the metadata (including properties and tags)
   * for the specified set of {@link Id.NamespacedId}s.
   */
  @Override
  public Set<MetadataRecord> getMetadata(final Set<Id.NamespacedId> entityIds) {
    return execute(new TransactionExecutor.Function<BusinessMetadataDataset, Set<MetadataRecord>>() {
      @Override
      public Set<MetadataRecord> apply(BusinessMetadataDataset input) throws Exception {
        Set<MetadataRecord> metadataRecords = new HashSet<>(entityIds.size());
        for (Id.NamespacedId entityId : entityIds) {
          Map<String, String> properties = input.getProperties(entityId);
          Set<String> tags = input.getTags(entityId);
          metadataRecords.add(new MetadataRecord(entityId, properties, tags));
        }
        return metadataRecords;
      }
    });
  }

  /**
   * @return the metadata for the specified {@link Id.NamespacedId}
   */
  @Override
  public Map<String, String> getProperties(final Id.NamespacedId entityId) {
    return execute(new TransactionExecutor.Function<BusinessMetadataDataset, Map<String, String>>() {
      @Override
      public Map<String, String> apply(BusinessMetadataDataset input) throws Exception {
        return input.getProperties(entityId);
      }
    });
  }

  /**
   * @return the tags for the specified {@link Id.NamespacedId}
   */
  @Override
  public Set<String> getTags(final Id.NamespacedId entityId) {
    return execute(new TransactionExecutor.Function<BusinessMetadataDataset, Set<String>>() {
      @Override
      public Set<String> apply(BusinessMetadataDataset input) throws Exception {
        return input.getTags(entityId);
      }
    });
  }

  /**
   * Removes all metadata (including properties and tags) for the specified {@link Id.NamespacedId}.
   */
  @Override
  public void removeMetadata(final Id.NamespacedId entityId) {
    if (!cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED)) {
      removeMetadataNoPublish(entityId);
      return;
    }
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<BusinessMetadataDataset>() {
      @Override
      public void apply(BusinessMetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, input.getProperties(entityId), input.getTags(entityId)));
        input.removeProperties(entityId);
        input.removeTags(entityId);
      }
    });
    MetadataRecord previous = previousRef.get();
    publish(previous, new MetadataRecord(entityId), new MetadataRecord(previous));
  }

  private void removeMetadataNoPublish(final Id.NamespacedId entityId) {
    execute(new TransactionExecutor.Procedure<BusinessMetadataDataset>() {
      @Override
      public void apply(BusinessMetadataDataset input) throws Exception {
        input.removeProperties(entityId);
        input.removeTags(entityId);
      }
    });
  }

  /**
   * Removes all properties for the specified {@link Id.NamespacedId}.
   */
  @Override
  public void removeProperties(final Id.NamespacedId entityId) {
    if (!cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED)) {
      removePropertiesNoPublish(entityId);
      return;
    }
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<BusinessMetadataDataset>() {
      @Override
      public void apply(BusinessMetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, input.getProperties(entityId), input.getTags(entityId)));
        input.removeProperties(entityId);
      }
    });
    publish(previousRef.get(), new MetadataRecord(entityId),
            new MetadataRecord(entityId, previousRef.get().getProperties(), EMPTY_TAGS));
  }

  private void removePropertiesNoPublish(final Id.NamespacedId entityId) {
    execute(new TransactionExecutor.Procedure<BusinessMetadataDataset>() {
      @Override
      public void apply(BusinessMetadataDataset input) throws Exception {
        input.removeProperties(entityId);
      }
    });
  }

  /**
   * Removes the specified properties of the {@link Id.NamespacedId}.
   */
  @Override
  public void removeProperties(final Id.NamespacedId entityId, final String... keys) {
    if (!cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED)) {
      removePropertiesNoPublish(entityId, keys);
      return;
    }
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    final ImmutableMap.Builder<String, String> deletesBuilder = ImmutableMap.builder();
    execute(new TransactionExecutor.Procedure<BusinessMetadataDataset>() {
      @Override
      public void apply(BusinessMetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, input.getProperties(entityId), input.getTags(entityId)));
        for (String key : keys) {
          BusinessMetadataRecord record = input.getProperty(entityId, key);
          if (record == null) {
            continue;
          }
          deletesBuilder.put(record.getKey(), record.getValue());
        }
        input.removeProperties(entityId, keys);
      }
    });
    publish(previousRef.get(), new MetadataRecord(entityId),
            new MetadataRecord(entityId, deletesBuilder.build(), EMPTY_TAGS));
  }

  private void removePropertiesNoPublish(final Id.NamespacedId entityId, final String... keys) {
    execute(new TransactionExecutor.Procedure<BusinessMetadataDataset>() {
      @Override
      public void apply(BusinessMetadataDataset input) throws Exception {
        input.removeProperties(entityId, keys);
      }
    });
  }

  /**
   * Removes all the tags from the {@link Id.NamespacedId}
   */
  @Override
  public void removeTags(final Id.NamespacedId entityId) {
    if (!cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED)) {
      removeTagsNoPublish(entityId);
      return;
    }
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<BusinessMetadataDataset>() {
      @Override
      public void apply(BusinessMetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, input.getProperties(entityId), input.getTags(entityId)));
        input.removeTags(entityId);
      }
    });
    MetadataRecord previous = previousRef.get();
    publish(previous, new MetadataRecord(entityId), new MetadataRecord(entityId, EMPTY_PROPERTIES, previous.getTags()));
  }

  private void removeTagsNoPublish(final Id.NamespacedId entityId) {
    execute(new TransactionExecutor.Procedure<BusinessMetadataDataset>() {
      @Override
      public void apply(BusinessMetadataDataset input) throws Exception {
        input.removeTags(entityId);
      }
    });
  }

  /**
   * Removes the specified tags from the {@link Id.NamespacedId}
   */
  @Override
  public void removeTags(final Id.NamespacedId entityId, final String ... tagsToRemove) {
    if (!cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED)) {
      removeTagsNoPublish(entityId, tagsToRemove);
      return;
    }
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    execute(new TransactionExecutor.Procedure<BusinessMetadataDataset>() {
      @Override
      public void apply(BusinessMetadataDataset input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, input.getProperties(entityId), input.getTags(entityId)));
        input.removeTags(entityId, tagsToRemove);
      }
    });
    publish(previousRef.get(), new MetadataRecord(entityId),
            new MetadataRecord(entityId, EMPTY_PROPERTIES, Sets.newHashSet(tagsToRemove)));
  }

  private void removeTagsNoPublish(final Id.NamespacedId entityId, final String ... tagsToRemove) {
    execute(new TransactionExecutor.Procedure<BusinessMetadataDataset>() {
      @Override
      public void apply(BusinessMetadataDataset input) throws Exception {
        input.removeTags(entityId, tagsToRemove);
      }
    });
  }

  /**
   * Search to the underlying Business Metadata Dataset.
   */
  @Override
  public Iterable<BusinessMetadataRecord> searchMetadata(final String searchQuery) {
    return searchMetadataOnType(searchQuery, MetadataSearchTargetType.ALL);
  }

  /**
   * Search to the underlying Business Metadata Dataset for a target type.
   */
  @Override
  public Iterable<BusinessMetadataRecord> searchMetadataOnType(final String searchQuery,
                                                               final MetadataSearchTargetType type) {
    return execute(new TransactionExecutor.Function<BusinessMetadataDataset, Iterable<BusinessMetadataRecord>>() {
      @Override
      public Iterable<BusinessMetadataRecord> apply(BusinessMetadataDataset input) throws Exception {
        // Currently we support two types of search formats: value and key:value.
        // Check for existence of separator char to make sure we did search in the right indexed column.
        if (searchQuery.contains(BusinessMetadataDataset.KEYVALUE_SEPARATOR)) {
          // key=value search
          return input.findBusinessMetadataOnKeyValue(searchQuery, type);
        }
        // value search
        return input.findBusinessMetadataOnValue(searchQuery, type);
      }
    });
  }

  private void publish(MetadataRecord previous, MetadataRecord additions, MetadataRecord deletions) {
    MetadataChangeRecord.MetadataDiffRecord diff = new MetadataChangeRecord.MetadataDiffRecord(additions, deletions);
    MetadataChangeRecord changeRecord = new MetadataChangeRecord(previous, diff, System.currentTimeMillis());
    changePublisher.publish(changeRecord);
  }

  private <T> T execute(TransactionExecutor.Function<BusinessMetadataDataset, T> func) {
    BusinessMetadataDataset businessMetadataDataset = newBusinessMetadataDataset();
    TransactionExecutor txExecutor = Transactions.createTransactionExecutor(txExecutorFactory, businessMetadataDataset);
    return txExecutor.executeUnchecked(func, businessMetadataDataset);
  }

  private void execute(TransactionExecutor.Procedure<BusinessMetadataDataset> func) {
    BusinessMetadataDataset businessMetadataDataset = newBusinessMetadataDataset();
    TransactionExecutor txExecutor = Transactions.createTransactionExecutor(txExecutorFactory, businessMetadataDataset);
    txExecutor.executeUnchecked(func, businessMetadataDataset);
  }

  private BusinessMetadataDataset newBusinessMetadataDataset() {
    try {
      return DatasetsUtil.getOrCreateDataset(
        dsFramework, BUSINESS_METADATA_INSTANCE_ID, BusinessMetadataDataset.class.getName(),
        DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS, null);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
