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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.data2.metadata.dataset.BusinessMetadataDataset;
import co.cask.cdap.data2.metadata.dataset.BusinessMetadataRecord;
import co.cask.cdap.data2.metadata.publisher.MetadataChangePublisher;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataChangeRecord;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of {@link BusinessMetadataStore} used in distributed mode.
 */
public class DistributedBusinessMetadataStore implements BusinessMetadataStore {

  private static final Id.DatasetInstance BUSINESS_METADATA_INSTANCE_ID =
    Id.DatasetInstance.from(Id.Namespace.SYSTEM, "business.metadata");
  private static final Map<String, String> EMPTY_PROPERTIES = ImmutableMap.of();
  private static final Set<String> EMPTY_TAGS = ImmutableSet.of();

  private final Transactional<BusinessMdsIterable, BusinessMetadataDataset> txnl;
  private final CConfiguration cConf;
  private final MetadataChangePublisher changePublisher;

  @Inject
  DistributedBusinessMetadataStore(TransactionExecutorFactory txExecutorFactory,
                                   @Named(DataSetsModules.BASIC_DATASET_FRAMEWORK) final DatasetFramework dsFramework,
                                   CConfiguration cConf, MetadataChangePublisher changePublisher) {
    this.txnl = Transactional.of(txExecutorFactory, new Supplier<BusinessMdsIterable>() {
      @Override
      public BusinessMdsIterable get() {
        try {
          BusinessMetadataDataset dataset =
            DatasetsUtil.getOrCreateDataset(dsFramework, BUSINESS_METADATA_INSTANCE_ID,
                                            BusinessMetadataDataset.class.getName(),
                                            DatasetProperties.EMPTY, null, null);
          return new BusinessMdsIterable(dataset);
        } catch (DatasetManagementException | IOException | ServiceUnavailableException e) {
          throw Throwables.propagate(e);
        }
      }
    });
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
    final ImmutableMap.Builder<String, String> propAdditions = ImmutableMap.builder();
    final ImmutableMap.Builder<String, String> propDeletions = ImmutableMap.builder();
    final AtomicReference<MetadataRecord> previousRef = new AtomicReference<>();
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        Map<String, String> existingProperties = input.businessMds.getProperties(entityId);
        Set<String> existingTags = input.businessMds.getTags(entityId);
        previousRef.set(new MetadataRecord(entityId, existingProperties, existingTags));
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          String existingValue = existingProperties.get(entry.getKey());
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
          input.businessMds.setProperty(entityId, entry.getKey(), entry.getValue());
        }
        return null;
      }
    });
    publish(previousRef.get(), new MetadataRecord(entityId, propAdditions.build(), EMPTY_TAGS),
            new MetadataRecord(entityId, propDeletions.build(), EMPTY_TAGS));
  }

  private void setPropertiesNoPublish(final Id.NamespacedId entityId, final Map<String, String> properties) {
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          input.businessMds.setProperty(entityId, entry.getKey(), entry.getValue());
        }
        return null;
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
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        Map<String, String> existingProperties = input.businessMds.getProperties(entityId);
        Set<String> existingTags = input.businessMds.getTags(entityId);
        previousRef.set(new MetadataRecord(entityId, existingProperties, existingTags));
        input.businessMds.addTags(entityId, tagsToAdd);
        return null;
      }
    });
    publish(previousRef.get(), new MetadataRecord(entityId, EMPTY_PROPERTIES, Sets.newHashSet(tagsToAdd)),
            new MetadataRecord(entityId));
  }

  private void addTagsNoPublish(final Id.NamespacedId entityId, final String... tagsToAdd) {
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        input.businessMds.addTags(entityId, tagsToAdd);
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
    return txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, MetadataRecord>() {
      @Override
      public MetadataRecord apply(BusinessMdsIterable input) throws Exception {
        Map<String, String> properties = input.businessMds.getProperties(entityId);
        Set<String> tags = input.businessMds.getTags(entityId);
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
    return txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Set<MetadataRecord>>() {
      @Override
      public Set<MetadataRecord> apply(BusinessMdsIterable input) throws Exception {
        Set<MetadataRecord> metadataRecords = new HashSet<>(entityIds.size());
        for (Id.NamespacedId entityId : entityIds) {
          Map<String, String> properties = input.businessMds.getProperties(entityId);
          Set<String> tags = input.businessMds.getTags(entityId);
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
    return txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Map<String, String>>() {
      @Override
      public Map<String, String> apply(BusinessMdsIterable input) throws Exception {
        return input.businessMds.getProperties(entityId);
      }
    });
  }

  /**
   * @return the tags for the specified {@link Id.NamespacedId}
   */
  @Override
  public Set<String> getTags(final Id.NamespacedId entityId) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Set<String>>() {
      @Override
      public Set<String> apply(BusinessMdsIterable input) throws Exception {
        return input.businessMds.getTags(entityId);
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
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, input.businessMds.getProperties(entityId),
                                           input.businessMds.getTags(entityId)));
        input.businessMds.removeProperties(entityId);
        input.businessMds.removeTags(entityId);
        return null;
      }
    });
    MetadataRecord previous = previousRef.get();
    publish(previous, new MetadataRecord(entityId), new MetadataRecord(previous));
  }

  void removeMetadataNoPublish(final Id.NamespacedId entityId) {
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        input.businessMds.removeProperties(entityId);
        input.businessMds.removeTags(entityId);
        return null;
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
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, input.businessMds.getProperties(entityId),
                                           input.businessMds.getTags(entityId)));
        input.businessMds.removeProperties(entityId);
        return null;
      }
    });
    publish(previousRef.get(), new MetadataRecord(entityId),
            new MetadataRecord(entityId, previousRef.get().getProperties(), EMPTY_TAGS));
  }

  void removePropertiesNoPublish(final Id.NamespacedId entityId) {
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        input.businessMds.removeProperties(entityId);
        return null;
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
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, input.businessMds.getProperties(entityId),
                                           input.businessMds.getTags(entityId)));
        for (String key : keys) {
          BusinessMetadataRecord record = input.businessMds.getProperty(entityId, key);
          if (record == null) {
            continue;
          }
          deletesBuilder.put(record.getKey(), record.getValue());
        }
        input.businessMds.removeProperties(entityId, keys);
        return null;
      }
    });
    publish(previousRef.get(), new MetadataRecord(entityId),
            new MetadataRecord(entityId, deletesBuilder.build(), EMPTY_TAGS));
  }

  private void removePropertiesNoPublish(final Id.NamespacedId entityId, final String... keys) {
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        input.businessMds.removeProperties(entityId, keys);
        return null;
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
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, input.businessMds.getProperties(entityId),
                                           input.businessMds.getTags(entityId)));
        input.businessMds.removeTags(entityId);
        return null;
      }
    });
    MetadataRecord previous = previousRef.get();
    publish(previous, new MetadataRecord(entityId), new MetadataRecord(entityId, EMPTY_PROPERTIES, previous.getTags()));
  }

  void removeTagsNoPublish(final Id.NamespacedId entityId) {
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        input.businessMds.removeTags(entityId);
        return null;
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
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        previousRef.set(new MetadataRecord(entityId, input.businessMds.getProperties(entityId),
                                           input.businessMds.getTags(entityId)));
        input.businessMds.removeTags(entityId, tagsToRemove);
        return null;
      }
    });
    publish(previousRef.get(), new MetadataRecord(entityId),
            new MetadataRecord(entityId, EMPTY_PROPERTIES, Sets.newHashSet(tagsToRemove)));
  }

  private void removeTagsNoPublish(final Id.NamespacedId entityId, final String ... tagsToRemove) {
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        input.businessMds.removeTags(entityId, tagsToRemove);
        return null;
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
    return txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable,
      Iterable<BusinessMetadataRecord>>() {
      @Override
      public Iterable<BusinessMetadataRecord> apply(BusinessMdsIterable input) throws Exception {
        // Currently we support two types of search formats: value and key:value.
        // Check for existence of separator char to make sure we did search in the right indexed column.
        if (searchQuery.contains(BusinessMetadataDataset.KEYVALUE_SEPARATOR)) {
          // key=value search
          return input.businessMds.findBusinessMetadataOnKeyValue(searchQuery, type);
        }
        // value search
        return input.businessMds.findBusinessMetadataOnValue(searchQuery, type);
      }
    });
  }

  private void publish(MetadataRecord previous, MetadataRecord additions, MetadataRecord deletions) {
    MetadataChangeRecord.MetadataDiffRecord diff = new MetadataChangeRecord.MetadataDiffRecord(additions, deletions);
    MetadataChangeRecord changeRecord = new MetadataChangeRecord(previous, diff, System.currentTimeMillis());
    changePublisher.publish(changeRecord);
  }

  private static final class BusinessMdsIterable implements Iterable<BusinessMetadataDataset> {
    private final BusinessMetadataDataset businessMds;

    private BusinessMdsIterable(BusinessMetadataDataset mdsTable) {
      this.businessMds = mdsTable;
    }

    @Override
    public Iterator<BusinessMetadataDataset> iterator() {
      return Iterators.singletonIterator(businessMds);
    }
  }

}
