/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.data2.metadata.dataset.BusinessMetadataDataset;
import co.cask.cdap.data2.metadata.dataset.BusinessMetadataRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Implements operations on {@link BusinessMetadataDataset} transactionally.
 */
public class BusinessMetadataStore {

  private static final Id.DatasetInstance BUSINESS_METADATA_INSTANCE_ID =
    Id.DatasetInstance.from(Id.Namespace.SYSTEM, "business.metadata");
  private final Transactional<BusinessMdsIterable, BusinessMetadataDataset> txnl;

  @Inject
  public BusinessMetadataStore(TransactionExecutorFactory txExecutorFactory,
                               @Named(DataSetsModules.BASIC_DATASET_FRAMEWORK) final DatasetFramework dsFramework) {
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
  }

  /**
   * Adds/updates metadata for the specified {@link Id.NamespacedId}.
   */
  public void setProperties(final Id.NamespacedId entityId, final Map<String, String> metadata) {
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
          input.businessMds.setProperty(entityId, entry.getKey(), entry.getValue());
        }
        return null;
      }
    });
  }

  /**
   * Adds tags for the specified {@link Id.NamespacedId}.
   */
  public void addTags(final Id.NamespacedId entityId, final String ... tagsToAdd) {
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
   * @return the metadata for the specified {@link Id.NamespacedId}
   */
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
  public void removeMetadata(final Id.NamespacedId entityId) {
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
  public void removeProperties(final Id.NamespacedId entityId) {
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
  public void removeProperties(final Id.NamespacedId entityId, final String... keys) {
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
  public void removeTags(final Id.NamespacedId entityId) {
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
  public void removeTags(final Id.NamespacedId entityId, final String ... tagsToRemove) {
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
  public Iterable<BusinessMetadataRecord> searchMetadata(final String searchQuery) {
    return searchMetadataOnType(searchQuery, MetadataSearchTargetType.ALL);
  }

  /**
   * Search to the underlying Business Metadata Dataset for a target type.
   */
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
