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
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.data2.metadata.dataset.BusinessMetadataDataset;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * Implements operations on {@link BusinessMetadataDataset} transactionally.
 */
public class BusinessMetadataStore {

  private static final Id.DatasetInstance BUSINESS_METADATA_INSTANCE_ID =
    Id.DatasetInstance.from(Id.Namespace.SYSTEM, "business.metadata");
  private final Transactional<BusinessMdsIterable, BusinessMetadataDataset> txnl;

  @Inject
  public BusinessMetadataStore(TransactionExecutorFactory txExecutorFactory, final DatasetFramework dsFramework) {
    this.txnl = Transactional.of(txExecutorFactory, new Supplier<BusinessMdsIterable>() {
      @Override
      public BusinessMdsIterable get() {
        try {
          Object dataset = DatasetsUtil.getOrCreateDataset(dsFramework, BUSINESS_METADATA_INSTANCE_ID,
                                                            BusinessMetadataDataset.class.getSimpleName(),
                                                            DatasetProperties.EMPTY, null, null);
          if (dataset instanceof BusinessMetadataDataset) {
            return new BusinessMdsIterable((BusinessMetadataDataset) dataset);
          }
          return new BusinessMdsIterable(new BusinessMetadataDataset((IndexedTable) dataset));
        } catch (DatasetManagementException | IOException | ServiceUnavailableException e) {
          throw Throwables.propagate(e);
        }
      }
    });
  }

  /**
   * Adds/updates metadata for the specified {@link Id.NamespacedId}.
   *
   * @param entityId the {@link Id.NamespacedId} for which metadata is to be updated
   * @param metadata metadata (represented as a map) to update
   */
  void addMetadata(final Id.NamespacedId entityId, final Map<String, String> metadata) {
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
          input.businessMds.createBusinessMetadata(entityId, entry.getKey(), entry.getValue());
        }
        return null;
      }
    });
  }

  /**
   * Adds tags for the specified {@link Id.NamespacedId}.
   */
  void addTags(final Id.NamespacedId entityId, final String ... tagsToAdd) {
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        Map<String, String> businessMetadata = input.businessMds.getBusinessMetadata(entityId);
        Iterable<String> existingTags = getTags(businessMetadata);
        Iterable<String> newTags = Iterables.concat(existingTags, Arrays.asList(tagsToAdd));
        input.businessMds.createBusinessMetadata(entityId, "tags", Joiner.on(",").join(newTags));
        return null;
      }
    });
  }

  /**
   * @return the metadata for the specified {@link Id.NamespacedId}
   */
  Map<String, String> getMetadata(final Id.NamespacedId entityId) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Map<String, String>>() {
      @Override
      public Map<String, String> apply(BusinessMdsIterable input) throws Exception {
        return input.businessMds.getBusinessMetadata(entityId);
      }
    });
  }

  /**
   * @return the tags for the specified {@link Id.NamespacedId}
   */
  Iterable<String> getTags(final Id.NamespacedId entityId) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Iterable<String>>() {
      @Override
      public Iterable<String> apply(BusinessMdsIterable input) throws Exception {
        Map<String, String> businessMetadata = input.businessMds.getBusinessMetadata(entityId);
        return getTags(businessMetadata);
      }
    });
  }

  /**
   * Removes all metadata for the specified {@link Id.NamespacedId}.
   */
  void removeMetadata(final Id.NamespacedId entityId) {
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        input.businessMds.removeMetadata(entityId);
        return null;
      }
    });
  }

  void removeMetadata(final Id.NamespacedId entityId, final String ... keys) {
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        input.businessMds.removeMetadata(entityId, keys);
        return null;
      }
    });
  }

  /**
   * Removes the specified tags from the {@link Id.NamespacedId}
   */
  void removeTags(final Id.NamespacedId entityId, final String ... tagsToRemove) {
    txnl.executeUnchecked(new TransactionExecutor.Function<BusinessMdsIterable, Void>() {
      @Override
      public Void apply(BusinessMdsIterable input) throws Exception {
        Map<String, String> businessMetadata = input.businessMds.getBusinessMetadata(entityId);
        Iterable<String> existingTags = getTags(businessMetadata);
        Iterables.removeAll(existingTags, Arrays.asList(tagsToRemove));
        input.businessMds.createBusinessMetadata(entityId, "tags", Joiner.on(",").join(existingTags));
        return null;
      }
    });
  }

  private Iterable<String> getTags(Map<String, String> businessMetadata) {
    if (!businessMetadata.containsKey("tags")) {
      return new ArrayList<>();
    }
    String tags = businessMetadata.get("tags");
    return Splitter.on(",").omitEmptyStrings().split(tags);
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
