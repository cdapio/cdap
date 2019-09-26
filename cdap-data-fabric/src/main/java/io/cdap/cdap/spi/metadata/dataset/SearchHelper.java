/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.spi.metadata.dataset;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.Transactionals;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.lib.IndexedTableDefinition;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.metadata.dataset.MetadataDataset;
import io.cdap.cdap.data2.metadata.dataset.MetadataDatasetDefinition;
import io.cdap.cdap.data2.metadata.dataset.MetadataEntry;
import io.cdap.cdap.data2.metadata.dataset.SearchRequest;
import io.cdap.cdap.data2.metadata.dataset.SearchResults;
import io.cdap.cdap.data2.metadata.dataset.SortInfo;
import io.cdap.cdap.data2.transaction.Transactions;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.metadata.MetadataSearchResponse;
import io.cdap.cdap.proto.metadata.MetadataSearchResultRecord;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

import static io.cdap.cdap.api.metadata.MetadataScope.SYSTEM;
import static io.cdap.cdap.api.metadata.MetadataScope.USER;

/**
 * Implements the metadata search over metadata datasets.
 */
public class SearchHelper {

  private static final Logger LOG = LoggerFactory.getLogger(SearchHelper.class);

  private static final DatasetId BUSINESS_METADATA_INSTANCE_ID = NamespaceId.SYSTEM.dataset("meta.business");
  private static final DatasetId SYSTEM_METADATA_INSTANCE_ID = NamespaceId.SYSTEM.dataset("meta.system");

  private static final DatasetContext SYSTEM_CONTEXT = DatasetContext.from(NamespaceId.SYSTEM.getNamespace());

  private static final Comparator<Map.Entry<MetadataEntity, Integer>> SEARCH_RESULT_DESC_SCORE_COMPARATOR =
    // sort in descending order
    (o1, o2) -> o2.getValue() - o1.getValue();

  private final DatasetDefinition<MetadataDataset, DatasetAdmin> metaDatasetDefinition;
  private final Map<String, DatasetSpecification> datasetSpecs;
  protected final Transactional transactional;

  @Inject
  public SearchHelper(TransactionSystemClient txClient,
                      @Named(Constants.Dataset.TABLE_TYPE) DatasetDefinition tableDefinition) {
    //noinspection unchecked
    this.metaDatasetDefinition = new MetadataDatasetDefinition(MetadataDataset.TYPE,
                                                               new IndexedTableDefinition("indexedTable",
                                                                                          tableDefinition));
    this.datasetSpecs = ImmutableMap.of(MetadataScope.SYSTEM.name(), createDatasetSpec(metaDatasetDefinition, SYSTEM),
                                        MetadataScope.USER.name(), createDatasetSpec(metaDatasetDefinition, USER));
    this.transactional = Transactions.createTransactionalWithRetry(
      createTransactional(txClient), RetryStrategies.retryOnConflict(20, 100));
  }

  void createDatasets() throws IOException {
    for (MetadataScope scope : MetadataScope.ALL) {
      DatasetAdmin admin = metaDatasetDefinition.getAdmin(SYSTEM_CONTEXT, datasetSpecs.get(scope.name()), null);
      if (!admin.exists()) {
        admin.create();
      }
    }
  }

  void dropDatasets() throws IOException {
    for (MetadataScope scope : MetadataScope.ALL) {
      DatasetAdmin admin = metaDatasetDefinition.getAdmin(SYSTEM_CONTEXT, datasetSpecs.get(scope.name()), null);
      if (admin.exists()) {
        admin.drop();
      }
    }
  }

  private static DatasetSpecification createDatasetSpec(DatasetDefinition def, MetadataScope scope) {
    return def.configure(getMetadataDatasetInstance(scope).getEntityName(),
                         DatasetProperties.builder().add(MetadataDatasetDefinition.SCOPE_KEY, scope.name()).build());
  }

  private static DatasetId getMetadataDatasetInstance(MetadataScope scope) {
    return USER == scope ? BUSINESS_METADATA_INSTANCE_ID : SYSTEM_METADATA_INSTANCE_ID;
  }

  private class MetaOnlyDatasetContext implements io.cdap.cdap.api.data.DatasetContext, AutoCloseable {
    private final Map<String, MetadataDataset> datasets = new HashMap<>();
    private final TransactionContext txContext;

    private MetaOnlyDatasetContext(TransactionContext txContext) {
      this.txContext = txContext;
    }

    @Override
    public <T extends Dataset> T getDataset(String scope) throws DatasetInstantiationException {
      MetadataDataset dataset = datasets.get(scope);
      if (dataset == null) {
        try {
          dataset = metaDatasetDefinition.getDataset(
            SYSTEM_CONTEXT, datasetSpecs.get(scope), Collections.emptyMap(), null);
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
        datasets.put(scope, dataset);
        txContext.addTransactionAware(dataset);
      }
      @SuppressWarnings("unchecked")
      T t = (T) dataset;
      return t;
    }

    @Override
    public <T extends Dataset> T getDataset(String namespace, String name) throws DatasetInstantiationException {
      throw new UnsupportedOperationException("This class intentionally does not implement this method");
    }

    @Override
    public <T extends Dataset> T getDataset(String name, Map<String, String> arguments) {
      throw new UnsupportedOperationException("This class intentionally does not implement this method");
    }

    @Override
    public <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments) {
      throw new UnsupportedOperationException("This class intentionally does not implement this method");
    }

    @Override
    public void releaseDataset(Dataset dataset) {
      // no-op
    }

    @Override
    public void discardDataset(Dataset dataset) {
      // no-op
    }

    @Override
    public void close() {
      for (String scope : datasets.keySet()) {
        Closeables.closeQuietly(datasets.get(scope));
      }
      datasets.clear();
    }
  }

  /**
   * Create a transactional for metadata datasets.
   *
   * @param txClient transaction client
   * @return transactional for the metadata tables
   */
  private Transactional createTransactional(TransactionSystemClient txClient) {
    return new Transactional() {
      @Override
      public void execute(io.cdap.cdap.api.TxRunnable runnable) throws TransactionFailureException {
        TransactionContext txContext = new TransactionContext(txClient);
        try (MetaOnlyDatasetContext datasetContext = new MetaOnlyDatasetContext(txContext)) {
          txContext.start();
          finishExecute(txContext, datasetContext, runnable);
        } catch (Exception e) {
          Throwables.propagateIfPossible(e, TransactionFailureException.class);
        }
      }

      @Override
      public void execute(int timeout, io.cdap.cdap.api.TxRunnable runnable) throws TransactionFailureException {
        TransactionContext txContext = new TransactionContext(txClient);
        try (MetaOnlyDatasetContext datasetContext = new MetaOnlyDatasetContext(txContext)) {
          txContext.start(timeout);
          finishExecute(txContext, datasetContext, runnable);
        } catch (Exception e) {
          Throwables.propagateIfPossible(e, TransactionFailureException.class);
        }
      }

      private void finishExecute(TransactionContext txContext, io.cdap.cdap.api.data.DatasetContext dsContext,
                                 io.cdap.cdap.api.TxRunnable runnable)
        throws TransactionFailureException {
        try {
          runnable.run(dsContext);
        } catch (Exception e) {
          txContext.abort(new TransactionFailureException("Exception raised from TxRunnable.run() " + runnable, e));
        }
        // The call the txContext.abort above will always have exception thrown
        // Hence we'll only reach here if and only if the runnable.run() returns normally.
        txContext.finish();
      }
    };
  }

  protected <T> T execute(TransactionExecutor.Function<MetadataDatasetContext, T> func) {
    return Transactionals.execute(transactional, context -> {
      return func.apply(scope -> context.getDataset(scope.name()));
    });
  }

  public MetadataSearchResponse search(SearchRequest request, @Nullable MetadataScope scope) {
    Set<MetadataScope> searchScopes = scope == null ? EnumSet.allOf(MetadataScope.class) : Collections.singleton(scope);
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
        searchScopes = EnumSet.of(SYSTEM);
      }
    }
    return search(searchScopes, request);
  }

  private MetadataSearchResponse search(Set<MetadataScope> scopes, SearchRequest request) {
    List<MetadataEntry> results = new LinkedList<>();
    List<String> cursors = new LinkedList<>();
    for (MetadataScope scope : scopes) {
      SearchResults searchResults = execute(context -> context.getDataset(scope).search(request));
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
    Set<MetadataEntity> subSortedEntities = new LinkedHashSet<>(
      ImmutableList.copyOf(sortedEntities).subList(startIndex, endIndex)
    );

    // Fetch metadata for entities in the result list
    // Note: since the fetch is happening in a different transaction, the metadata for entities may have been
    // removed. It is okay not to have metadata for some results in case this happens.
    Set<MetadataSearchResultRecord> finalResults = execute(
      context -> addMetadataToEntities(subSortedEntities,
                                       fetchMetadata(context.getDataset(SYSTEM), subSortedEntities),
                                       fetchMetadata(context.getDataset(USER), subSortedEntities)));

    return new MetadataSearchResponse(
      sortInfo.getSortBy() + " " + sortInfo.getSortOrder(), offset, limit, request.getNumCursors(), total,
      finalResults, cursors, request.shouldShowHidden(), request.getEntityScopes());
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

  private Map<MetadataEntity, MetadataDataset.Record> fetchMetadata(MetadataDataset mds,
                                                                    final Set<MetadataEntity> metadataEntities) {
    Set<MetadataDataset.Record> metadataSet = mds.getMetadata(metadataEntities);
    Map<MetadataEntity, MetadataDataset.Record> metadataMap = new HashMap<>();
    for (MetadataDataset.Record m : metadataSet) {
      metadataMap.put(m.getMetadataEntity(), m);
    }
    return metadataMap;
  }

  private Set<MetadataSearchResultRecord>
  addMetadataToEntities(Set<MetadataEntity> entities,
                        Map<MetadataEntity, MetadataDataset.Record> systemMetadata,
                        Map<MetadataEntity, MetadataDataset.Record> userMetadata) {
    Set<MetadataSearchResultRecord> result = new LinkedHashSet<>();
    for (MetadataEntity entity : entities) {
      ImmutableMap.Builder<MetadataScope, Metadata> builder = ImmutableMap.builder();
      // Add system metadata
      MetadataDataset.Record metadata = systemMetadata.get(entity);
      if (metadata != null) {
        builder.put(SYSTEM, new Metadata(metadata.getProperties(), metadata.getTags()));
      }

      // Add user metadata
      metadata = userMetadata.get(entity);
      if (metadata != null) {
        builder.put(MetadataScope.USER, new Metadata(metadata.getProperties(), metadata.getTags()));
      }

      // Create result
      result.add(new MetadataSearchResultRecord(entity, builder.build()));
    }
    return result;
  }
}
