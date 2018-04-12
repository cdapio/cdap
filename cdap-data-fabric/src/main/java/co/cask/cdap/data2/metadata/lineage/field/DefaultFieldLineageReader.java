/*
 * Copyright © 2018 Cask Data, Inc.
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


package co.cask.cdap.data2.metadata.lineage.field;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.Operation;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;

import java.util.Set;

/**
 * Implementation of {@link FieldLineageReader} for reading the field lineage information
 * from {@link FieldLineageDataset}.
 */
public class DefaultFieldLineageReader implements FieldLineageReader {
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final DatasetId fieldLineageDatasetId;

  @Inject
  DefaultFieldLineageReader(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this(datasetFramework, txClient, FieldLineageDataset.FIELDLINEAGE_DATASET_ID);
  }

  @VisibleForTesting
  public DefaultFieldLineageReader(DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                   DatasetId fieldLineageDatasetId) {
    this.datasetFramework = datasetFramework;
    this.fieldLineageDatasetId = fieldLineageDatasetId;
    this.transactional = Transactions.createTransactional(new MultiThreadDatasetCache(
      new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(txClient),
      NamespaceId.SYSTEM, ImmutableMap.of(), null, null));
  }

  @Override
  public Set<FieldLineageInfo> getFieldLineageInfos(EndPoint endPoint, long start, long end) {
    return Transactionals.execute(transactional, context -> {
      FieldLineageDataset fieldLineageDataset = FieldLineageDataset.getFieldLineageDataset(context, datasetFramework,
                                                                                           fieldLineageDatasetId);
      return fieldLineageDataset.getFieldLineageInfosInRange(endPoint, start, end);
    });
  }
}
