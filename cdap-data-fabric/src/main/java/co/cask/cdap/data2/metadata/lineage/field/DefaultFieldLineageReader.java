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
import co.cask.cdap.proto.metadata.lineage.ProgramRunOperations;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;

import java.util.Collections;
import java.util.HashSet;
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
    this(datasetFramework, txClient, FieldLineageDataset.FIELD_LINEAGE_DATASET_ID);
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
  public Set<String> getFields(EndPoint endPoint, long start, long end) {
    return Transactionals.execute(transactional, context -> {
      FieldLineageDataset fieldLineageDataset = FieldLineageDataset.getFieldLineageDataset(context, datasetFramework,
                                                                                           fieldLineageDatasetId);

      return fieldLineageDataset.getFields(endPoint, start, end);
    });
  }

  @Override
  public Set<EndPointField> getIncomingSummary(EndPointField endPointField, long start, long end) {
    return Transactionals.execute(transactional, context -> {
      FieldLineageDataset fieldLineageDataset = FieldLineageDataset.getFieldLineageDataset(context, datasetFramework,
                                                                                           fieldLineageDatasetId);

      return fieldLineageDataset.getIncomingSummary(endPointField, start, end);
    });
  }

  @Override
  public Set<EndPointField> getOutgoingSummary(EndPointField endPointField, long start, long end) {
    return Transactionals.execute(transactional, context -> {
      FieldLineageDataset fieldLineageDataset = FieldLineageDataset.getFieldLineageDataset(context, datasetFramework,
                                                                                           fieldLineageDatasetId);

      return fieldLineageDataset.getOutgoingSummary(endPointField, start, end);
    });
  }

  @Override
  public Set<ProgramRunOperations> getIncomingOperations(EndPointField endPointField, long start, long end) {
    return computeFieldOperations(true, endPointField, start, end);
  }

  @Override
  public Set<ProgramRunOperations> getOutgoingOperations(EndPointField endPointField, long start, long end) {
    return computeFieldOperations(false, endPointField, start, end);
  }

  private Set<ProgramRunOperations> computeFieldOperations(boolean incoming, EndPointField endPointField,
                                                           long start, long end) {
    Set<ProgramRunOperations> endPointOperations = Transactionals.execute(transactional, context -> {
      FieldLineageDataset fieldLineageDataset = FieldLineageDataset.getFieldLineageDataset(context, datasetFramework,
              fieldLineageDatasetId);

      return incoming ? fieldLineageDataset.getIncomingOperations(endPointField.getEndPoint(), start, end)
              : fieldLineageDataset.getOutgoingOperations(endPointField.getEndPoint(), start, end);
    });

    Set<ProgramRunOperations> endPointFieldOperations = new HashSet<>();
    for (ProgramRunOperations programRunOperation : endPointOperations) {
      try {
        FieldLineageInfo info = new FieldLineageInfo(programRunOperation.getOperations());
        Set<Operation> fieldOperations = incoming ?
                info.getIncomingOperationsForField(endPointField)
                : Collections.EMPTY_SET;
        ProgramRunOperations result = new ProgramRunOperations(programRunOperation.getProgramRunIds(), fieldOperations);
        endPointFieldOperations.add(result);
      } catch (Throwable e) {
        // TODO: possibly relax validation logic when info object created from here
      }
    }
    return endPointFieldOperations;
  }
}
