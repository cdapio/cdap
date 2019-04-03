/*
 * Copyright © 2018-2019 Cask Data, Inc.
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


package io.cdap.cdap.data2.metadata.lineage.field;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.proto.metadata.lineage.ProgramRunOperations;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Implementation of {@link FieldLineageReader} for reading the field lineage information
 * from {@link FieldLineageTable}.
 */
public class DefaultFieldLineageReader implements FieldLineageReader {
  private final TransactionRunner transactionRunner;

  @Inject
  @VisibleForTesting
  public DefaultFieldLineageReader(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  @Override
  public Set<String> getFields(EndPoint endPoint, long start, long end) {
    return TransactionRunners.run(transactionRunner, context -> {
      FieldLineageTable fieldLineageTable = FieldLineageTable.create(context);
      return fieldLineageTable.getFields(endPoint, start, end);
    });
  }

  @Override
  public Set<EndPointField> getIncomingSummary(EndPointField endPointField, long start, long end) {
    return TransactionRunners.run(transactionRunner, context -> {
      FieldLineageTable fieldLineageTable = FieldLineageTable.create(context);
      return fieldLineageTable.getIncomingSummary(endPointField, start, end);
    });
  }

  @Override
  public Set<EndPointField> getOutgoingSummary(EndPointField endPointField, long start, long end) {
    return TransactionRunners.run(transactionRunner, context -> {
      FieldLineageTable fieldLineageTable = FieldLineageTable.create(context);
      return fieldLineageTable.getOutgoingSummary(endPointField, start, end);
    });
  }

  @Override
  public List<ProgramRunOperations> getIncomingOperations(EndPointField endPointField, long start, long end) {
    return computeFieldOperations(true, endPointField, start, end);
  }

  @Override
  public List<ProgramRunOperations> getOutgoingOperations(EndPointField endPointField, long start, long end) {
    return computeFieldOperations(false, endPointField, start, end);
  }

  private List<ProgramRunOperations> computeFieldOperations(boolean incoming, EndPointField endPointField,
                                                            long start, long end) {
    Set<ProgramRunOperations> endPointOperations = TransactionRunners.run(transactionRunner, context -> {
      FieldLineageTable fieldLineageTable = FieldLineageTable.create(context);

      return incoming ? fieldLineageTable.getIncomingOperations(endPointField.getEndPoint(), start, end)
        : fieldLineageTable.getOutgoingOperations(endPointField.getEndPoint(), start, end);
    });

    List<ProgramRunOperations> endPointFieldOperations = new ArrayList<>();
    for (ProgramRunOperations programRunOperation : endPointOperations) {
      try {
        // No need to compute summaries here.
        FieldLineageInfo info = new FieldLineageInfo(programRunOperation.getOperations(), false);
        Set<Operation> fieldOperations = incoming ?
          info.getIncomingOperationsForField(endPointField) : info.getOutgoingOperationsForField(endPointField);
        ProgramRunOperations result = new ProgramRunOperations(programRunOperation.getProgramRunIds(), fieldOperations);
        endPointFieldOperations.add(result);
      } catch (Throwable e) {
        // TODO: possibly relax validation logic when info object created from here
      }
    }
    return endPointFieldOperations;
  }
}
