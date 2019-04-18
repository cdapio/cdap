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

package io.cdap.cdap.internal.provision;

import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Stores information used for provisioning.
 *
 * Stores subscriber offset information for TMS, cluster information for program runs, and state information for
 * each provision and deprovision operation.
 *
 * Provisioner Store uses transactionRunners to perform underlying CRUD operations.
 */
final class ProvisionerStore {

  private final TransactionRunner txRunner;

  private ProvisionerTable getProvisionerTable(StructuredTableContext context) throws TableNotFoundException {
    return new ProvisionerTable(context);
  }

  @Inject
  ProvisionerStore(TransactionRunner txRunner) {
    this.txRunner = txRunner;
  }

  List<ProvisioningTaskInfo> listTaskInfo() throws IOException {
    return TransactionRunners.run(txRunner, context -> {
      return getProvisionerTable(context).listTaskInfo();
    }, IOException.class);
  }

  @Nullable
  ProvisioningTaskInfo getTaskInfo(final ProvisioningTaskKey key) throws IOException {
    return TransactionRunners.run(txRunner, context -> {
      return getProvisionerTable(context).getTaskInfo(key);
    }, IOException.class);
  }

  void putTaskInfo(final ProvisioningTaskInfo taskInfo) throws IOException {
    TransactionRunners.run(txRunner, context -> {
      getProvisionerTable(context).putTaskInfo(taskInfo);
    }, IOException.class);
  }

  void deleteTaskInfo(ProgramRunId programRunId) throws IOException {
    TransactionRunners.run(txRunner, context -> {
      getProvisionerTable(context).deleteTaskInfo(programRunId);
    }, IOException.class);
  }

  @Nullable
  ProvisioningTaskInfo getExistingAndCancel(final ProvisioningTaskKey taskKey) throws IOException {
    return TransactionRunners.run(txRunner, context -> {
      ProvisionerTable table = getProvisionerTable(context);
      ProvisioningTaskInfo currentTaskInfo = table.getTaskInfo(taskKey);
      if (currentTaskInfo == null) {
        return null;
      }
      // write that the state has been cancelled. This is in case CDAP dies or is killed before the cluster can
      // be deprovisioned and the task state cleaned up. When CDAP starts back up, it will see that the task is
      // cancelled and will not resume the task.
      ProvisioningOp newOp =
        new ProvisioningOp(currentTaskInfo.getProvisioningOp().getType(), ProvisioningOp.Status.CANCELLED);
      ProvisioningTaskInfo newTaskInfo = new ProvisioningTaskInfo(currentTaskInfo, newOp, currentTaskInfo.getCluster());
      table.putTaskInfo(newTaskInfo);
      return currentTaskInfo;
    }, IOException.class);
  }
}
