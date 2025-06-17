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

import com.google.common.collect.Lists;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.internal.provision.adapters.ProvisioningTaskInfoAdapter;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Stores information used for provisioning.
 *
 * <p>Stores subscriber offset information for TMS, cluster information for program runs, and state
 * information for each provision and deprovision operation.</p>
 *
 * <p>Provisioner Store uses transactionRunners to perform underlying CRUD operations.</p>
 */
public final class ProvisionerStore {

  private final TransactionRunner txRunner;

  private StructuredTable getProvisionerTable(StructuredTableContext context)
      throws TableNotFoundException {
    return context.getTable(StoreDefinition.ProvisionerStore.PROVISIONER_TABLE);
  }

  @Inject
  ProvisionerStore(TransactionRunner txRunner) {
    this.txRunner = txRunner;
  }

  /**
   * @return List of {@link ProvisioningTaskInfo}
   * @throws IOException if there is an error reading from underlying structured table.
   */
  List<ProvisioningTaskInfo> listTaskInfo() throws IOException {
    return TransactionRunners.run(txRunner, context -> {
      List<ProvisioningTaskInfo> result = new ArrayList<>();
      try (CloseableIterator<StructuredRow> iterator = getProvisionerTable(context).scan(
          Range.all(), Integer.MAX_VALUE)) {
        while (iterator.hasNext()) {
          StructuredRow row = iterator.next();
          result.add(ProvisioningTaskInfoAdapter.fromJson(
              row.getString(StoreDefinition.ProvisionerStore.PROVISIONER_TASK_INFO_FIELD),
              context));
        }
      }
      return result;
    }, IOException.class);
  }

  /**
   * Fetch Provisioning Task Information.
   *
   * @param key ProvisioningTaskKey for the corresponding task info.
   * @return instance of {@link ProvisioningTaskInfo}.
   * @throws IOException if there is an issue reading from underlying structured table.
   */
  @Nullable
  public ProvisioningTaskInfo getTaskInfo(final ProvisioningTaskKey key) throws IOException {
    return TransactionRunners.run(txRunner, context -> {
      return fetchTaskInfo(context, key);
    }, IOException.class);
  }

  /**
   * Persist the provisioning taskInfo.
   *
   * @param taskInfo {@link ProvisioningTaskInfo}to be persisted.
   * @throws IOException if there is an issue writing to the underlying structured table.
   */
  public void putTaskInfo(final ProvisioningTaskInfo taskInfo) throws IOException {
    TransactionRunners.run(txRunner, context -> {
      persistTaskInfo(context, taskInfo);
    }, IOException.class);
  }

  /**
   * Delete provisioning task info for the corresponding program run id.
   *
   * @param runId to delete.
   * @throws IOException if there is an issue deleting from the underlying structured table.
   */
  void deleteTaskInfo(ProgramRunId programRunId) throws IOException {
    TransactionRunners.run(txRunner, context -> {
      // Delete the keys with Provision and Deprovision, type is set to null to delete provision and deprovision types
      getProvisionerTable(context).deleteAll(Range.singleton(createPrimaryKey(programRunId, null)));
    }, IOException.class);
  }

  @Nullable
  ProvisioningTaskInfo getExistingAndCancel(final ProvisioningTaskKey taskKey) throws IOException {
    return TransactionRunners.run(txRunner, context -> {
      ProvisioningTaskInfo currentTaskInfo = fetchTaskInfo(context, taskKey);
      if (currentTaskInfo == null) {
        return null;
      }
      // write that the state has been cancelled. This is in case CDAP dies or is killed before the cluster can
      // be deprovisioned and the task state cleaned up. When CDAP starts back up, it will see that the task is
      // cancelled and will not resume the task.
      ProvisioningOp newOp = new ProvisioningOp(currentTaskInfo.getProvisioningOp().getType(),
          ProvisioningOp.Status.CANCELLED);
      ProvisioningTaskInfo newTaskInfo = new ProvisioningTaskInfo(currentTaskInfo, newOp,
          currentTaskInfo.getCluster());
      persistTaskInfo(context, newTaskInfo);
      return currentTaskInfo;
    }, IOException.class);
  }

  private List<Field<?>> createPrimaryKey(ProgramRunId runId, @Nullable ProvisioningOp.Type type) {
    List<Field<?>> fields = Lists.newArrayList(
        Fields.stringField(StoreDefinition.ProvisionerStore.NAMESPACE_FIELD, runId.getNamespace()),
        Fields.stringField(StoreDefinition.ProvisionerStore.APPLICATION_FIELD,
            runId.getApplication()),
        Fields.stringField(StoreDefinition.ProvisionerStore.VERSION_FIELD, runId.getVersion()),
        Fields.stringField(StoreDefinition.ProvisionerStore.PROGRAM_TYPE_FIELD,
            runId.getType().name()),
        Fields.stringField(StoreDefinition.ProvisionerStore.PROGRAM_FIELD, runId.getProgram()),
        Fields.stringField(StoreDefinition.ProvisionerStore.RUN_FIELD, runId.getRun()));

    if (null != type) {
      fields.add(Fields.stringField(StoreDefinition.ProvisionerStore.KEY_TYPE, type.name()));
    }
    return fields;
  }

  /**
   * Persists {@link ProvisioningTaskInfo} in the provisioner table.
   */
  private void persistTaskInfo(StructuredTableContext context, ProvisioningTaskInfo taskInfo)
      throws IOException {
    String serializedTaskInfo = ProvisioningTaskInfoAdapter.toJson(taskInfo, context);
    List<Field<?>> fields = createPrimaryKey(taskInfo.getTaskKey().getProgramRunId(),
        taskInfo.getTaskKey().getType());
    fields.add(Fields.stringField(StoreDefinition.ProvisionerStore.PROVISIONER_TASK_INFO_FIELD,
        serializedTaskInfo));
    getProvisionerTable(context).upsert(fields);
  }

  /**
   * Fetches {@link ProvisioningTaskInfo} from the provisioner table.
   */
  private ProvisioningTaskInfo fetchTaskInfo(StructuredTableContext context,
      ProvisioningTaskKey key) throws IOException {
    Optional<StructuredRow> row = getProvisionerTable(context).read(
        createPrimaryKey(key.getProgramRunId(), key.getType()));
    String taskInfoJson = row.map(structuredRow -> structuredRow.getString(
        StoreDefinition.ProvisionerStore.PROVISIONER_TASK_INFO_FIELD)).orElse(null);
    return ProvisioningTaskInfoAdapter.fromJson(taskInfoJson, context);
  }
}
