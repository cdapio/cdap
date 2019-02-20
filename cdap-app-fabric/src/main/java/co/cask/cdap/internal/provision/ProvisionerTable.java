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

package co.cask.cdap.internal.provision;

import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.master.spi.program.Arguments;
import co.cask.cdap.master.spi.program.ProgramOptions;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.StructuredTableContext;
import co.cask.cdap.spi.data.TableNotFoundException;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.Fields;
import co.cask.cdap.spi.data.table.field.Range;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Operations on top of StructuredTable for Provisioning related CRUD operations
 */
public class ProvisionerTable {
  private final StructuredTable table;
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .create();
  public ProvisionerTable(StructuredTableContext context) throws TableNotFoundException {
    this.table = context.getTable(StoreDefinition.ProvisionerStore.PROVISIONER_TABLE);
  }

  /**
   * @return List of {@link ProvisioningTaskInfo}
   * @throws IOException if there is an error reading from underlying structured table.
   */
  public List<ProvisioningTaskInfo> listTaskInfo() throws IOException {
    Iterator<StructuredRow> iterator = table.scan(Range.all(), Integer.MAX_VALUE);
    List<ProvisioningTaskInfo> result = new ArrayList<>();
    while (iterator.hasNext()) {
      result.add(deserialize(iterator.next().getString(StoreDefinition.ProvisionerStore.PROVISIONER_TASK_INFO_FIELD)));
    }
    return result;
  }

  /**
   * Fetch Provisioning Task Information
   * @param key ProvisioningTaskKey for the corresponding task info.
   * @return instance of {@link ProvisioningTaskInfo}.
   * @throws IOException if there is an issue reading from underlying structured table.
   */
  @Nullable
  public ProvisioningTaskInfo getTaskInfo(ProvisioningTaskKey key) throws IOException {
    Optional<StructuredRow> row = table.read(createPrimaryKey(key.getProgramRunId(), key.getType()));
    return row.isPresent() ?
      deserialize(row.get().getString(StoreDefinition.ProvisionerStore.PROVISIONER_TASK_INFO_FIELD)) :
      null;
  }

  /**
   * Persist the provisioning taskInfo.
   * @param taskInfo {@link ProvisioningTaskInfo}to be persisted.
   * @throws IOException if there is an issue writing to the underlying structured table.
   */
  public void putTaskInfo(ProvisioningTaskInfo taskInfo) throws IOException {
    List<Field<?>> fields = createPrimaryKey(taskInfo.getTaskKey().getProgramRunId(), taskInfo.getTaskKey().getType());
    fields.add(Fields.stringField(StoreDefinition.ProvisionerStore.PROVISIONER_TASK_INFO_FIELD, serialize(taskInfo)));
    table.upsert(fields);
  }

  /**
   * Delete provisioning task info for the corresponding program run id.
   * @param runId to delete.
   * @throws IOException if there is an issue deleting from the underlying structured table.
   */
  public void deleteTaskInfo(ProgramRunId runId) throws IOException {
    // Delete the keys with Provision and Deprovision, type is set to null to delete provision and deprovision types
    table.deleteAll(Range.singleton(createPrimaryKey(runId, null)));
  }

  private List<Field<?>> createPrimaryKey(ProgramRunId runId, @Nullable ProvisioningOp.Type type) {
    List<Field<?>> fields = Lists.newArrayList(
      Fields.stringField(StoreDefinition.ProvisionerStore.NAMESPACE_FIELD, runId.getNamespace()),
      Fields.stringField(StoreDefinition.ProvisionerStore.APPLICATION_FIELD, runId.getApplication()),
      Fields.stringField(StoreDefinition.ProvisionerStore.VERSION_FIELD, runId.getVersion()),
      Fields.stringField(StoreDefinition.ProvisionerStore.PROGRAM_TYPE_FIELD, runId.getType().name()),
      Fields.stringField(StoreDefinition.ProvisionerStore.PROGRAM_FIELD, runId.getProgram()),
      Fields.stringField(StoreDefinition.ProvisionerStore.RUN_FIELD, runId.getRun()));

      if (null != type) {
        fields.add(Fields.stringField(StoreDefinition.ProvisionerStore.KEY_TYPE, type.name()));
      }
      return fields;
  }

  private ProvisioningTaskInfo deserialize(String provisioningTaskInfo) {
    return GSON.fromJson(provisioningTaskInfo, ProvisioningTaskInfo.class);
  }

  private String serialize(ProvisioningTaskInfo taskInfo) {
    return GSON.toJson(taskInfo, ProvisioningTaskInfo.class);
  }
}
