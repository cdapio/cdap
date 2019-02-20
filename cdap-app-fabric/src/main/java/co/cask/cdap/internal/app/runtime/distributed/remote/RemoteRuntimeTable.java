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

package co.cask.cdap.internal.app.runtime.distributed.remote;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.master.spi.program.Arguments;
import co.cask.cdap.master.spi.program.ProgramOptions;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.StructuredTableContext;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.Fields;
import co.cask.cdap.spi.data.table.field.Range;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides data logic for the program runtime.
 */
public class RemoteRuntimeTable {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .create();
  private final StructuredTable table;

  /**
   * Gets an instance of {@link RemoteRuntimeTable}.
   *
   * @return a new instance of {@link RemoteRuntimeTable}
   */
  public static RemoteRuntimeTable create(StructuredTableContext structuredTableContext) {
    return new RemoteRuntimeTable(structuredTableContext.getTable(StoreDefinition.RemoteRuntimeStore.RUNTIMES));
  }

  private RemoteRuntimeTable(StructuredTable table) {
    this.table = table;
  }

  @VisibleForTesting
  void deleteAll() throws IOException {
    table.deleteAll(Range.all());
  }

  /**
   * Writes the information for the given program run.
   *
   * @param programRunId the program run id for the run
   * @param programOptions the program options for the run
   */
  public void write(ProgramRunId programRunId, ProgramOptions programOptions) throws IOException {
    List<Field<?>> key = getKey(programRunId);
    key.add(Fields.stringField(StoreDefinition.RemoteRuntimeStore.PROGRAM_OPTIONS_FIELD, GSON.toJson(programOptions)));
    table.upsert(key);
  }

  /**
   * Deletes the program run information.
   *
   * @param programRunId the program run id to delete
   */
  public void delete(ProgramRunId programRunId) throws IOException {
    table.delete(getKey(programRunId));
  }

  /**
   * Scans for program run information
   *
   * @param limit maximum number of task info to return
   * @param lastProgramRunId an optional {@link ProgramRunId} returned from previous scan to continue the scanning
   * @return a {@link List} of {@link ProgramRunId} to the corresponding {@link ProgramOptions} for each run
   */
  public List<Map.Entry<ProgramRunId, ProgramOptions>> scan(int limit, @Nullable ProgramRunId lastProgramRunId)
    throws IOException {
    Range range = Range.all();
    if (lastProgramRunId != null) {
      // If there is a start program run id, scan from that id instead of from the beginning
      range = Range.from(getKey(lastProgramRunId), Range.Bound.EXCLUSIVE);
    }

    List<Map.Entry<ProgramRunId, ProgramOptions>> result = new LinkedList<>();
    try (CloseableIterator<StructuredRow> iterator = table.scan(range, limit)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        ProgramRunId programRunId = getProgramRunId(row);

        result.add(
          Maps.immutableEntry(
            programRunId, GSON.fromJson(
              row.getString(StoreDefinition.RemoteRuntimeStore.PROGRAM_OPTIONS_FIELD), ProgramOptions.class)));
      }
    }

    return result;
  }

  private List<Field<?>> getKey(ProgramRunId programRunId) {
    List<Field<?>> fields = new ArrayList();
    fields.add(Fields.stringField(StoreDefinition.RemoteRuntimeStore.NAMESPACE_FIELD, programRunId.getNamespace()));
    fields.add(Fields.stringField(StoreDefinition.RemoteRuntimeStore.APPLICATION_FIELD, programRunId.getApplication()));
    fields.add(Fields.stringField(StoreDefinition.RemoteRuntimeStore.VERSION_FIELD, programRunId.getVersion()));
    fields.add(
      Fields.stringField(StoreDefinition.RemoteRuntimeStore.PROGRAM_TYPE_FIELD, programRunId.getType().name()));
    fields.add(Fields.stringField(StoreDefinition.RemoteRuntimeStore.PROGRAM_FIELD, programRunId.getProgram()));
    fields.add(Fields.stringField(StoreDefinition.RemoteRuntimeStore.RUN_FIELD, programRunId.getRun()));
    return fields;
  }

  /**
   * Parses the {@link ProgramRunId} from the {@link StructuredRow}.
   */
  private ProgramRunId getProgramRunId(StructuredRow row) {
    return new ProgramRunId(new ApplicationId(row.getString(StoreDefinition.RemoteRuntimeStore.NAMESPACE_FIELD),
                                              row.getString(StoreDefinition.RemoteRuntimeStore.APPLICATION_FIELD),
                                              row.getString(StoreDefinition.RemoteRuntimeStore.VERSION_FIELD)),
                            ProgramType.valueOf(row.getString(StoreDefinition.RemoteRuntimeStore.PROGRAM_TYPE_FIELD)),
                            row.getString(StoreDefinition.RemoteRuntimeStore.PROGRAM_FIELD),
                            row.getString(StoreDefinition.RemoteRuntimeStore.RUN_FIELD));
  }
}
