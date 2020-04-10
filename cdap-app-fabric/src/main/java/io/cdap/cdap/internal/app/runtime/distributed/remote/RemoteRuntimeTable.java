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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
   * Checks if the given program run id exists.
   *
   * @param programRunId the program run id to check
   * @return {@code true} if the id exists; {@code false} otherwise
   * @throws IOException if failed to read from the underlying table
   */
  public boolean exists(ProgramRunId programRunId) throws IOException {
    List<Field<?>> key = getKey(programRunId);
    return table.read(key).isPresent();
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
        result.add(Maps.immutableEntry(getProgramRunId(row), getProgramOptions(row)));
      }
    }

    return result;
  }

  private List<Field<?>> getKey(ProgramRunId programRunId) {
    return new ArrayList<>(Arrays.asList(
      Fields.stringField(StoreDefinition.RemoteRuntimeStore.NAMESPACE_FIELD, programRunId.getNamespace()),
      Fields.stringField(StoreDefinition.RemoteRuntimeStore.APPLICATION_FIELD, programRunId.getApplication()),
      Fields.stringField(StoreDefinition.RemoteRuntimeStore.VERSION_FIELD, programRunId.getVersion()),
      Fields.stringField(StoreDefinition.RemoteRuntimeStore.PROGRAM_TYPE_FIELD, programRunId.getType().name()),
      Fields.stringField(StoreDefinition.RemoteRuntimeStore.PROGRAM_FIELD, programRunId.getProgram()),
      Fields.stringField(StoreDefinition.RemoteRuntimeStore.RUN_FIELD, programRunId.getRun())
    ));
  }

  /**
   * Parses the {@link ProgramRunId} from the {@link StructuredRow}.
   */
  private ProgramRunId getProgramRunId(StructuredRow row) {
    return new ProgramRunId(
      new ApplicationId(row.getString(StoreDefinition.RemoteRuntimeStore.NAMESPACE_FIELD),
                        row.getString(StoreDefinition.RemoteRuntimeStore.APPLICATION_FIELD),
                        row.getString(StoreDefinition.RemoteRuntimeStore.VERSION_FIELD)),
      ProgramType.valueOf(row.getString(StoreDefinition.RemoteRuntimeStore.PROGRAM_TYPE_FIELD)),
      row.getString(StoreDefinition.RemoteRuntimeStore.PROGRAM_FIELD),
      row.getString(StoreDefinition.RemoteRuntimeStore.RUN_FIELD)
    );
  }

  private ProgramOptions getProgramOptions(StructuredRow row) {
    return GSON.fromJson(row.getString(StoreDefinition.RemoteRuntimeStore.PROGRAM_OPTIONS_FIELD), ProgramOptions.class);
  }
}
