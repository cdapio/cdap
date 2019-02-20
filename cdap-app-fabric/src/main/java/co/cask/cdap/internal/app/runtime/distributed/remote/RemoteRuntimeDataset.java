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

package co.cask.cdap.internal.app.runtime.distributed.remote;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.master.spi.program.Arguments;
import co.cask.cdap.master.spi.program.ProgramOptions;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides data logic for the program runtime. It uses the {@code app.meta} {@link Table}
 * dataset for persisting data. The row has the following format
 *
 * <pre>
 * {@literal
 *   Key = rt.state<ExecutionState><runId>
 * }
 * </pre>
 */
public class RemoteRuntimeDataset extends MetadataStoreDataset {

  private static final byte[] KEY_PREFIX = Bytes.toBytes("rt.state");
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .create();

  /**
   * Gets an instance of {@link RemoteRuntimeDataset}. If the underlying dataset instance does not exist, a new instance
   * will be created.
   *
   * @param datasetContext the {@link DatasetContext} for getting the dataset instance
   * @param datasetFramework the {@link DatasetFramework} for creating the dataset instance if it does not exist
   * @return a new instance of {@link RemoteRuntimeDataset}
   */
  public static RemoteRuntimeDataset create(DatasetContext datasetContext, DatasetFramework datasetFramework) {
    try {
      Table table = DatasetsUtil.getOrCreateDataset(datasetContext, datasetFramework,
                                                    AppMetadataStore.APP_META_INSTANCE_ID, Table.class.getName(),
                                                    DatasetProperties.EMPTY);
      return new RemoteRuntimeDataset(table);
    } catch (DatasetManagementException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private RemoteRuntimeDataset(Table table) {
    super(table, GSON);
  }

  /**
   * Writes the information for the given program run.
   *
   * @param programRunId the program run id for the run
   * @param programOptions the program options for the run
   */
  public void write(ProgramRunId programRunId, ProgramOptions programOptions) {
    MDSKey key = getKey(programRunId);
    write(key, programOptions);
  }

  /**
   * Deletes the program run information.
   *
   * @param programRunId the program run id to delete
   */
  public void delete(ProgramRunId programRunId) {
    delete(getKey(programRunId));
  }

  /**
   * Scans for program run information
   *
   * @param limit maximum number of task info to return
   * @param lastProgramRunId an optional {@link ProgramRunId} returned from previous scan to continue the scanning
   * @return a {@link List} of {@link ProgramRunId} to the corresponding {@link ProgramOptions} for each run
   */
  public List<Map.Entry<ProgramRunId, ProgramOptions>> scan(int limit, @Nullable ProgramRunId lastProgramRunId) {
    MDSKey startKey = new MDSKey.Builder().add(KEY_PREFIX).build();
    MDSKey stopKey = new MDSKey(Bytes.stopKeyForPrefix(startKey.getKey()));

    if (lastProgramRunId != null) {
      // If there is a start program run id, scan from that id instead of from the beginning
      startKey = getKey(lastProgramRunId);
    }

    List<Map.Entry<ProgramRunId, ProgramOptions>> result = new LinkedList<>();
    super.<ProgramOptions>scan(startKey, stopKey, ProgramOptions.class, keyValue -> {
      if (result.size() >= limit) {
        return false;
      }

      ProgramRunId programRunId = getProgramRunId(keyValue.getKey());

      // Skip the lastProgramRunId
      if (lastProgramRunId != null && lastProgramRunId.equals(programRunId)) {
        return true;
      }

      result.add(Maps.immutableEntry(programRunId, keyValue.getValue()));
      return true;
    });

    return result;
  }

  private MDSKey getKey(ProgramRunId programRunId) {
    return new MDSKey.Builder().add(KEY_PREFIX)
      .add(programRunId.getNamespace())
      .add(programRunId.getApplication())
      .add(programRunId.getVersion())
      .add(programRunId.getType().name())
      .add(programRunId.getProgram())
      .add(programRunId.getRun())
      .build();
  }

  /**
   * Parses the {@link ProgramRunId} from the {@link MDSKey}.
   */
  private ProgramRunId getProgramRunId(MDSKey key) {
    MDSKey.Splitter st = key.split();
    st.skipString();     // Skip prefix
    return new ProgramRunId(new ApplicationId(st.getString(), st.getString(), st.getString()),
                            ProgramType.valueOf(st.getString()), st.getString(), st.getString());
  }
}
