/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.data2.registry;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.data2.registry.internal.keymaker.DatasetKeyMaker;
import co.cask.cdap.data2.registry.internal.keymaker.ProgramKeyMaker;
import co.cask.cdap.data2.registry.internal.keymaker.StreamKeyMaker;
import co.cask.cdap.data2.registry.internal.pair.KeyMaker;
import co.cask.cdap.data2.registry.internal.pair.OrderedPair;
import co.cask.cdap.data2.registry.internal.pair.OrderedPairs;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Store program -> dataset/stream usage information.
 */
public class UsageDataset extends MetadataStoreDataset {

  public static final DatasetId USAGE_INSTANCE_ID = NamespaceId.SYSTEM.dataset("usage.registry");

  // The following constants are used as row key prefixes. Any changes to these will make existing data unusable.
  private static final String PROGRAM = "p";
  private static final String DATASET = "d";
  private static final String STREAM = "s";

  private final OrderedPairs orderedPairs;

  /**
   * Gets an instance of {@link UsageDataset}. If no such dataset instance exists, it will be created.
   *
   * @param datasetContext the {@link DatasetContext} for getting the {@link UsageDataset} instance
   * @param datasetFramework the {@link DatasetFramework} for creating the {@link UsageDataset}
   * @param datasetId the {@link DatasetId} to use for the usage dataset
   * @return a {@link UsageDataset} instance
   */
  @VisibleForTesting
  public static UsageDataset getUsageDataset(DatasetContext datasetContext, DatasetFramework datasetFramework,
                                             DatasetId datasetId) {
    try {
      return DatasetsUtil.getOrCreateDataset(datasetContext, datasetFramework, datasetId,
                                             UsageDataset.class.getSimpleName(), DatasetProperties.EMPTY);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework} used by usage registry.
   *
   * @param datasetFramework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework datasetFramework) throws IOException, DatasetManagementException {
    datasetFramework.addInstance(UsageDataset.class.getSimpleName(), USAGE_INSTANCE_ID, DatasetProperties.EMPTY);
  }

  public UsageDataset(Table table) {
    super(table);

    Map<String, KeyMaker<? extends EntityId>> keyMakers =
      ImmutableMap.<String, KeyMaker<? extends EntityId>>builder()
        .put(PROGRAM, new ProgramKeyMaker())
        .put(DATASET, new DatasetKeyMaker())
        .put(STREAM, new StreamKeyMaker())
        .build();
    orderedPairs = new OrderedPairs(keyMakers);
  }

  /**
   * Registers usage of a dataset by a program.
   * @param programId program
   * @param datasetInstanceId dataset
   */
  public void register(ProgramId programId, DatasetId datasetInstanceId) {
    write(orderedPairs.get(PROGRAM, DATASET).makeKey(programId, datasetInstanceId), true);
    write(orderedPairs.get(DATASET, PROGRAM).makeKey(datasetInstanceId, programId), true);
  }

  /**
   * Registers usage of a stream by a program.
   * @param programId program
   * @param streamId stream
   */
  public void register(ProgramId programId, StreamId streamId) {
    write(orderedPairs.get(PROGRAM, STREAM).makeKey(programId, streamId), true);
    write(orderedPairs.get(STREAM, PROGRAM).makeKey(streamId, programId), true);
  }

  /**
   * Unregisters all usage information of an application.
   * @param applicationId application
   */
  public void unregister(ApplicationId applicationId) {
    ProgramId programId = ProgramKeyMaker.getProgramId(applicationId);

    // Delete datasets associated with applicationId
    for (DatasetId datasetInstanceId : getDatasets(applicationId)) {
      deleteAll(orderedPairs.get(DATASET, PROGRAM).makeKey(datasetInstanceId, programId));
    }

    // Delete streams associated with applicationId
    for (StreamId streamId : getStreams(applicationId)) {
      deleteAll(orderedPairs.get(STREAM, PROGRAM).makeKey(streamId, programId));
    }

    // Delete all mappings for applicationId
    deleteAll(orderedPairs.get(PROGRAM, DATASET).makeScanKey(programId));
    deleteAll(orderedPairs.get(PROGRAM, STREAM).makeScanKey(programId));
  }

  /**
   * Returns datasets used by a program.
   * @param programId program
   * @return datasets used by programId
   */
  public Set<DatasetId> getDatasets(ProgramId programId) {
    OrderedPair<ProgramId, DatasetId> orderedPair = orderedPairs.get(PROGRAM, DATASET);
    Map<MDSKey, Boolean> datasetKeys = listKV(orderedPair.makeScanKey(programId), Boolean.TYPE);
    return orderedPair.getSecond(datasetKeys.keySet());
  }

  /**
   * Returns datasets used by an application.
   * @param applicationId application
   * @return datasets used by applicaionId
   */
  public Set<DatasetId> getDatasets(ApplicationId applicationId) {
    ProgramId programId = ProgramKeyMaker.getProgramId(applicationId);
    OrderedPair<ProgramId, DatasetId> orderedPair = orderedPairs.get(PROGRAM, DATASET);
    Map<MDSKey, Boolean> datasetKeys = listKV(orderedPair.makeScanKey(programId), Boolean.TYPE);
    return orderedPair.getSecond(datasetKeys.keySet());
  }

  /**
   * Returns streams used by a program.
   * @param programId program
   * @return streams used by programId
   */
  public Set<StreamId> getStreams(ProgramId programId) {
    OrderedPair<ProgramId, StreamId> orderedPair = orderedPairs.get(PROGRAM, STREAM);
    Map<MDSKey, Boolean> datasetKeys = listKV(orderedPair.makeScanKey(programId), Boolean.TYPE);
    return orderedPair.getSecond(datasetKeys.keySet());
  }

  /**
   * Returns streams used by an application.
   * @param applicationId application
   * @return streams used by applicaionId
   */
  public Set<StreamId> getStreams(ApplicationId applicationId) {
    ProgramId programId = ProgramKeyMaker.getProgramId(applicationId);
    OrderedPair<ProgramId, StreamId> orderedPair = orderedPairs.get(PROGRAM, STREAM);
    Map<MDSKey, Boolean> datasetKeys = listKV(orderedPair.makeScanKey(programId), Boolean.TYPE);
    return orderedPair.getSecond(datasetKeys.keySet());
  }

  /**
   * Returns programs using dataset.
   * @param datasetInstanceId dataset
   * @return programs using datasetInstanceId
   */
  public Set<ProgramId> getPrograms(DatasetId datasetInstanceId) {
    OrderedPair<DatasetId, ProgramId> orderedPair = orderedPairs.get(DATASET, PROGRAM);
    Map<MDSKey, Boolean> programKeys = listKV(orderedPair.makeScanKey(datasetInstanceId), Boolean.TYPE);
    return orderedPair.getSecond(programKeys.keySet());
  }

  /**
   * Returns programs using stream.
   * @param streamId stream
   * @return programs using streamId
   */
  public Set<ProgramId> getPrograms(StreamId streamId) {
    OrderedPair<StreamId, ProgramId> orderedPair = orderedPairs.get(STREAM, PROGRAM);
    Map<MDSKey, Boolean> programKeys = listKV(orderedPair.makeScanKey(streamId), Boolean.TYPE);
    return orderedPair.getSecond(programKeys.keySet());
  }
}
