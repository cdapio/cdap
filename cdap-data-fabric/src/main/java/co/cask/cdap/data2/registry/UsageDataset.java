/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.data2.registry.internal.keymaker.DatasetKeyMaker;
import co.cask.cdap.data2.registry.internal.keymaker.ProgramKeyMaker;
import co.cask.cdap.data2.registry.internal.keymaker.StreamKeyMaker;
import co.cask.cdap.data2.registry.internal.pair.KeyMaker;
import co.cask.cdap.data2.registry.internal.pair.OrderedPair;
import co.cask.cdap.data2.registry.internal.pair.OrderedPairs;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;

/**
 * Store program -> dataset/stream usage information.
 */
public class UsageDataset extends MetadataStoreDataset {
  // The following constants are used as row key prefixes. Any changes to these will make existing data unusable.
  private static final String PROGRAM = "p";
  private static final String DATASET = "d";
  private static final String STREAM = "s";

  private final OrderedPairs orderedPairs;

  public UsageDataset(Table table) {
    super(table);

    Map<String, KeyMaker<? extends Id>> keyMakers =
      ImmutableMap.<String, KeyMaker<? extends Id>>builder()
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
  public void register(Id.Program programId, Id.DatasetInstance datasetInstanceId) {
    write(orderedPairs.get(PROGRAM, DATASET).makeKey(programId, datasetInstanceId), true);
    write(orderedPairs.get(DATASET, PROGRAM).makeKey(datasetInstanceId, programId), true);
  }

  /**
   * Registers usage of a stream by a program.
   * @param programId program
   * @param streamId stream
   */
  public void register(Id.Program programId, Id.Stream streamId) {
    write(orderedPairs.get(PROGRAM, STREAM).makeKey(programId, streamId), true);
    write(orderedPairs.get(STREAM, PROGRAM).makeKey(streamId, programId), true);
  }

  /**
   * Unregisters all usage information of an application.
   * @param applicationId application
   */
  public void unregister(Id.Application applicationId) {
    Id.Program programId = ProgramKeyMaker.getProgramId(applicationId);

    // Delete datasets associated with applicationId
    for (Id.DatasetInstance datasetInstanceId : getDatasets(applicationId)) {
      deleteAll(orderedPairs.get(DATASET, PROGRAM).makeKey(datasetInstanceId, programId));
    }

    // Delete streams associated with applicationId
    for (Id.Stream streamId : getStreams(applicationId)) {
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
  public Set<Id.DatasetInstance> getDatasets(Id.Program programId) {
    OrderedPair<Id.Program, Id.DatasetInstance> orderedPair = orderedPairs.get(PROGRAM, DATASET);
    Map<MDSKey, Boolean> datasetKeys = listKV(orderedPair.makeScanKey(programId), Boolean.TYPE);
    return orderedPair.getSecond(datasetKeys.keySet());
  }

  /**
   * Returns datasets used by an application.
   * @param applicationId application
   * @return datasets used by applicaionId
   */
  public Set<Id.DatasetInstance> getDatasets(Id.Application applicationId) {
    Id.Program programId = ProgramKeyMaker.getProgramId(applicationId);
    OrderedPair<Id.Program, Id.DatasetInstance> orderedPair = orderedPairs.get(PROGRAM, DATASET);
    Map<MDSKey, Boolean> datasetKeys = listKV(orderedPair.makeScanKey(programId), Boolean.TYPE);
    return orderedPair.getSecond(datasetKeys.keySet());
  }

  /**
   * Returns streams used by a program.
   * @param programId program
   * @return streams used by programId
   */
  public Set<Id.Stream> getStreams(Id.Program programId) {
    OrderedPair<Id.Program, Id.Stream> orderedPair = orderedPairs.get(PROGRAM, STREAM);
    Map<MDSKey, Boolean> datasetKeys = listKV(orderedPair.makeScanKey(programId), Boolean.TYPE);
    return orderedPair.getSecond(datasetKeys.keySet());
  }

  /**
   * Returns streams used by an application.
   * @param applicationId application
   * @return streams used by applicaionId
   */
  public Set<Id.Stream> getStreams(Id.Application applicationId) {
    Id.Program programId = ProgramKeyMaker.getProgramId(applicationId);
    OrderedPair<Id.Program, Id.Stream> orderedPair = orderedPairs.get(PROGRAM, STREAM);
    Map<MDSKey, Boolean> datasetKeys = listKV(orderedPair.makeScanKey(programId), Boolean.TYPE);
    return orderedPair.getSecond(datasetKeys.keySet());
  }

  /**
   * Returns programs using dataset.
   * @param datasetInstanceId dataset
   * @return programs using datasetInstanceId
   */
  public Set<Id.Program> getPrograms(Id.DatasetInstance datasetInstanceId) {
    OrderedPair<Id.DatasetInstance, Id.Program> orderedPair = orderedPairs.get(DATASET, PROGRAM);
    Map<MDSKey, Boolean> programKeys = listKV(orderedPair.makeScanKey(datasetInstanceId), Boolean.TYPE);
    return orderedPair.getSecond(programKeys.keySet());
  }

  /**
   * Returns programs using stream.
   * @param streamId stream
   * @return programs using streamId
   */
  public Set<Id.Program> getPrograms(Id.Stream streamId) {
    OrderedPair<Id.Stream, Id.Program> orderedPair = orderedPairs.get(STREAM, PROGRAM);
    Map<MDSKey, Boolean> programKeys = listKV(orderedPair.makeScanKey(streamId), Boolean.TYPE);
    return orderedPair.getSecond(programKeys.keySet());
  }
}
