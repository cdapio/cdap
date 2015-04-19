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

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

/**
 * Store program/adapter -> dataset/stream usage information.
 */
public class UsageDataset extends MetadataStoreDataset {
  public UsageDataset(Table table) {
    super(table);
  }

  public UsageDataset(DatasetSpecification spec, @EmbeddedDataset("") Table table) {
    this(table);
  }

  /**
   * Registers usage of a dataset by a program.
   * @param programId program
   * @param datasetInstanceId dataset
   */
  public void register(Id.Program programId, Id.DatasetInstance datasetInstanceId) {
    write(ProgramDataset.makeKey(programId, datasetInstanceId), true);
    write(DatasetProgram.makeKey(datasetInstanceId, programId), true);
  }

  /**
   * Registers usage of a dataset by an adapter.
   * @param adapterId adapter
   * @param datasetInstanceId dataset
   */
  public void register(Id.Adapter adapterId, Id.DatasetInstance datasetInstanceId) {

  }

  /**
   * Registers usage of a stream by a program.
   * @param programId program
   * @param streamId stream
   */
  public void register(Id.Program programId, Id.Stream streamId) {

  }

  /**
   * Registers usage of a stream by an adapter.
   * @param adapterId adapter
   * @param streamId stream
   */
  public void register(Id.Adapter adapterId, Id.Stream streamId) {

  }

  /**
   * Unregisters all usage information of an application.
   * @param applicationId application
   */
  public void unregister(Id.Application applicationId) {
    // Delete datasets associated with applicationId
    for (Id.DatasetInstance datasetInstanceId : getDatasets(applicationId)) {
      deleteAll(DatasetProgram.makeKey(datasetInstanceId, applicationId));
    }

    // Delete streams associated with applicationId

    // Delete all mappings for applicaionId
    deleteAll(ProgramDataset.makeScanKey(applicationId));
  }

  /**
   * Unregisters all usage information of an adapter.
   * @param adapterId
   */
  public void unregister(Id.Adapter adapterId) {

  }

  /**
   * Returns datasets used by a program.
   * @param programId program
   * @return datasets used by programId
   */
  public Set<Id.DatasetInstance> getDatasets(Id.Program programId) {
    Map<MDSKey, Boolean> datasetKeys = listKV(ProgramDataset.makeScanKey(programId), Boolean.TYPE);
    return ProgramDataset.toDatasetInstanceIds(datasetKeys.keySet());
  }

  /**
   * Returns datasets used by an application.
   * @param applicationId application
   * @return datasets used by applicaionId
   */
  public Set<Id.DatasetInstance> getDatasets(Id.Application applicationId) {
    Map<MDSKey, Boolean> datasetKeys = listKV(ProgramDataset.makeScanKey(applicationId), Boolean.TYPE);
    return ProgramDataset.toDatasetInstanceIds(datasetKeys.keySet());
  }

  /**
   * Returns datasets used by an adapter.
   * @param adapterId adapter
   * @return datasets used by adapterId
   */
  public Set<Id.DatasetInstance> getDatasets(Id.Adapter adapterId) {
    return null;
  }

  /**
   * Returns streams used by a program.
   * @param programId program
   * @return streams used by programId
   */
  public Set<Id.Stream> getStreams(Id.Program programId) {
    return null;
  }

  /**
   * Returns streams used by an application.
   * @param applicationId application
   * @return streams used by applicaionId
   */
  public Set<Id.Stream> getStreams(Id.Application applicationId) {
    return null;
  }

  /**
   * Returns sterams used by an adapter.
   * @param adapterId adapter
   * @return streams used by adapterId
   */
  public Set<Id.Stream> getStreams(Id.Adapter adapterId) {
    return null;
  }

  /**
   * Returns programs using dataset.
   * @param datasetInstanceId dataset
   * @return programs using datasetInstanceId
   */
  public Set<Id.Program> getPrograms(Id.DatasetInstance datasetInstanceId) {
    return null;
  }

  /**
   * Returns programs using stream.
   * @param streamId stream
   * @return programs using streamId
   */
  public Set<Id.Program> getPrograms(Id.Stream streamId) {
    return null;
  }

  /**
   * Returns adapters using dataset.
   * @param datasetInstanceId dataset
   * @return adapters using datasetInstanceId
   */
  public Set<Id.Adapter> getAdapters(Id.DatasetInstance datasetInstanceId) {
    return null;
  }

  /**
   * Returns adapters using stream.
   * @param streamId stream
   * @return adapters using streamId
   */
  public Set<Id.Adapter> getAdapters(Id.Stream streamId) {
    return null;
  }

  /**
   * Represents a Program - Dataset mapping.
   */
  private static final class ProgramDataset {
    private static final String PREFIX = "pd_";

    public static MDSKey makeKey(Id.Program programId, Id.DatasetInstance datasetInstanceId) {
      return getBuilder()
        .add(programId.getNamespaceId())
        .add(programId.getApplicationId())
        .add(programId.getType().getCategoryName())
        .add(programId.getId())
        .add(datasetInstanceId.getNamespaceId())
        .add(datasetInstanceId.getId())
        .build();
    }

    public static MDSKey makeScanKey(Id.Program programId) {
      return getBuilder()
        .add(programId.getNamespaceId())
        .add(programId.getApplicationId())
        .add(programId.getType().getCategoryName())
        .add(programId.getId())
        .build();
    }

    public static MDSKey makeScanKey(Id.Application applicationId) {
      return getBuilder()
        .add(applicationId.getNamespaceId())
        .add(applicationId.getId())
        .build();
    }

    public static Set<Id.DatasetInstance> toDatasetInstanceIds(Set<MDSKey> programDatasetKeys) {
      Set<Id.DatasetInstance> datasetInstances = Sets.newHashSetWithExpectedSize(programDatasetKeys.size());
      for (MDSKey mdsKey : programDatasetKeys) {
        MDSKey.Splitter splitter = getSplitter(mdsKey);
        splitter.skipString(); // namespace
        splitter.skipString(); // app
        splitter.skipString(); // type
        splitter.skipString(); // program
        datasetInstances.add(Id.DatasetInstance.from(splitter.getString(), splitter.getString()));
      }
      return datasetInstances;
    }

    private static MDSKey.Builder getBuilder() {
      return new MDSKey.Builder().add(PREFIX);
    }

    private static MDSKey.Splitter getSplitter(MDSKey mdsKey) {
      MDSKey.Splitter splitter = mdsKey.split();
      splitter.skipString();
      return splitter;
    }
  }

  /**
   * Represnts a Dataset - Program mapping.
   */
  private static final class DatasetProgram {
    private static final String PREFIX = "dp_";

    public static MDSKey makeKey(Id.DatasetInstance datasetInstanceId, Id.Program programId) {
      return getBuilder()
        .add(datasetInstanceId.getNamespaceId())
        .add(datasetInstanceId.getId())
        .add(programId.getNamespaceId())
        .add(programId.getApplicationId())
        .add(programId.getType().getCategoryName())
        .add(programId.getId())
        .build();
    }

    public static MDSKey makeKey(Id.DatasetInstance datasetInstanceId, Id.Application applicationId) {
      return getBuilder()
        .add(datasetInstanceId.getNamespaceId())
        .add(datasetInstanceId.getId())
        .add(applicationId.getNamespaceId())
        .add(applicationId.getId())
        .build();
    }

    public static MDSKey makeScanKey(Id.DatasetInstance datasetInstanceId) {
      return getBuilder()
        .add(datasetInstanceId.getNamespaceId())
        .add(datasetInstanceId.getId())
        .build();
    }

    public static Set<Id.Program> toProgramIds(Set<MDSKey> datasetProgramKeys) {
      Set<Id.Program> programIds = Sets.newHashSetWithExpectedSize(datasetProgramKeys.size());
      for (MDSKey mdsKey : datasetProgramKeys) {
        MDSKey.Splitter splitter = getSplitter(mdsKey);
        splitter.skipString(); // namespace
        splitter.skipString(); // instance
        programIds.add(Id.Program.from(splitter.getString(), splitter.getString(),
                                       ProgramType.valueOfCategoryName(splitter.getString()), splitter.getString()));
      }
      return programIds;
    }

    private static MDSKey.Builder getBuilder() {
      return new MDSKey.Builder().add(PREFIX);
    }

    private static MDSKey.Splitter getSplitter(MDSKey mdsKey) {
      MDSKey.Splitter splitter = mdsKey.split();
      splitter.skipString();
      return splitter;
    }
  }
}
