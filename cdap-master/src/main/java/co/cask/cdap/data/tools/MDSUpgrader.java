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

package co.cask.cdap.data.tools;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.tx.DatasetContext;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.internal.app.store.ApplicationMeta;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.internal.app.store.ProgramArgs;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;

/**
 * Upgraded the Meta Data for applications
 */
public class MDSUpgrader extends AbstractUpgrader {

  private static final Logger LOG = LoggerFactory.getLogger(MDSUpgrader.class);
  private final Transactional<DatasetContext<Table>, Table> appMDS;
  private final CConfiguration cConf;
  private final Store store;
  private final Set<String> appStreams;

  @Inject
  private MDSUpgrader(LocationFactory locationFactory, TransactionExecutorFactory executorFactory,
                      @Named("dsFramework") final DatasetFramework dsFramework, CConfiguration cConf,
                      @Named("defaultStore") final Store store) {
    super(locationFactory);
    this.cConf = cConf;
    this.store = store;
    this.appMDS = Transactional.of(executorFactory, new Supplier<DatasetContext<Table>>() {
      @Override
      public DatasetContext<Table> get() {
        try {
          Table table = DatasetsUtil.getOrCreateDataset(dsFramework, Id.DatasetInstance.from
                                                          (Constants.DEFAULT_NAMESPACE_ID, Joiner.on(".").join(
                                                            Constants.SYSTEM_NAMESPACE, DefaultStore.APP_META_TABLE)),
                                                        "table", DatasetProperties.EMPTY,
                                                        DatasetDefinition.NO_ARGUMENTS, null);
          return DatasetContext.of(table);
        } catch (Exception e) {
          LOG.error("Failed to access {} table", Joiner.on(".").join(Constants.SYSTEM_NAMESPACE,
                                                                     DefaultStore.APP_META_TABLE), e);
          throw Throwables.propagate(e);
        }
      }
    });
    appStreams = Sets.newHashSet();
  }

  @Override
  public void upgrade() throws Exception {
    appMDS.executeUnchecked(new TransactionExecutor.
      Function<DatasetContext<Table>, Void>() {
      @Override
      public Void apply(DatasetContext<Table> configTable) throws Exception {
        byte[] appMetaRecordPrefix = new MDSKey.Builder().add(AppMetadataStore.TYPE_APP_META).build().getKey();
        Scanner rows = configTable.get().scan(appMetaRecordPrefix, Bytes.stopKeyForPrefix(appMetaRecordPrefix));
        Row row;
        while ((row = rows.next()) != null) {
          ApplicationMeta appMeta = GSON.fromJson(Bytes.toString(row.get(COLUMN)), ApplicationMeta.class);
          appSpecHandler(appMeta.getId(), appMeta.getSpec());
          appMetaUpgrader(row);
        }
        return null;
      }
    });

    // Scans for all the streams in the meta table
    appMDS.executeUnchecked(new TransactionExecutor.
      Function<DatasetContext<Table>, Void>() {
      @Override
      public Void apply(DatasetContext<Table> configTable) throws Exception {
        byte[] appMetaRecordPrefix = new MDSKey.Builder().add(AppMetadataStore.TYPE_STREAM).build().getKey();
        Scanner rows = configTable.get().scan(appMetaRecordPrefix, Bytes.stopKeyForPrefix(appMetaRecordPrefix));
        Row row;
        while ((row = rows.next()) != null) {
          StreamSpecification streamSpec = GSON.fromJson(Bytes.toString(row.get(COLUMN)), StreamSpecification.class);
          // if this stream was not a part of any application
          if (!appStreams.contains(streamSpec.getName())) {
            streamHandler(streamSpec);
          }
        }
        return null;
      }
    });
  }

  /**
   * Upgrades the stream record in the meta table
   *
   * @param streamSpec the {@link StreamSpecification} of the stream
   */
  private void streamHandler(StreamSpecification streamSpec) {
    store.addStream(Id.Namespace.from(Constants.DEFAULT_NAMESPACE), streamSpec);
  }

  /**
   * For an application calls {@link MDSUpgrader#programHandler} for the different program types whose meta data
   * might exist in the meta table and will need upgrade
   *
   * @param appId   the application id
   * @param appSpec the {@link ApplicationSpecification} of the application
   */
  private void appSpecHandler(String appId, ApplicationSpecification appSpec) {
    programHandler(appId, appSpec.getFlows().keySet(), ProgramType.FLOW);
    programHandler(appId, appSpec.getMapReduce().keySet(), ProgramType.MAPREDUCE);
    programHandler(appId, appSpec.getSpark().keySet(), ProgramType.SPARK);
    programHandler(appId, appSpec.getWorkflows().keySet(), ProgramType.WORKFLOW);
    programHandler(appId, appSpec.getServices().keySet(), ProgramType.SERVICE);
    programHandler(appId, appSpec.getProcedures().keySet(), ProgramType.PROCEDURE);
  }

  /**
   * Calls different functions to upgrade different kind of meta data of a program
   *
   * @param appId       the application id
   * @param programIds  the program ids for the programs in this application
   * @param programType the {@link ProgramType} of these program
   */
  private void programHandler(String appId, Set<String> programIds, ProgramType programType) {
    for (String programId : programIds) {
      runRecordStartedHandler(appId, programId, programType);
      runRecordCompletedHandler(appId, programId, programType);
      programArgsHandler(appId, programId, programType);
    }
  }

  /**
   * Handles the {@link AppMetadataStore#TYPE_PROGRAM_ARGS} meta data and writes it back with namespace
   *
   * @param appId       the application id to which this program belongs to
   * @param programId   the program id of the program
   * @param programType the {@link ProgramType} of the program
   */
  private void programArgsHandler(final String appId, final String programId, final ProgramType programType) {
    final byte[] partialKey = new MDSKey.Builder().add(AppMetadataStore.TYPE_RUN_RECORD_STARTED, DEVELOPER_ACCOUNT,
                                                       appId, programId).build().getKey();
    appMDS.executeUnchecked(new TransactionExecutor.Function<DatasetContext<Table>, Void>() {
      @Override
      public Void apply(DatasetContext<Table> input) throws Exception {
        Scanner rows = input.get().scan(partialKey, Bytes.stopKeyForPrefix(partialKey));
        Row row;
        while ((row = rows.next()) != null) {
          ProgramArgs programArgs = GSON.fromJson(Bytes.toString(row.get(COLUMN)), ProgramArgs.class);
          store.storeRunArguments(Id.Program.from(Id.Application.from(Constants.DEFAULT_NAMESPACE, appId), programType,
                                                  programId), programArgs.getArgs());
        }
        return null;
      }
    });
  }

  /**
   * Handles the {@link AppMetadataStore#TYPE_RUN_RECORD_STARTED} meta data and writes it back with namespace
   *
   * @param appId       the application id to which this program belongs to
   * @param programId   the program id of the program
   * @param programType the {@link ProgramType} of the program
   */
  private void runRecordStartedHandler(final String appId, final String programId, final ProgramType programType) {
    final byte[] partialKey = new MDSKey.Builder().add(AppMetadataStore.TYPE_RUN_RECORD_STARTED, DEVELOPER_ACCOUNT,
                                                       appId, programId).build().getKey();
    appMDS.executeUnchecked(new TransactionExecutor.Function<DatasetContext<Table>, Void>() {
      @Override
      public Void apply(DatasetContext<Table> input) throws Exception {
        Scanner rows = input.get().scan(partialKey, Bytes.stopKeyForPrefix(partialKey));
        Row row;
        while ((row = rows.next()) != null) {
          RunRecord runRecord = GSON.fromJson(Bytes.toString(row.get(COLUMN)), RunRecord.class);

          MDSKey.Splitter keyParts = new MDSKey(row.getRow()).split();
          // skip runRecordStarted
          keyParts.getString();
          // skip default
          keyParts.skipString();
          // skip appId
          keyParts.skipString();
          // skip programId
          keyParts.skipString();
          String pId = keyParts.getString();

          store.setStart(Id.Program.from(Id.Application.from(Constants.DEFAULT_NAMESPACE, appId), programType,
                                         programId), pId, runRecord.getStartTs());
        }
        return null;
      }
    });
  }

  /**
   * Handles the {@link AppMetadataStore#TYPE_RUN_RECORD_COMPLETED} meta data and writes it back with namespace
   *
   * @param appId       the application id to which this program belongs to
   * @param programId   the program id of the program
   * @param programType the {@link ProgramType} of the program
   */
  private void runRecordCompletedHandler(final String appId, final String programId, final ProgramType programType) {

    final byte[] partialKey = new MDSKey.Builder().add(AppMetadataStore.TYPE_RUN_RECORD_COMPLETED, DEVELOPER_ACCOUNT,
                                                       appId, programId).build().getKey();
    appMDS.executeUnchecked(new TransactionExecutor.Function<DatasetContext<Table>, Void>() {
      @Override
      public Void apply(DatasetContext<Table> input) throws Exception {
        Scanner rows = input.get().scan(partialKey, Bytes.stopKeyForPrefix(partialKey));
        Row row;
        while ((row = rows.next()) != null) {
          RunRecord runRecord = GSON.fromJson(Bytes.toString(row.get(COLUMN)), RunRecord.class);

          MDSKey.Splitter keyParts = new MDSKey(row.getRow()).split();
          // skip runRecordStarted
          keyParts.getString();
          // skip default
          keyParts.skipString();
          // skip appId
          keyParts.skipString();
          // skip programId
          keyParts.skipString();
          long startTs = keyParts.getLong();
          String pId = keyParts.getString();

          // we cannot add a runRecordCompleted if there is no runRecordStarted for the run so write it first
          // the next setStop call deletes the runRecordStarted for that run so we don't have to delete it here.
          writeTempRunRecordStart(appId, programType, programId, pId, startTs);
          store.setStop(Id.Program.from(Id.Application.from(Constants.DEFAULT_NAMESPACE, appId), programType,
                                        programId), pId, runRecord.getStopTs(),
                        getControllerStateByStatus(runRecord.getStatus()));
        }
        return null;
      }
    });
  }

  /**
   * Writes the {@link AppMetadataStore#TYPE_RUN_RECORD_STARTED} entry in the app meta table so that
   * {@link AppMetadataStore#TYPE_RUN_RECORD_COMPLETED} can be written which deleted the started record.
   *
   * @param appId       the application id
   * @param programType {@link ProgramType} of this program
   * @param programId   the program id of this program
   * @param pId         the process id of the run
   * @param startTs     the startTs
   */
  private void writeTempRunRecordStart(String appId, ProgramType programType, String programId, String pId,
                                       long startTs) {
    store.setStart(Id.Program.from(Id.Application.from(Constants.DEFAULT_NAMESPACE, appId), programType, programId),
                   pId, Long.MAX_VALUE - startTs);
  }

  /**
   * Writes the updated application metadata through the {@link DefaultStore}
   *
   * @param row the {@link Row} containing the application metadata
   * @throws URISyntaxException if failed to create {@link URI} from the archive location in metadata
   */
  private void appMetaUpgrader(Row row) throws URISyntaxException, IOException {
    ApplicationMeta appMeta = GSON.fromJson(Bytes.toString(row.get(COLUMN)), ApplicationMeta.class);
    store.addApplication(Id.Application.from(Constants.DEFAULT_NAMESPACE, appMeta.getId()), appMeta.getSpec(),
                         updateAppArchiveLocation(appMeta.getId(), new URI(appMeta.getArchiveLocation())));
    // store the stream names which belongs to this app as we want to filter out streams which do not belong to any app
    // later
    appStreams.addAll(appMeta.getSpec().getStreams().keySet());
  }

  /**
   * Creates the new archive location for application with namespace
   *
   * @param appId           : the application id
   * @param archiveLocation : the application archive location
   * @return {@link Location} for the new archive location with namespace
   */
  private Location updateAppArchiveLocation(String appId, URI archiveLocation) throws IOException {
    String archiveFilename = locationFactory.create(archiveLocation).getName();

    return locationFactory.create(Constants.DEFAULT_NAMESPACE).append(
      cConf.get(Constants.AppFabric.OUTPUT_DIR)).append(appId).append(Constants.ARCHIVE_DIR).append(archiveFilename);
  }

  /**
   * Gives the {@link ProgramController.State} for a given {@link ProgramRunStatus}
   * Note: This will given the state as {@link ProgramController.State#STARTING} for {@link ProgramRunStatus#RUNNING}
   * even though running has multiple mapping but that is fine in our case as we use this
   * {@link ProgramController.State} to write the runRecordCompleted through {@link DefaultStore#setStop} which converts
   * it back to {@link ProgramRunStatus#RUNNING}. So we don't really care about the temporary intermediate
   * {@link ProgramController.State}
   *
   * @param status : the status
   * @return the state for the status or null if there is no defined state for the given status
   */
  private static ProgramController.State getControllerStateByStatus(ProgramRunStatus status) {
    for (ProgramController.State state : ProgramController.State.values()) {
      if (state.getRunStatus() == status) {
        return state;
      }
    }
    return null;
  }
}
