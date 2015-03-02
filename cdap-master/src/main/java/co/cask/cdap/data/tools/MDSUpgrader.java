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
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.internal.app.store.ApplicationMeta;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.notifications.feeds.service.MDSNotificationFeedStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.RunRecord;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

/**
 * Upgraded the Meta Data for applications
 */
public class MDSUpgrader extends AbstractUpgrader {

  private static final Logger LOG = LoggerFactory.getLogger(MDSUpgrader.class);
  private static final String[] OTHERS = {AppMetadataStore.TYPE_STREAM, AppMetadataStore.TYPE_NAMESPACE,
    MDSNotificationFeedStore.TYPE_NOTIFICATION_FEED};

  private final Transactional<UpgradeTable, Table> appMDS;
  private final CConfiguration cConf;
  private final Store store;

  @Inject
  public MDSUpgrader(LocationFactory locationFactory, TransactionExecutorFactory executorFactory,
                     @Named("namespacedDSFramework") final DatasetFramework namespacedFramework, CConfiguration cConf,
                     @Named("nonNamespacedStore") final Store store) {
    super(locationFactory);
    this.cConf = cConf;
    this.store = store;
    this.appMDS = Transactional.of(executorFactory, new Supplier<UpgradeTable>() {
      @Override
      public UpgradeTable get() {
        try {
          Table table = DatasetsUtil.getOrCreateDataset(namespacedFramework, Id.DatasetInstance.from
                                                          (Constants.SYSTEM_NAMESPACE_ID, DefaultStore.APP_META_TABLE),
                                                        "table", DatasetProperties.EMPTY,
                                                        DatasetDefinition.NO_ARGUMENTS, null);
          return new UpgradeTable(table);
        } catch (Exception e) {
          LOG.error("Failed to access {} table", DefaultStore.APP_META_TABLE, e);
          throw Throwables.propagate(e);
        }
      }
    });
  }

  @Override
  public void upgrade() throws Exception {
    appMDS.executeUnchecked(new TransactionExecutor.
      Function<UpgradeTable, Void>() {
      @Override
      public Void apply(UpgradeTable configTable) throws Exception {
        Scanner rows = configTable.table.scan(null, null);
        Row row;
        while ((row = rows.next()) != null) {
          String key = Bytes.toString(row.getRow()).trim();
          MDSKey.Splitter keyParts = new MDSKey(row.getRow()).split();
          String metaType = keyParts.getString();
          if (metaType.equalsIgnoreCase(AppMetadataStore.TYPE_APP_META)) {
            // Application metadata
            applicationMetadataHandler(row);
          } else if (metaType.equalsIgnoreCase(AppMetadataStore.TYPE_RUN_RECORD_STARTED)) {
            runRecordStartedHandler(row, keyParts);
          } else if (metaType.equalsIgnoreCase(AppMetadataStore.TYPE_RUN_RECORD_COMPLETED)) {
            // run record metadata
            runRecordCompletedHandler(row, keyParts);
          } else if (!isKeyValid(key, OTHERS)) {
            LOG.warn("Invalid Metadata with key {} found in {}", key, DefaultStore.APP_META_TABLE);
            throw new RuntimeException("Invalid metadata with key " + key);
          }
        }
        return null;
      }
    });
  }

  /**
   * Handled the {@link AppMetadataStore#TYPE_RUN_RECORD_STARTED} meta data and writes it back with namespace
   *
   * @param row      the {@link Row} containing the {@link AppMetadataStore#TYPE_RUN_RECORD_STARTED} metadata
   * @param keyParts the {@link MDSKey.Splitter} for the key
   */
  private void runRecordStartedHandler(Row row, MDSKey.Splitter keyParts) {
    RunRecord runRecord = GSON.fromJson(Bytes.toString(row.get(COLUMN)), RunRecord.class);

    // skip default
    keyParts.skipString();
    String appId = keyParts.getString();
    String programId = keyParts.getString();
    String pId = keyParts.getString();

    store.setStart(Id.Program.from(Constants.DEFAULT_NAMESPACE, appId, programId), pId, runRecord.getStartTs());
  }

  /**
   * Handled the {@link AppMetadataStore#TYPE_RUN_RECORD_COMPLETED} meta data and writes it back with namespace
   *
   * @param row      the {@link Row} containing the {@link AppMetadataStore#TYPE_RUN_RECORD_COMPLETED} metadata
   * @param keyParts the {@link MDSKey.Splitter} for the key
   */
  private void runRecordCompletedHandler(Row row, MDSKey.Splitter keyParts) {
    RunRecord runRecord = GSON.fromJson(Bytes.toString(row.get(COLUMN)), RunRecord.class);
    // skip default
    keyParts.skipString();
    String appId = keyParts.getString();
    String programId = keyParts.getString();
    long startTs = keyParts.getLong();
    String pId = keyParts.getString();

    writeTempRunRecordStart(appId, programId, pId, startTs);
    store.setStop(Id.Program.from(Constants.DEFAULT_NAMESPACE, appId, programId), pId, runRecord.getStopTs(),
                  ProgramController.State.getControllerStateByStatus(runRecord.getStatus()));
  }

  /**
   * Writes the {@link AppMetadataStore#TYPE_RUN_RECORD_STARTED} entry in the app meta table so that
   * {@link AppMetadataStore#TYPE_RUN_RECORD_COMPLETED} can be written which deleted the started record.
   */
  private void writeTempRunRecordStart(String appId, String programId, String pId, long startTs) {
    store.setStart(Id.Program.from(Constants.DEFAULT_NAMESPACE, appId, programId), pId,
                   Long.MAX_VALUE - startTs);
  }

  /**
   * Writes the updated application metadata through the {@link DefaultStore}
   *
   * @param row the {@link Row} containing the application metadata
   * @throws URISyntaxException if failed to create {@link URI} from the archive location in metadata
   */
  private void applicationMetadataHandler(Row row) throws URISyntaxException, IOException {
    ApplicationMeta appMeta = GSON.fromJson(Bytes.toString(row.get(COLUMN)), ApplicationMeta.class);
    store.addApplication(Id.Application.from(Constants.DEFAULT_NAMESPACE, appMeta.getId()), appMeta.getSpec(),
                         updateAppArchiveLocation(appMeta.getId(), new URI(appMeta.getArchiveLocation())));
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

  private static final class UpgradeTable implements Iterable<Table> {
    final Table table;

    private UpgradeTable(Table table) {
      this.table = table;
    }

    @Override
    public Iterator<Table> iterator() {
      return Iterators.singletonIterator(table);
    }
  }
}
