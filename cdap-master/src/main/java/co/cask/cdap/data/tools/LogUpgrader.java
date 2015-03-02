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
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.tx.DatasetContext;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.logging.save.LogSaverTableUtil;
import co.cask.cdap.logging.write.FileMetaDataManager;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Upgrades the Logs
 */
public class LogUpgrader extends AbstractUpgrader {

  private static final Logger LOG = LoggerFactory.getLogger(LogUpgrader.class);
  private static final String[] OTHERS = {"logs", Constants.SYSTEM_NAMESPACE, Constants.DEFAULT_NAMESPACE};

  private final Transactional<DatasetContext<Table>, Table> logMDS;
  private final CConfiguration cConf;
  private final TransactionSystemClient txClient;
  private final DatasetFramework nonNamespaedFramework;


  @Inject
  public LogUpgrader(LocationFactory locationFactory, final TransactionSystemClient txClient,
                     final CConfiguration cConf,
                     @Named("nonNamespacedDSFramework") final DatasetFramework nonNamespacedFramework) {
    super(locationFactory);
    this.cConf = cConf;
    this.txClient = txClient;
    this.nonNamespaedFramework = nonNamespacedFramework;
    this.logMDS = Transactional.of(
      new TransactionExecutorFactory() {
        @Override
        public TransactionExecutor createExecutor(Iterable<TransactionAware> txAwares) {
          return new DefaultTransactionExecutor(txClient, txAwares);
        }
      },
      new Supplier<DatasetContext<Table>>() {
        @Override
        public DatasetContext<Table> get() {
          try {
            return DatasetContext.of(new LogSaverTableUtil(nonNamespaedFramework, cConf).getMetaTable());
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      });
  }

  @Override
  public void upgrade() throws Exception {
    final FileMetaDataManager fileMetaDataManager = new FileMetaDataManager(new LogSaverTableUtil(
      nonNamespaedFramework, cConf), txClient, locationFactory);
    logMDS.execute(new TransactionExecutor.Function<DatasetContext<Table>, Void>() {
      @Override
      public Void apply(DatasetContext<Table> ctx) throws Exception {
        Scanner rows = ctx.get().scan(null, null);
        Row row;
        while ((row = rows.next()) != null) {
          String key = Bytes.toString(row.getRow(), FileMetaDataManager.ROW_KEY_PREFIX.length,
                                      (row.getRow().length - FileMetaDataManager.ROW_KEY_PREFIX.length));
          Map<byte[], byte[]> allColumns = row.getColumns();
          for (Map.Entry<byte[], byte[]> entry : allColumns.entrySet()) {
            long startTimeMs = Bytes.toLong(entry.getKey());
            String oldPath = Bytes.toString(entry.getValue());
            Location newPath;
            String newKey;
            if (key.startsWith(Constants.Logging.SYSTEM_NAME) || key.startsWith(DEVELOPER_STRING)) {
              newPath = upgradePath(key, oldPath);
              newKey = upgradeKey(key);
              try {
                fileMetaDataManager.writeMetaData(newKey, startTimeMs, newPath);
                LOG.info("Upgrading logs for {}", key);
              } catch (Throwable t) {
                LOG.warn("No updating log file path in log meta table");
                throw Throwables.propagate(t);
              }
            } else if (!isKeyValid(key, OTHERS)) {
              LOG.warn("Unknown key {} found. Failed to Upgrade", key);
            }
          }
        }
        return null;
      }
    });
  }

  /**
   * Adds the namespace in the key which is the log partition
   *
   * @param key the key which is the log partition
   * @return the new key with namespace
   */
  private String upgradeKey(String key) {
    if (key.startsWith(Constants.Logging.SYSTEM_NAME)) {
      return key.replace(Constants.Logging.SYSTEM_NAME, Constants.SYSTEM_NAMESPACE);
    }
    return key.replace(DEVELOPER_STRING, Constants.DEFAULT_NAMESPACE);
  }

  /**
   * Creates a new path depending on the old log file path
   *
   * @param key         the key for this log meta entry
   * @param oldLocation the old log {@link Location}
   * @return the {@link Location}
   * @throws IOException
   * @throws URISyntaxException
   */
  private Location upgradePath(String key, String oldLocation) throws IOException, URISyntaxException {
    Location location = locationFactory.create(new URI(oldLocation));
    Location newLocation = null;
    if (key.startsWith(Constants.Logging.SYSTEM_NAME)) {
      if (location != null) {
        // Example path of this type: hdfs://cdap28-161119-1000.dev.continuuity.
        // net/cdap/logs/avro/cdap/services/service-appfabric/2015-02-26/1424988452088.avro
        // removes the second occurrence of "cdap/" in the path
        newLocation = updateLogLocation(location, Constants.SYSTEM_NAMESPACE);
        // newLocation will be: hdfs://cdap28-161119-1000.dev.continuuity.net/cdap/system/logs/avro/
        // services/service-appfabric/2015-02-26/1424988452088.avro
      }
    } else if (key.startsWith(DEVELOPER_STRING)) {
      if (location != null) {
        // Exmaple of this type: hdfs://cdap28-161119-1000.dev.continuuity.net/cdap/logs/avro/developer/HelloWorld/
        // flow-WhoFlow/2015-02-26/1424988484082.avro
        newLocation = updateLogLocation(location, Constants.DEFAULT_NAMESPACE);
        // newLocation will be: dfs://cdap28-161119-1000.dev.continuuity.net/cdap/default/logs/avro/HelloWorld/
        // flow-WhoFlow/2015-02-26/1424988484082.avro
      }
    }
    return newLocation;
  }

  /**
   * Strips different parts from the old log location and creates a new one
   *
   * @param location  the old log {@link Location}
   * @param nameSpace the namespace which will be added to the new log location
   * @return the log {@link Location}
   * @throws IOException
   */
  private Location updateLogLocation(Location location, String nameSpace) throws IOException {
    String logFilename = location.getName();
    Location parentLocation = Locations.getParent(location);  // strip filename
    String date = parentLocation.getName();
    parentLocation = Locations.getParent(parentLocation); // strip date
    String programName = parentLocation.getName();
    parentLocation = Locations.getParent(parentLocation); // strip program name
    String programType = parentLocation.getName();
    parentLocation = Locations.getParent(parentLocation); // strip program type

    parentLocation = Locations.getParent(parentLocation); // strip old namespace
    String avro = parentLocation.getName();

    parentLocation = Locations.getParent(parentLocation); // strip avro

    String logs = parentLocation.getName();

    return locationFactory.create(nameSpace).append(logs).append(avro).append(programType).append(programName)
      .append(date).append(logFilename);
  }
}
