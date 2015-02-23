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
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.tx.DatasetContext;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.logging.save.LogSaverTableUtil;
import co.cask.cdap.logging.write.FileMetaDataManager;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.Map;

/**
 * Upgrades the Logs
 */
public class LogUpgrade extends AbstractUpgrade implements Upgrade {

  private static final Logger LOG = LoggerFactory.getLogger(LogUpgrade.class);
  private static final String[] OTHERS = {"logs", Constants.SYSTEM_NAMESPACE, Constants.DEFAULT_NAMESPACE};

  private final Transactional<DatasetContext<OrderedTable>, OrderedTable> logMDS;

  public LogUpgrade() {
    this.logMDS = Transactional.of(
      new TransactionExecutorFactory() {
        @Override
        public TransactionExecutor createExecutor(Iterable<TransactionAware> txAwares) {
          return new DefaultTransactionExecutor(txClient, txAwares);
        }
      },
      new Supplier<DatasetContext<OrderedTable>>() {
        @Override
        public DatasetContext<OrderedTable> get() {
          try {
            return DatasetContext.of(new LogSaverTableUtil(nonNamespaedFramework, cConf).getMetaTable());
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      });
  }

  @Override
  public void upgrade(Injector injector) throws Exception {
    final FileMetaDataManager fileMetaDataManager = new FileMetaDataManager(new LogSaverTableUtil(
      nonNamespaedFramework, cConf), txClient, locationFactory);
    logMDS.execute(new TransactionExecutor.Function<DatasetContext<OrderedTable>, Void>() {
      @Override
      public Void apply(DatasetContext<OrderedTable> ctx) throws Exception {
        Scanner rows = ctx.get().scan(null, null);
        Row row;
        while ((row = rows.next()) != null) {
          String key = Bytes.toString(Arrays.copyOfRange(row.getRow(), FileMetaDataManager.ROW_KEY_PREFIX.length,
                                                         row.getRow().length)).trim();
          Map<byte[], byte[]> allColumns = row.getColumns();
          for (byte[] startTime : allColumns.keySet()) {
            long startTimeMs = Bytes.toLong(startTime);
            String oldPath = Bytes.toString(allColumns.get(startTime));
            String newPath;
            String newKey;
            if (key.startsWith(Constants.Logging.SYSTEM_NAME) || key.startsWith(DEVELOPER_STRING)) {
              newPath = upgradePath(key, oldPath);
              newKey = upgradeKey(key);
              renameLocation(new URI(oldPath).resolve("."), new URI(newPath).resolve("."));
              fileMetaDataManager.writeMetaData(newKey, startTimeMs, locationFactory.create(new URI(newPath)));
              LOG.info("Upgrading logs for {}", key);
            } else if (!checkKeyValidality(key, OTHERS)) {
              LOG.warn("Unknown key {} found. Failed to Upgrade", key);
              throw new RuntimeException("Unknown key: " + key);
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
   * Upgrades the path by adding the namespace in the log archive location
   *
   * @param key      the key which is log partition
   * @param location the log archive location
   * @return the new location which has the appropriate namespace
   */
  public String upgradePath(String key, String location) {
    String newLocation = null;
    if (key.startsWith(Constants.Logging.SYSTEM_NAME)) {
      if (location != null) {
        newLocation = location.substring(0, location.lastIndexOf(CDAP_WITH_FORWARD_SLASH)) +
          location.substring(location.lastIndexOf(CDAP_WITH_FORWARD_SLASH) + (CDAP_WITH_FORWARD_SLASH).length());

        newLocation = newLocation.substring(0, (newLocation.lastIndexOf(CDAP_WITH_FORWARD_SLASH) +
          CDAP_WITH_FORWARD_SLASH.length())) + Constants.SYSTEM_NAMESPACE + FORWARD_SLASH +
          newLocation.substring(newLocation.lastIndexOf(CDAP_WITH_FORWARD_SLASH) + CDAP_WITH_FORWARD_SLASH.length());
      }
    } else if (key.startsWith(DEVELOPER_STRING)) {
      if (location != null) {
        newLocation = location.replace(DEVELOPER_STRING + FORWARD_SLASH, "");
        newLocation = newLocation.substring(0, (newLocation.lastIndexOf(CDAP_WITH_FORWARD_SLASH) +
          CDAP_WITH_FORWARD_SLASH.length())) + Constants.DEFAULT_NAMESPACE + FORWARD_SLASH +
          newLocation.substring(newLocation.lastIndexOf(CDAP_WITH_FORWARD_SLASH) + CDAP_WITH_FORWARD_SLASH.length());
      }
    }
    return newLocation;
  }
}
