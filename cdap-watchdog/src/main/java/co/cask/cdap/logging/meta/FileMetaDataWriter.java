/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.meta;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManager;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.logging.appender.system.LogPathIdentifier;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Class for writing log file meta data.
 */
public class FileMetaDataWriter {

  private static final Logger LOG = LoggerFactory.getLogger(FileMetaDataWriter.class);

  private final Transactional transactional;
  private final DatasetManager datasetManager;

  public FileMetaDataWriter(DatasetManager datasetManager, Transactional transactional) {
    this.transactional = transactional;
    this.datasetManager = datasetManager;
  }

  /**
   * Persists meta data associated with a log file.
   *
   * @param identifier logging context identifier.
   * @param eventTimeMs start log time associated with the file.
   * @param currentTimeMs current time during file creation.
   * @param location log file location.
   */
  public void writeMetaData(final LogPathIdentifier identifier,
                            final long eventTimeMs,
                            final long currentTimeMs,
                            final Location location) throws Exception {
    LOG.debug("Writing meta data for logging context {} with startTimeMs {} sequence Id {} and location {}",
              identifier.getRowkey(), eventTimeMs, currentTimeMs, location);
    Transactionals.execute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        Table table = LoggingStoreTableUtil.getMetadataTable(context, datasetManager);
        table.put(getRowKey(identifier, eventTimeMs, currentTimeMs),
                  LoggingStoreTableUtil.META_TABLE_COLUMN_KEY, Bytes.toBytes(location.toURI().getPath()));
      }
    }, Exception.class);
  }

  private byte[] getRowKey(LogPathIdentifier identifier, long eventTime, long currentTime) {
    return Bytes.concat(LoggingStoreTableUtil.NEW_FILE_META_ROW_KEY_PREFIX,
                        identifier.getRowkey().getBytes(StandardCharsets.UTF_8),
                        Bytes.toBytes(eventTime),
                        Bytes.toBytes(currentTime));
  }
}
