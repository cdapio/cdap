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
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManager;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.logging.framework.LogPathIdentifier;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Class for writing log file meta data.
 */
public class FileMetaDataWriter {

  private static final Logger LOG = LoggerFactory.getLogger(FileMetaDataWriter.class);
  private static final byte VERSION = 1;

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
              identifier.getRowKey(), eventTimeMs, currentTimeMs, location);

    transactional.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        // add column version prefix for new format
        byte[] columnKey = new byte[1 + Bytes.SIZEOF_LONG * 2];
        columnKey[0] = VERSION;
        Bytes.putLong(columnKey, Bytes.putLong(columnKey, 1, eventTimeMs), currentTimeMs);
        Table table = LoggingStoreTableUtil.getMetadataTable(context, datasetManager);
        table.put(getRowKey(identifier), columnKey, Bytes.toBytes(location.toURI().getPath()));
      }
    });
  }

  private byte[] getRowKey(LogPathIdentifier identifier) {
    return Bytes.concat(LoggingStoreTableUtil.FILE_META_ROW_KEY_PREFIX,
                        identifier.getRowKey().getBytes(StandardCharsets.UTF_8));
  }
}
