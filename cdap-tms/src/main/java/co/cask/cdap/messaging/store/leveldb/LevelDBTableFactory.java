/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.messaging.store.leveldb;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.messaging.store.PayloadTable;
import co.cask.cdap.messaging.store.TableFactory;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.inject.Inject;
import org.apache.twill.common.Threads;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A {@link TableFactory} for creating tables used by the messaging system using the LevelDB implementation.
 */
public final class LevelDBTableFactory implements TableFactory {

  private static final Logger LOG = LoggerFactory.getLogger(LevelDBTableFactory.class);
  private static final Iq80DBFactory LEVEL_DB_FACTORY = Iq80DBFactory.factory;

  private final File baseDir;
  private final Options dbOptions;
  private final ScheduledExecutorService executor;

  private LevelDBMetadataTable metadataTable;
  private LevelDBMessageTable messageTable;
  private LevelDBPayloadTable payloadTable;

  @Inject
  LevelDBTableFactory(CConfiguration cConf) {
    this.baseDir = new File(cConf.get(Constants.MessagingSystem.LOCAL_DATA_DIR));
    this.dbOptions = new Options()
      .blockSize(cConf.getInt(Constants.CFG_DATA_LEVELDB_BLOCKSIZE, Constants.DEFAULT_DATA_LEVELDB_BLOCKSIZE))
      .cacheSize(cConf.getLong(Constants.CFG_DATA_LEVELDB_CACHESIZE, Constants.DEFAULT_DATA_LEVELDB_CACHESIZE))
      .errorIfExists(false)
      .createIfMissing(true);
    this.executor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("leveldb-tms-data-cleanup"));
    this.executor.scheduleAtFixedRate(new DataCleanup(), 0L,
                                      Long.parseLong(cConf.get(Constants.MessagingSystem.LOCAL_DATA_CLEANUP_FREQUENCY)),
                                      TimeUnit.SECONDS);
  }

  @Override
  public synchronized MetadataTable createMetadataTable(String tableName) throws IOException {
    if (metadataTable != null) {
      return metadataTable;
    }

    File dbPath = ensureDirExists(new File(baseDir, NamespaceId.SYSTEM.getNamespace() + "." + tableName));
    metadataTable = new LevelDBMetadataTable(LEVEL_DB_FACTORY.open(dbPath, dbOptions));
    LOG.info("Messaging metadata table created at {}", dbPath);
    return metadataTable;
  }

  @Override
  public synchronized MessageTable createMessageTable(String tableName) throws IOException {
    if (messageTable != null) {
      return messageTable;
    }

    File dbPath = ensureDirExists(new File(baseDir, NamespaceId.SYSTEM.getNamespace() + "." + tableName));
    messageTable = new LevelDBMessageTable(LEVEL_DB_FACTORY.open(dbPath, dbOptions));
    LOG.info("Messaging message table created at {}", dbPath);
    return messageTable;
  }

  @Override
  public synchronized PayloadTable createPayloadTable(String tableName) throws IOException {
    if (payloadTable != null) {
      return payloadTable;
    }

    File dbPath = ensureDirExists(new File(baseDir, NamespaceId.SYSTEM.getNamespace() + "." + tableName));
    payloadTable = new LevelDBPayloadTable(LEVEL_DB_FACTORY.open(dbPath, dbOptions));
    LOG.info("Messaging payload table created at {}", dbPath);
    return payloadTable;
  }

  @Override
  public void upgradeMessageTable(String tableName) throws IOException {
    // no-op
  }

  @Override
  public void upgradePayloadTable(String tableName) throws IOException {
    // no-op
  }

  private File ensureDirExists(File dir) throws IOException {
    if (!DirUtils.mkdirs(dir)) {
      throw new IOException("Failed to create local directory " + dir + " for the messaging system.");
    }
    return dir;
  }

  private class DataCleanup implements Runnable {

    @Override
    public void run() {
      if (metadataTable == null || payloadTable == null || messageTable == null) {
        return;
      }

      long timeStamp = System.currentTimeMillis();
      try (CloseableIterator<TopicMetadata> metadataIterator = metadataTable.scanTopics()) {
        while (metadataIterator.hasNext()) {
          TopicMetadata metadata = metadataIterator.next();
          messageTable.pruneMessages(metadata, timeStamp);
          payloadTable.pruneMessages(metadata, timeStamp);
        }
      } catch (IOException ex) {
        LOG.debug("Unable to perform data cleanup in TMS LevelDB tables", ex);
      }
    }
  }
}
