/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.messaging.store.leveldb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.store.MessageTable;
import io.cdap.cdap.messaging.store.MetadataTable;
import io.cdap.cdap.messaging.store.PayloadTable;
import io.cdap.cdap.messaging.store.TableFactory;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import org.apache.twill.common.Threads;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
  private final String metadataTableName;
  private final String messageTableName;
  private final String payloadTableName;
  private final ConcurrentMap<File, DB> levelDBs;

  private volatile LevelDBMetadataTable metadataTable;
  private volatile boolean closed;

  @VisibleForTesting
  @Inject
  public LevelDBTableFactory(CConfiguration cConf) {
    this.baseDir = new File(cConf.get(Constants.MessagingSystem.LOCAL_DATA_DIR));
    this.dbOptions = new Options()
      .blockSize(cConf.getInt(Constants.CFG_DATA_LEVELDB_BLOCKSIZE, Constants.DEFAULT_DATA_LEVELDB_BLOCKSIZE))
      .cacheSize(cConf.getLong(Constants.CFG_DATA_LEVELDB_CACHESIZE, Constants.DEFAULT_DATA_LEVELDB_CACHESIZE))
      .errorIfExists(false)
      .createIfMissing(true);
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("leveldb-tms-data-cleanup"));
    executor.scheduleAtFixedRate(new DataCleanup(), 0L,
                                 Long.parseLong(cConf.get(Constants.MessagingSystem.LOCAL_DATA_CLEANUP_FREQUENCY)),
                                 TimeUnit.SECONDS);

    this.metadataTableName = cConf.get(Constants.MessagingSystem.METADATA_TABLE_NAME);
    this.messageTableName = cConf.get(Constants.MessagingSystem.MESSAGE_TABLE_NAME);
    this.payloadTableName = cConf.get(Constants.MessagingSystem.PAYLOAD_TABLE_NAME);
    this.levelDBs = new ConcurrentHashMap<>();
  }

  @Override
  public synchronized MetadataTable createMetadataTable() throws IOException {
    if (closed) {
      throw new IOException("The LevelDBTableFactory is already closed");
    }

    if (metadataTable != null) {
      return metadataTable;
    }

    File dbPath = getMetadataDBPath(metadataTableName);
    metadataTable = new LevelDBMetadataTable(LEVEL_DB_FACTORY.open(dbPath, dbOptions));
    LOG.info("Messaging metadata table created at {}", dbPath);
    return metadataTable;
  }

  @Override
  public MessageTable createMessageTable(TopicMetadata topicMetadata) throws IOException {
    return new LevelDBMessageTable(getLevelDB(topicMetadata, messageTableName), topicMetadata);
  }

  @Override
  public PayloadTable createPayloadTable(TopicMetadata topicMetadata) throws IOException {
    return new LevelDBPayloadTable(getLevelDB(topicMetadata, payloadTableName), topicMetadata);
  }

  @Override
  public void close() {
    LevelDBMetadataTable metadataTable;
    synchronized (this) {
      closed = true;
      metadataTable = this.metadataTable;
      this.metadataTable = null;
    }
    if (metadataTable != null) {
      Closeables.closeQuietly(metadataTable.getLevelDB());
    }
    Collection<DB> dbs = levelDBs.values();
    dbs.forEach(Closeables::closeQuietly);
    dbs.clear();
  }

  /**
   * Returns the LevelDB {@link DB} object for the given {@link TopicMetadata}, which stores on the given file path.
   */
  private DB getLevelDB(TopicMetadata topicMetadata, String tablePrefix) throws IOException {
    File dbPath = getDataDBPath(tablePrefix, topicMetadata.getTopicId(), topicMetadata.getGeneration());

    DB db = levelDBs.get(dbPath);
    if (db != null) {
      return db;
    }

    synchronized (this) {
      if (closed) {
        throw new IOException("The LevelDBTableFactory is already closed");
      }
      // Check again to make sure no new instance was being created while this thread is acquiring the lock
      db = levelDBs.get(dbPath);
      if (db != null) {
        return db;
      }

      db = LEVEL_DB_FACTORY.open(ensureDirExists(dbPath), dbOptions);
      levelDBs.put(dbPath, db);
    }

    LOG.debug("Messaging levelDB table created at {}", dbPath);
    return db;
  }

  private File getDataDBPath(String tableName, TopicId topicId, int generation) {
    String fileName = String.format("%s.%s.%s.%d", topicId.getNamespace(), tableName, topicId.getTopic(), generation);
    return new File(baseDir, fileName);
  }

  private File getMetadataDBPath(String tableName) throws IOException {
    String fileName = String.format("%s.%s", NamespaceId.SYSTEM, tableName);
    return ensureDirExists(new File(baseDir, fileName));
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
      LevelDBMetadataTable metadataTable;
      synchronized (LevelDBTableFactory.this) {
        if (closed) {
          return;
        }
        metadataTable = LevelDBTableFactory.this.metadataTable;
      }

      long now = System.currentTimeMillis();

      // First delete all older generation files
      try (CloseableIterator<TopicMetadata> metadataIterator = metadataTable.scanTopics()) {
        while (metadataIterator.hasNext()) {
          TopicMetadata metadata = metadataIterator.next();
          int currGeneration = metadata.getGeneration();

          // We can safely remove all generations that are less than `cleanOlderThan`.
          int cleanOlderThan = currGeneration < 0 ? currGeneration * -1 + 1 : currGeneration;

          // Find the generations older than `cleanOlderThan`, that have data on disk, and remove them in reverse order.
          // We do it in reverse order, so that in case there is a failure in deleting one of them, we can repeat
          // the same process next iteration and not lose track of generations that need to be deleted.
          Deque<File> filesToDelete = new LinkedList<>();
          for (int olderGeneration = cleanOlderThan - 1; olderGeneration > 0; olderGeneration--) {
            // Message table
            File dataDBPath = getDataDBPath(messageTableName, metadata.getTopicId(), olderGeneration);
            if (!dataDBPath.exists()) {
              break;
            }
            // We can safely remove and close the levelDB as no one should be accessing them anymore
            Closeables.closeQuietly(levelDBs.remove(dataDBPath));
            filesToDelete.add(dataDBPath);

            // Payload table
            dataDBPath = getDataDBPath(payloadTableName, metadata.getTopicId(), olderGeneration);
            if (!dataDBPath.exists()) {
              break;
            }
            // We can safely remove and close the levelDB as no one should be accessing them anymore
            Closeables.closeQuietly(levelDBs.remove(dataDBPath));
            filesToDelete.add(dataDBPath);
          }

          Iterator<File> descendingIterator = filesToDelete.descendingIterator();
          while (descendingIterator.hasNext()) {
            File dataDBPath = descendingIterator.next();
            LOG.info("Deleting file: {}", dataDBPath);
            DirUtils.deleteDirectoryContents(dataDBPath);
          }

          // Prune the current generation
          // Message table
          File dataDBPath = getDataDBPath(messageTableName, metadata.getTopicId(), metadata.getGeneration());
          DB levelDB = levelDBs.get(dataDBPath);
          if (levelDB != null && dataDBPath.exists()) {
            new LevelDBMessageTable(levelDB, metadata).pruneMessages(now);
          }

          // Payload table
          dataDBPath = getDataDBPath(payloadTableName, metadata.getTopicId(), metadata.getGeneration());
          levelDB = levelDBs.get(dataDBPath);
          if (levelDB != null && dataDBPath.exists()) {
            new LevelDBPayloadTable(levelDB, metadata).pruneMessages(now);
          }
        }
      } catch (IOException ex) {
        LOG.debug("Unable to perform data cleanup in TMS LevelDB tables", ex);
      }
    }
  }
}
