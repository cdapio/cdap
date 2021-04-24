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
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.messaging.MessagingUtils;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.store.ImmutableMessageTableEntry;
import io.cdap.cdap.messaging.store.ImmutablePayloadTableEntry;
import io.cdap.cdap.messaging.store.MessageTable;
import io.cdap.cdap.messaging.store.MetadataTable;
import io.cdap.cdap.messaging.store.PayloadTable;
import io.cdap.cdap.messaging.store.TableEntry;
import io.cdap.cdap.messaging.store.TableFactory;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import org.apache.twill.common.Threads;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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
  private final int cleanupBatchSize;

  private LevelDBMetadataTable metadataTable;

  @VisibleForTesting
  @Inject
  public LevelDBTableFactory(CConfiguration cConf) {
    this.baseDir = new File(cConf.get(Constants.MessagingSystem.LOCAL_DATA_DIR));
    this.dbOptions = new Options()
      .blockSize(cConf.getInt(Constants.CFG_DATA_LEVELDB_BLOCKSIZE, Constants.DEFAULT_DATA_LEVELDB_BLOCKSIZE))
      .cacheSize(cConf.getLong(Constants.CFG_DATA_LEVELDB_CACHESIZE, Constants.DEFAULT_DATA_LEVELDB_CACHESIZE))
      .errorIfExists(false)
      .createIfMissing(true);

    this.metadataTableName = cConf.get(Constants.MessagingSystem.METADATA_TABLE_NAME);
    this.messageTableName = cConf.get(Constants.MessagingSystem.MESSAGE_TABLE_NAME);
    this.payloadTableName = cConf.get(Constants.MessagingSystem.PAYLOAD_TABLE_NAME);
    this.cleanupBatchSize = cConf.getInt(Constants.MessagingSystem.LOCAL_DATA_CLEANUP_BATCH_SIZE);
    this.levelDBs = new ConcurrentHashMap<>();

    long cleanupFrequency = Long.parseLong(cConf.get(Constants.MessagingSystem.LOCAL_DATA_CLEANUP_FREQUENCY));
    // For testing, disable automatic cleanup by having frequency set to <= 0.
    if (cleanupFrequency > 0) {
      ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("leveldb-tms-data-cleanup"));
      executor.scheduleAtFixedRate(this::cleanup, 0L, cleanupFrequency, TimeUnit.SECONDS);
    }
  }

  @Override
  public synchronized MetadataTable createMetadataTable() throws IOException {
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
    return new LevelDBMessageTable(getLevelDB(topicMetadata, messageTableName));
  }

  @Override
  public PayloadTable createPayloadTable(TopicMetadata topicMetadata) throws IOException {
    return new LevelDBPayloadTable(getLevelDB(topicMetadata, payloadTableName));
  }

  @Override
  public void close() {
    LevelDBMetadataTable metadataTable;
    synchronized (this) {
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

  private void cleanup() {
    performCleanup(System.currentTimeMillis());
  }

  /**
   * Performs data cleanup of LevelDB tables and table entries. It removes old topics and old topic generations,
   * followed by removing old table entries that exceeded TTL.
   */
  @VisibleForTesting
  void performCleanup(long currentTime) {
    if (metadataTable == null) {
      return;
    }

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
          // The payload is not needed for pruning, hence passing in null.
          pruneMessages(currentTime, levelDB, metadata, e -> new ImmutableMessageTableEntry(e.getKey(), null, null));
        }

        // Payload table
        dataDBPath = getDataDBPath(payloadTableName, metadata.getTopicId(), metadata.getGeneration());
        levelDB = levelDBs.get(dataDBPath);
        if (levelDB != null && dataDBPath.exists()) {
          // The payload is not needed for pruning, hence passing in null.
          pruneMessages(currentTime, levelDB, metadata, e -> new ImmutablePayloadTableEntry(e.getKey(),
                                                                                            Bytes.EMPTY_BYTE_ARRAY));
        }
      }
    } catch (IOException ex) {
      LOG.debug("Unable to perform data cleanup in TMS LevelDB tables", ex);
    }
  }

  /**
   * Delete messages of a {@link TopicId} that has exceeded the TTL or if it belongs to an older generation
   *
   * @param now current timestamp
   * @throws IOException error occurred while trying to delete a row in LevelDB
   */
  private void pruneMessages(long now, DB levelDB, TopicMetadata topicMetadata,
                             Function<Map.Entry<byte[], byte[]>, TableEntry> tableEntryFunc) throws IOException {
    TopicId topicId = topicMetadata.getTopicId();
    int topicGeneration = topicMetadata.getGeneration();

    WriteBatch writeBatch = levelDB.createWriteBatch();
    long ttlMillis = TimeUnit.SECONDS.toMillis(topicMetadata.getTTL());
    byte[] startRow = MessagingUtils.toDataKeyPrefix(topicId,
                                                     Integer.parseInt(MessagingUtils.Constants.DEFAULT_GENERATION));
    byte[] stopRow = Bytes.stopKeyForPrefix(startRow);
    boolean done = false;
    int batchSize = 0;

    while (!done) {
      done = true;
      try (CloseableIterator<Map.Entry<byte[], byte[]>> rowIterator = new DBScanIterator(levelDB, startRow, stopRow)) {
        while (rowIterator.hasNext()) {
          done = false;
          Map.Entry<byte[], byte[]> entry = rowIterator.next();
          TableEntry tableEntry = tableEntryFunc.apply(entry);

          int dataGeneration = tableEntry.getGeneration();
          if (!topicId.equals(tableEntry.getTopicId())) {
            LOG.warn("Ignore table entry with topic '{}' that doesn't match with the topic '{}'",
                     tableEntry.getTopicId(), topicId);
            continue;
          }
          if (topicGeneration != dataGeneration) {
            LOG.warn("Ignore table entry with topic generation '{}' that doesn't match with the topic generation '{}'",
                     tableEntry.getGeneration(), dataGeneration);
            continue;
          }

          if (MessagingUtils.isOlderGeneration(dataGeneration, topicGeneration)) {
            writeBatch.delete(entry.getKey());
          } else if ((dataGeneration == Math.abs(topicGeneration)) && ((now - tableEntry.getTimestamp()) > ttlMillis)) {
            writeBatch.delete(entry.getKey());
          } else {
            // terminate scanning table once an entry with write time after TTL is found to avoid scanning whole table,
            // since the entries are sorted by time.
            done = true;
            break;
          }
          batchSize++;
          if (batchSize >= cleanupBatchSize) {
            break;
          }
        }
      }

      try {
        levelDB.write(writeBatch, new WriteOptions().sync(true));
        batchSize = 0;
      } catch (DBException ex) {
        throw new IOException(ex);
      }
    }
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
}
