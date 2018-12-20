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
import co.cask.cdap.proto.id.TopicId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import org.apache.twill.common.Threads;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.ParametersAreNonnullByDefault;

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

  private LevelDBMetadataTable metadataTable;

  private final LoadingCache<TopicMetadata, LevelDBMessageTable> messageTableCache;
  private final LoadingCache<TopicMetadata, LevelDBPayloadTable> payloadTableCache;

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
    this.messageTableCache = CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.HOURS)
      .removalListener((RemovalListener<TopicMetadata, LevelDBMessageTable>)
                         removalNotification -> Closeables.closeQuietly(removalNotification.getValue()))
      .build(new CacheLoader<TopicMetadata, LevelDBMessageTable>() {
      @ParametersAreNonnullByDefault
      @Override
      public LevelDBMessageTable load(TopicMetadata key) throws Exception {
        File dbPath = getDataDBPath(messageTableName, key.getTopicId(), key.getGeneration());
        LevelDBMessageTable messageTable =
          new LevelDBMessageTable(LEVEL_DB_FACTORY.open(dbPath, dbOptions), key);
        LOG.debug("Messaging message table created at {}", dbPath);
        return messageTable;
      }
    });

    this.payloadTableCache = CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.HOURS)
      .removalListener((RemovalListener<TopicMetadata, LevelDBPayloadTable>)
                         removalNotification -> Closeables.closeQuietly(removalNotification.getValue()))
      .build(new CacheLoader<TopicMetadata, LevelDBPayloadTable>() {
      @ParametersAreNonnullByDefault
      @Override
      public LevelDBPayloadTable load(TopicMetadata key) throws Exception {
        File dbPath = getDataDBPath(payloadTableName, key.getTopicId(), key.getGeneration());
        LevelDBPayloadTable payloadTable =
          new LevelDBPayloadTable(LEVEL_DB_FACTORY.open(dbPath, dbOptions), key);
        LOG.debug("Messaging payload table created at {}", dbPath);
        return payloadTable;
      }
    });
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
  public MessageTable createMessageTable(TopicMetadata topicMetadata) {
    return messageTableCache.getUnchecked(topicMetadata);
  }

  @Override
  public PayloadTable createPayloadTable(TopicMetadata topicMetadata) {
    return payloadTableCache.getUnchecked(topicMetadata);
  }

  private File getDataDBPath(String tableName, TopicId topicId, int generation) throws IOException {
    return getDataDBPath(tableName, topicId, generation, true);
  }

  private File getDataDBPath(String tableName, TopicId topicId, int generation,
                             boolean ensureExists) throws IOException {
    String fileName = String.format("%s.%s.%s.%d", topicId.getNamespace(), tableName, topicId.getTopic(), generation);
    File file = new File(baseDir, fileName);
    if (ensureExists) {
      ensureDirExists(file);
    }
    return file;
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
      if (metadataTable == null) {
        return;
      }

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
            File dataDBPath = getDataDBPath(messageTableName, metadata.getTopicId(), olderGeneration, false);
            if (!dataDBPath.exists()) {
              break;
            }
            filesToDelete.add(dataDBPath);
          }

          Iterator<File> descendingIterator = filesToDelete.descendingIterator();
          while (descendingIterator.hasNext()) {
            File dataDBPath = descendingIterator.next();
            LOG.info("Deleting file: {}", dataDBPath);
            DirUtils.deleteDirectoryContents(dataDBPath);
          }
        }
      } catch (IOException ex) {
        LOG.debug("Unable to perform data cleanup in TMS LevelDB tables", ex);
      }

      long timeStamp = System.currentTimeMillis();
      try {
        for (Map.Entry<TopicMetadata, LevelDBMessageTable> messageTableEntry : messageTableCache.asMap().entrySet()) {
          messageTableEntry.getValue().pruneMessages(messageTableEntry.getKey(), timeStamp);
        }
        for (Map.Entry<TopicMetadata, LevelDBPayloadTable> payloadTableEntry : payloadTableCache.asMap().entrySet()) {
          payloadTableEntry.getValue().pruneMessages(payloadTableEntry.getKey(), timeStamp);
        }
      } catch (IOException ex) {
        LOG.debug("Unable to perform data cleanup in TMS LevelDB tables", ex);
      }
    }
  }
}
