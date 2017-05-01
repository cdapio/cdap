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

package co.cask.cdap.messaging.store.cache;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.metrics.NoopMetricsContext;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.utils.TimeProvider;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.cache.MessageCache;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.messaging.store.ImmutableMessageTableEntry;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.messaging.store.leveldb.LevelDBMessageTableTest;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import org.apache.tephra.Transaction;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit test for {@link CachingMessageTable}.
 */
public class CachingMessageTableTest extends LevelDBMessageTableTest {

  private static MessageTableCacheProvider cacheProvider;

  @BeforeClass
  public static void initCache() {
    final LoadingCache<TopicId, MessageCache<MessageTable.Entry>> caches = CacheBuilder
      .newBuilder().build(new CacheLoader<TopicId, MessageCache<MessageTable.Entry>>() {
        @Override
        public MessageCache<MessageTable.Entry> load(TopicId key) throws Exception {
          return new MessageCache<>(new MessageTableEntryComparator(), new MessageTableEntryWeigher(),
                                    new MessageCache.Limits(500, 700, 1000), new NoopMetricsContext());
        }
      });

    cacheProvider = new MessageTableCacheProvider() {
      @Override
      public MessageCache<MessageTable.Entry> getMessageCache(TopicId topicId) {
        return caches.getUnchecked(topicId);
      }
    };
  }

  @Override
  protected MessageTable getMessageTable() throws Exception {
    MessageTable messageTable = super.getMessageTable();
    return new CachingMessageTable(cConf, messageTable, cacheProvider);
  }

  @Test
  public void testCachePruning() throws Exception {
    long txGracePeriod = 6;
    CConfiguration cConf = CConfiguration.create();
    cConf.setLong(CachingMessageTable.PRUNE_GRACE_PERIOD, txGracePeriod);

    // Creates a CachingMessageTable with a controlled time provider
    final AtomicLong currentTimeMillis = new AtomicLong(0);
    MessageTable messageTable = new CachingMessageTable(cConf, super.getMessageTable(),
                                                        cacheProvider, new TimeProvider() {
      @Override
      public long currentTimeMillis() {
        return currentTimeMillis.get();
      }
    });

    // Insert 10 entries, with different publish time
    TopicMetadata metadata = new TopicMetadata(NamespaceId.DEFAULT.topic("test"),
                                               TopicMetadata.GENERATION_KEY, 1,
                                               TopicMetadata.TTL_KEY, 86400);
    for (int i = 0; i < 10; i++) {
      // Key is (topic, generation, publish time, sequence id)
      byte[] key = Bytes.concat(MessagingUtils.toDataKeyPrefix(metadata.getTopicId(), metadata.getGeneration()),
                                Bytes.toBytes((long) i), Bytes.toBytes((short) 0));
      // Store a message with a write pointer
      messageTable.store(
        Collections.singleton(new ImmutableMessageTableEntry(key, Bytes.toBytes("Payload " + i),
                                                             Bytes.toBytes((long) i))).iterator());
    }

    // Update the current time to 11
    currentTimeMillis.set(11);

    // Fetch from the table without transaction, should get all entries.
    try (CloseableIterator<MessageTable.Entry> iter = messageTable.fetch(metadata, 0, 100, null)) {
      List<MessageTable.Entry> entries = Lists.newArrayList(iter);
      Assert.assertEquals(10, entries.size());
      // All entries must be from the cache
      for (MessageTable.Entry entry : entries) {
        Assert.assertTrue(entry instanceof CachingMessageTable.CacheMessageTableEntry);
      }
    }

    // Fetch with a transaction, with start time older than tx grace period / 2
    Transaction tx = new Transaction(10, 11, new long[0], new long[0], 11);
    long startTime = currentTimeMillis.get() - txGracePeriod / 2 - 1;
    try (CloseableIterator<MessageTable.Entry> iter = messageTable.fetch(metadata, startTime, 100, tx)) {
      List<MessageTable.Entry> entries = Lists.newArrayList(iter);

      // Should get three entries (7, 8, 9)
      Assert.assertEquals(3, entries.size());

      // The first entry should be from the table, while the last two entries (timestamp 8 and 9) should be
      // from cache (current time = 11, grace period = 3)
      Iterator<MessageTable.Entry> iterator = entries.iterator();
      Assert.assertFalse(iterator.next() instanceof CachingMessageTable.CacheMessageTableEntry);
      Assert.assertTrue(iterator.next() instanceof CachingMessageTable.CacheMessageTableEntry);
      Assert.assertTrue(iterator.next() instanceof CachingMessageTable.CacheMessageTableEntry);
    }

    // Fetch with a transaction, with start messageId publish time older than tx grace period / 2
    byte[] rawId = new byte[MessageId.RAW_ID_SIZE];
    MessageId.putRawId(startTime, (short) 0, 0L, (short) 0, rawId, 0);
    MessageId messageId = new MessageId(rawId);
    try (CloseableIterator<MessageTable.Entry> iter = messageTable.fetch(metadata, messageId, true, 100, tx)) {
      List<MessageTable.Entry> entries = Lists.newArrayList(iter);

      // Should get three entries (7, 8, 9)
      Assert.assertEquals(3, entries.size());

      // The first entry should be from the table, while the last two entries (timestamp 8 and 9) should be
      // from cache (current time = 11, grace period = 3)
      Iterator<MessageTable.Entry> iterator = entries.iterator();
      Assert.assertFalse(iterator.next() instanceof CachingMessageTable.CacheMessageTableEntry);
      Assert.assertTrue(iterator.next() instanceof CachingMessageTable.CacheMessageTableEntry);
      Assert.assertTrue(iterator.next() instanceof CachingMessageTable.CacheMessageTableEntry);
    }
  }
}
