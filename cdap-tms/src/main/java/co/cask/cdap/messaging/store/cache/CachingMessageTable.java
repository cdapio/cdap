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

import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.utils.TimeProvider;
import co.cask.cdap.messaging.RollbackDetail;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.cache.MessageCache;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.messaging.store.MessageFilter;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.messaging.store.TransactionMessageFilter;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.tephra.Transaction;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link MessageTable} that uses {@link MessageCache} for caching recently published messages.
 */
final class CachingMessageTable implements MessageTable {

  // Copied from TxConstants in Tephra because TMS doesn't depends on tephra-core.
  @VisibleForTesting
  static final String PRUNE_GRACE_PERIOD = "data.tx.grace.period";

  private final long gracePeriod;
  private final MessageTable messageTable;
  private final MessageTableCacheProvider cacheProvider;
  private final TimeProvider timeProvider;

  CachingMessageTable(CConfiguration cConf, MessageTable messageTable, MessageTableCacheProvider cacheProvider) {
    this(cConf, messageTable, cacheProvider, TimeProvider.SYSTEM_TIME);
  }

  @VisibleForTesting
  CachingMessageTable(CConfiguration cConf, MessageTable messageTable,
                      MessageTableCacheProvider cacheProvider, TimeProvider timeProvider) {
    // Half the tx pruning grace period to be the grace period for scanning the message cache.
    // This is to make sure we won't scan for cached entries that might be pruned.
    this.gracePeriod = cConf.getLong(PRUNE_GRACE_PERIOD) / 2;
    this.messageTable = messageTable;
    this.cacheProvider = cacheProvider;
    this.timeProvider = timeProvider;
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicMetadata metadata, long startTime,
                                        int limit, @Nullable Transaction transaction) throws IOException {
    MessageCache<Entry> messageCache = cacheProvider.getMessageCache(metadata.getTopicId());
    if (messageCache == null) {
      // If no caching for the given topic, just return result from table directly
      return messageTable.fetch(metadata, startTime, limit, transaction);
    }

    // First scan from cache.
    Entry lookupEntry = new CacheMessageTableEntry(metadata, startTime, (short) 0);
    // Adjust the cache scan start time based on the pruning grace period if fetch with transaction
    Entry adjustedEntry = transaction == null ? lookupEntry : adjustLookupEntry(metadata, lookupEntry);
    MessageCache.Scanner<Entry> scanner = messageCache.scan(adjustedEntry, true,
                                                            limit, createFilter(metadata, transaction));

    // No need to scan the table if there is no adjustment on the start time and the cache has everything needed
    if (lookupEntry == adjustedEntry && cacheHasAllEntries(lookupEntry, scanner, messageCache.getComparator())) {
      return scanner;
    }

    // Otherwise scan the table and return a combine result.
    CloseableIterator<Entry> tableIterator = messageTable.fetch(metadata, startTime, limit, transaction);
    return new CombineMessageEntryIterator(tableIterator, scanner, messageCache.getComparator(), limit);
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicMetadata metadata, MessageId messageId, boolean inclusive,
                                        int limit, @Nullable Transaction transaction) throws IOException {

    MessageCache<Entry> messageCache = cacheProvider.getMessageCache(metadata.getTopicId());
    if (messageCache == null) {
      // If no caching for the given topic, just return result from table directly
      return messageTable.fetch(metadata, messageId, inclusive, limit, transaction);
    }

    // First scan from cache, starting from the given message id
    Entry lookupEntry = new CacheMessageTableEntry(metadata,
                                                   messageId.getPublishTimestamp(), messageId.getSequenceId());

    // Adjust the cache scan start time based on the pruning grace period if fetch with transaction
    Entry adjustedEntry = transaction == null ? lookupEntry : adjustLookupEntry(metadata, lookupEntry);
    MessageCache.Scanner<Entry> scanner = messageCache.scan(adjustedEntry, inclusive,
                                                            limit, createFilter(metadata, transaction));

    // No need to scan the table if there is no adjustment on the start messageId and the cache has everything needed
    if (lookupEntry == adjustedEntry && cacheHasAllEntries(lookupEntry, scanner, messageCache.getComparator())) {
      return scanner;
    }

    // Otherwise scan the table and return a combine result.
    CloseableIterator<Entry> tableIterator = messageTable.fetch(metadata, messageId, inclusive, limit, transaction);
    return new CombineMessageEntryIterator(tableIterator, scanner, messageCache.getComparator(), limit);
  }

  @Override
  public void store(Iterator<? extends Entry> entries) throws IOException {
    // Write it to the message table first
    CopyingIterator iterator = new CopyingIterator(entries);
    messageTable.store(iterator);

    Multimap<TopicId, Entry> topicEntries = iterator.getEntries();
    for (Map.Entry<TopicId, Collection<Entry>> entry : topicEntries.asMap().entrySet()) {
      MessageCache<Entry> messageCache = cacheProvider.getMessageCache(entry.getKey());
      // Write it to the cache if it is enabled for the topic
      if (messageCache != null) {
        messageCache.addAll(entry.getValue().iterator());
      }
    }
  }

  @Override
  public void rollback(TopicMetadata metadata, RollbackDetail rollbackDetail) throws IOException {
    MessageCache<Entry> messageCache = cacheProvider.getMessageCache(metadata.getTopicId());
    if (messageCache != null) {
      // Rollback from the cache first so that we don't have to worry about invalid list pruning for the cache,
      // assuming the rollback from cache shouldn't fail.
      Entry startEntry = new CacheMessageTableEntry(metadata,
                                                    rollbackDetail.getStartTimestamp(),
                                                    (short) rollbackDetail.getStartSequenceId());
      Entry endEntry = new CacheMessageTableEntry(metadata,
                                                  rollbackDetail.getEndTimestamp(),
                                                  (short) rollbackDetail.getEndSequenceId());
      messageCache.updateEntries(startEntry, endEntry, new MessageCache.EntryUpdater<Entry>() {
        @Override
        public void updateEntry(Entry entry) {
          if (!(entry instanceof CacheMessageTableEntry)) {
            // This shouldn't happen
            throw new IllegalStateException("Entries in MessageCache must be of type "
                                              + CacheMessageTableEntry.class.getName()
                                              + ", but got type " + entry.getClass().getName() + " instead.");
          }
          ((CacheMessageTableEntry) entry).rollback();
        }
      });
    }

    // Rollback from the table
    messageTable.rollback(metadata, rollbackDetail);
  }

  @Override
  public void close() throws IOException {
    messageTable.close();
  }

  /**
   * Adjusts the given {@link Entry} based on the grace period.
   *
   * @param metadata the topic metadata
   * @param entry the {@link Entry} to adjust
   * @return the same {@link Entry} instance if no adjustment was made; otherwise return a new instance
   *         with the publish timestamp adjusted base on the grace period.
   */
  private Entry adjustLookupEntry(TopicMetadata metadata, Entry entry) {
    long minStartTime = timeProvider.currentTimeMillis() - gracePeriod;
    if (entry.getPublishTimestamp() >= minStartTime) {
      return entry;
    }
    return new CacheMessageTableEntry(metadata, minStartTime, (short) 0);
  }

  /**
   * Returns {@code true} if the scanner created from the message cache contains all entries starting from the given
   * start entry; otherwise return {@code false}.
   */
  private boolean cacheHasAllEntries(Entry startEntry, MessageCache.Scanner<Entry> scanner,
                                     Comparator<MessageTable.Entry> comparator) {
    Entry firstInCache = scanner.getFirstInCache();
    return firstInCache != null && comparator.compare(firstInCache, startEntry) <= 0;
  }

  /**
   * Creates a {@link MessageFilter} for scanning entries from the {@link MessageCache}.
   */
  private MessageFilter<Entry> createFilter(TopicMetadata metadata, @Nullable Transaction transaction) {
    final int generation = metadata.getGeneration();

    // If there is no transaction in reading, the filter just need to match with the topic generation
    if (transaction == null) {
      return new MessageFilter<Entry>() {
        @Override
        public Result apply(Entry entry) {
          return generation ==  entry.getGeneration() ? Result.ACCEPT : Result.SKIP;
        }
      };
    }

    // If transaction is present, need to match the generation and skip entries that has been rollback.
    return new TransactionMessageFilter(transaction) {
      @Override
      public Result apply(Entry entry) {
        if (generation != entry.getGeneration()) {
          return Result.SKIP;
        }
        if (entry instanceof CacheMessageTableEntry && ((CacheMessageTableEntry) entry).isRollback()) {
          return Result.SKIP;
        }
        return super.apply(entry);
      }
    };
  }

  /**
   * A {@link CloseableIterator} of {@link Entry} by combine entries scanned from {@link MessageTable}
   * and from {@link MessageCache}.
   */
  private static final class CombineMessageEntryIterator extends AbstractCloseableIterator<Entry> {

    private final CloseableIterator<Entry> tableIterator;
    private final MessageCache.Scanner<Entry> scanner;
    private final Comparator<Entry> comparator;
    private boolean iterateCache;
    private Entry firstCachedEntry;
    private int count;

    private CombineMessageEntryIterator(CloseableIterator<Entry> tableIterator,
                                        MessageCache.Scanner<Entry> scanner,
                                        Comparator<Entry> comparator,
                                        int limit) {
      this.tableIterator = tableIterator;
      this.scanner = scanner;
      this.comparator = comparator;
      this.firstCachedEntry = scanner.hasNext() ? scanner.next() : null;
      this.count = limit;
    }


    @Override
    protected Entry computeNext() {
      if (count <= 0) {
        return endOfData();
      }
      count--;

      if (iterateCache) {
        return scanner.hasNext() ? scanner.next() : endOfData();
      }

      // If the table iterator is exhausted, doesn't matter what's in the cache, as the table is the source of truth
      if (!tableIterator.hasNext()) {
        return endOfData();
      }

      // If the table iterator return the same entry as the first one in the cache,
      // switch to scan from the cache onward.
      Entry entry = tableIterator.next();
      if (firstCachedEntry != null && comparator.compare(entry, firstCachedEntry) == 0) {
        entry = firstCachedEntry;
        firstCachedEntry = null;
        iterateCache = true;
      }

      return entry;
    }

    @Override
    public void close() {
      try {
        tableIterator.close();
      } finally {
        scanner.close();
      }
    }
  }

  /**
   * An {@link Iterator} of {@link Entry} that memorize the entries that have been iterated on.
   */
  private static final class CopyingIterator extends AbstractIterator<Entry> {

    private final Iterator<? extends Entry> iterator;
    private final Multimap<TopicId, Entry> entries;

    private CopyingIterator(Iterator<? extends Entry> iterator) {
      this.iterator = iterator;
      this.entries = LinkedListMultimap.create();
    }

    @Override
    protected Entry computeNext() {
      if (!iterator.hasNext()) {
        return endOfData();
      }
      Entry entry = iterator.next();
      entries.put(entry.getTopicId(), copyEntry(entry));

      return entry;
    }

    Multimap<TopicId, Entry> getEntries() {
      return entries;
    }

    private Entry copyEntry(Entry other) {
      return new CacheMessageTableEntry(other);
    }
  }

  /**
   * A {@link Entry} implementation used for entries in {@link MessageCache}, which allows
   * altering the transaction write point for rollback purpose of messages that were published transactionally.
   */
  @VisibleForTesting
  static final class CacheMessageTableEntry implements Entry {

    private final boolean lookupOnly;
    private final TopicId topicId;
    private final int generation;
    private final boolean transactional;
    private final byte[] payload;
    private final long publishTimestamp;
    private final short sequenceId;
    private long transactionWritePointer;
    private boolean rollback;

    CacheMessageTableEntry(TopicMetadata topicMetadata, long publishTimestamp, short sequenceId) {
      this.lookupOnly = true;
      this.topicId = topicMetadata.getTopicId();
      this.generation = topicMetadata.getGeneration();
      this.transactional = false;
      this.payload = null;
      this.publishTimestamp = publishTimestamp;
      this.sequenceId = sequenceId;
    }

    CacheMessageTableEntry(Entry other) {
      this.lookupOnly = false;
      this.topicId = other.getTopicId();
      this.generation = other.getGeneration();
      this.transactional = other.isTransactional();
      this.transactionWritePointer = other.getTransactionWritePointer();
      this.payload = other.getPayload();
      this.publishTimestamp = other.getPublishTimestamp();
      this.sequenceId = other.getSequenceId();
    }

    void rollback() {
      if (isTransactional()) {
        rollback = true;
      }
    }

    public boolean isRollback() {
      return rollback;
    }

    @Override
    public TopicId getTopicId() {
      return topicId;
    }

    @Override
    public int getGeneration() {
      return generation;
    }

    @Override
    public boolean isPayloadReference() {
      return getPayload() == null;
    }

    @Override
    public boolean isTransactional() {
      if (lookupOnly) {
        throw new UnsupportedOperationException();
      }
      return transactional;
    }

    @Override
    public long getTransactionWritePointer() {
      if (lookupOnly) {
        throw new UnsupportedOperationException();
      }
      return transactionWritePointer;
    }

    @Nullable
    @Override
    public byte[] getPayload() {
      if (lookupOnly) {
        throw new UnsupportedOperationException();
      }
      return payload;
    }

    @Override
    public long getPublishTimestamp() {
      return publishTimestamp;
    }

    @Override
    public short getSequenceId() {
      return sequenceId;
    }
  }
}
