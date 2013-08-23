/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.engine.leveldb.KeyValue;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 *
 */
public final class LevelDBQueueClientFactory implements QueueClientFactory {

  private static final String DB_FILE_PREFIX = "ldb_";

  private static final LevelDBQueueClientFactory INSTANCE = new LevelDBQueueClientFactory();

  public static LevelDBQueueClientFactory getInstance() {
    return INSTANCE;
  }

  @Inject
  @Named("LevelDBOVCTableHandleBasePath")
  private String basePath;

  @Inject
  @Named("LevelDBOVCTableHandleBlockSize")
  private Integer blockSize;

  @Inject
  @Named("LevelDBOVCTableHandleCacheSize")
  private Long cacheSize;

  @Inject
  @Named("DataFabricOperationExecutorConfig")
  private CConfiguration cConf;

  private final LoadingCache<String, DB> dbCache;

  private LevelDBQueueClientFactory() {
    dbCache = CacheBuilder.newBuilder().build(new CacheLoader<String, DB>() {
      @Override
      public DB load(String key) throws Exception {
        try {
          return JniDBFactory.factory.open(generateDBPath(key), generateDBOptions(false, false));
        } catch (Exception e) {
          // Table not exists, try creating it
          return JniDBFactory.factory.open(generateDBPath(key), generateDBOptions(true, false));
        }
      }
    });
  }

  @Override
  public Queue2Producer createProducer(QueueName queueName) throws IOException {
    return createProducer(queueName, QueueMetrics.NOOP_QUEUE_METRICS);
  }

  @Override
  public Queue2Consumer createConsumer(QueueName queueName,
                                       ConsumerConfig consumerConfig, int numGroups) throws IOException {
    return new LevelDBQueue2Consumer(dbCache.getUnchecked(cConf.get(QueueConstants.ConfigKeys.QUEUE_TABLE_NAME)),
                                     queueName, consumerConfig);
  }

  @Override
  public Queue2Producer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
    return new LevelDBQueue2Producer(dbCache.getUnchecked(cConf.get(QueueConstants.ConfigKeys.QUEUE_TABLE_NAME)),
                                     queueName, queueMetrics);

  }

  private File generateDBPath(String tableName) throws UnsupportedEncodingException {
    return new File(basePath, DB_FILE_PREFIX + URLEncoder.encode(tableName, "UTF-8"));
  }

  private Options generateDBOptions(boolean createIfMissing, boolean errorIfExists) {
    Options options = new Options();
    options.createIfMissing(createIfMissing);
    options.errorIfExists(errorIfExists);
    options.comparator(new KeyValueDBComparator());
    options.blockSize(blockSize);
    options.cacheSize(cacheSize);
    return options;
  }

  /**
   * A comparator for the keys of {@link KeyValue} pairs.
   */
  public static class KeyValueDBComparator implements DBComparator {

    @Override
    public int compare(byte[] left, byte[] right) {
      return KeyValue.KEY_COMPARATOR.compare(left, right);
    }

    @Override
    public byte[] findShortSuccessor(byte[] key) {
      return key;
    }

    @Override
    public byte[] findShortestSeparator(byte[] start, byte[] limit) {
      return start;
    }

    @Override
    public String name() {
      return "ldb-kv";
    }
  }
}
