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

package co.cask.cdap.data2.transaction.messaging.coprocessor.hbase11;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.transaction.coprocessor.DefaultTransactionStateCacheSupplier;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.CConfigurationReader;
import co.cask.cdap.data2.util.hbase.HTable11NameConverter;
import co.cask.cdap.data2.util.hbase.HTableNameConverter;
import co.cask.cdap.messaging.MessagingUtils;
import com.google.common.base.Supplier;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.coprocessor.TransactionStateCache;
import org.apache.tephra.persist.TransactionVisibilityState;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/**
 * RegionObserver for the Message Table of Transactional Messaging Service.
 *
 * This region observer does row eviction during flush time and compact time by using
 *
 * i) Time-To-Live (TTL) information in MetadataTable to determine if a message has expired its TTL.
 * ii) Generation information in MetadataTable to determine if the message belongs to older generation.
 * iii) Mark data entries with invalidated transactions (loaded from tx.snapshot) so that invalid list can be pruned.
 *
 */
public class MessageTableRegionObserver extends BaseRegionObserver {

  private static final Log LOG = LogFactory.getLog(MessageTableRegionObserver.class);
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private static final byte[] COL_FAMILY = MessagingUtils.Constants.COLUMN_FAMILY;
  private static final byte[] COL = MessagingUtils.Constants.METADATA_COLUMN;

  private int prefixLength;
  private LoadingCache<ByteBuffer, Map<String, String>> topicCache;
  private TransactionStateCache txStateCache;

  private String metadataTableNamespace;
  private String hbaseNamespacePrefix;
  private CConfigurationReader cConfReader;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      HTableDescriptor tableDesc = ((RegionCoprocessorEnvironment) env).getRegion().getTableDesc();
      metadataTableNamespace = tableDesc.getValue(Constants.MessagingSystem.HBASE_METADATA_TABLE_NAMESPACE);
      hbaseNamespacePrefix = tableDesc.getValue(Constants.Dataset.TABLE_PREFIX);
      prefixLength = Integer.valueOf(tableDesc.getValue(
        Constants.MessagingSystem.HBASE_MESSAGING_TABLE_PREFIX_NUM_BYTES));

      HTableNameConverter nameConverter = new HTable11NameConverter();
      String sysConfigTablePrefix = nameConverter.getSysConfigTablePrefix(hbaseNamespacePrefix);
      cConfReader = new CConfigurationReader(env.getConfiguration(), sysConfigTablePrefix);

      Supplier<TransactionStateCache> cacheSupplier = getTransactionStateCacheSupplier(hbaseNamespacePrefix,
                                                                                       env.getConfiguration());
      txStateCache = cacheSupplier.get();
    }
  }

  @Nullable
  private synchronized LoadingCache<ByteBuffer, Map<String, String>> getTopicCache(RegionCoprocessorEnvironment env)
    throws IOException {
    throw new IOException("Intentionally not returning a topic cache");
//    if (topicCache != null) {
//      return topicCache;
//    }
//
//    CConfiguration cConf = cConfReader.read();
//    if (cConf == null) {
//      throw new IOException("cConf was null.");
//    }
//
//    String metadataTableName = cConf.get(Constants.MessagingSystem.METADATA_TABLE_NAME);
//    HTableNameConverter nameConverter = new HTable11NameConverter();
//    final HTableInterface metadataTable = env.getTable(nameConverter.toTableName(
//      hbaseNamespacePrefix, TableId.from(metadataTableNamespace, metadataTableName)));
//
//    final long metadataCacheExpiry = cConf.getLong(
//      Constants.MessagingSystem.COPROCESSOR_METADATA_CACHE_EXPIRATION_SECONDS);
//
//    topicCache = CacheBuilder.newBuilder()
//      .expireAfterWrite(metadataCacheExpiry, TimeUnit.SECONDS)
//      .maximumSize(1000)
//      .build(new CacheLoader<ByteBuffer, Map<String, String>>() {
//
//        @Override
//        public Map<String, String> load(ByteBuffer topicBytes) throws Exception {
//          byte[] getBytes = topicBytes.array().length == topicBytes.remaining() ?
//            topicBytes.array() : Bytes.toBytes(topicBytes);
//          Get get = new Get(getBytes);
//          Result result = metadataTable.get(get);
//          byte[] properties = result.getValue(COL_FAMILY, COL);
//          Map<String, String> propertyMap = GSON.fromJson(Bytes.toString(properties), MAP_TYPE);
//          String ttl = propertyMap.get(MessagingUtils.Constants.TTL_KEY);
//          long ttlInMs = TimeUnit.SECONDS.toMillis(Long.parseLong(ttl));
//          propertyMap.put(MessagingUtils.Constants.TTL_KEY, Long.toString(ttlInMs));
//          return propertyMap;
//        }
//      });
//    return topicCache;
  }

  @Override
  public InternalScanner preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                             KeyValueScanner memstoreScanner, InternalScanner s) throws IOException {
    if (!c.getEnvironment().getRegion().isAvailable()) {
      return super.preFlushScannerOpen(c, store, memstoreScanner, s);
    }

    LoadingCache<ByteBuffer, Map<String, String>> cache;
    try {
      cache = getTopicCache(c.getEnvironment());
    } catch (IOException ex) {
      LOG.warn("preFlush, could not create topicCache. using default scanner. " + ex.getMessage());
      LOG.debug("StackTrace: ", ex);
      return super.preFlushScannerOpen(c, store, memstoreScanner, s);
    }

    LOG.info("preFlush, filter using MessageDataFilter");
    TransactionVisibilityState txVisibilityState = txStateCache.getLatestState();
    Scan scan = new Scan();
    scan.setFilter(new MessageDataFilter(c.getEnvironment(), System.currentTimeMillis(),
                                         prefixLength, cache, txVisibilityState));
    return new LoggingInternalScanner("MessageDataFilter", "preFlush",
                                      new StoreScanner(store, store.getScanInfo(), scan,
                                                       Collections.singletonList(memstoreScanner),
                                                       ScanType.COMPACT_DROP_DELETES, store.getSmallestReadPoint(),
                                                       HConstants.OLDEST_TIMESTAMP), txVisibilityState);
  }

  @Override
  public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                               List<? extends KeyValueScanner> scanners, ScanType scanType,
                                               long earliestPutTs, InternalScanner s,
                                               CompactionRequest request) throws IOException {
    if (!c.getEnvironment().getRegion().isAvailable()) {
      return super.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s, request);
    }

    LoadingCache<ByteBuffer, Map<String, String>> cache;
    try {
      cache = getTopicCache(c.getEnvironment());
    } catch (IOException ex) {
      LOG.warn("preCompact, could not create topicCache. using default scanner. " + ex.getMessage());
      LOG.debug("StackTrace: ", ex);
      return super.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s, request);
    }

    LOG.info("preCompact, filter using MessageDataFilter");
    TransactionVisibilityState txVisibilityState = txStateCache.getLatestState();
    Scan scan = new Scan();
    scan.setFilter(new MessageDataFilter(c.getEnvironment(), System.currentTimeMillis(),
                   prefixLength, cache, txVisibilityState));
    return new LoggingInternalScanner("MessageDataFilter", "preCompact",
                                      new StoreScanner(store, store.getScanInfo(), scan, scanners, scanType,
                                                       store.getSmallestReadPoint(), earliestPutTs), txVisibilityState);
  }

  private Supplier<TransactionStateCache> getTransactionStateCacheSupplier(String tablePrefix, Configuration conf) {
    String sysConfigTablePrefix = new HTable11NameConverter().getSysConfigTablePrefix(tablePrefix);
    return new DefaultTransactionStateCacheSupplier(sysConfigTablePrefix, conf);
  }

  private static final class LoggingInternalScanner implements InternalScanner {
    private static final Log LOG = LogFactory.getLog(LoggingInternalScanner.class);

    private final String filterName;
    private final String operation;
    private final InternalScanner delegate;
    private final TransactionVisibilityState state;

    LoggingInternalScanner(String filterName, String operation, InternalScanner delegate,
                           @Nullable TransactionVisibilityState state) {
      this.filterName = filterName;
      this.operation = operation;
      this.delegate = delegate;
      this.state = state;
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
      return delegate.next(results);
    }

    @Override
    public boolean next(List<Cell> list, ScannerContext scannerContext) throws IOException {
      return delegate.next(list, scannerContext);
    }

    @Override
    public void close() throws IOException {
      if (state != null) {
        LOG.info("During " + operation + ", Filter " + filterName + " completed using tx.snapshot of timestamp "
                   + state.getTimestamp() + " which had the visibility bound of " + state.getVisibilityUpperBound());
      }
      delegate.close();
    }
  }

  private static final class MessageDataFilter extends FilterBase {
    private final RegionCoprocessorEnvironment env;
    private final long timestamp;
    private final TransactionVisibilityState state;
    private final int prefixLength;
    private final LoadingCache<ByteBuffer, Map<String, String>> topicCache;

    private byte[] prevTopicIdBytes;
    private Long currentTTL;
    private Integer currentGen;

    MessageDataFilter(RegionCoprocessorEnvironment env, long timestamp, int prefixLength, LoadingCache<ByteBuffer,
      Map<String, String>> topicCache, @Nullable TransactionVisibilityState state) {
      this.env = env;
      this.timestamp = timestamp;
      this.prefixLength = prefixLength;
      this.topicCache = topicCache;
      this.state = state;
    }

    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
      int rowKeyOffset = cell.getRowOffset() + prefixLength;
      int sizeOfRowKey = cell.getRowLength() - prefixLength;
      long publishTimestamp = MessagingUtils.getPublishTimestamp(cell.getRowArray(), rowKeyOffset, sizeOfRowKey);
      int topicIdLength = MessagingUtils.getTopicLengthMessageEntry(sizeOfRowKey) - Bytes.SIZEOF_INT;
      int generationId = Bytes.toInt(cell.getRowArray(), rowKeyOffset + topicIdLength);

      try {
        if (prevTopicIdBytes == null || currentTTL == null || currentGen == null ||
          (!Bytes.equals(prevTopicIdBytes, 0, prevTopicIdBytes.length,
                         cell.getRowArray(), rowKeyOffset, topicIdLength))) {
          prevTopicIdBytes = Arrays.copyOfRange(cell.getRowArray(), rowKeyOffset, rowKeyOffset + topicIdLength);
          Map<String, String> properties = topicCache.get(ByteBuffer.wrap(prevTopicIdBytes));
          currentTTL = Long.parseLong(properties.get(MessagingUtils.Constants.TTL_KEY));
          currentGen = Integer.parseInt(properties.get(MessagingUtils.Constants.GENERATION_KEY));
        }

        // Old Generation (or deleted current generation) cleanup
        if (MessagingUtils.isOlderGeneration(generationId, currentGen)) {
          return ReturnCode.SKIP;
        }

        // TTL expiration cleanup only if the generation of metadata and row key are the same
        if ((generationId == currentGen) && ((timestamp - publishTimestamp) > currentTTL)) {
          return ReturnCode.SKIP;
        }
      } catch (ExecutionException ex) {
        LOG.info("Region " + env.getRegionInfo().getRegionNameAsString() + ", exception while" +
                   "trying to fetch properties of topicId " + MessagingUtils.toTopicId(prevTopicIdBytes)
                   + "\n" + ex.getMessage());
        LOG.debug("StackTrace: ", ex);
      }
      return ReturnCode.INCLUDE;
    }

    @Override
    public Cell transformCell(Cell cell) throws IOException {
      // Rollback expired/invalidated transactions
      byte[] txCol = MessagingUtils.Constants.TX_COL;
      if (Bytes.equals(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                       txCol, 0, txCol.length)) {
        // This is a TX COL
        long txWritePtr = Bytes.toLong(cell.getValueArray(), cell.getValueOffset());
        if (txWritePtr > 0 && state != null && state.getInvalid().contains(txWritePtr)) {
          // tx is invalid and it hasn't been rolled back.
          byte[] backingArray = cell.getRowArray();
          Bytes.putLong(backingArray, cell.getValueOffset(), -1 * txWritePtr);
          return cell;
        }
      }
      return super.transformCell(cell);
    }
  }
}
