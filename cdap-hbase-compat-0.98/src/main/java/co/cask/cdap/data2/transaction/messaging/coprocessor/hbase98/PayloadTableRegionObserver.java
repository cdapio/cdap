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

package co.cask.cdap.data2.transaction.messaging.coprocessor.hbase98;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HTable98NameConverter;
import co.cask.cdap.data2.util.hbase.HTableNameConverter;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.store.ImmutablePayloadTableEntry;
import co.cask.cdap.messaging.store.hbase.HBaseMetadataTable;
import co.cask.cdap.messaging.store.hbase.HBaseTableFactory;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * RegionObserver for the Payload Table of Transactional Messaging Service.
 *
 * This region observer does row eviction during flush time and compact time by using Time-To-Live (TTL) information
 * in MetadataTable to determine if a message has expired its TTL.
 *
 * TODO: Add logic to remove data corresponding to invalidated transactions
 */
public class PayloadTableRegionObserver extends BaseRegionObserver {

  private static final Log LOG = LogFactory.getLog(PayloadTableRegionObserver.class);
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<SortedMap<String, String>>() { }.getType();

  private static final byte[] COL_FAMILY = HBaseTableFactory.COLUMN_FAMILY;
  private static final byte[] COL = HBaseMetadataTable.COL;

  private int prefixLength;
  private LoadingCache<TopicId, TopicMetadata> topicCache;
  private HTableInterface metadataTable;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      HTableDescriptor tableDesc = ((RegionCoprocessorEnvironment) env).getRegion().getTableDesc();
      prefixLength = Integer.parseInt(tableDesc.getValue(Constants.MessagingSystem.TABLE_PREFIX_BYTES));
      String metadataTableNamespace = tableDesc.getValue(Constants.MessagingSystem.HBASE_METADATA_TABLE_NAMESPACE);
      String metadataTableName = tableDesc.getValue(Constants.MessagingSystem.HBASE_METADATA_TABLE_NAME);
      String hbaseNamespacePrefix = tableDesc.getValue(Constants.Dataset.TABLE_PREFIX);
      HTableNameConverter nameConverter = new HTable98NameConverter();
      metadataTable = env.getTable(nameConverter.toTableName(hbaseNamespacePrefix,
                                                             TableId.from(metadataTableNamespace, metadataTableName)));
      topicCache = CacheBuilder.newBuilder()
        .expireAfterWrite(2, TimeUnit.MINUTES)
        .maximumSize(1000)
        .build(new CacheLoader<TopicId, TopicMetadata>() {

          @Override
          public TopicMetadata load(TopicId topicId) throws Exception {
            Get get = new Get(MessagingUtils.toRowKeyPrefix(topicId));
            Result result = metadataTable.get(get);
            byte[] properties = result.getValue(COL_FAMILY, COL);
            Map<String, String> propertyMap = GSON.fromJson(Bytes.toString(properties), MAP_TYPE);
            return new TopicMetadata(topicId, propertyMap);
          }
        });
    }
  }

  @Override
  public InternalScanner preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                             KeyValueScanner memstoreScanner, InternalScanner s) throws IOException {
    LOG.info("preFlush, filter using TTLFilter");
    Scan scan = new Scan();
    scan.setFilter(new TTLFilter(c.getEnvironment(), System.currentTimeMillis()));
    return new StoreScanner(store, store.getScanInfo(), scan, Collections.singletonList(memstoreScanner),
                            ScanType.COMPACT_DROP_DELETES, store.getSmallestReadPoint(), HConstants.OLDEST_TIMESTAMP);
  }

  @Override
  public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                               List<? extends KeyValueScanner> scanners, ScanType scanType,
                                               long earliestPutTs, InternalScanner s,
                                               CompactionRequest request) throws IOException {
    LOG.info("preCompact, filter using TTLFilter");
    Scan scan = new Scan();
    scan.setFilter(new TTLFilter(c.getEnvironment(), System.currentTimeMillis()));
    return new StoreScanner(store, store.getScanInfo(), scan, scanners, scanType, store.getSmallestReadPoint(),
                            earliestPutTs);
  }

  private final class TTLFilter extends FilterBase {
    private final RegionCoprocessorEnvironment env;
    private final long timestamp;

    TTLFilter(RegionCoprocessorEnvironment env, long timestamp) {
      this.env = env;
      this.timestamp = timestamp;
    }

    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
      byte[] rowKey = Arrays.copyOfRange(cell.getRow(), prefixLength, cell.getRowLength());
      ImmutablePayloadTableEntry entry = new ImmutablePayloadTableEntry(rowKey, null);
      try {
        TopicMetadata metadata = topicCache.get(entry.getTopicId());
        if ((timestamp - entry.getPayloadWriteTimestamp()) > TimeUnit.SECONDS.toMillis(metadata.getTTL())) {
          return ReturnCode.SKIP;
        }
      } catch (ExecutionException ex) {
        LOG.debug("Region " + env.getRegion().getRegionNameAsString() + ", exception while" +
                    "trying to fetch properties of topicId" + entry.getTopicId(), ex);
      }
      return ReturnCode.INCLUDE;
    }
  }
}
