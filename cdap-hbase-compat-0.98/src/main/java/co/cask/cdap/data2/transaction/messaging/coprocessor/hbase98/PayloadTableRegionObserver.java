/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.CConfigurationReader;
import co.cask.cdap.data2.util.hbase.DefaultScanBuilder;
import co.cask.cdap.data2.util.hbase.HTableNameConverter;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.messaging.TopicMetadataCache;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * RegionObserver for the Payload Table of Transactional Messaging Service.
 *
 * This region observer does row eviction during flush time and compact time by using
 *
 * i) Time-To-Live (TTL) information in MetadataTable to determine if a message has expired its TTL.
 * ii) Generation information in MetadataTable to determine if the message belongs to older generation.
 *
 */
public class PayloadTableRegionObserver extends BaseRegionObserver {

  private static final Log LOG = LogFactory.getLog(PayloadTableRegionObserver.class);

  private int prefixLength;

  private HTableNameConverter nameConverter;
  private String metadataTableNamespace;
  private String hbaseNamespacePrefix;
  private CConfigurationReader cConfReader;
  private TopicMetadataCache topicMetadataCache;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      HTableDescriptor tableDesc = ((RegionCoprocessorEnvironment) env).getRegion().getTableDesc();
      metadataTableNamespace = tableDesc.getValue(Constants.MessagingSystem.HBASE_METADATA_TABLE_NAMESPACE);
      hbaseNamespacePrefix = tableDesc.getValue(Constants.Dataset.TABLE_PREFIX);
      prefixLength = Integer.valueOf(tableDesc.getValue(
        Constants.MessagingSystem.HBASE_MESSAGING_TABLE_PREFIX_NUM_BYTES));

      nameConverter = new HTableNameConverter();
      String sysConfigTablePrefix = nameConverter.getSysConfigTablePrefix(hbaseNamespacePrefix);
      cConfReader = new CConfigurationReader(env.getConfiguration(), sysConfigTablePrefix);
      topicMetadataCache = createTopicMetadataCache((RegionCoprocessorEnvironment) env);
    }
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    if (e instanceof RegionCoprocessorEnvironment) {
      getTopicMetadataCache((RegionCoprocessorEnvironment) e).stop();
    }
  }

  @Override
  public InternalScanner preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                             KeyValueScanner memstoreScanner, InternalScanner s) throws IOException {
    TopicMetadataCache metadataCache = getTopicMetadataCache(c.getEnvironment());
    LOG.info("preFlush, filter using PayloadDataFilter");
    Scan scan = new Scan();
    scan.setFilter(new PayloadDataFilter(c.getEnvironment(), System.currentTimeMillis(), prefixLength, metadataCache));
    return new StoreScanner(store, store.getScanInfo(), scan, Collections.singletonList(memstoreScanner),
                            ScanType.COMPACT_DROP_DELETES, store.getSmallestReadPoint(), HConstants.OLDEST_TIMESTAMP);
  }

  @Override
  public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                               List<? extends KeyValueScanner> scanners, ScanType scanType,
                                               long earliestPutTs, InternalScanner s,
                                               CompactionRequest request) throws IOException {
    TopicMetadataCache metadataCache = getTopicMetadataCache(c.getEnvironment());
    LOG.info("preCompact, filter using PayloadDataFilter");
    Scan scan = new Scan();
    scan.setFilter(new PayloadDataFilter(c.getEnvironment(), System.currentTimeMillis(), prefixLength, metadataCache));
    return new StoreScanner(store, store.getScanInfo(), scan, scanners, scanType, store.getSmallestReadPoint(),
                            earliestPutTs);
  }

  private TopicMetadataCache getTopicMetadataCache(RegionCoprocessorEnvironment env) {
    if (!topicMetadataCache.isAlive()) {
      topicMetadataCache = createTopicMetadataCache(env);
    }
    return topicMetadataCache;
  }

  private TopicMetadataCache createTopicMetadataCache(RegionCoprocessorEnvironment env) {
    return new TopicMetadataCache(env, cConfReader, nameConverter, hbaseNamespacePrefix, metadataTableNamespace,
                                  new DefaultScanBuilder());
  }

  private static final class PayloadDataFilter extends FilterBase {
    private final RegionCoprocessorEnvironment env;
    private final long timestamp;
    private final int prefixLength;
    private final TopicMetadataCache metadataCache;

    private byte[] prevTopicIdBytes;
    private Long currentTTL;
    private Integer currentGen;

    PayloadDataFilter(RegionCoprocessorEnvironment env, long timestamp, int prefixLength,
                      TopicMetadataCache metadataCache) {
      this.env = env;
      this.timestamp = timestamp;
      this.prefixLength = prefixLength;
      this.metadataCache = metadataCache;
    }

    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
      int rowKeyOffset = cell.getRowOffset() + prefixLength;
      int sizeOfRowKey = cell.getRowLength() - prefixLength;
      long writeTimestamp = MessagingUtils.getWriteTimestamp(cell.getRowArray(), rowKeyOffset, sizeOfRowKey);
      int topicIdLength = MessagingUtils.getTopicLengthPayloadEntry(sizeOfRowKey) - Bytes.SIZEOF_INT;
      int generationId = Bytes.toInt(cell.getRowArray(), rowKeyOffset + topicIdLength);


      if (prevTopicIdBytes == null || currentTTL == null || currentGen == null ||
        (!Bytes.equals(prevTopicIdBytes, 0, prevTopicIdBytes.length,
                       cell.getRowArray(), rowKeyOffset, topicIdLength))) {
        prevTopicIdBytes = Arrays.copyOfRange(cell.getRowArray(), rowKeyOffset, rowKeyOffset + topicIdLength);
        Map<String, String> properties = metadataCache.getTopicMetadata(ByteBuffer.wrap(prevTopicIdBytes));
        if (properties == null) {
          LOG.debug("Region " + env.getRegion().getRegionNameAsString() + ", could not get properties of topicId "
                      + MessagingUtils.toTopicId(prevTopicIdBytes));
          return ReturnCode.INCLUDE;
        }
        currentTTL = Long.parseLong(properties.get(MessagingUtils.Constants.TTL_KEY));
        currentGen = Integer.parseInt(properties.get(MessagingUtils.Constants.GENERATION_KEY));
      }

      // Old Generation (or deleted current generation) cleanup
      if (MessagingUtils.isOlderGeneration(generationId, currentGen)) {
        return ReturnCode.SKIP;
      }

      // TTL expiration cleanup only if the generation of metadata and row key are the same
      if ((generationId == currentGen) && ((timestamp - writeTimestamp) > currentTTL)) {
        return ReturnCode.SKIP;
      }
      return ReturnCode.INCLUDE;
    }
  }
}
