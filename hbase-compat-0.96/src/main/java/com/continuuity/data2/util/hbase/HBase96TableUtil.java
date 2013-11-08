package com.continuuity.data2.util.hbase;

import com.continuuity.data2.transaction.coprocessor.hbase96.TransactionDataJanitor;
import com.continuuity.data2.transaction.queue.coprocessor.hbase96.DequeueScanObserver;
import com.continuuity.data2.transaction.queue.coprocessor.hbase96.HBaseQueueRegionObserver;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression;

/**
 *
 */
public class HBase96TableUtil extends HBaseTableUtil {
  @Override
  public void setCompression(HColumnDescriptor columnDescriptor, CompressionType type) {
    switch (type) {
      case LZO:
        columnDescriptor.setCompressionType(Compression.Algorithm.LZO);
        break;
      case SNAPPY:
        columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
        break;
      case GZIP:
        columnDescriptor.setCompressionType(Compression.Algorithm.GZ);
        break;
      case NONE:
        columnDescriptor.setCompressionType(Compression.Algorithm.NONE);
        break;
      default:
        throw new IllegalArgumentException("Unsupported compression type: " + type);
    }
  }

  @Override
  public void setBloomFilter(HColumnDescriptor columnDescriptor, BloomType type) {
    switch (type) {
      case ROW:
        columnDescriptor.setBloomFilterType(org.apache.hadoop.hbase.regionserver.BloomType.ROW);
        break;
      case ROWCOL:
        columnDescriptor.setBloomFilterType(org.apache.hadoop.hbase.regionserver.BloomType.ROWCOL);
        break;
      case NONE:
        columnDescriptor.setBloomFilterType(org.apache.hadoop.hbase.regionserver.BloomType.NONE);
        break;
      default:
        throw new IllegalArgumentException("Unsupported bloom filter type: " + type);
    }
  }

  @Override
  public Class<?> getTransactionDataJanitorClassForVersion() {
    return TransactionDataJanitor.class;
  }

  @Override
  public Class<?> getQueueRegionObserverClassForVersion() {
    return HBaseQueueRegionObserver.class;
  }

  @Override
  public Class<?> getDequeueScanObserverClassForVersion() {
    return DequeueScanObserver.class;
  }
}
