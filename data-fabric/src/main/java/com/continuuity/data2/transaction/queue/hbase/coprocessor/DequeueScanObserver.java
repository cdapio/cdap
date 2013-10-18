package com.continuuity.data2.transaction.queue.hbase.coprocessor;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.queue.QueueEntryRow;
import com.continuuity.data2.transaction.queue.hbase.DequeueScanAttributes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 *
 */
public class DequeueScanObserver extends BaseRegionObserver {
  @Override
  public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
    throws IOException {
    ConsumerConfig consumerConfig = DequeueScanAttributes.getConsumerConfig(scan);
    Transaction tx = DequeueScanAttributes.getTx(scan);
    byte[] queueName = DequeueScanAttributes.getQueueName(scan);

    if (consumerConfig == null || tx == null || queueName == null) {
      return super.preScannerOpen(e, scan, s);
    }

    Filter dequeueFilter = new DequeueFilter(queueName, consumerConfig, tx);

    Filter existing = scan.getFilter();
    if (existing != null) {
      Filter combined = new FilterList(FilterList.Operator.MUST_PASS_ALL, existing, dequeueFilter);
      scan.setFilter(combined);
    } else {
      scan.setFilter(dequeueFilter);
    }

    return super.preScannerOpen(e, scan, s);
  }
}
