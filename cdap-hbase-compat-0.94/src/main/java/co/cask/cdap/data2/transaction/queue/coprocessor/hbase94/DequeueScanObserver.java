/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.queue.coprocessor.hbase94;

import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.transaction.queue.hbase.DequeueScanAttributes;
import co.cask.tephra.Transaction;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;

/**
 *
 */
public class DequeueScanObserver extends BaseRegionObserver {
  @Override
  public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
    throws IOException {
    ConsumerConfig consumerConfig = DequeueScanAttributes.getConsumerConfig(scan);
    Transaction tx = DequeueScanAttributes.getTx(scan);

    if (consumerConfig == null || tx == null) {
      return super.preScannerOpen(e, scan, s);
    }

    Filter dequeueFilter = new DequeueFilter(consumerConfig, tx);

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
