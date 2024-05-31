/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.data2.txprune;

import io.cdap.cdap.data2.transaction.coprocessor.hbase10cdh.DefaultTransactionProcessor;
import io.cdap.cdap.data2.transaction.messaging.coprocessor.hbase10cdh.MessageTableRegionObserver;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.tephra.hbase.txprune.HBaseTransactionPruningPlugin;
import org.apache.tephra.txprune.TransactionPruningPlugin;

/**
 * {@link TransactionPruningPlugin} for CDAP Datasets, TMS Message Table, HBase Queues.
 */
public class DefaultHBaseTransactionPruningPlugin extends HBaseTransactionPruningPlugin {

  @Override
  protected boolean isTransactionalTable(HTableDescriptor tableDescriptor) {
    return tableDescriptor.hasCoprocessor(DefaultTransactionProcessor.class.getName())
        || tableDescriptor.hasCoprocessor(MessageTableRegionObserver.class.getName());
  }
}
