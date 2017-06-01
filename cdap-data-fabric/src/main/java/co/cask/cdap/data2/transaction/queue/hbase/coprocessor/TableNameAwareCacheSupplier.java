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

package co.cask.cdap.data2.transaction.queue.hbase.coprocessor;

import co.cask.cdap.data2.util.ReferenceCountedSupplier;
import com.google.common.base.Supplier;
import com.google.common.io.InputSupplier;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.tephra.persist.TransactionVisibilityState;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides {@link ReferenceCountedSupplier} of {@link ConsumerConfigCache} for a given {@link TableName}.
 */
public class TableNameAwareCacheSupplier {
  private static final ConcurrentMap<TableName, ConsumerConfigCacheSupplier> SUPPLIER_MAP =
    new ConcurrentHashMap<>();

  private TableNameAwareCacheSupplier() {
    // no default constructor
  }

  public static ConsumerConfigCacheSupplier getSupplier(TableName tableName, CConfigurationReader cConfReader,
                                                        Supplier<TransactionVisibilityState> snapshotSupplier,
                                                        InputSupplier<HTableInterface> hTableSuplpier) {
    ConsumerConfigCacheSupplier supplier = SUPPLIER_MAP.get(tableName);
    if (supplier == null) {
      supplier = new ConsumerConfigCacheSupplier(tableName, cConfReader, snapshotSupplier, hTableSuplpier);
      if (SUPPLIER_MAP.putIfAbsent(tableName, supplier) != null) {
        // discard our instance and re-retrieve, since someone else has set it
        supplier = SUPPLIER_MAP.get(tableName);
      }
    }
    return supplier;
  }
}
