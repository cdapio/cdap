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

import co.cask.cdap.data2.transaction.coprocessor.CacheSupplier;
import co.cask.cdap.data2.util.ReferenceCountedSupplier;
import com.google.common.base.Supplier;
import com.google.common.io.InputSupplier;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.tephra.persist.TransactionVisibilityState;

/**
 * Supplies instances of {@link ConsumerConfigCache} implementations.
 */
public class ConsumerConfigCacheSupplier implements CacheSupplier<ConsumerConfigCache> {
  private final ReferenceCountedSupplier<ConsumerConfigCache> referenceCountedSupplier;
  private final Supplier<ConsumerConfigCache> supplier;

  public ConsumerConfigCacheSupplier(final TableName tableName, final CConfigurationReader cConfReader,
                                     final Supplier<TransactionVisibilityState> transactionSnapshotSupplier,
                                     final InputSupplier<HTableInterface> hTableSupplier) {
    this.supplier = new Supplier<ConsumerConfigCache>() {
      @Override
      public ConsumerConfigCache get() {
        return new ConsumerConfigCache(tableName, cConfReader, transactionSnapshotSupplier, hTableSupplier);
      }
    };
    this.referenceCountedSupplier = new ReferenceCountedSupplier<>(
      String.format("%s-%s:%s", ConsumerConfigCache.class.getSimpleName(), tableName.getNamespaceAsString(),
                    tableName.getNameAsString()));
  }

  @Override
  public ConsumerConfigCache get() {
    return referenceCountedSupplier.getOrCreate(supplier);
  }

  @Override
  public void release() {
    referenceCountedSupplier.release();
  }
}
