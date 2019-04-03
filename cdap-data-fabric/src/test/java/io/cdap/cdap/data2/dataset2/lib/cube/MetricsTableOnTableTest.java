/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.cube;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTableTest;
import co.cask.cdap.proto.id.DatasetId;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 *
 */
public class MetricsTableOnTableTest extends MetricsTableTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  @Override
  protected MetricsTable getTable(String name) throws Exception {
    DatasetId id = DatasetFrameworkTestUtil.NAMESPACE_ID.dataset(name);
    if (dsFrameworkUtil.getInstance(id) == null) {
      dsFrameworkUtil.createInstance(Table.class.getName(), id, DatasetProperties.EMPTY);
    }
    Table table = dsFrameworkUtil.getInstance(id);
    return new MetricsTableTxnlWrapper(new MetricsTableOnTable(table), table);
  }

  @Override
  public void testConcurrentSwap() throws Exception {
    // Do not test: MetricsTableOnTable is not thread-safe
  }

  @Override
  public void testConcurrentIncrement() throws Exception {
    // Do not test: MetricsTableOnTable is not thread-safe
  }

  private static final class MetricsTableTxnlWrapper implements MetricsTable {
    private final MetricsTable delegate;
    private final TransactionExecutor txnl;

    private MetricsTableTxnlWrapper(MetricsTable delegate, Table table) {
      this.delegate = delegate;
      this.txnl = dsFrameworkUtil.newTransactionExecutor((TransactionAware) table);
    }

    @Nullable
    @Override
    public byte[] get(final byte[] row, final byte[] column) {
      return txnl.executeUnchecked(new Callable<byte[]>() {
        @Override
        public byte[] call() {
          return delegate.get(row, column);
        }
      });
    }

    @Override
    public void put(final SortedMap<byte[], ? extends SortedMap<byte[], Long>> updates) {
      txnl.executeUnchecked(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          delegate.put(updates);
        }
      });
    }

    @Override
    public void putBytes(final SortedMap<byte[], ? extends SortedMap<byte[], byte[]>> updates) {
      txnl.executeUnchecked(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          delegate.putBytes(updates);
        }
      });
    }

    @Override
    public boolean swap(final byte[] row, final byte[] column,
                        final byte[] oldValue, final byte[] newValue) {
      return txnl.executeUnchecked(new Callable<Boolean>() {
        @Override
        public Boolean call() {
          return delegate.swap(row, column, oldValue, newValue);
        }
      });
    }

    @Override
    public void increment(final byte[] row, final Map<byte[], Long> increments) {
      txnl.executeUnchecked(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          delegate.increment(row, increments);
        }
      });
    }

    @Override
    public void increment(final NavigableMap<byte[], NavigableMap<byte[], Long>> updates) {
      txnl.executeUnchecked(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          delegate.increment(updates);
        }
      });
    }

    @Override
    public long incrementAndGet(final byte[] row, final byte[] column, final long delta) {
      return txnl.executeUnchecked(new Callable<Long>() {
        @Override
        public Long call() {
          return delegate.incrementAndGet(row, column, delta);
        }
      });
    }

    @Override
    public void delete(final byte[] row, final byte[][] columns) {
      txnl.executeUnchecked(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          delegate.delete(row, columns);
        }
      });
    }

    @Override
    public Scanner scan(@Nullable final byte[] start, @Nullable final byte[] stop,
                        @Nullable final FuzzyRowFilter filter) {
      return txnl.executeUnchecked(new Callable<Scanner>() {
        @Override
        public Scanner call() {
          return delegate.scan(start, stop, filter);
        }
      });
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
