/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.spi.metadata.dataset;

import com.google.inject.Guice;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.table.Delete;
import io.cdap.cdap.api.dataset.table.Get;
import io.cdap.cdap.api.dataset.table.Increment;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scan;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableAdmin;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableDefinition;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import io.cdap.cdap.data2.metadata.dataset.MetadataDataset;
import io.cdap.cdap.data2.metadata.dataset.SearchRequest;
import io.cdap.cdap.data2.metadata.dataset.SortInfo;
import io.cdap.cdap.proto.EntityScope;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.metadata.MetadataSearchResponse;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.inmemory.MinimalTxSystemClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SearchHelper} dataset instantiation error propagation and quiet close.
 */
public class SearchHelperTest {

  @After
  public void tearDown() {
    InMemoryTableService.reset();
  }

  @Test
  public void testSearchAndQuietCloseWhenUnderlyingDatasetCloseThrowsIoException()
      throws Exception {
    CConfiguration cConf = CConfiguration.create();
    AtomicInteger closeCalls = new AtomicInteger();

    InMemoryTableDefinition delegateDef = new InMemoryTableDefinition(Constants.Dataset.TABLE_TYPE) {
      @Override
      public Table getDataset(DatasetContext datasetContext, DatasetSpecification spec,
          Map<String, String> arguments, ClassLoader classLoader) {
        Table base = super.getDataset(datasetContext, spec, arguments, classLoader);
        return new CloseThrowingTable(base, closeCalls);
      }
    };
    Guice.createInjector(new ConfigModule(cConf)).injectMembers(delegateDef);

    SearchHelper searchHelper = new SearchHelper(new MinimalTxSystemClient(), delegateDef);
    searchHelper.createDatasets();

    MetadataEntity entity = NamespaceId.DEFAULT.dataset("orders").toMetadataEntity();
    searchHelper.execute(context -> {
      MetadataDataset userMds = context.getDataset(MetadataScope.USER);
      MetadataDataset sysMds = context.getDataset(MetadataScope.SYSTEM);
      userMds.addProperty(entity, "owner", "alice");
      userMds.addTags(entity, Collections.singleton("pii"));
      sysMds.addProperty(entity, "schema", "v1");
      sysMds.addTags(entity, Collections.singleton("table"));
      return null;
    });
    // Both USER and SYSTEM datasets were closed cleanly despite CloseThrowingTable throwing IOException
    Assert.assertEquals(2, closeCalls.get());

    SearchRequest request = new SearchRequest(
        NamespaceId.DEFAULT, "pii", Collections.emptySet(), SortInfo.DEFAULT,
        0, 10, 0, null, false, EnumSet.of(EntityScope.USER));
    MetadataSearchResponse response = searchHelper.search(request, null);
    Assert.assertEquals(1, response.getTotal());
    Assert.assertEquals(entity, response.getResults().iterator().next().getMetadataEntity());
    Assert.assertTrue(closeCalls.get() >= 4);

    searchHelper.dropDatasets();
  }

  @Test
  public void testMetaOnlyDatasetContextPropagatesGetDatasetIoException() {
    CConfiguration cConf = CConfiguration.create();
    final IOException expectedIo = new IOException("table open failed");

    DatasetDefinition<Table, InMemoryTableAdmin> failingTableDef =
        new InMemoryTableDefinition(Constants.Dataset.TABLE_TYPE) {
          @Override
          public Table getDataset(DatasetContext datasetContext, DatasetSpecification spec,
              Map<String, String> arguments, ClassLoader classLoader) {
            throwSneaky(expectedIo);
            return null;
          }
        };
    Guice.createInjector(new ConfigModule(cConf)).injectMembers(failingTableDef);

    SearchHelper searchHelper = new SearchHelper(new MinimalTxSystemClient(), failingTableDef);
    try {
      searchHelper.execute(context -> context.getDataset(MetadataScope.SYSTEM));
      Assert.fail("Expected RuntimeException wrapping TransactionFailureException -> IOException");
    } catch (RuntimeException actual) {
      Throwable current = actual;
      boolean foundExpectedIo = false;
      while (current != null) {
        if (current == expectedIo) {
          foundExpectedIo = true;
          break;
        }
        current = current.getCause();
      }
      Assert.assertTrue("Expected cause chain to contain exact IOException instance",
          foundExpectedIo);
    }
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void throwSneaky(Throwable t) throws E {
    throw (E) t;
  }

  private static final class CloseThrowingTable implements Table, TransactionAware {
    private final Table delegate;
    private final TransactionAware txDelegate;
    private final AtomicInteger closeCalls;

    CloseThrowingTable(Table delegate, AtomicInteger closeCalls) {
      this.delegate = delegate;
      this.txDelegate = (TransactionAware) delegate;
      this.closeCalls = closeCalls;
    }

    @Override
    public void close() throws IOException {
      closeCalls.incrementAndGet();
      delegate.close();
      throw new IOException("simulated close failure");
    }

    @Override
    public Row get(byte[] row) {
      return delegate.get(row);
    }

    @Override
    public byte[] get(byte[] row, byte[] column) {
      return delegate.get(row, column);
    }

    @Override
    public Row get(byte[] row, byte[][] columns) {
      return delegate.get(row, columns);
    }

    @Override
    public Row get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit) {
      return delegate.get(row, startColumn, stopColumn, limit);
    }

    @Override
    public Row get(Get get) {
      return delegate.get(get);
    }

    @Override
    public List<Row> get(List<Get> gets) {
      return delegate.get(gets);
    }

    @Override
    public void put(byte[] row, byte[] column, byte[] value) {
      delegate.put(row, column, value);
    }

    @Override
    public void put(byte[] row, byte[][] columns, byte[][] values) {
      delegate.put(row, columns, values);
    }

    @Override
    public void put(Put put) {
      delegate.put(put);
    }

    @Override
    public void delete(byte[] row) {
      delegate.delete(row);
    }

    @Override
    public void delete(byte[] row, byte[] column) {
      delegate.delete(row, column);
    }

    @Override
    public void delete(byte[] row, byte[][] columns) {
      delegate.delete(row, columns);
    }

    @Override
    public void delete(Delete delete) {
      delegate.delete(delete);
    }

    @Override
    public Row incrementAndGet(byte[] row, byte[][] columns, long[] amounts) {
      return delegate.incrementAndGet(row, columns, amounts);
    }

    @Override
    public long incrementAndGet(byte[] row, byte[] column, long amount) {
      return delegate.incrementAndGet(row, column, amount);
    }

    @Override
    public Row incrementAndGet(Increment increment) {
      return delegate.incrementAndGet(increment);
    }

    @Override
    public void increment(byte[] row, byte[][] columns, long[] amounts) {
      delegate.increment(row, columns, amounts);
    }

    @Override
    public void increment(byte[] row, byte[] column, long amount) {
      delegate.increment(row, column, amount);
    }

    @Override
    public void increment(Increment increment) {
      delegate.increment(increment);
    }

    @Override
    public boolean compareAndSwap(byte[] row, byte[] column, byte[] oldValue, byte[] newValue) {
      return delegate.compareAndSwap(row, column, oldValue, newValue);
    }

    @Override
    public Scanner scan(byte[] startRow, byte[] stopRow) {
      return delegate.scan(startRow, stopRow);
    }

    @Override
    public Scanner scan(Scan scan) {
      return delegate.scan(scan);
    }

    @Override
    public Type getRecordType() {
      return delegate.getRecordType();
    }

    @Override
    public List<io.cdap.cdap.api.data.batch.Split> getSplits() {
      return delegate.getSplits();
    }

    @Override
    public List<io.cdap.cdap.api.data.batch.Split> getSplits(
        int numSplits, byte[] start, byte[] stop) {
      return delegate.getSplits(numSplits, start, stop);
    }

    @Override
    public io.cdap.cdap.api.data.batch.RecordScanner<io.cdap.cdap.api.data.format.StructuredRecord>
        createSplitRecordScanner(io.cdap.cdap.api.data.batch.Split split) {
      return delegate.createSplitRecordScanner(split);
    }

    @Override
    public io.cdap.cdap.api.data.batch.SplitReader<byte[], Row> createSplitReader(
        io.cdap.cdap.api.data.batch.Split split) {
      return delegate.createSplitReader(split);
    }

    @Override
    public void write(io.cdap.cdap.api.data.format.StructuredRecord record) throws IOException {
      delegate.write(record);
    }

    @Override
    public void write(byte[] key, Put put) {
      delegate.write(key, put);
    }

    @Override
    public void startTx(Transaction tx) {
      txDelegate.startTx(tx);
    }

    @Override
    public void updateTx(Transaction tx) {
      txDelegate.updateTx(tx);
    }

    @Override
    public Collection<byte[]> getTxChanges() {
      return txDelegate.getTxChanges();
    }

    @Override
    public boolean commitTx() throws Exception {
      return txDelegate.commitTx();
    }

    @Override
    public void postTxCommit() {
      txDelegate.postTxCommit();
    }

    @Override
    public boolean rollbackTx() throws Exception {
      return txDelegate.rollbackTx();
    }

    @Override
    public String getTransactionAwareName() {
      return txDelegate.getTransactionAwareName();
    }
  }
}
