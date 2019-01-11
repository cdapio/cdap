/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.data2.nosql;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.table.Result;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.SingleThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.StructuredTableTest;
import co.cask.cdap.spi.data.table.StructuredTableSchema;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

/**
 *
 */
public class NoSqlStructuredTableTest extends StructuredTableTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static final DatasetContext CONTEXT1 = DatasetContext.from("ns1");
  private static final NamespaceId NS1 = new NamespaceId(CONTEXT1.getNamespaceId());
  private static final StructuredTableSchema SCHEMA = new StructuredTableSchema(SIMPLE_SPEC);

  private static TransactionManager txManager;
  private static TransactionSystemClient txClient;
  private static NoSqlStructuredTableAdmin noSqlTableAdmin;

  @Override
  protected StructuredTableAdmin getStructuredTableAdmin() throws Exception {
    return noSqlTableAdmin;
  }

  @Override
  protected TransactionRunner getTransactionRunner() throws Exception {
    Transactional txnl = Transactions.createTransactional(
      new SingleThreadDatasetCache(new SystemDatasetInstantiator(dsFrameworkUtil.getFramework()),
                                   new TransactionSystemClientAdapter(txClient),
                                   NS1,
                                   Collections.emptyMap(), null, null));
    return new NoSqlTransactionRunner(getStructuredTableAdmin(), NS1, txnl);
  }

  @BeforeClass
  public static void beforeClass() {
    Configuration txConf = HBaseConfiguration.create();
    txManager = new TransactionManager(txConf);
    txManager.startAndWait();
    txClient = new InMemoryTxSystemClient(txManager);
    noSqlTableAdmin = new NoSqlStructuredTableAdmin(dsFrameworkUtil.getFramework(), NS1);
  }

  @AfterClass
  public static void afterClass() {
    if (txManager != null) {
      txManager.stopAndWait();
    }
  }

  @Test
  public void testScannerIteratorSingle() throws Exception {
    testScannerIterator(1);
  }

  @Test
  public void testScannerIteratorEmpty() throws Exception {
    testScannerIterator(0);
  }

  @Test
  public void testScannerIteratorMulti() throws Exception {
    testScannerIterator(10);
  }

  private void testScannerIterator(int max) throws Exception {
    List<Integer> expected = IntStream.range(0, max).boxed().collect(Collectors.toList());
    MockScanner scanner = new MockScanner(expected.iterator());

    List<Integer> actual = new ArrayList<>();
    try (NoSqlStructuredTable.ScannerIterator closeableIterator =
           new NoSqlStructuredTable.ScannerIterator(scanner, SCHEMA)) {
      while (closeableIterator.hasNext()) {
        actual.add(closeableIterator.next().getInteger("key"));
        Assert.assertFalse(scanner.isClosed());
      }
    }
    Assert.assertTrue(scanner.isClosed());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testLimitIteratorSingle() throws Exception {
    testLimitIterator(1, 0);
    testLimitIterator(1, 1);
    testLimitIterator(1, 2);
  }

  @Test
  public void testLimitIteratorEmpty() throws Exception {
    testLimitIterator(0, 0);
    testLimitIterator(0, 1);
  }

  @Test
  public void testLimitIteratorMulti() throws Exception {
    testLimitIterator(10, 0);
    testLimitIterator(10, 5);
    testLimitIterator(10, 10);
    testLimitIterator(10, 11);
  }

  private void testLimitIterator(int max, int limit) throws Exception {
    List<Integer> expected = IntStream.range(0, max).boxed().collect(Collectors.toList());
    MockScanner scanner = new MockScanner(expected.iterator());

    List<Integer> actual = new ArrayList<>();
    try (NoSqlStructuredTable.LimitIterator closeableIterator =
           new NoSqlStructuredTable.LimitIterator(new NoSqlStructuredTable.ScannerIterator(scanner, SCHEMA), limit)) {
      while (closeableIterator.hasNext()) {
        actual.add(closeableIterator.next().getInteger("key"));
        Assert.assertFalse(scanner.isClosed());
      }
    }
    Assert.assertTrue(scanner.isClosed());
    Assert.assertEquals(expected.subList(0, Math.min(max, limit)), actual);
  }

  private static class MockScanner implements Scanner {
    private final Iterator<Integer> iterator;
    private boolean closed;

    MockScanner(Iterator<Integer> iterator) {
      this.iterator = iterator;
    }

    @Nullable
    @Override
    public Row next() {
      if (iterator.hasNext()) {
        int i = iterator.next();
        return createResult(i, "c" + i, "v" + i);
      }
      return null;
    }

    @Override
    public void close() {
      closed = true;
    }

    public boolean isClosed() {
      return closed;
    }
  }

  private static Result createResult(int row, String col, String val) {
    return new Result(Bytes.toBytes(row), Collections.singletonMap(Bytes.toBytes(col), Bytes.toBytes(val)));
  }
}
