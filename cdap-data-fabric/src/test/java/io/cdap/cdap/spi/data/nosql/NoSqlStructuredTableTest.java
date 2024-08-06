/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.spi.data.nosql;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.table.Result;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import io.cdap.cdap.data2.dataset2.lib.table.MDSKey;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.StructuredTableTest;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Unit test for nosql structured table.
 */
public class NoSqlStructuredTableTest extends StructuredTableTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static final StructuredTableSchema SCHEMA = new StructuredTableSchema(SIMPLE_SPEC);

  private static TransactionManager txManager;
  private static StructuredTableAdmin noSqlTableAdmin;
  private static TransactionRunner transactionRunner;

  @Override
  protected StructuredTableAdmin getStructuredTableAdmin() {
    return noSqlTableAdmin;
  }

  @Override
  protected TransactionRunner getTransactionRunner() {
    return transactionRunner;
  }

  @BeforeClass
  public static void beforeClass() throws IOException {
    Configuration txConf = new Configuration();
    txManager = new TransactionManager(txConf);
    txManager.startAndWait();

    CConfiguration cConf = dsFrameworkUtil.getConfiguration();
    cConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_NOSQL);
    noSqlTableAdmin = dsFrameworkUtil.getInjector().getInstance(StructuredTableAdmin.class);
    transactionRunner = dsFrameworkUtil.getInjector().getInstance(TransactionRunner.class);
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

  @Override
  @Ignore
  public void testSortedPrimaryKeyFilteredIndexScan() throws Exception {
    // no implementation
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
    try (NoSqlStructuredTable.LimitIterator closeableIterator = new NoSqlStructuredTable.LimitIterator(
      Collections.singleton(new NoSqlStructuredTable.ScannerIterator(scanner, SCHEMA)).iterator(), limit)) {
      while (closeableIterator.hasNext()) {
        actual.add(closeableIterator.next().getInteger("key"));
        Assert.assertFalse(scanner.isClosed());
      }
    }
    Assert.assertTrue(scanner.isClosed());
    Assert.assertEquals(expected.subList(0, Math.min(max, limit)), actual);
  }

  @Test
  public void testFilterByIndexIteratorSingleMatch() {
    List<Integer> expected = IntStream.range(0, 10).boxed().collect(Collectors.toList());
    MockScanner scanner = new MockScanner(expected.iterator());
    Field<?> filterIndex = Fields.intField("key", 9);
    List<Integer> actual = new ArrayList<>();
    try (NoSqlStructuredTable.FilterByFieldIterator closeableIterator = new NoSqlStructuredTable.FilterByFieldIterator(
      (new NoSqlStructuredTable.ScannerIterator(scanner, SCHEMA)), Collections.singleton(filterIndex), SCHEMA)) {
      while (closeableIterator.hasNext()) {
        actual.add(closeableIterator.next().getInteger("key"));
        Assert.assertFalse(scanner.isClosed());
      }
    }
    Assert.assertTrue(scanner.isClosed());
    Assert.assertEquals(actual.size(), 1);
    Assert.assertEquals(actual.get(0), filterIndex.getValue());
  }

  @Test
  public void testFilterByIndexIteratorMultiMatch() {
    List<Integer> expected = new ArrayList<>(Arrays.asList(9, 9, 9, 7));
    MockScanner scanner = new MockScanner(expected.iterator());
    Field<?> filterIndex = Fields.intField("key", 9);
    List<Integer> actual = new ArrayList<>();
    try (NoSqlStructuredTable.FilterByFieldIterator closeableIterator = new NoSqlStructuredTable.FilterByFieldIterator(
      new NoSqlStructuredTable.ScannerIterator(scanner, SCHEMA), Collections.singleton(filterIndex), SCHEMA)) {
      while (closeableIterator.hasNext()) {
        actual.add(closeableIterator.next().getInteger("key"));
        Assert.assertFalse(scanner.isClosed());
      }
    }
    Assert.assertTrue(scanner.isClosed());
    Assert.assertEquals(actual.size(), 3);
    Assert.assertEquals(actual.get(0), filterIndex.getValue());
    Assert.assertEquals(actual.get(1), filterIndex.getValue());
    Assert.assertEquals(actual.get(2), filterIndex.getValue());
  }

  @Test(expected = TransactionException.class)
  @Override
  public void testScanDeleteAll() throws Exception{
    super.testScanDeleteAll();
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
        return createResult(new MDSKey.Builder().add("tableNamePrefix").add(i).add((long) i).add("key3")
                              .build().getKey(),
                            "c" + i, "v" + i);
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

  private static Result createResult(byte[] row, String col, String val) {
    return new Result(row, Collections.singletonMap(Bytes.toBytes(col), Bytes.toBytes(val)));
  }
}
