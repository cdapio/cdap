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

package io.cdap.cdap.spi.data;

import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests concurrent operations on {@link StructuredTable}.
 */
public abstract class StructuredTableConcurrencyTest {
  private static final Logger LOG = LoggerFactory.getLogger(StructuredTableConcurrencyTest.class);

  private static final StructuredTableSpecification TEST_SPEC;
  private static final StructuredTableId TEST_TABLE = new StructuredTableId("simpleTable");
  private static final String KEY = "key";
  private static final String STRING_COL = "str_col";
  private static final String LONG_COL = "long_col";

  static {
    try {
      TEST_SPEC = new StructuredTableSpecification.Builder()
        .withId(TEST_TABLE)
        .withFields(Fields.intType(KEY), Fields.stringType(STRING_COL), Fields.longType(LONG_COL))
        .withPrimaryKeys(KEY)
        .withIndexes(STRING_COL)
        .build();
    } catch (InvalidFieldException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract StructuredTableAdmin getStructuredTableAdmin();
  protected abstract TransactionRunner getTransactionRunner();

  private final TransactionRunner transactionRunner = getTransactionRunner();

  @Before
  public void init() throws Exception {
    getStructuredTableAdmin().create(TEST_SPEC);
  }

  @After
  public void teardown() throws Exception {
    getStructuredTableAdmin().drop(TEST_TABLE);
  }

  @Test
  public void testConcurrentRead() throws Exception {
    // Write one row
    Field<Integer> key = Fields.intField(KEY, 1);
    String strVal = "str1";
    Long longVal = 100L;
    List<Field<?>> fields = Arrays.asList(key,
                                          Fields.stringField(STRING_COL, strVal),
                                          Fields.longField(LONG_COL, longVal));
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TEST_TABLE);
      table.upsert(fields);
    });

    // Read it concurrently
    int numThreads = 15;
    Queue<Optional<StructuredRow>> actual = new ConcurrentLinkedQueue<>();
    runConcurrentOperation("concurrent-read", numThreads, () ->
      TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TEST_TABLE);
      actual.add(table.read(Collections.singleton(key)));
    }));

    // Verify
    Assert.assertEquals(numThreads, actual.size());
    for (Optional<StructuredRow> optional : actual) {
      Assert.assertTrue(optional.isPresent());
      Assert.assertEquals(strVal, optional.get().getString(STRING_COL));
      Assert.assertEquals(longVal, optional.get().getLong(LONG_COL));
    }
  }

  @Test
  public void testConcurrentUpsert() throws Exception {
    // Concurrently write rows
    int numThreads = 20;
    AtomicInteger idGenerator = new AtomicInteger();
    String strVal = "write-val-";
    runConcurrentOperation("concurrent-upsert", numThreads, () ->
      TransactionRunners.run(transactionRunner, context -> {
        StructuredTable table = context.getTable(TEST_TABLE);
        int id = idGenerator.getAndIncrement();
        List<Field<?>> fields = Arrays.asList(Fields.intField(KEY, id),
                                              Fields.stringField(STRING_COL, strVal + id),
                                              Fields.longField(LONG_COL, (long) id));
        table.upsert(fields);
      })
    );

    // Verify all writes
    AtomicInteger count = new AtomicInteger();
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TEST_TABLE);
      try (CloseableIterator<StructuredRow> iterator = table.scan(Range.all(), Integer.MAX_VALUE)) {
        while (iterator.hasNext()) {
          StructuredRow row = iterator.next();
          Assert.assertEquals(Integer.valueOf(count.get()), row.getInteger(KEY));
          Assert.assertEquals(strVal + count.get(), row.getString(STRING_COL));
          Assert.assertEquals(Long.valueOf(count.get()), row.getLong(LONG_COL));
          count.incrementAndGet();
        }
      }
    });
    Assert.assertEquals(numThreads, count.get());
  }

  @Test
  public void testConcurrentScan() throws Exception {
    // Write few rows of data
    int numThreads = 20;
    int numRows = 99;
    String strVal = "write-batch-";
    List<Collection<Field<?>>> expected = new ArrayList<>();
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TEST_TABLE);
      for (int i = 0; i < numRows; ++i) {
        List<Field<?>> fields = Arrays.asList(Fields.intField(KEY, i),
                                              Fields.stringField(STRING_COL, strVal + i),
                                              Fields.longField(LONG_COL, (long) i));
        expected.add(fields);
        table.upsert(fields);
      }
    });
    Assert.assertEquals(numRows, expected.size());

    // Scan concurrently and add per thread list to queue
    Queue<List<Collection<Field<?>>>> actual = new ConcurrentLinkedQueue<>();
    runConcurrentOperation("concurrent-scan", numThreads, () ->
      TransactionRunners.run(transactionRunner, context -> {
        StructuredTable table = context.getTable(TEST_TABLE);
        try (CloseableIterator<StructuredRow> iterator = table.scan(Range.all(), Integer.MAX_VALUE)) {
          List<Collection<Field<?>>> result = new ArrayList<>();
          while (iterator.hasNext()) {
            StructuredRow row = iterator.next();
            result.add(Arrays.asList(Fields.intField(KEY, row.getInteger(KEY)),
                                     Fields.stringField(STRING_COL, row.getString(STRING_COL)),
                                     Fields.longField(LONG_COL, row.getLong(LONG_COL))));
          }
          actual.add(result);
        }
      })
    );

    // Verify reads per thread
    Assert.assertEquals(numThreads, actual.size());
    for (List<Collection<Field<?>>> readPerThread : actual) {
      Assert.assertEquals(expected, readPerThread);
    }
  }

  @Test
  public void testConcurrentIndexScan() throws Exception {
    // Write few rows of data
    int numThreads = 20;
    int numRows = 99;
    int indexBatch = 9;  // 9 rows per index
    String strVal = "write-batch-";
    String readIndexValue = strVal + 5;
    // Map of index value -> list of rows for the index value
    Map<String, List<Collection<Field<?>>>> expected = new HashMap<>();
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TEST_TABLE);
      for (int i = 0; i < numRows; ++i) {
        String indexValue = strVal + i / indexBatch;
        List<Field<?>> fields = Arrays.asList(Fields.intField(KEY, i),
                                              Fields.stringField(STRING_COL, indexValue),
                                              Fields.longField(LONG_COL, (long) i));
        List<Collection<Field<?>>> rows = expected.computeIfAbsent(indexValue, k -> new ArrayList<>());
        rows.add(fields);
        table.upsert(fields);
      }
    });
    Assert.assertEquals(indexBatch, expected.get(readIndexValue).size());

    // Scan index concurrently and add per thread list to queue
    Queue<List<Collection<Field<?>>>> actual = new ConcurrentLinkedQueue<>();
    runConcurrentOperation("concurrent-index-scan", numThreads, () ->
      TransactionRunners.run(transactionRunner, context -> {
        StructuredTable table = context.getTable(TEST_TABLE);
        try (CloseableIterator<StructuredRow> iterator = table.scan(Fields.stringField(STRING_COL, readIndexValue))) {
          List<Collection<Field<?>>> result = new ArrayList<>();
          while (iterator.hasNext()) {
            StructuredRow row = iterator.next();
            result.add(Arrays.asList(Fields.intField(KEY, row.getInteger(KEY)),
                                     Fields.stringField(STRING_COL, row.getString(STRING_COL)),
                                     Fields.longField(LONG_COL, row.getLong(LONG_COL))));
          }
          actual.add(result);
        }
      })
    );

    // Verify reads per thread
    Assert.assertEquals(numThreads, actual.size());
    for (List<Collection<Field<?>>> readPerThread : actual) {
      Assert.assertEquals(expected.get(readIndexValue), readPerThread);
    }
  }

  @Test
  public void testConcurrentIncrement() throws Exception {
    // Concurrently increment a long
    int numThreads = 20;
    Set<Field<?>> keys = Collections.singleton(Fields.intField(KEY, 10));
    runConcurrentOperation("concurrent-increment", numThreads, () ->
      TransactionRunners.run(transactionRunner, context -> {
        StructuredTable table = context.getTable(TEST_TABLE);
        table.increment(keys, LONG_COL, 1);
      }));

    // Verify increments
    Optional<StructuredRow> row = TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TEST_TABLE);
      return table.read(keys, Collections.singleton(LONG_COL));
    });
    Assert.assertTrue(row.isPresent());
    Assert.assertEquals(Long.valueOf(numThreads), row.get().getLong(LONG_COL));
  }

  @Test
  public void testConcurrentCompareAndSwap() throws Exception {
    int numThreads = 20;
    Set<Field<?>> keys = Collections.singleton(Fields.intField(KEY, 50));
    Long value = 777L;
    Map<Long, Boolean> updateMap = new ConcurrentHashMap<>();
    runConcurrentOperation("concurrent-compare-and-swap", numThreads, () ->
      TransactionRunners.run(transactionRunner, context -> {
        StructuredTable table = context.getTable(TEST_TABLE);
        boolean success = table.compareAndSwap(keys, Fields.longField(LONG_COL, null),
                                               Fields.longField(LONG_COL, value));
        updateMap.put(Thread.currentThread().getId(), success);
      }));

    Assert.assertEquals(numThreads, updateMap.size());
    // Assert only one compare and swap was successful
    int numSuccess = 0;
    for (Map.Entry<Long, Boolean> entry : updateMap.entrySet()) {
      if (entry.getValue()) {
        numSuccess++;
      }
    }
    Assert.assertEquals(1, numSuccess);
    // Assert value
    Optional<StructuredRow> row = TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TEST_TABLE);
      return table.read(keys, Collections.singleton(LONG_COL));
    });
    Assert.assertTrue(row.isPresent());
    Assert.assertEquals(value, row.get().getLong(LONG_COL));
  }

  @Test
  public void testConcurrentDelete() throws Exception {
    // Write one row
    Field<Integer> key = Fields.intField(KEY, 1);
    String strVal = "str1";
    Long longVal = 350L;
    List<Field<?>> fields = Arrays.asList(key,
                                          Fields.stringField(STRING_COL, strVal),
                                          Fields.longField(LONG_COL, longVal));
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TEST_TABLE);
      table.upsert(fields);
    });

    // Assert write
    Optional<StructuredRow> row = TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TEST_TABLE);
      return table.read(Collections.singleton(key));
    });
    Assert.assertTrue(row.isPresent());
    Assert.assertEquals(longVal, row.get().getLong(LONG_COL));

    // delete it concurrently
    int numThreads = 15;
    runConcurrentOperation("concurrent-read", numThreads, () ->
      TransactionRunners.run(transactionRunner, context -> {
        StructuredTable table = context.getTable(TEST_TABLE);
        table.delete(Collections.singleton(key));
      }));

    // Verify delete
    row = TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TEST_TABLE);
      return table.read(Collections.singleton(key));
    });
    Assert.assertFalse(row.isPresent());
  }

  private void runConcurrentOperation(String name, int numThreads, Runnable runnable) throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    try {
      for (int i = 0; i < numThreads; ++i) {
        executorService.submit(() -> {
          try {
            startLatch.await();
            runnable.run();
            doneLatch.countDown();
          } catch (Exception e) {
            LOG.error("Error performing concurrent operation {}", name, e);
          }
        });
      }

      startLatch.countDown();
      Assert.assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
    } finally {
      executorService.shutdown();
    }
  }
}
