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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This is a test base for {@link StructuredTable}.
 * TODO: CDAP-14750 add more unit tests for the StructuredTable
 */
public abstract class StructuredTableTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  // TODO: test complex schema will all allowed data types
  protected static final StructuredTableSpecification SIMPLE_SPEC;

  private static final StructuredTableId SIMPLE_TABLE = new StructuredTableId("simpleTable");
  private static final String KEY = "key";
  private static final String KEY2 = "key2";
  private static final String STRING_COL = "col1";
  private static final String DOUBLE_COL = "col2";
  private static final String FLOAT_COL = "col3";
  private static final String BYTES_COL = "col4";
  private static final String LONG_COL = "col5";
  private static final String VAL = "val";
  private static final StructuredTableSchema SIMPLE_SCHEMA;

  static {
    try {
      SIMPLE_SPEC = new StructuredTableSpecification.Builder()
       .withId(SIMPLE_TABLE)
       .withFields(Fields.intType(KEY), Fields.stringType(STRING_COL), Fields.longType(KEY2),
                   Fields.doubleType(DOUBLE_COL), Fields.floatType(FLOAT_COL), Fields.bytesType(BYTES_COL),
                   Fields.longType(LONG_COL))
       .withPrimaryKeys(KEY, KEY2)
       .withIndexes(STRING_COL)
       .build();
      SIMPLE_SCHEMA = new StructuredTableSchema(SIMPLE_SPEC);
    } catch (InvalidFieldException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract StructuredTableAdmin getStructuredTableAdmin() throws Exception;
  protected abstract TransactionRunner getTransactionRunner() throws Exception;

  @Before
  public void init() throws Exception {
    getStructuredTableAdmin().create(SIMPLE_SPEC);
  }

  @After
  public void teardown() throws Exception {
    getStructuredTableAdmin().drop(SIMPLE_TABLE);
  }

  @Test
  public void testMultipleKeyScan() throws Exception {
    int max = 10;
    // Write rows and read them, the rows will have keys (0, 0L), (2, 2L), ..., (9, 9L)
    List<Collection<Field<?>>> expected = writeSimpleStructuredRows(max, "");
    List<Collection<Field<?>>> actual = readSimpleStructuredRows(max);
    Assert.assertEquals(expected, actual);

    // scan from (1, 8L) inclusive to (3, 3L) inclusive, should return (2, 2L) and (3, 3L)
    List<Field<?>> lowerBound = new ArrayList<>(2);
    lowerBound.add(Fields.intField(KEY, 1));
    lowerBound.add(Fields.longField(KEY2, 8L));
    List<Field<?>> upperBound = new ArrayList<>(2);
    upperBound.add(Fields.intField(KEY, 3));
    upperBound.add(Fields.longField(KEY2, 3L));
    actual = scanSimpleStructuredRows(
      Range.create(lowerBound, Range.Bound.INCLUSIVE, upperBound, Range.Bound.INCLUSIVE), max);
    Assert.assertEquals(expected.subList(2, 4), actual);

    // scan from (1, 8L) inclusive to (3, 3L) exclusive, should only return (2, 2L)
    lowerBound.clear();
    lowerBound.add(Fields.intField(KEY, 1));
    lowerBound.add(Fields.longField(KEY2, 8L));
    upperBound.clear();
    upperBound.add(Fields.intField(KEY, 3));
    upperBound.add(Fields.longField(KEY2, 3L));
    actual = scanSimpleStructuredRows(
      Range.create(lowerBound, Range.Bound.INCLUSIVE, upperBound, Range.Bound.EXCLUSIVE), max);
    Assert.assertEquals(expected.subList(2, 3), actual);
  }

  @Test
  public void testSimpleReadWriteDelete() throws Exception {
    int max = 10;

    // No rows to read before any write
    List<Collection<Field<?>>> actual = readSimpleStructuredRows(max);
    Assert.assertEquals(Collections.emptyList(), actual);

    // Write rows and read them
    List<Collection<Field<?>>> expected = writeSimpleStructuredRows(max, "");
    actual = readSimpleStructuredRows(max);
    Assert.assertEquals(expected, actual);

    // Delete all the rows and verify
    deleteSimpleStructuredRows(max);
    actual = readSimpleStructuredRows(max);
    Assert.assertEquals(Collections.emptyList(), actual);
  }

  @Test
  public void testMultiRead() throws Exception {
    int max = 10;

    // Write multiple rows with sequential keys
    writeSimpleStructuredRows(max, "");

    // Read all of them back using multiRead
    Collection<Collection<Field<?>>> keys = new ArrayList<>();
    for (int i = 0; i < max; i++) {
      keys.add(Arrays.asList(Fields.intField(KEY, i), Fields.longField(KEY2, (long) i)));
    }
    // There is no particular ordering of the result, hence use a set to compare
    Set<Collection<Field<?>>> result = TransactionRunners.run(getTransactionRunner(), context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      return new HashSet<>(convertRowsToFields(table.multiRead(keys).iterator(), Arrays.asList(KEY, KEY2)));
    });

    Assert.assertEquals(10, result.size());
    Assert.assertEquals(new HashSet<>(keys), result);
  }

  @Test
  public void testSimpleScan() throws Exception {
    int max = 100;

    List<Collection<Field<?>>> expected = writeSimpleStructuredRows(max, "");

    List<Collection<Field<?>>> actual =
      scanSimpleStructuredRows(
        Range.create(Collections.singleton(Fields.intField(KEY, 5)), Range.Bound.INCLUSIVE,
                     Collections.singleton(Fields.intField(KEY, 15)), Range.Bound.EXCLUSIVE), max);
    Assert.assertEquals(expected.subList(5, 15), actual);

    actual =
      scanSimpleStructuredRows(
        Range.create(Collections.singleton(Fields.intField(KEY, 5)), Range.Bound.EXCLUSIVE,
                     Collections.singleton(Fields.intField(KEY, 15)), Range.Bound.INCLUSIVE), max);

    Assert.assertEquals(expected.subList(6, 16), actual);

    actual = scanSimpleStructuredRows(
      Range.singleton(Collections.singleton(Fields.intField(KEY, 46))), max);
    Assert.assertEquals(expected.subList(46, 47), actual);

    // TODO: test invalid range
    // TODO: test begin only range
    // TODO: test end only range
  }

  @Test
  public void testMultiScan() throws Exception {
    int max = 100;

    // Write multiple rows with sequential keys
    writeSimpleStructuredRows(max, "");

    // Empty range scan
    List<Collection<Field<?>>> result = runMultiScan(Collections.emptyList(), Integer.MAX_VALUE, KEY, KEY2);
    Assert.assertTrue(result.isEmpty());

    // Multiscan with a range that scans all.
    Collection<Range> ranges = Arrays.asList(
      Range.create(Collections.singleton(Fields.intField(KEY, 0)), Range.Bound.INCLUSIVE,
                   Collections.singleton(Fields.intField(KEY, 10)), Range.Bound.EXCLUSIVE),
      Range.all());
    result = runMultiScan(ranges, Integer.MAX_VALUE, KEY, KEY2);
    List<Collection<Field<?>>> expected = new ArrayList<>();
    for (int i = 0; i < max; i++) {
      expected.add(Arrays.asList(Fields.intField(KEY, i), Fields.longField(KEY2, (long) i)));
    }
    Assert.assertEquals(expected, result);

    // Constructs multiple scan ranges.
    ranges = new ArrayList<>();
    expected = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      int begin = i * 20;
      int end = begin + 10;
      ranges.add(Range.create(Collections.singleton(Fields.intField(KEY, begin)), Range.Bound.INCLUSIVE,
                              Collections.singleton(Fields.intField(KEY, end)), Range.Bound.EXCLUSIVE));
      for (int j = begin; j < end; j++) {
        expected.add(Arrays.asList(Fields.intField(KEY, j), Fields.longField(KEY2, (long) j)));
      }
    }

    // Scan without limit
    result = runMultiScan(ranges, Integer.MAX_VALUE, KEY, KEY2);
    Assert.assertEquals(expected, result);

    // Scan with limit
    int limit = 25;
    result = runMultiScan(ranges, limit, KEY, KEY2);
    Assert.assertEquals(expected.subList(0, limit), result);
  }

  @Test
  public void testSingletonMultiScan() throws Exception {
    int max = 100;

    // Write rows with duplicate prefixes
    for (int i = 0; i < max; i++) {
      List<Field<?>> fields = Arrays.asList(Fields.intField(KEY, i / 10),
                                            Fields.longField(KEY2, (long) i),
                                            Fields.stringField(STRING_COL, VAL + i),
                                            Fields.doubleField(DOUBLE_COL, (double) i),
                                            Fields.floatField(FLOAT_COL, (float) i),
                                            Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-" + i)));
      getTransactionRunner().run(context -> {
        StructuredTable table = context.getTable(SIMPLE_TABLE);
        table.upsert(fields);
      });
    }

    List<List<? extends Field<?>>> expected = IntStream.concat(IntStream.range(10, 20), IntStream.range(50, 60))
      .mapToObj(i -> Arrays.asList(Fields.intField(KEY, i / 10), Fields.longField(KEY2, (long) i)))
      .collect(Collectors.toList());

    // Multiscan with singleton ranges only
    List<Range> ranges = Arrays.asList(
      Range.singleton(Collections.singleton(Fields.intField(KEY, 5))),
      Range.singleton(Collections.singleton(Fields.intField(KEY, 1)))
    );

    List<Collection<Field<?>>> result = runMultiScan(ranges, Integer.MAX_VALUE, KEY, KEY2);
    Assert.assertEquals(expected, result);

    // Multiscan with a mix of singleton and range
    ranges = Arrays.asList(
      Range.singleton(Collections.singleton(Fields.intField(KEY, 5))),
      Range.create(Collections.singleton(Fields.intField(KEY, 1)), Range.Bound.INCLUSIVE,
                   Collections.singleton(Fields.intField(KEY, 2)), Range.Bound.EXCLUSIVE)
    );
    result = runMultiScan(ranges, Integer.MAX_VALUE, KEY, KEY2);
    Assert.assertEquals(expected, result);
  }

  private List<Collection<Field<?>>> runMultiScan(Collection<Range> ranges, int limit,
                                                  String... outputFields) throws Exception {
    return TransactionRunners.run(getTransactionRunner(), context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      try (CloseableIterator<StructuredRow> iterator = table.multiScan(ranges, limit)) {
        return convertRowsToFields(iterator, Arrays.asList(outputFields));
      }
    });
  }

  @Test
  public void testSimpleUpdate() throws Exception {
    int max = 10;
    // write to table and read
    List<Collection<Field<?>>> expected = writeSimpleStructuredRows(max, "");
    List<Collection<Field<?>>> actual = readSimpleStructuredRows(max);
    Assert.assertEquals(expected, actual);

    // update the same row keys with different value
    expected = writeSimpleStructuredRows(max, "newVal");
    actual = readSimpleStructuredRows(max);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testCompareAndSwap() throws Exception {
    // Write a record
    int max = 1;
    List<Collection<Field<?>>> fields = writeSimpleStructuredRows(max, "");
    Assert.assertEquals(max, fields.size());

    Map<String, Field<?>> oldValues = new HashMap<>();
    for (Field<?> field : fields.get(0)) {
      oldValues.put(field.getName(), field);
    }
    oldValues.put(LONG_COL, Fields.longField(LONG_COL, null));

    Map<String, Field<?>> newValues = new HashMap<>();
    newValues.put(STRING_COL, Fields.stringField(STRING_COL, VAL + 100));
    newValues.put(DOUBLE_COL, Fields.doubleField(DOUBLE_COL, 100.0));
    newValues.put(FLOAT_COL, Fields.floatField(FLOAT_COL, 10.0f));
    newValues.put(BYTES_COL, Fields.bytesField(BYTES_COL, Bytes.toBytes("new-bytes")));
    newValues.put(LONG_COL, Fields.longField(LONG_COL, 500L));

    // Compare and swap an existing row
    Collection<Field<?>> keys = Arrays.asList(oldValues.get(KEY), oldValues.get(KEY2));
    for (Map.Entry<String, Field<?>> newField : newValues.entrySet()) {
      getTransactionRunner().run(context -> {
        StructuredTable table = context.getTable(SIMPLE_TABLE);
        boolean result = table.compareAndSwap(keys, oldValues.get(newField.getKey()), newField.getValue());
        Assert.assertTrue(result);
      });
    }

    // Compare and swap of a wrong old value should return false
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      boolean result = table.compareAndSwap(keys, oldValues.get(STRING_COL), Fields.stringField(STRING_COL, "invalid"));
      Assert.assertFalse(result);
    });

    Collection<Field<?>> expected = new HashSet<>();
    expected.addAll(newValues.values());
    expected.addAll(keys);
    List<Collection<Field<?>>> actual = readSimpleStructuredRows(max, Collections.singletonList(LONG_COL));
    Assert.assertEquals(expected, new HashSet<>(actual.get(0)));

    // Compare and swap on primary key should fail
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      try {
        table.compareAndSwap(keys, Fields.intField(KEY, 1), Fields.intField(KEY, 5));
        Assert.fail("Expected IllegalArgumentException since primary key column cannot be swapped");
      } catch (IllegalArgumentException e) {
        // Expected
      }
    });
  }

  @Test
  public void testCompareAndSwapNonExistent() throws Exception {
    // Compare and swap a non-existent row
    Collection<Field<?>> nonExistentKeys = Arrays.asList(Fields.intField(KEY, -100), Fields.longField(KEY2, -10L));

    // Verify row does not exist first
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      Optional<StructuredRow> row = table.read(nonExistentKeys);
      Assert.assertFalse(row.isPresent());
    });

    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      boolean result = table.compareAndSwap(nonExistentKeys,
                                            Fields.stringField(STRING_COL, "non-existent"),
                                            Fields.stringField(STRING_COL, "new-val"));
      Assert.assertFalse(result);
      result = table.compareAndSwap(nonExistentKeys,
                                    Fields.stringField(STRING_COL, null),
                                    Fields.stringField(STRING_COL, "new-val"));
      Assert.assertTrue(result);
    });

    // Read and verify
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      Optional<StructuredRow> row = table.read(nonExistentKeys);
      Assert.assertTrue(row.isPresent());
      Assert.assertEquals("new-val", row.get().getString(STRING_COL));
    });
  }

  @Test
  public void testIncrement() throws Exception {
    Collection<Field<?>> keys = Arrays.asList(Fields.intField(KEY, 100), Fields.longField(KEY2, 200L));

    // Increment
    long increment = 30;
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      table.increment(keys, LONG_COL, increment);
      try {
        table.increment(keys, FLOAT_COL, increment);
        Assert.fail("Expected IllegalArgumentException since only long columns can be incremented");
      } catch (IllegalArgumentException e) {
        // Expected, see the exception message above
      }
    });

    // Verify increment
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      Optional<StructuredRow> rowOptional = table.read(keys);
      Assert.assertTrue(rowOptional.isPresent());
      Assert.assertEquals(Long.valueOf(increment), rowOptional.get().getLong(LONG_COL));
    });

    // Increment
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      table.increment(keys, LONG_COL, increment);
      try {
        table.increment(keys, FLOAT_COL, increment);
        Assert.fail("Expected IllegalArgumentException since only long columns can be incremented");
      } catch (IllegalArgumentException e) {
        // Expected, see the exception message above
      }
    });

    // Verify increment
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      Optional<StructuredRow> rowOptional = table.read(keys);
      Assert.assertTrue(rowOptional.isPresent());
      //noinspection ConstantConditions
      Assert.assertEquals(increment + increment, (long) rowOptional.get().getLong(LONG_COL));
    });

    // Increment on primary key should fail
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      try {
        table.increment(keys, KEY2, increment);
        Assert.fail("Expected IllegalArgumentException since primary key column cannot be incremented");
      } catch (IllegalArgumentException e) {
        // Expected
      }
    });
  }

  @Test
  public void testDeleteAll() throws Exception {
    int max = 10;
    List<Collection<Field<?>>> expected = writeSimpleStructuredRows(max, "");
    Assert.assertEquals(max, expected.size());

    // Delete 6-8 (both inclusive) using the first and second keys
    expected.subList(6, 9).clear();
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      Range range = Range.create(Arrays.asList(Fields.intField(KEY, 6), Fields.longField(KEY2, 6L)),
                                 Range.Bound.INCLUSIVE,
                                 Arrays.asList(Fields.intField(KEY, 8), Fields.longField(KEY2, 8L)),
                                 Range.Bound.INCLUSIVE);
      table.deleteAll(range);
    });
    // Verify the deletion
    Assert.assertEquals(expected, scanSimpleStructuredRows(Range.all(), max));

    // Delete 2-5 (end exclusive) using the first key only
    expected.subList(2, 5).clear();
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      Range range = Range.create(Collections.singletonList(Fields.intField(KEY, 2)), Range.Bound.INCLUSIVE,
                                 Collections.singletonList(Fields.intField(KEY, 5)), Range.Bound.EXCLUSIVE);
      table.deleteAll(range);
    });
    // Verify the deletion
    Assert.assertEquals(expected, scanSimpleStructuredRows(Range.all(), max));

    // Use a range outside the element list, nothing should get deleted
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      Range range = Range.create(Collections.singletonList(Fields.intField(KEY, max + 1)), Range.Bound.INCLUSIVE,
                                 Collections.singletonList(Fields.intField(KEY, max + 5)), Range.Bound.EXCLUSIVE);
      table.deleteAll(range);
    });
    // Verify the deletion
    Assert.assertEquals(expected, scanSimpleStructuredRows(Range.all(), max));

    // Delete all the remaining
    expected.clear();
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      table.deleteAll(Range.all());
    });
    // Verify the deletion
    Assert.assertEquals(expected, scanSimpleStructuredRows(Range.all(), max));
  }

  @Test
  public void testIndexScan() throws Exception {
    int num = 5;
    // Write a few records
    List<Collection<Field<?>>> expected = new ArrayList<>();
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      int counter = 0;
      for (String value : Arrays.asList("abc", "def", "ghi")) {
        for (int i = 0; i < num; ++i) {
          Collection<Field<?>> keys = Arrays.asList(Fields.intField(KEY, counter),
                                                    Fields.longField(KEY2, counter * 100L));
          Collection<Field<?>> fields = new ArrayList<>(keys);
          fields.add(Fields.stringField(STRING_COL, value));
          table.upsert(fields);
          expected.add(fields);
          ++counter;
        }
      }
    });

    // Verify write
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      try (CloseableIterator<StructuredRow> iterator = table.scan(Range.all(), 1000)) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, Arrays.asList(KEY, KEY2, STRING_COL));
        Assert.assertEquals(expected, rows);
      }
    });

    // Scan by index
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      try (CloseableIterator<StructuredRow> iterator = table.scan(Fields.stringField(STRING_COL, "def"))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, Arrays.asList(KEY, KEY2, STRING_COL));
        Assert.assertEquals(expected.subList(num, 2 * num), rows);
      }

      try (CloseableIterator<StructuredRow> iterator = table.scan(Fields.stringField(STRING_COL, "abc"))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, Arrays.asList(KEY, KEY2, STRING_COL));
        Assert.assertEquals(expected.subList(0, num), rows);
      }

      // non-existent index value
      try (CloseableIterator<StructuredRow> iterator = table.scan(Fields.stringField(STRING_COL, "non"))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, Arrays.asList(KEY, KEY2, STRING_COL));
        Assert.assertEquals(Collections.emptyList(), rows);
      }

      // non-index column
      try {
        table.scan(Fields.longField(LONG_COL, 1L));
        Assert.fail("Expected InvalidFieldException for scanning a non-index column");
      } catch (InvalidFieldException e) {
        // Expected
      }
    });
  }

  @Test
  public void testCount() throws Exception {
    // Write records
    int max = 5;
    List<Collection<Field<?>>> fields = writeSimpleStructuredRows(max, "");
    Assert.assertEquals(max, fields.size());
    // Verify count
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      Assert.assertEquals(max, table.count(Collections.singleton(Range.all())));

      Collection<Range> ranges = Arrays.asList(Range.to(Collections.singletonList(Fields.intField(KEY, 2)),
                                                        Range.Bound.INCLUSIVE),
                                               Range.from(Collections.singletonList(Fields.intField(KEY, 2)),
                                                          Range.Bound.EXCLUSIVE));
      Assert.assertEquals(max, table.count(ranges));
    });
  }

  private List<Collection<Field<?>>> writeSimpleStructuredRows(int max, String suffix) throws Exception {
    List<Collection<Field<?>>> expected = new ArrayList<>(max);
    // Write rows in reverse order to test sorting
    for (int i = max - 1; i >= 0; i--) {
      List<Field<?>> fields = Arrays.asList(Fields.intField(KEY, i),
                                            Fields.longField(KEY2, (long) i),
                                            Fields.stringField(STRING_COL, VAL + i + suffix),
                                            Fields.doubleField(DOUBLE_COL, (double) i),
                                            Fields.floatField(FLOAT_COL, (float) i),
                                            Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-" + i)));
      expected.add(fields);

      getTransactionRunner().run(context -> {
        StructuredTable table = context.getTable(SIMPLE_TABLE);
        table.upsert(fields);
      });
    }
    Collections.reverse(expected);
    return expected;
  }

  private List<Collection<Field<?>>> readSimpleStructuredRows(int max) throws Exception {
    return readSimpleStructuredRows(max, Collections.emptyList());
  }

  private List<Collection<Field<?>>> readSimpleStructuredRows(int max, List<String> extraColumns) throws Exception {
    List<String> columns = new ArrayList<>();
    columns.add(STRING_COL);
    columns.add(DOUBLE_COL);
    columns.add(FLOAT_COL);
    columns.add(BYTES_COL);
    columns.addAll(extraColumns);

    List<String> fields = new ArrayList<>();
    fields.add(KEY);
    fields.add(KEY2);
    fields.add(STRING_COL);
    fields.add(DOUBLE_COL);
    fields.add(FLOAT_COL);
    fields.add(BYTES_COL);
    fields.addAll(extraColumns);

    List<Collection<Field<?>>> actual = new ArrayList<>(max);
    for (int i = 0; i < max; i++) {
      List<Field<?>> compoundKey = new ArrayList<>(2);
      compoundKey.add(Fields.intField(KEY, i));
      compoundKey.add(Fields.longField(KEY2, (long) i));
      AtomicReference<Optional<StructuredRow>> rowRef = new AtomicReference<>();

      getTransactionRunner().run(context -> {
        StructuredTable table = context.getTable(SIMPLE_TABLE);
        rowRef.set(table.read(compoundKey, columns));
      });

      Optional<StructuredRow> row = rowRef.get();
      row.ifPresent(
        structuredRow -> actual.add(
          convertRowToFields(structuredRow, fields)));
    }
    return actual;
  }

  private List<Collection<Field<?>>> convertRowsToFields(Iterator<StructuredRow> iterator, List<String> columns) {
    List<Collection<Field<?>>> rows = new ArrayList<>();
    while (iterator.hasNext()) {
      StructuredRow row = iterator.next();
      rows.add(convertRowToFields(row, columns));
    }
    return rows;
  }

  private List<Field<?>> convertRowToFields(StructuredRow row, List<String> columns) {
    List<Field<?>> fields = new ArrayList<>();
    for (String name : columns) {
      FieldType.Type type = SIMPLE_SCHEMA.getType(name);
      if (type == null) {
        throw new InvalidFieldException(SIMPLE_TABLE, name);
      }
      switch (type) {
        case BYTES:
          fields.add(Fields.bytesField(name, row.getBytes(name)));
          break;
        case STRING:
          fields.add(Fields.stringField(name, row.getString(name)));
          break;
        case FLOAT:
          fields.add(Fields.floatField(name, row.getFloat(name)));
          break;
        case DOUBLE:
          fields.add(Fields.doubleField(name, row.getDouble(name)));
          break;
        case INTEGER:
          fields.add(Fields.intField(name, row.getInteger(name)));
          break;
        case LONG:
          fields.add(Fields.longField(name, row.getLong(name)));
          break;
      }
    }
    return fields;
  }

  private void deleteSimpleStructuredRows(int max) throws Exception {
    for (int i = 0; i < max; i++) {
      List<Field<?>> compoundKey = new ArrayList<>(2);
      compoundKey.add(Fields.intField(KEY, i));
      compoundKey.add(Fields.longField(KEY2, (long) i));

      getTransactionRunner().run(context -> {
        StructuredTable table = context.getTable(SIMPLE_TABLE);
        table.delete(compoundKey);
      });
    }
  }

  private List<Collection<Field<?>>> scanSimpleStructuredRows(Range range, int max) throws Exception {
    List<Collection<Field<?>>> actual = new ArrayList<>(max);
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      try (CloseableIterator<StructuredRow> iterator = table.scan(range, max)) {
        while (iterator.hasNext()) {
          StructuredRow row = iterator.next();
          actual.add(Arrays.asList(Fields.intField(KEY, row.getInteger(KEY)),
                                   Fields.longField(KEY2, row.getLong(KEY2)),
                                   Fields.stringField(STRING_COL, row.getString(STRING_COL)),
                                   Fields.doubleField(DOUBLE_COL, row.getDouble(DOUBLE_COL)),
                                   Fields.floatField(FLOAT_COL, row.getFloat(FLOAT_COL)),
                                   Fields.bytesField(BYTES_COL, row.getBytes(BYTES_COL))));
        }
      }
    });
    return actual;
  }
}
