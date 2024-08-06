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
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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
  private static final String KEY3 = "key3";
  private static final String STRING_COL = "col1";
  private static final String DOUBLE_COL = "col2";
  private static final String FLOAT_COL = "col3";
  private static final String BYTES_COL = "col4";
  private static final String LONG_COL = "col5";
  private static final String IDX_COL = "col6";
  private static final String BOOL_COL = "col7";
  private static final String VAL = "val";
  private static final StructuredTableSchema SIMPLE_SCHEMA;

  static {
    try {
      SIMPLE_SPEC = new StructuredTableSpecification.Builder()
        .withId(SIMPLE_TABLE)
        .withFields(Fields.intType(KEY), Fields.longType(KEY2), Fields.stringType(KEY3), Fields.stringType(STRING_COL),
                    Fields.doubleType(DOUBLE_COL), Fields.floatType(FLOAT_COL), Fields.bytesType(BYTES_COL),
                    Fields.longType(LONG_COL), Fields.longType(IDX_COL), Fields.booleanType(BOOL_COL))
        .withPrimaryKeys(KEY, KEY2, KEY3)
        .withIndexes(STRING_COL, IDX_COL, BOOL_COL)
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
    getStructuredTableAdmin().createOrUpdate(SIMPLE_SPEC);
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
    int max = 100;

    // Write multiple rows with sequential keys
    writeSimpleStructuredRows(max, "");

    // Write other rows that share partial primary keys
    for (int i = max - 1; i >= 0; i--) {
      List<Field<?>> fields = Arrays.asList(Fields.intField(KEY, i),
                                            Fields.longField(KEY2, (long) i),
                                            Fields.stringField(KEY3, "new key3"),
                                            Fields.stringField(STRING_COL, VAL + i),
                                            Fields.doubleField(DOUBLE_COL, (double) i),
                                            Fields.floatField(FLOAT_COL, (float) i),
                                            Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-" + i)));
      getTransactionRunner().run(context -> {
        StructuredTable table = context.getTable(SIMPLE_TABLE);
        table.upsert(fields);
      });
    }

    // Read all of them back using multiRead
    Collection<Collection<Field<?>>> keys = new ArrayList<>();
    for (int i = 0; i < max; i++) {
      keys.add(Arrays.asList(Fields.intField(KEY, i),
                             Fields.longField(KEY2, (long) i),
                             // Mix the shared partial keys to verify that we get rows that exactly match each key set
                             Fields.stringField(KEY3, i % 2 == 0 ? "key3" : "new key3")));
    }
    // There is no particular ordering of the result, hence use a set to compare
    Set<Collection<Field<?>>> result = TransactionRunners.run(getTransactionRunner(), context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      return new HashSet<>(convertRowsToFields(table.multiRead(keys).iterator(), Arrays.asList(KEY, KEY2, KEY3)));
    });

    Assert.assertEquals(100, result.size());
    Assert.assertEquals(new HashSet<>(keys), result);
  }

  @Test
  public void testWritingNullFields() throws Exception {
    int max = 100;

    // Write rows with null field values
    for (int i = max - 1; i >= 0; i--) {
      List<Field<?>> fields = Arrays.asList(Fields.intField(KEY, i),
          Fields.longField(KEY2, (long) i),
          Fields.stringField(KEY3, "new key3"),
          Fields.stringField(STRING_COL, VAL + i),
          Fields.doubleField(DOUBLE_COL, null),
          Fields.floatField(FLOAT_COL, null),
          Fields.bytesField(BYTES_COL, null));
      getTransactionRunner().run(context -> {
        StructuredTable table = context.getTable(SIMPLE_TABLE);
        table.upsert(fields);
      });
    }

    // Read all of them back using multiRead
    Collection<Collection<Field<?>>> keys = new ArrayList<>();
    for (int i = 0; i < max; i++) {
      keys.add(Arrays.asList(Fields.intField(KEY, i),
          Fields.longField(KEY2, (long) i),
          Fields.stringField(KEY3, "new key3")));
    }
    // There is no particular ordering of the result, hence use a set to compare
    Set<Collection<Field<?>>> result = TransactionRunners.run(getTransactionRunner(), context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      return new HashSet<>(convertRowsToFields(table.multiRead(keys).iterator(), Arrays.asList(KEY, KEY2, KEY3)));
    });

    Assert.assertEquals(100, result.size());
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

    // Test partial keys singleton scan
    actual =
      scanSimpleStructuredRows(
        Range.singleton(Arrays.asList(Fields.intField(KEY, 2), Fields.longField(KEY2, (long) 35))), max);
    Assert.assertTrue(actual.isEmpty());

    // Test partial keys actual range scan
    actual =
      scanSimpleStructuredRows(
        Range.create(Collections.singleton(Fields.intField(KEY, 30)),
                     Range.Bound.EXCLUSIVE,
                     Arrays.asList(Fields.intField(KEY, 40),
                                   Fields.longField(KEY2, (long) 40)),
                     Range.Bound.EXCLUSIVE), max);
    Assert.assertEquals(expected.subList(31, 40), actual);

    actual =
      scanSimpleStructuredRows(
        Range.create(Collections.singleton(Fields.intField(KEY, 5)),
                     Range.Bound.INCLUSIVE,
                     Arrays.asList(Fields.intField(KEY, 15),
                                   Fields.longField(KEY2, (long) 15)),
                     Range.Bound.EXCLUSIVE), max);
    Assert.assertEquals(expected.subList(5, 15), actual);

    // Test partial keys actual range scan with mixed type
    actual =
        scanSimpleStructuredRows(
            Range.create(
                Collections.singleton(Fields.intField(KEY, 5)),
                Range.Bound.INCLUSIVE,
                Arrays.asList(Fields.intField(KEY, 15), Fields.stringField(KEY3, "key3")),
                Range.Bound.INCLUSIVE),
            max);
    Assert.assertEquals(expected.subList(5, 16), actual);

    // Test begin only range
    actual =
      scanSimpleStructuredRows(
        Range.create(Collections.singleton(Fields.intField(KEY, 5)), Range.Bound.EXCLUSIVE,
                     null, Range.Bound.INCLUSIVE), max);
    Assert.assertEquals(expected.subList(6, max), actual);

    // Test end only range
    actual =
      scanSimpleStructuredRows(
        Range.create(null, Range.Bound.INCLUSIVE,
                     Collections.singleton(Fields.intField(KEY, 18)), Range.Bound.INCLUSIVE), max);
    Assert.assertEquals(expected.subList(0, 19), actual);

    // Test invalid range
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      try {
        table.scan(
          Range.create(Collections.singleton(Fields.stringField("NON-EXIST_FIELD", "invalid")), Range.Bound.EXCLUSIVE,
                       Collections.singleton(Fields.longField(KEY2, (long) 40)), Range.Bound.EXCLUSIVE), max);
        Assert.fail("Expected InvalidFieldException since STRING_COL is not primary key");
      } catch (InvalidFieldException e) {
        // Expected, see the exception message above
      }
    });
  }

  @Test
  public void testMultiRangeScanDuplicateKeyPrefix() throws Exception {
    int max = 100;

    List<Collection<Field<?>>> expectedAll = writeRowsWithDuplicateKeyPrefix(max, "");

    Collection<Range> ranges = Collections.singletonList(
      Range.singleton(Arrays.asList(Fields.intField(KEY, 6), Fields.longField(KEY2, (long) 26))));

    List<String> outPutFields = Arrays.asList(KEY, KEY2, KEY3, STRING_COL, DOUBLE_COL, FLOAT_COL, BYTES_COL);

    List<Collection<Field<?>>> actual = runMultiScan(ranges, max, outPutFields);
    Assert.assertEquals(expectedAll.subList(26, 27), actual);

    // MultiScan with inclusive beginning and exclusive ending
    ranges = Collections.singletonList(
      Range.create(Collections.singletonList(Fields.intField(KEY, 3)), Range.Bound.INCLUSIVE,
                   Collections.singletonList(Fields.intField(KEY, 5)), Range.Bound.EXCLUSIVE));
    actual = runMultiScan(ranges, max, outPutFields);
    List<Collection<Field<?>>> expected = new LinkedList<>();
    for (int i = 3; i < 5; i++) {
      for (int j = 0; j < max / 10; j++) {
        expected.add(expectedAll.get(j * 10 + i));
      }
    }

    Assert.assertEquals(expected, actual);

    // MultiScan with inclusive beginning and inclusive ending
    ranges = Collections.singletonList(
      Range.create(Collections.singletonList(Fields.intField(KEY, 3)), Range.Bound.INCLUSIVE,
                   Collections.singletonList(Fields.intField(KEY, 5)), Range.Bound.INCLUSIVE));
    actual = runMultiScan(ranges, max, outPutFields);
    expected = new LinkedList<>();
    for (int i = 3; i < 6; i++) {
      for (int j = 0; j < max / 10; j++) {
        expected.add(expectedAll.get(j * 10 + i));
      }
    }
    Assert.assertEquals(expected, actual);

    // MultiScan with exclusive beginning and inclusive ending
    ranges = Collections.singletonList(
      Range.create(Collections.singletonList(Fields.intField(KEY, 3)), Range.Bound.EXCLUSIVE,
                   Collections.singletonList(Fields.intField(KEY, 5)), Range.Bound.INCLUSIVE));
    actual = runMultiScan(ranges, max, outPutFields);
    expected = new LinkedList<>();
    for (int i = 4; i < 6; i++) {
      for (int j = 0; j < max / 10; j++) {
        expected.add(expectedAll.get(j * 10 + i));
      }
    }
    Assert.assertEquals(expected, actual);

    // MultiScan with singleton ranges
    ranges = Arrays.asList(
      Range.singleton(Collections.singletonList(Fields.intField(KEY, 3))),
      Range.singleton(Collections.singletonList(Fields.intField(KEY, 4))));
    actual = runMultiScan(ranges, max, outPutFields);
    expected = new LinkedList<>();
    for (int i = 3; i < 5; i++) {
      for (int j = 0; j < max / 10; j++) {
        expected.add(expectedAll.get(j * 10 + i));
      }
    }
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testPartialKeysRangeScan() throws Exception {
    int max = 100;

    List<Collection<Field<?>>> expectedAll = writeStructuredRowsWithDuplicatePrefix(max, "");

    Collection<Range> ranges = Collections.singletonList(
      Range.singleton(Arrays.asList(Fields.intField(KEY, 2), Fields.longField(KEY2, (long) 26))));

    List<String> outPutFields = Arrays.asList(KEY, KEY2, KEY3, STRING_COL, DOUBLE_COL, FLOAT_COL, BYTES_COL);

    List<Collection<Field<?>>> actual = runMultiScan(ranges, max, outPutFields);
    Assert.assertEquals(expectedAll.subList(26, 27), actual);

    // MultiScan with multiple ranges
    ranges = Arrays.asList(
      // INCLUSIVE beginning and INCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 1), Fields.longField(KEY2, (long) 11)),
                   Range.Bound.INCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 1), Fields.longField(KEY2, (long) 15)),
                   Range.Bound.INCLUSIVE),
      // INCLUSIVE beginning and EXCLUSIVE ending
      Range.create(Collections.singletonList(Fields.intField(KEY, 2)),
                   Range.Bound.INCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 2), Fields.longField(KEY2, (long) 25)),
                   Range.Bound.EXCLUSIVE),
      // EXCLUSIVE beginning and EXCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 3), Fields.longField(KEY2, (long) 31)),
                   Range.Bound.EXCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 3), Fields.longField(KEY2, (long) 35)),
                   Range.Bound.EXCLUSIVE),
      // EXCLUSIVE beginning and INCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 4), Fields.longField(KEY2, (long) 41)),
                   Range.Bound.EXCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 4), Fields.longField(KEY2, (long) 49)),
                   Range.Bound.INCLUSIVE));

    actual = runMultiScan(ranges, max, outPutFields);
    List<Collection<Field<?>>> newExpected = new LinkedList<>();
    newExpected.addAll(expectedAll.subList(11, 16));
    newExpected.addAll(expectedAll.subList(20, 25));
    newExpected.addAll(expectedAll.subList(32, 35));
    newExpected.addAll(expectedAll.subList(42, 50));
    Assert.assertEquals(newExpected, actual);

    // MultiScan with multiple ranges that cannot merge the longest prefix keys
    ranges = Arrays.asList(
      // INCLUSIVE beginning and INCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 1), Fields.stringField(KEY3, String.valueOf(11))),
                   Range.Bound.INCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 1), Fields.stringField(KEY3, String.valueOf(15))),
                   Range.Bound.INCLUSIVE),
      // INCLUSIVE beginning and EXCLUSIVE ending
      Range.create(Collections.singletonList(Fields.intField(KEY, 3)),
                   Range.Bound.INCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 3), Fields.stringField(KEY3, String.valueOf(35))),
                   Range.Bound.EXCLUSIVE),
      // EXCLUSIVE beginning and EXCLUSIVE ending
      Range.create(Collections.singletonList(Fields.intField(KEY, 4)),
                   Range.Bound.EXCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 5), Fields.stringField(KEY3, String.valueOf(58))),
                   Range.Bound.EXCLUSIVE),
      // EXCLUSIVE beginning and INCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 7), Fields.stringField(KEY3, String.valueOf(71))),
                   Range.Bound.EXCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 7), Fields.stringField(KEY3, String.valueOf(79))),
                   Range.Bound.INCLUSIVE));

    actual = runMultiScan(ranges, max, outPutFields);
    newExpected = new LinkedList<>();
    newExpected.addAll(expectedAll.subList(11, 16));
    newExpected.addAll(expectedAll.subList(30, 35));
    newExpected.addAll(expectedAll.subList(50, 58));
    newExpected.addAll(expectedAll.subList(72, 80));
    Assert.assertEquals(newExpected, actual);

    actual = scanSimpleStructuredRows(
      // INCLUSIVE beginning and INCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 1), Fields.stringField(KEY3, String.valueOf(11))),
                   Range.Bound.INCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 1), Fields.stringField(KEY3, String.valueOf(15))),
                   Range.Bound.INCLUSIVE), max);
    Assert.assertEquals(expectedAll.subList(11, 16), actual);

    actual = scanSimpleStructuredRows(
      // INCLUSIVE beginning and INCLUSIVE ending, unmatched partial keys
      Range.create(Collections.singletonList(Fields.intField(KEY, 1)),
                   Range.Bound.INCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 1), Fields.longField(KEY2, (long) 15)),
                   Range.Bound.INCLUSIVE), max);
    Assert.assertEquals(expectedAll.subList(10, 16), actual);

    actual = scanSimpleStructuredRows(
      // INCLUSIVE beginning and EXCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 3), Fields.stringField(KEY3, String.valueOf(31))),
                   Range.Bound.INCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 3), Fields.stringField(KEY3, String.valueOf(35))),
                   Range.Bound.EXCLUSIVE), max);
    Assert.assertEquals(expectedAll.subList(31, 35), actual);

    actual = scanSimpleStructuredRows(
      // INCLUSIVE beginning and EXCLUSIVE ending, unmatched partial keys
      Range.create(Arrays.asList(Fields.intField(KEY, 3), Fields.stringField(KEY3, String.valueOf(31))),
                   Range.Bound.INCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 3), Fields.longField(KEY2, (long) 35)),
                   Range.Bound.EXCLUSIVE), max);
    Assert.assertEquals(expectedAll.subList(31, 35), actual);

    actual = scanSimpleStructuredRows(
      // EXCLUSIVE beginning and EXCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 5), Fields.stringField(KEY3, String.valueOf(52))),
                   Range.Bound.EXCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 5), Fields.stringField(KEY3, String.valueOf(58))),
                   Range.Bound.EXCLUSIVE), max);
    Assert.assertEquals(expectedAll.subList(53, 58), actual);

    actual = scanSimpleStructuredRows(
      // EXCLUSIVE beginning and EXCLUSIVE ending, unmatched partial keys
      Range.create(Arrays.asList(Fields.intField(KEY, 5), Fields.longField(KEY2, (long) 52)),
                   Range.Bound.EXCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 5), Fields.stringField(KEY3, String.valueOf(58))),
                   Range.Bound.EXCLUSIVE), max);
    Assert.assertEquals(expectedAll.subList(53, 58), actual);

    actual = scanSimpleStructuredRows(
      // EXCLUSIVE beginning and INCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 7),
                                 Fields.stringField(KEY3, String.valueOf(71))),
                   Range.Bound.EXCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 7),
                                 Fields.longField(KEY2, (long) 79),
                                 Fields.stringField(KEY3, String.valueOf(79))),
                   Range.Bound.INCLUSIVE), max);
    Assert.assertEquals(expectedAll.subList(72, 80), actual);

    // MultiScan partialKeys with multiple collections of fields
    ranges = Arrays.asList(
      Range.singleton(Collections.singletonList(Fields.intField(KEY, 3))),
      Range.singleton(Collections.singletonList(Fields.intField(KEY, 4))));
    actual = runMultiScan(ranges, max, outPutFields);
    Assert.assertEquals(expectedAll.subList(30, 50), actual);

    // MultiScan partialKeys with a mix of singleton and range, mixed ordering
    ranges = Arrays.asList(
      Range.singleton(Arrays.asList(Fields.intField(KEY, 2), Fields.longField(KEY2, (long) 29))),
      Range.singleton(Arrays.asList(Fields.intField(KEY, 2), Fields.longField(KEY2, (long) 28))),
      Range.singleton(Collections.singletonList(Fields.intField(KEY, 3))));
    actual = runMultiScan(ranges, max, outPutFields);
    Assert.assertEquals(expectedAll.subList(28, 40), actual);

    // MultiScan partialKeys with overlapping ranges
    ranges = Arrays.asList(
      Range.singleton(Arrays.asList(Fields.intField(KEY, 2), Fields.longField(KEY2, (long) 29))),
      Range.singleton(Arrays.asList(Fields.intField(KEY, 2), Fields.longField(KEY2, (long) 28))),
      Range.singleton(Collections.singletonList(Fields.intField(KEY, 2))));

    actual = runMultiScan(ranges, max, outPutFields);
    Assert.assertEquals(expectedAll.subList(20, 30), actual);

    ranges = Arrays.asList(
      Range.create(Arrays.asList(Fields.intField(KEY, 0), Fields.longField(KEY2, (long) 5)),
                   Range.Bound.EXCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 4), Fields.longField(KEY2, (long) 45)),
                   Range.Bound.EXCLUSIVE),
      Range.create(Arrays.asList(Fields.intField(KEY, 1), Fields.longField(KEY2, (long) 15)),
                   Range.Bound.INCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 6), Fields.longField(KEY2, (long) 60)),
                   Range.Bound.INCLUSIVE));

    actual = runMultiScan(ranges, max, outPutFields);
    Assert.assertEquals(expectedAll.subList(6, 61), actual);

    // MultiScan partialKeys with limit
    ranges = Arrays.asList(
      Range.singleton(Collections.singletonList(Fields.intField(KEY, 4))),
      Range.singleton(Collections.singletonList(Fields.intField(KEY, 3))));
    actual = runMultiScan(ranges, 15, outPutFields);

    Assert.assertEquals(expectedAll.subList(30, 45), actual);
  }

  @Test
  public void testNonPrimaryKeysRangeScan() throws Exception {
    int max = 100;
    List<Collection<Field<?>>> expectedAll = writeStructuredRowsWithDuplicatePrefix(max, "");
    List<String> outPutFields = Arrays.asList(KEY, KEY2, KEY3, STRING_COL, DOUBLE_COL, FLOAT_COL, BYTES_COL);

    // Scan on single range
    List<Collection<Field<?>>> actual = scanSimpleStructuredRows(
      // INCLUSIVE beginning and INCLUSIVE ending
      Range.create(Collections.singletonList(Fields.intField(KEY, 1)),
                   Range.Bound.INCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 1), Fields.doubleField(DOUBLE_COL, (double) 115)),
                   Range.Bound.INCLUSIVE), max);
    Assert.assertEquals(expectedAll.subList(10, 20), actual);

    actual = scanSimpleStructuredRows(
      // INCLUSIVE beginning and EXCLUSIVE ending
      Range.create(Collections.singletonList(Fields.intField(KEY, 3)),
                   Range.Bound.INCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 3), Fields.stringField(STRING_COL, VAL + "35")),
                   Range.Bound.EXCLUSIVE), max);
    Assert.assertEquals(expectedAll.subList(30, 35), actual);

    actual = scanSimpleStructuredRows(
      // EXCLUSIVE beginning and EXCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 5), Fields.doubleField(DOUBLE_COL, (double) 52)),
                   Range.Bound.EXCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 5), Fields.doubleField(DOUBLE_COL, (double) 58)),
                   Range.Bound.EXCLUSIVE), max);
    Assert.assertEquals(expectedAll.subList(53, 58), actual);

    actual = scanSimpleStructuredRows(
      // EXCLUSIVE beginning and INCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 7), Fields.stringField(STRING_COL, VAL + "71")),
                   Range.Bound.EXCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 7), Fields.stringField(STRING_COL, VAL + "79")),
                   Range.Bound.INCLUSIVE), max);
    Assert.assertEquals(expectedAll.subList(72, 80), actual);

    // MultiScan with multiple ranges that cannot merge the longest prefix keys
    Collection<Range> ranges = Arrays.asList(
      // INCLUSIVE beginning and INCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 1), Fields.doubleField(DOUBLE_COL, (double) 11)),
                   Range.Bound.INCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 1), Fields.doubleField(DOUBLE_COL, (double) 15)),
                   Range.Bound.INCLUSIVE),
      // INCLUSIVE beginning and EXCLUSIVE ending
      Range.create(Collections.singletonList(Fields.intField(KEY, 3)),
                   Range.Bound.INCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 3), Fields.doubleField(DOUBLE_COL, (double) 39)),
                   Range.Bound.EXCLUSIVE),
      // EXCLUSIVE beginning and EXCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 5),
                                 Fields.stringField(STRING_COL, VAL + "53")),
                   Range.Bound.EXCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 5),
                                 Fields.longField(KEY2, (long) 55),
                                 Fields.stringField(STRING_COL, VAL + "55")),
                   Range.Bound.EXCLUSIVE),
      // EXCLUSIVE beginning and INCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 7), Fields.doubleField(DOUBLE_COL, (double) 70)),
                   Range.Bound.EXCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 7), Fields.doubleField(DOUBLE_COL, (double) 79)),
                   Range.Bound.INCLUSIVE));

    actual = runMultiScan(ranges, max, outPutFields);
    List<Collection<Field<?>>> newExpected = new LinkedList<>();
    newExpected.addAll(expectedAll.subList(11, 16));
    newExpected.addAll(expectedAll.subList(30, 39));
    newExpected.addAll(expectedAll.subList(54, 55));
    newExpected.addAll(expectedAll.subList(71, 80));
    Assert.assertEquals(newExpected, actual);

    // MultiScan with multiple ranges
    ranges = Arrays.asList(
      // INCLUSIVE beginning and INCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 1), Fields.doubleField(DOUBLE_COL, (double) 11)),
                   Range.Bound.INCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 1), Fields.doubleField(DOUBLE_COL, (double) 15)),
                   Range.Bound.INCLUSIVE),
      // INCLUSIVE beginning and EXCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 2), Fields.doubleField(DOUBLE_COL, (double) 22)),
                   Range.Bound.INCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 2), Fields.doubleField(DOUBLE_COL, (double) 29)),
                   Range.Bound.EXCLUSIVE),
      // EXCLUSIVE beginning and EXCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 3), Fields.stringField(STRING_COL, VAL + "33")),
                   Range.Bound.EXCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 3), Fields.stringField(STRING_COL, VAL + "37")),
                   Range.Bound.EXCLUSIVE),
      // EXCLUSIVE beginning and INCLUSIVE ending
      Range.create(Arrays.asList(Fields.intField(KEY, 4), Fields.doubleField(DOUBLE_COL, (double) 40)),
                   Range.Bound.EXCLUSIVE,
                   Arrays.asList(Fields.intField(KEY, 4), Fields.doubleField(DOUBLE_COL, (double) 79)),
                   Range.Bound.INCLUSIVE));

    actual = runMultiScan(ranges, max, outPutFields);
    newExpected = new LinkedList<>();
    newExpected.addAll(expectedAll.subList(11, 16));
    newExpected.addAll(expectedAll.subList(22, 29));
    newExpected.addAll(expectedAll.subList(34, 37));
    newExpected.addAll(expectedAll.subList(41, 50));
    Assert.assertEquals(newExpected, actual);


    // MultiScan partialKeys with non-primary keys
    ranges = Arrays.asList(
      Range.singleton(Arrays.asList(Fields.intField(KEY, 2),
                                    Fields.stringField(STRING_COL, VAL + "29"))),
      Range.singleton(Arrays.asList(Fields.intField(KEY, 3),
                                    Fields.stringField(STRING_COL, VAL + "30")))
    );

    actual = runMultiScan(ranges, max, outPutFields);
    Assert.assertEquals(expectedAll.subList(29, 31), actual);
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

    // Add a singleton range that are not in the ranges constructed in the for-loop above.
    ranges.add(Range.singleton(Collections.singleton(Fields.intField(KEY, 11))));
    // The expected result should be after the 0-10 range.
    expected.add(10, Arrays.asList(Fields.intField(KEY, 11), Fields.longField(KEY2, 11L)));

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
                                            Fields.stringField(KEY3, "key3"),
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

    // Multiscan with singleton ranges of mixed fields
    expected = IntStream.concat(IntStream.range(1, 2), IntStream.range(51, 52))
      .mapToObj(i -> Arrays.asList(Fields.intField(KEY, i / 10), Fields.longField(KEY2, (long) i)))
      .collect(Collectors.toList());

    ranges = Arrays.asList(
      Range.singleton(Arrays.asList(Fields.intField(KEY, 0),
                                    Fields.longField(KEY2, (long) 1))),
      Range.singleton(Arrays.asList(Fields.intField(KEY, 5),
                                    Fields.longField(KEY2, (long) 51)))
    );

    result = runMultiScan(ranges, Integer.MAX_VALUE, KEY, KEY2);
    Assert.assertEquals(expected, result);

    expected = IntStream.concat(IntStream.range(1, 2), IntStream.range(50, 61))
      .mapToObj(i -> Arrays.asList(Fields.intField(KEY, i / 10), Fields.longField(KEY2, (long) i)))
      .collect(Collectors.toList());

    ranges = Arrays.asList(
      Range.singleton(Arrays.asList(Fields.intField(KEY, 0),
                                    Fields.longField(KEY2, (long) 1))),
      Range.singleton(Collections.singletonList(Fields.intField(KEY, 5))),
      Range.singleton(Arrays.asList(Fields.intField(KEY, 6),
                                    Fields.longField(KEY2, (long) 60)))
    );

    result = runMultiScan(ranges, Integer.MAX_VALUE, KEY, KEY2);
    Assert.assertEquals(expected, result);
  }

  private List<Collection<Field<?>>> runMultiScan(Collection<Range> ranges, int limit,
                                                  String... outputFields) throws Exception {
    return runMultiScan(ranges, limit, Arrays.asList(outputFields));
  }

  private List<Collection<Field<?>>> runMultiScan(Collection<Range> ranges, int limit,
                                                  List<String> outputFields) throws Exception {
    return TransactionRunners.run(getTransactionRunner(), context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      try (CloseableIterator<StructuredRow> iterator = table.multiScan(ranges, limit)) {
        return convertRowsToFields(iterator, outputFields);
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
    Collection<Field<?>> keys = Arrays.asList(oldValues.get(KEY), oldValues.get(KEY2), oldValues.get(KEY3));
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
    Collection<Field<?>> nonExistentKeys = Arrays.asList(Fields.intField(KEY, -100),
                                                         Fields.longField(KEY2, -10L),
                                                         Fields.stringField(KEY3, "key3"));

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
    Collection<Field<?>> keys = Arrays.asList(Fields.intField(KEY, 100),
                                              Fields.longField(KEY2, 200L),
                                              Fields.stringField(KEY3, "key3"));

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
  public void testScanDeleteAll() throws Exception {
    int max = 10;
    List<Collection<Field<?>>> expected = writeSimpleStructuredRows(max, "");
    Assert.assertEquals(max, expected.size());
    // Delete 7-9 (both inclusive) using a column which is part of PK but not the first PK.
    expected.subList(7, 10).clear();
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      Range range = Range.create(Arrays.asList(Fields.longField(KEY2, 7L)),
          Range.Bound.INCLUSIVE,
          Arrays.asList(Fields.longField(KEY2, 9L)),
          Range.Bound.INCLUSIVE);
      table.scanDeleteAll(range);
    });
    // Verify the deletion
    Assert.assertEquals(expected, scanSimpleStructuredRows(Range.all(), max));

    // Delete 6  using a random column
    expected.subList(6, 7).clear();
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      Range range = Range.create(Arrays.asList(Fields.doubleField(DOUBLE_COL, 6.0)),
          Range.Bound.INCLUSIVE,
          Arrays.asList(Fields.doubleField(DOUBLE_COL, 6.0)),
          Range.Bound.INCLUSIVE);
      table.scanDeleteAll(range);
    });

    // Verify the deletion
    Assert.assertEquals(expected, scanSimpleStructuredRows(Range.all(), max));

    // Delete 2-5 (end exclusive) using the first key only
    expected.subList(2, 5).clear();
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      Range range = Range.create(Collections.singletonList(Fields.intField(KEY, 2)), Range.Bound.INCLUSIVE,
          Collections.singletonList(Fields.intField(KEY, 5)), Range.Bound.EXCLUSIVE);
      table.scanDeleteAll(range);
    });
    // Verify the deletion
    Assert.assertEquals(expected, scanSimpleStructuredRows(Range.all(), max));

    // Use a range outside the element list, nothing should get deleted
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      Range range = Range.create(Collections.singletonList(Fields.intField(KEY, max + 1)), Range.Bound.INCLUSIVE,
          Collections.singletonList(Fields.intField(KEY, max + 5)), Range.Bound.EXCLUSIVE);
      table.scanDeleteAll(range);
    });
    // Verify the deletion
    Assert.assertEquals(expected, scanSimpleStructuredRows(Range.all(), max));

    // Delete all the remaining
    expected.clear();
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      table.scanDeleteAll(Range.all());
    });
    // Verify the deletion
    Assert.assertEquals(expected, scanSimpleStructuredRows(Range.all(), max));
  }


  @Test
  public void testIndexScanIndexStringFieldType() throws Exception {
    int num = 5;
    // Write a few records
    List<Collection<Field<?>>> expected = new ArrayList<>();
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      int counter = 0;
      for (String value : Arrays.asList("abc", "def", "ghi")) {
        for (int i = 0; i < num; ++i) {
          Collection<Field<?>> keys = Arrays.asList(Fields.intField(KEY, counter),
                                                    Fields.longField(KEY2, counter * 100L),
                                                    Fields.stringField(KEY3, "key3"));
          Collection<Field<?>> fields = new ArrayList<>(keys);
          fields.add(Fields.stringField(STRING_COL, value));
          table.upsert(fields);
          expected.add(fields);
          ++counter;
        }
      }
    });

    List<String> columns = Arrays.asList(KEY, KEY2, KEY3, STRING_COL);

    // Verify write
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      try (CloseableIterator<StructuredRow> iterator = table.scan(Range.all(), 1000)) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected, rows);
      }
    });

    // Scan by index
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      try (CloseableIterator<StructuredRow> iterator = table.scan(Fields.stringField(STRING_COL, "def"))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected.subList(num, 2 * num), rows);
      }

      try (CloseableIterator<StructuredRow> iterator = table.scan(Fields.stringField(STRING_COL, "abc"))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected.subList(0, num), rows);
      }

      // non-existent index value
      try (CloseableIterator<StructuredRow> iterator = table.scan(Fields.stringField(STRING_COL, "non"))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
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
  public void testIndexScanBooleanFieldType() throws Exception {
    int num = 5;
    // Write a few records
    List<Collection<Field<?>>> expected = new ArrayList<>();
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      int counter = 0;
      for (Boolean value : Arrays.asList(false, true, false)) {
        for (int i = 0; i < num; ++i) {
          Collection<Field<?>> keys = Arrays.asList(Fields.intField(KEY, counter),
                                                    Fields.longField(KEY2, counter * 100L),
                                                    Fields.stringField(KEY3, "key3"));
          Collection<Field<?>> fields = new ArrayList<>(keys);
          fields.add(Fields.booleanField(BOOL_COL, value));
          table.upsert(fields);
          expected.add(fields);
          ++counter;
        }
      }
    });

    List<String> columns = Arrays.asList(KEY, KEY2, KEY3, BOOL_COL);

    // Verify write
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      try (CloseableIterator<StructuredRow> iterator = table.scan(Range.all(), 1000)) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected, rows);
      }
    });

    // Scan by index
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      try (CloseableIterator<StructuredRow> iterator = table.scan(Fields.booleanField(BOOL_COL, true))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected.subList(num, 2 * num), rows);
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
  public void testIndexScanWithRangeStringFieldType() throws Exception {
    int num = 100;
    // Write a few records
    List<Collection<Field<?>>> expected = new ArrayList<>();
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      int counter = 0;
      for (String value : Arrays.asList("abc", "def", "ghi")) {
        for (int i = 0; i < num; ++i) {
          Collection<Field<?>> fields = Arrays.asList(Fields.intField(KEY, counter),
                                                      Fields.longField(KEY2, counter * 100L),
                                                      Fields.stringField(KEY3, "key3"),
                                                      Fields.stringField(STRING_COL, value));
          table.upsert(fields);
          expected.add(fields);
          ++counter;
        }
      }
    });

    List<String> columns = Arrays.asList(KEY, KEY2, KEY3, STRING_COL);

    // Verify write
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      try (CloseableIterator<StructuredRow> iterator = table.scan(Range.all(), 1000)) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected, rows);
      }
    });

    // Scan by range and index
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      // Scan single index
      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, 0)), Range.Bound.INCLUSIVE,
                                     Collections.singleton(Fields.intField(KEY, num * 2)), Range.Bound.EXCLUSIVE),
                        num * 10,
                        Collections.singletonList(Fields.stringField(STRING_COL, "abc")))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected.subList(0, num), rows);
      }

      // Scan multiple indexes
      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, 0)), Range.Bound.INCLUSIVE,
                                     Collections.singleton(Fields.intField(KEY, num * 2)), Range.Bound.EXCLUSIVE),
                        num * 10,
                        Arrays.asList(Fields.stringField(STRING_COL, "abc"),
                                      Fields.stringField(STRING_COL, "def")))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected.subList(0, num * 2), rows);
      }

      // Scan multiple indexes with null values
      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, 0)), Range.Bound.INCLUSIVE,
                                     Collections.singleton(Fields.intField(KEY, num * 2)), Range.Bound.EXCLUSIVE),
                        num * 10,
                        Arrays.asList(Fields.stringField(STRING_COL, "abc"),
                                      Fields.longField(IDX_COL, null)))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected.subList(0, num * 2), rows);
      }

      // Partial primary keys range
      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, 0)),
                                     Range.Bound.INCLUSIVE,
                                     Arrays.asList(Fields.intField(KEY, num * 2),
                                                   Fields.longField(KEY2, num * 200L)),
                                     Range.Bound.EXCLUSIVE),
                        num * 10,
                        Collections.singletonList(Fields.stringField(STRING_COL, "abc")))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected.subList(0, num), rows);
      }

      // Partial primary keys range with multiple indexes
      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, 0)),
                                     Range.Bound.INCLUSIVE,
                                     Arrays.asList(Fields.intField(KEY, num * 2),
                                                   Fields.longField(KEY2, num * 200L)),
                                     Range.Bound.EXCLUSIVE),
                        num * 10,
                        Arrays.asList(Fields.stringField(STRING_COL, "abc"),
                                      Fields.stringField(STRING_COL, "def")))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected.subList(0, num * 2), rows);
      }

      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, num)), Range.Bound.INCLUSIVE,
                                     Collections.singleton(Fields.intField(KEY, num * 2)), Range.Bound.EXCLUSIVE),
                        num * 10,
                        Collections.singletonList(Fields.stringField(STRING_COL, "def")))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected.subList(num, 2 * num), rows);
      }

      // index value not within range
      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, 0)), Range.Bound.INCLUSIVE,
                                     Collections.singleton(Fields.intField(KEY, num)), Range.Bound.EXCLUSIVE),
                        num * 10,
                        Collections.singletonList(Fields.stringField(STRING_COL, "def")))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(Collections.emptyList(), rows);
      }

      // non-existent index value
      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, num)), Range.Bound.INCLUSIVE,
                                     Collections.singleton(Fields.intField(KEY, num * 2)), Range.Bound.EXCLUSIVE),
                        num * 10,
                        Collections.singletonList(Fields.stringField(STRING_COL, "non")))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(Collections.emptyList(), rows);
      }

      // non-index column
      try {
        table.scan(Range.create(Collections.singleton(Fields.intField(KEY, num)), Range.Bound.INCLUSIVE,
                                Collections.singleton(Fields.intField(KEY, num * 2)), Range.Bound.EXCLUSIVE),
                   num * 10,
                   Collections.singletonList(Fields.longField(LONG_COL, 1L)));
        Assert.fail("Expected InvalidFieldException for scanning a non-index column");
      } catch (InvalidFieldException e) {
        // Expected
      }
    });
  }

  @Test
  public void testIndexScanWithRangeBooleanFieldType() throws Exception {
    int num = 100;
    // Write a few records
    List<Collection<Field<?>>> expected = new ArrayList<>();
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      int counter = 0;
      for (Boolean value : Arrays.asList(false, true, false)) {
        for (int i = 0; i < num; ++i) {
          Collection<Field<?>> fields = Arrays.asList(Fields.intField(KEY, counter),
                                                      Fields.longField(KEY2, counter * 100L),
                                                      Fields.stringField(KEY3, "key3"),
                                                      Fields.booleanField(BOOL_COL, value));
          table.upsert(fields);
          expected.add(fields);
          ++counter;
        }
      }
    });

    List<String> columns = Arrays.asList(KEY, KEY2, KEY3, BOOL_COL);

    // Verify write
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      try (CloseableIterator<StructuredRow> iterator = table.scan(Range.all(), 1000)) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected, rows);
      }
    });

    // Scan by range and index
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      // Scan single index
      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, 0)), Range.Bound.INCLUSIVE,
                                     Collections.singleton(Fields.intField(KEY, num * 2)), Range.Bound.EXCLUSIVE),
                        num * 10,
                        Collections.singletonList(Fields.booleanField(BOOL_COL, true)))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected.subList(num, 2 * num), rows);
      }

      // Scan multiple indexes
      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, 0)), Range.Bound.INCLUSIVE,
                                     Collections.singleton(Fields.intField(KEY, num * 2)), Range.Bound.EXCLUSIVE),
                        num * 10,
                        Arrays.asList(Fields.booleanField(BOOL_COL, true),
                                      Fields.booleanField(BOOL_COL, false)))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected.subList(0, 2 * num), rows);
      }

      // non-existent index value
      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, 0)), Range.Bound.INCLUSIVE,
                                     Collections.singleton(Fields.intField(KEY, num * 2)), Range.Bound.EXCLUSIVE),
                        num * 10,
                        Arrays.asList(Fields.booleanField(BOOL_COL, null)))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(0, rows.size());
      }

      // Partial primary keys range
      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, 0)),
                                     Range.Bound.INCLUSIVE,
                                     Arrays.asList(Fields.intField(KEY, num * 2),
                                                   Fields.longField(KEY2, num * 200L)),
                                     Range.Bound.EXCLUSIVE),
                        num * 10,
                        Collections.singletonList(Fields.booleanField(BOOL_COL, true)))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected.subList(num, 2 * num), rows);
      }

      // Partial primary keys range with multiple indexes
      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, 0)),
                                     Range.Bound.INCLUSIVE,
                                     Arrays.asList(Fields.intField(KEY, num * 2),
                                                   Fields.longField(KEY2, num * 200L)),
                                     Range.Bound.EXCLUSIVE),
                        num * 10,
                        Arrays.asList(Fields.booleanField(BOOL_COL, true),
                                      Fields.booleanField(BOOL_COL, false)))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected.subList(0, num * 2), rows);
      }

      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, num)), Range.Bound.INCLUSIVE,
                                     Collections.singleton(Fields.intField(KEY, num * 2)), Range.Bound.EXCLUSIVE),
                        num * 10,
                        Collections.singletonList(Fields.booleanField(BOOL_COL, true)))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected.subList(num, 2 * num), rows);
      }

      // index value not within range
      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, 0)), Range.Bound.INCLUSIVE,
                                     Collections.singleton(Fields.intField(KEY, num)), Range.Bound.EXCLUSIVE),
                        num * 10,
                        Collections.singletonList(Fields.booleanField(BOOL_COL, true)))) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(Collections.emptyList(), rows);
      }

      // non-index column
      try {
        table.scan(Range.create(Collections.singleton(Fields.intField(KEY, num)), Range.Bound.INCLUSIVE,
                                Collections.singleton(Fields.intField(KEY, num * 2)), Range.Bound.EXCLUSIVE),
                   num * 10,
                   Collections.singletonList(Fields.longField(LONG_COL, 1L)));
        Assert.fail("Expected InvalidFieldException for scanning a non-index column");
      } catch (InvalidFieldException e) {
        // Expected
      }
    });
  }

  @Test
  public void testSortedIndexScan() throws Exception {
    int num = 100;
    // Write a few records
    List<Collection<Field<?>>> expected = new ArrayList<>();

    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      int counter = 0;
      for (int i = 0; i < num * 3; ++i) {
        // insert data ascending by primary keys and descending by IDX_COL
        Collection<Field<?>> fields = Arrays.asList(Fields.intField(KEY, counter),
                                                    Fields.longField(KEY2, counter * 100L),
                                                    Fields.stringField(KEY3, "key3"),
                                                    Fields.longField(IDX_COL, (long) -i));
        table.upsert(fields);
        expected.add(fields);
        ++counter;
      }
    });

    List<String> columns = Arrays.asList(KEY, KEY2, KEY3, IDX_COL);

    // Verify write
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      try (CloseableIterator<StructuredRow> iterator = table.scan(Range.all(), num * 10)) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected, rows);
      }
    });

    // Scan by range and sort by index
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);

      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, 0)), Range.Bound.INCLUSIVE,
                                     Collections.singleton(Fields.intField(KEY, num * 2)), Range.Bound.EXCLUSIVE),
                        num * 10, IDX_COL, SortOrder.DESC)) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        List<Collection<Field<?>>> expectedSubList = expected.subList(0, num * 2);

        Assert.assertEquals(expectedSubList, rows);
      }

      // Partial primary keys range
      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, 0)),
                                     Range.Bound.INCLUSIVE,
                                     Arrays.asList(Fields.intField(KEY, num * 2),
                                                   Fields.longField(KEY2, num * 200L)),
                                     Range.Bound.EXCLUSIVE),
                        num * 10, IDX_COL, SortOrder.DESC)) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        List<Collection<Field<?>>> expectedSubList = expected.subList(0, num * 2);

        Assert.assertEquals(expectedSubList, rows);
      }

      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, num)), Range.Bound.INCLUSIVE,
                                     Collections.singleton(Fields.intField(KEY, num * 3)), Range.Bound.EXCLUSIVE),
                        num * 10, IDX_COL, SortOrder.ASC)) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        List<Collection<Field<?>>> expectedSubList = expected.subList(num, num * 3);
        Collections.reverse(expectedSubList);

        Assert.assertEquals(expectedSubList, rows);
      }

      // non-index column
      try {
        table.scan(Range.create(Collections.singleton(Fields.intField(KEY, num)), Range.Bound.INCLUSIVE,
                                Collections.singleton(Fields.intField(KEY, num * 2)), Range.Bound.EXCLUSIVE),
                   num * 10, LONG_COL, SortOrder.ASC);
        Assert.fail("Expected InvalidFieldException for scanning a non-index column");
      } catch (InvalidFieldException e) {
        // Expected
      }
    });
  }

  @Test
  public void testSortedPrimaryKeyFilteredIndexScan() throws Exception {
    int num = 100;
    // Write a few records
    List<Collection<Field<?>>> expected = new ArrayList<>();

    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      int counter = 0;
      for (int i = 0; i < num * 3; ++i) {
        Collection<Field<?>> fields = Arrays.asList(Fields.intField(KEY, counter),
                                                    Fields.longField(KEY2, counter * 100L),
                                                    Fields.stringField(KEY3, "key3"),
                                                    Fields.longField(IDX_COL, (long) 100));
        table.upsert(fields);
        expected.add(fields);
        ++counter;
      }
    });
    List<String> columns = Arrays.asList(KEY, KEY2, KEY3, IDX_COL);

    // Verify write
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      try (CloseableIterator<StructuredRow> iterator = table.scan(Range.all(), num * 10)) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        Assert.assertEquals(expected, rows);
      }
    });

    // Scan by keyRange and filter on index
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);

      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, num)), Range.Bound.INCLUSIVE,
                                     Collections.singleton(Fields.intField(KEY, num * 3)), Range.Bound.EXCLUSIVE),
                        num * 10,
                        Collections.singletonList(Fields.longField(IDX_COL, (long) 100)),
                        SortOrder.ASC)) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        List<Collection<Field<?>>> expectedSubList = expected.subList(num, num * 3);

        Assert.assertEquals(expectedSubList, rows);
      }

      try (CloseableIterator<StructuredRow> iterator =
             table.scan(Range.create(Collections.singleton(Fields.intField(KEY, 0)), Range.Bound.INCLUSIVE,
                                     Collections.singleton(Fields.intField(KEY, num * 2)), Range.Bound.EXCLUSIVE),
                        num * 10,
                        Collections.singletonList(Fields.longField(IDX_COL, (long) 100)),
                        SortOrder.DESC)) {
        List<Collection<Field<?>>> rows = convertRowsToFields(iterator, columns);
        List<Collection<Field<?>>> expectedSubList = expected.subList(0, num * 2);
        Collections.reverse(expectedSubList);

        Assert.assertEquals(expectedSubList, rows);
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
                                               // Intentionally create overlapping range
                                               Range.from(Collections.singletonList(Fields.intField(KEY, 0)),
                                                          Range.Bound.EXCLUSIVE));
      Assert.assertEquals(max, table.count(ranges));

      // Partial primary keys range
      ranges = Arrays.asList(Range.to(Collections.singletonList(Fields.intField(KEY, 4)),
                                      Range.Bound.INCLUSIVE),
                             // Intentionally create overlapping range
                             Range.from(Collections.singletonList(Fields.intField(KEY, 0)),
                                        Range.Bound.EXCLUSIVE));
      Assert.assertEquals(max, table.count(ranges));
    });
  }

  @Test
  public void testUpdate() throws Exception {
    List<Field<?>> fields = Arrays.asList(Fields.intField(KEY, 1),
                                          Fields.longField(KEY2, 2L),
                                          Fields.stringField(KEY3, "key3"),
                                          Fields.stringField(STRING_COL, "str1"),
                                          Fields.doubleField(DOUBLE_COL, (double) 1.0),
                                          Fields.floatField(FLOAT_COL, (float) 1.0),
                                          Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes")));
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      table.upsert(fields);
    });

    List<Field<?>> updates = Arrays.asList(Fields.intField(KEY, 1),
                                           Fields.longField(KEY2, 2L),
                                           Fields.stringField(KEY3, "key3"),
                                           Fields.stringField(STRING_COL, "str2"),
                                           Fields.floatField(FLOAT_COL, (float) 2.0));
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      table.update(updates);
    });
    List<Field<?>> compoundKey = new ArrayList<>(3);
    compoundKey.add(Fields.intField(KEY, 1));
    compoundKey.add(Fields.longField(KEY2, 2L));
    compoundKey.add(Fields.stringField(KEY3, "key3"));
    AtomicReference<Optional<StructuredRow>> rowRef = new AtomicReference<>();

    List<String> columns = new ArrayList<>();
    columns.add(STRING_COL);
    columns.add(DOUBLE_COL);
    columns.add(FLOAT_COL);
    columns.add(BYTES_COL);
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      rowRef.set(table.read(compoundKey, columns));
    });

    Optional<StructuredRow> row = rowRef.get();
    Assert.assertTrue(row.isPresent());
    List<Field<?>> updatedFields = convertRowToFields(row.get(),
                                                      fields.stream().map(Field::getName).collect(Collectors.toList()));
    List<Field<?>> expectedFields = Arrays.asList(Fields.intField(KEY, 1),
                                                  Fields.longField(KEY2, 2L),
                                                  Fields.stringField(KEY3, "key3"),
                                                  Fields.stringField(STRING_COL, "str2"),
                                                  Fields.doubleField(DOUBLE_COL, (double) 1.0),
                                                  Fields.floatField(FLOAT_COL, (float) 2.0),
                                                  Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes")));
    Assert.assertEquals(new HashSet<>(updatedFields), new HashSet<>(expectedFields));
  }

  @Test
  public void testUpdateAll() throws Exception {
    int max = 10;
    List<Collection<Field<?>>> expected = writeSimpleStructuredRows(max, "");
    List<Collection<Field<?>>> beforeUpdateSublist = expected.subList(6, 8);

    List<Field<?>> expectedFields1 = Arrays.asList(Fields.intField(KEY, 6),
                                                  Fields.longField(KEY2, 6L),
                                                  Fields.stringField(KEY3, "key3"),
                                                  Fields.stringField(STRING_COL, "val6"),
                                                  Fields.doubleField(DOUBLE_COL, (double) 6.0),
                                                  Fields.floatField(FLOAT_COL, (float) 6.0),
                                                  Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-6")));
    List<Field<?>> expectedFields2 = Arrays.asList(Fields.intField(KEY, 7),
                                                   Fields.longField(KEY2, 7L),
                                                   Fields.stringField(KEY3, "key3"),
                                                   Fields.stringField(STRING_COL, "val7"),
                                                   Fields.doubleField(DOUBLE_COL, (double) 7.0),
                                                   Fields.floatField(FLOAT_COL, (float) 7.0),
                                                   Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-7")));
    Assert.assertEquals(new HashSet<>(beforeUpdateSublist.get(0)), new HashSet<>(expectedFields1));
    Assert.assertEquals(new HashSet<>(beforeUpdateSublist.get(1)), new HashSet<>(expectedFields2));

    List<Field<?>> updates = Arrays.asList(Fields.stringField(STRING_COL, "val0"),
                                           Fields.floatField(FLOAT_COL, (float) 0.0));
    Range range = Range.create(Arrays.asList(Fields.intField(KEY, 6), Fields.longField(KEY2, 6L)),
                               Range.Bound.INCLUSIVE,
                               Arrays.asList(Fields.intField(KEY, 8), Fields.longField(KEY2, 8L)),
                               Range.Bound.EXCLUSIVE);
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      table.updateAll(range, updates);
    });


    expectedFields1 = Arrays.asList(Fields.intField(KEY, 6),
                                    Fields.longField(KEY2, 6L),
                                    Fields.stringField(KEY3, "key3"),
                                    Fields.stringField(STRING_COL, "val0"),
                                    Fields.doubleField(DOUBLE_COL, (double) 6.0),
                                    Fields.floatField(FLOAT_COL, (float) 0.0),
                                    Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-6")));

    expectedFields2 = Arrays.asList(Fields.intField(KEY, 7),
                                                   Fields.longField(KEY2, 7L),
                                                   Fields.stringField(KEY3, "key3"),
                                                   Fields.stringField(STRING_COL, "val0"),
                                                   Fields.doubleField(DOUBLE_COL, (double) 7.0),
                                                   Fields.floatField(FLOAT_COL, (float) 0.0),
                                                   Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-7")));

    List<Field<?>> expectedFields3 = Arrays.asList(Fields.intField(KEY, 2),
                                                   Fields.longField(KEY2, 2L),
                                                   Fields.stringField(KEY3, "key3"),
                                                   Fields.stringField(STRING_COL, "val2"),
                                                   Fields.doubleField(DOUBLE_COL, (double) 2.0),
                                                   Fields.floatField(FLOAT_COL, (float) 2.0),
                                                   Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-2")));

    // Verify the updated rows
    List<Collection<Field<?>>> actualRows = scanSimpleStructuredRows(Range.all(), max);
    List<Collection<Field<?>>> updatedRows = actualRows.subList(6, 8);
    Assert.assertEquals(new HashSet<>(expectedFields1), new HashSet<>(updatedRows.get(0)));
    Assert.assertEquals(new HashSet<>(expectedFields2), new HashSet<>(updatedRows.get(1)));
    // Verify other rows are not updated
    Assert.assertEquals(new HashSet<>(expectedFields3), new HashSet<>(actualRows.get(2)));
  }

  @Test
  public void testUpdateAllWithNullColumn() throws Exception {
    int max = 10;
    List<Collection<Field<?>>> expected = writeSimpleStructuredRowsWithNullColumn(max, "");
    List<Collection<Field<?>>> beforeUpdateSublist = expected.subList(6, 8);

    List<Field<?>> expectedFields1 = Arrays.asList(Fields.intField(KEY, 6),
                                                   Fields.longField(KEY2, 6L),
                                                   Fields.stringField(KEY3, "key3"),
                                                   Fields.doubleField(DOUBLE_COL, (double) 6.0),
                                                   Fields.floatField(FLOAT_COL, (float) 6.0),
                                                   Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-6")));
    List<Field<?>> expectedFields2 = Arrays.asList(Fields.intField(KEY, 7),
                                                   Fields.longField(KEY2, 7L),
                                                   Fields.stringField(KEY3, "key3"),
                                                   Fields.doubleField(DOUBLE_COL, (double) 7.0),
                                                   Fields.floatField(FLOAT_COL, (float) 7.0),
                                                   Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-7")));
    Assert.assertEquals(new HashSet<>(beforeUpdateSublist.get(0)), new HashSet<>(expectedFields1));
    Assert.assertEquals(new HashSet<>(beforeUpdateSublist.get(1)), new HashSet<>(expectedFields2));

    List<Field<?>> updates = Arrays.asList(Fields.stringField(STRING_COL, "val0"),
                                           Fields.floatField(FLOAT_COL, (float) 0.0));
    Range range = Range.create(Arrays.asList(Fields.intField(KEY, 6), Fields.longField(KEY2, 6L)),
                               Range.Bound.INCLUSIVE,
                               Arrays.asList(Fields.intField(KEY, 8), Fields.longField(KEY2, 8L)),
                               Range.Bound.EXCLUSIVE);
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      table.updateAll(range, updates);
    });


    expectedFields1 = Arrays.asList(Fields.intField(KEY, 6),
                                    Fields.longField(KEY2, 6L),
                                    Fields.stringField(KEY3, "key3"),
                                    Fields.stringField(STRING_COL, "val0"),
                                    Fields.doubleField(DOUBLE_COL, (double) 6.0),
                                    Fields.floatField(FLOAT_COL, (float) 0.0),
                                    Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-6")));

    expectedFields2 = Arrays.asList(Fields.intField(KEY, 7),
                                    Fields.longField(KEY2, 7L),
                                    Fields.stringField(KEY3, "key3"),
                                    Fields.stringField(STRING_COL, "val0"),
                                    Fields.doubleField(DOUBLE_COL, (double) 7.0),
                                    Fields.floatField(FLOAT_COL, (float) 0.0),
                                    Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-7")));

    List<Field<?>> expectedFields3 = Arrays.asList(Fields.intField(KEY, 2),
                                                   Fields.longField(KEY2, 2L),
                                                   Fields.stringField(KEY3, "key3"),
                                                   Fields.stringField(STRING_COL, null),
                                                   Fields.doubleField(DOUBLE_COL, (double) 2.0),
                                                   Fields.floatField(FLOAT_COL, (float) 2.0),
                                                   Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-2")));

    // Verify the updated rows
    List<Collection<Field<?>>> actualRows = scanSimpleStructuredRows(Range.all(), max);
    List<Collection<Field<?>>> updatedRows = actualRows.subList(6, 8);
    Assert.assertEquals(new HashSet<>(expectedFields1), new HashSet<>(updatedRows.get(0)));
    Assert.assertEquals(new HashSet<>(expectedFields2), new HashSet<>(updatedRows.get(1)));
    // Verify other rows are not updated
    Assert.assertEquals(new HashSet<>(expectedFields3), new HashSet<>(actualRows.get(2)));
  }

  @Test(expected = TransactionException.class)
  public void testUpdateAllUpdatePrimaryKey() throws Exception {
    int max = 10;
    List<Collection<Field<?>>> expected = writeSimpleStructuredRows(max, "");
    List<Collection<Field<?>>> beforeUpdateSublist = expected.subList(6, 8);

    List<Field<?>> expectedFields1 = Arrays.asList(Fields.intField(KEY, 6),
                                                   Fields.longField(KEY2, 6L),
                                                   Fields.stringField(KEY3, "key3"),
                                                   Fields.stringField(STRING_COL, "val6"),
                                                   Fields.doubleField(DOUBLE_COL, (double) 6.0),
                                                   Fields.floatField(FLOAT_COL, (float) 6.0),
                                                   Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-6")));
    List<Field<?>> expectedFields2 = Arrays.asList(Fields.intField(KEY, 7),
                                                   Fields.longField(KEY2, 7L),
                                                   Fields.stringField(KEY3, "key3"),
                                                   Fields.stringField(STRING_COL, "val7"),
                                                   Fields.doubleField(DOUBLE_COL, (double) 7.0),
                                                   Fields.floatField(FLOAT_COL, (float) 7.0),
                                                   Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-7")));
    Assert.assertEquals(new HashSet<>(beforeUpdateSublist.get(0)), new HashSet<>(expectedFields1));
    Assert.assertEquals(new HashSet<>(beforeUpdateSublist.get(1)), new HashSet<>(expectedFields2));

    List<Field<?>> updates = Arrays.asList(Fields.stringField(KEY3, "key0"),
                                           Fields.stringField(STRING_COL, "val0"),
                                           Fields.floatField(FLOAT_COL, (float) 0.0));
    Range range = Range.create(Arrays.asList(Fields.intField(KEY, 6), Fields.longField(KEY2, 6L)),
                               Range.Bound.INCLUSIVE,
                               Arrays.asList(Fields.intField(KEY, 8), Fields.longField(KEY2, 8L)),
                               Range.Bound.EXCLUSIVE);
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      table.updateAll(range, updates);
    });
  }

  @Test(expected = TransactionException.class)
  public void testUpdateAllRangeIsNotPrimaryKeyPrefix() throws Exception {
    int max = 10;
    List<Collection<Field<?>>> expected = writeSimpleStructuredRows(max, "");
    List<Collection<Field<?>>> beforeUpdateSublist = expected.subList(6, 8);

    List<Field<?>> expectedFields1 = Arrays.asList(Fields.intField(KEY, 6),
                                                   Fields.longField(KEY2, 6L),
                                                   Fields.stringField(KEY3, "key3"),
                                                   Fields.stringField(STRING_COL, "val6"),
                                                   Fields.doubleField(DOUBLE_COL, (double) 6.0),
                                                   Fields.floatField(FLOAT_COL, (float) 6.0),
                                                   Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-6")));
    List<Field<?>> expectedFields2 = Arrays.asList(Fields.intField(KEY, 7),
                                                   Fields.longField(KEY2, 7L),
                                                   Fields.stringField(KEY3, "key3"),
                                                   Fields.stringField(STRING_COL, "val7"),
                                                   Fields.doubleField(DOUBLE_COL, (double) 7.0),
                                                   Fields.floatField(FLOAT_COL, (float) 7.0),
                                                   Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes-7")));
    Assert.assertEquals(new HashSet<>(beforeUpdateSublist.get(0)), new HashSet<>(expectedFields1));
    Assert.assertEquals(new HashSet<>(beforeUpdateSublist.get(1)), new HashSet<>(expectedFields2));

    List<Field<?>> updates = Arrays.asList(Fields.stringField(KEY3, "key0"),
                                           Fields.stringField(STRING_COL, "val0"),
                                           Fields.floatField(FLOAT_COL, (float) 0.0));
    Range range = Range.create(Arrays.asList(Fields.intField(KEY, 6), Fields.doubleField(DOUBLE_COL, 6.0)),
                               Range.Bound.INCLUSIVE,
                               Arrays.asList(Fields.intField(KEY, 8), Fields.doubleField(DOUBLE_COL, 8.0)),
                               Range.Bound.EXCLUSIVE);
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      table.updateAll(range, updates);
    });
  }

  @Test(expected = TransactionException.class)
  public void testUpdatePrimaryKeyNotSpecified() throws Exception {
    List<Field<?>> fields = Arrays.asList(Fields.intField(KEY, 1),
                                          Fields.longField(KEY2, 2L),
                                          Fields.stringField(KEY3, "key3"),
                                          Fields.stringField(STRING_COL, "str1"),
                                          Fields.doubleField(DOUBLE_COL, (double) 1.0),
                                          Fields.floatField(FLOAT_COL, (float) 1.0),
                                          Fields.bytesField(BYTES_COL, Bytes.toBytes("bytes")));
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      table.upsert(fields);
    });

    // KEY2 is not specified, should raise TransactionException
    List<Field<?>> updates = Arrays.asList(Fields.intField(KEY, 1),
                                           Fields.floatField(FLOAT_COL, (float) 2.0));
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      table.update(updates);
    });
  }

  @Test
  public void testUpdateKeyNotFound() throws Exception {
    List<Field<?>> updates = Arrays.asList(Fields.intField(KEY, 1),
                                           Fields.longField(KEY2, 2L),
                                           Fields.stringField(KEY3, "key3"),
                                           Fields.floatField(FLOAT_COL, (float) 2.0));
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      table.update(updates);
    });

    List<Field<?>> compoundKey = new ArrayList<>(2);
    compoundKey.add(Fields.intField(KEY, 1));
    compoundKey.add(Fields.longField(KEY2, 2L));
    compoundKey.add(Fields.stringField(KEY3, "key3"));
    AtomicReference<Optional<StructuredRow>> rowRef = new AtomicReference<>();
    List<String> columns = new ArrayList<>();
    columns.add(STRING_COL);
    columns.add(DOUBLE_COL);
    columns.add(FLOAT_COL);
    columns.add(BYTES_COL);
    getTransactionRunner().run(context -> {
      StructuredTable table = context.getTable(SIMPLE_TABLE);
      rowRef.set(table.read(compoundKey, columns));
    });

    Optional<StructuredRow> row = rowRef.get();
    Assert.assertFalse(row.isPresent());
  }

  private List<Collection<Field<?>>> writeSimpleStructuredRows(int max, String suffix) throws Exception {
    List<Collection<Field<?>>> expected = new ArrayList<>(max);
    // Write rows in reverse order to test sorting
    for (int i = max - 1; i >= 0; i--) {
      List<Field<?>> fields = Arrays.asList(Fields.intField(KEY, i),
                                            Fields.longField(KEY2, (long) i),
                                            Fields.stringField(KEY3, "key3"),
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

  private List<Collection<Field<?>>> writeSimpleStructuredRowsWithNullColumn(int max, String suffix) throws Exception {
    List<Collection<Field<?>>> expected = new ArrayList<>(max);
    // Write rows in reverse order to test sorting
    for (int i = max - 1; i >= 0; i--) {
      List<Field<?>> fields = Arrays.asList(Fields.intField(KEY, i),
                                            Fields.longField(KEY2, (long) i),
                                            Fields.stringField(KEY3, "key3"),
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

  private List<Collection<Field<?>>> writeStructuredRowsWithDuplicatePrefix(int max, String suffix) throws Exception {
    List<Collection<Field<?>>> expected = new ArrayList<>(max);
    // Write rows in reverse order to test sorting
    for (int i = max - 1; i >= 0; i--) {
      // Adding rows that have same "KEY" every 10 elements
      List<Field<?>> fields = Arrays.asList(Fields.intField(KEY, i / 10),
                                            Fields.longField(KEY2, (long) i),
                                            Fields.stringField(KEY3, String.valueOf(i)),
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

  private List<Collection<Field<?>>> writeRowsWithDuplicateKeyPrefix(int max, String suffix)
    throws Exception {
    List<Collection<Field<?>>> expected = new ArrayList<>(max);
    // Write rows in reverse order to test sorting
    for (int i = max - 1; i >= 0; i--) {
      // Adding rows that have same "KEY" every 10 elements
      List<Field<?>> fields = Arrays.asList(Fields.intField(KEY, i % 10),
                                            Fields.longField(KEY2, (long) i),
                                            Fields.stringField(KEY3, "key3"),
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
    fields.add(KEY3);
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
      compoundKey.add(Fields.stringField(KEY3, "key3"));
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
        case BOOLEAN:
          fields.add(Fields.booleanField(name, row.getBoolean(name)));
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
      compoundKey.add(Fields.stringField(KEY3, "key3"));

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
                                   Fields.stringField(KEY3, row.getString(KEY3)),
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
