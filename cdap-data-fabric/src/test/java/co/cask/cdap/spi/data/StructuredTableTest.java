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

package co.cask.cdap.spi.data;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.spi.data.table.StructuredTableId;
import co.cask.cdap.spi.data.table.StructuredTableSpecification;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.Fields;
import co.cask.cdap.spi.data.table.field.Range;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is a test base for {@link StructuredTable}.
 * TODO: CDAP-14750 add more unit tests for the StructuredTable
 */
public abstract class StructuredTableTest {
  // TODO: test complex schema will all allowed data types
  protected static final StructuredTableSpecification SIMPLE_SPEC;

  private static final StructuredTableId SIMPLE_TABLE = new StructuredTableId("simpleTable");
  private static final String KEY = "key";
  private static final String KEY2 = "key2";
  private static final String COL1 = "col1";
  private static final String COL2 = "col2";
  private static final String COL3 = "col3";
  private static final String COL4 = "col4";
  private static final String VAL = "val";

  static {
    StructuredTableSpecification specification = null;
    try {
       specification = new StructuredTableSpecification.Builder()
        .withId(SIMPLE_TABLE)
        .withFields(Fields.intType(KEY), Fields.stringType(COL1), Fields.longType(KEY2),
                    Fields.doubleType(COL2), Fields.floatType(COL3), Fields.bytesType(COL4))
        .withPrimaryKeys(KEY, KEY2)
        .build();
    } catch (InvalidFieldException e) {
      // this should not happen
    }
    SIMPLE_SPEC = specification;
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
    actual = scanSimpleStructuredRows(
      Range.create(ImmutableList.of(Fields.intField(KEY, 1),
                                    Fields.longField(KEY2, 8L)), Range.Bound.INCLUSIVE,
                   ImmutableList.of(Fields.intField(KEY, 3),
                                    Fields.longField(KEY2, 3L)), Range.Bound.INCLUSIVE), max);
    Assert.assertEquals(expected.subList(2, 4), actual);

    // scan from (1, 8L) inclusive to (3, 3L) exclusive, should only return (2, 2L)
    actual = scanSimpleStructuredRows(
      Range.create(ImmutableList.of(Fields.intField(KEY, 1),
                                    Fields.longField(KEY2, 8L)), Range.Bound.INCLUSIVE,
                   ImmutableList.of(Fields.intField(KEY, 3),
                                    Fields.longField(KEY2, 3L)), Range.Bound.EXCLUSIVE), max);
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

  private List<Collection<Field<?>>> writeSimpleStructuredRows(int max, String suffix) throws Exception {
    List<Collection<Field<?>>> expected = new ArrayList<>(max);
    for (int i = 0; i < max; i++) {
      List<Field<?>> fields = Arrays.asList(Fields.intField(KEY, i),
                                            Fields.longField(KEY2, (long) i),
                                            Fields.stringField(COL1, VAL + i + suffix),
                                            Fields.doubleField(COL2, (double) i),
                                            Fields.floatField(COL3, (float) i),
                                            Fields.bytesField(COL4, Bytes.toBytes("bytes-" + i)));
      expected.add(fields);

      getTransactionRunner().run(context -> {
        StructuredTable table = context.getTable(SIMPLE_TABLE);
        table.upsert(fields);
      });
    }
    return expected;
  }

  private List<Collection<Field<?>>> readSimpleStructuredRows(int max) throws Exception {
    List<Collection<Field<?>>> actual = new ArrayList<>(max);
    for (int i = 0; i < max; i++) {
      Field<Integer> key = Fields.intField(KEY, i);
      Field<Long> key2 = Fields.longField(KEY2, (long) i);
      final AtomicReference<Optional<StructuredRow>> rowRef = new AtomicReference<>();

      getTransactionRunner().run(context -> {
        StructuredTable table = context.getTable(SIMPLE_TABLE);
        rowRef.set(table.read(ImmutableList.of(key, key2), ImmutableList.of(COL1, COL2, COL3, COL4)));
      });

      Optional<StructuredRow> row = rowRef.get();
      row.ifPresent(
        structuredRow -> actual.add(Arrays.asList(Fields.intField(KEY, structuredRow.getInteger(KEY)),
                                                  Fields.longField(KEY2, structuredRow.getLong(KEY2)),
                                                  Fields.stringField(COL1, structuredRow.getString(COL1)),
                                                  Fields.doubleField(COL2, structuredRow.getDouble(COL2)),
                                                  Fields.floatField(COL3, structuredRow.getFloat(COL3)),
                                                  Fields.bytesField(COL4, structuredRow.getBytes(COL4)))));
    }
    return actual;
  }

  private void deleteSimpleStructuredRows(int max) throws Exception {
    for (int i = 0; i < max; i++) {
      Field<Integer> key = Fields.intField(KEY, i);
      Field<Long> key2 = Fields.longField(KEY2, (long) i);

      getTransactionRunner().run(context -> {
        StructuredTable table = context.getTable(SIMPLE_TABLE);
        table.delete(ImmutableList.of(key, key2));
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
                                   Fields.stringField(COL1, row.getString(COL1)),
                                   Fields.doubleField(COL2, row.getDouble(COL2)),
                                   Fields.floatField(COL3, row.getFloat(COL3)),
                                   Fields.bytesField(COL4, row.getBytes(COL4))));
        }
      }
    });
    return actual;
  }
}
