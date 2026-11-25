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

package io.cdap.cdap.data2.dataset2.lib.table;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.batch.RecordScanner;
import io.cdap.cdap.api.data.batch.Split;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.TableProperties;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryTable;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.PrefixedNamespaces;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import java.util.List;
import org.apache.tephra.Transaction;
import org.apache.tephra.inmemory.MinimalTxSystemClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AbstractTable} record scanning and {@code IOException} propagation.
 */
public class AbstractTableTest {

  private static final String TABLE_NAME = "abstractTableRecordScanTest";

  @After
  public void tearDown() {
    InMemoryTableService.reset();
  }

  @Test
  public void testStructuredRecordScannerHappyPathAndIoExceptionPropagation() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    String namespacedName =
        PrefixedNamespaces.namespace(cConf, NamespaceId.DEFAULT.getNamespace(), TABLE_NAME);
    InMemoryTableService.create(namespacedName);

    Schema schema = Schema.recordOf(
        "rowRecord",
        Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
        Schema.Field.of("count", Schema.of(Schema.Type.INT)));
    TableProperties.Builder propsBuilder = TableProperties.builder();
    TableProperties.setSchema(propsBuilder, schema);
    TableProperties.setRowFieldName(propsBuilder, "id");

    DatasetSpecification spec = DatasetSpecification.builder(TABLE_NAME, InMemoryTable.TYPE)
        .properties(propsBuilder.build().getProperties())
        .build();
    DatasetContext context = DatasetContext.from(NamespaceId.DEFAULT.getNamespace());

    MinimalTxSystemClient txClient = new MinimalTxSystemClient();
    InMemoryTable table = new InMemoryTable(context, spec, cConf);

    Transaction tx1 = txClient.startShort();
    table.startTx(tx1);
    table.write(StructuredRecord.builder(schema).set("id", "row1").set("count", 42).build());
    // Write a second row missing required non-nullable 'count' column so ReflectionRowRecordReader throws IOException
    table.put(new Put(Bytes.toBytes("row2")).add(Bytes.toBytes("other"), Bytes.toBytes("bad")));
    Assert.assertTrue(table.commitTx());
    txClient.commitOrThrow(tx1);
    table.postTxCommit();

    Transaction tx2 = txClient.startShort();
    table.startTx(tx2);
    List<Split> splits = table.getSplits(1, null, null);
    Assert.assertEquals(1, splits.size());

    RecordScanner<StructuredRecord> scanner = table.createSplitRecordScanner(splits.get(0));
    scanner.initialize(splits.get(0));
    try {
      Assert.assertTrue(scanner.nextRecord());
      StructuredRecord first = scanner.getCurrentRecord();
      Assert.assertEquals("row1", first.get("id"));
      Assert.assertEquals(Integer.valueOf(42), first.get("count"));

      Assert.assertTrue(scanner.nextRecord());
      try {
        scanner.getCurrentRecord();
        Assert.fail("Expected RuntimeException wrapping IOException from corrupt/missing column");
      } catch (RuntimeException actual) {
        Assert.assertTrue("Expected IOException cause, got: " + actual.getCause(),
            actual.getCause() instanceof IOException);
      }
    } finally {
      scanner.close();
      table.close();
    }
  }
}
