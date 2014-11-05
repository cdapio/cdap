/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.explore.service;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.TableInfo;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.Transaction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Painfully test a wide combination of types in a schema of a record scannable.
 */
@Category(SlowTests.class)
public class ExploreExtensiveSchemaTableTest extends BaseHiveExploreServiceTest {

  @BeforeClass
  public static void start() throws Exception {
    startServices(CConfiguration.create());

    datasetFramework.addModule("extensiveSchema", new ExtensiveSchemaTableDefinition.ExtensiveSchemaTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("ExtensiveSchemaTable", "my_table", DatasetProperties.EMPTY);

    // Accessing dataset instance to perform data operations
    ExtensiveSchemaTableDefinition.ExtensiveSchemaTable table =
      datasetFramework.getDataset("my_table", DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(table);

    Transaction tx1 = transactionManager.startShort(100);
    table.startTx(tx1);

    ExtensiveSchemaTableDefinition.ExtensiveSchema value1 =
      new ExtensiveSchemaTableDefinition.ExtensiveSchema(
        "foo", 1, 1.23f, 2.45d, (long) 1000, (byte) 100, true, (short) 8, new int[] { 10, 11 },
        new float[] { 1.67f, 2.89f }, new double[] { 10.56d, 8.78d }, new long [] { 101, 201 },
        new byte[] { 106, 110 }, new boolean[] { true, false }, new short[] { 50, 51 }, new String[] { "foo", "bar" },
        ImmutableList.of(10, 20), ImmutableList.of(10.45f, 20.98f), ImmutableList.of(10.99d, 20.90d),
        ImmutableList.of((long) 654, (long) 2897), ImmutableList.of((byte) 22, (byte) 23), ImmutableList.of(true, true),
        ImmutableList.of((short) 76, (short) 39), ImmutableList.of("foo2", "bar2"), ImmutableMap.of("foo3", 51),
        ImmutableMap.of(3.55f, 51.98d), ImmutableMap.of((long) 890, (byte) 45), ImmutableMap.of(true, (short) 27),
        new ExtensiveSchemaTableDefinition.Value("foo", 2),
        new ExtensiveSchemaTableDefinition.Value[] {
          new ExtensiveSchemaTableDefinition.Value("bar", 3), new ExtensiveSchemaTableDefinition.Value("foobar", 4)
        }, ImmutableList.of(new ExtensiveSchemaTableDefinition.Value("foobar2", 3)),
        ImmutableMap.of("key", new ExtensiveSchemaTableDefinition.Value("foobar3", 9)));
    value1.setExt(value1);
    table.put("1", value1);

    Assert.assertTrue(table.commitTx());

    transactionManager.canCommit(tx1, table.getTxChanges());
    transactionManager.commit(tx1);

    table.postTxCommit();

    Transaction tx2 = transactionManager.startShort(100);
    table.startTx(tx2);
  }

  @AfterClass
  public static void stop() throws Exception {
    datasetFramework.deleteInstance("my_table");
    datasetFramework.deleteModule("extensiveSchema");
  }

  @Test
  public void testExtensiveSchema() throws Exception {
    runCommand("show tables",
               true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(ImmutableList.of(QueryResult.ResultObject.of("my_table")))));

    runCommand("describe my_table",
               true, 100,
               Lists.newArrayList(
                 new ColumnDesc("col_name", "STRING", 1, "from deserializer"),
                 new ColumnDesc("data_type", "STRING", 2, "from deserializer"),
                 new ColumnDesc("comment", "STRING", 3, "from deserializer")
               ),
               Lists.newArrayList(
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("s"),
                   QueryResult.ResultObject.of("string"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("i"),
                   QueryResult.ResultObject.of("int"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("f"),
                   QueryResult.ResultObject.of("float"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("d"),
                   QueryResult.ResultObject.of("double"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("l"),
                   QueryResult.ResultObject.of("bigint"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("b"),
                   QueryResult.ResultObject.of("tinyint"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("bo"),
                   QueryResult.ResultObject.of("boolean"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("sh"),
                   QueryResult.ResultObject.of("smallint"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 // note: hive removes upper cases
                 // Arrays
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("iarr"),
                   QueryResult.ResultObject.of("array<int>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("farr"),
                   QueryResult.ResultObject.of("array<float>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("darr"),
                   QueryResult.ResultObject.of("array<double>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("larr"),
                   QueryResult.ResultObject.of("array<bigint>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("barr"),
                   QueryResult.ResultObject.of("binary"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("boarr"),
                   QueryResult.ResultObject.of("array<boolean>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("sharr"),
                   QueryResult.ResultObject.of("array<smallint>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("sarr"),
                   QueryResult.ResultObject.of("array<string>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 // Lists
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("ilist"),
                   QueryResult.ResultObject.of("array<int>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("flist"),
                   QueryResult.ResultObject.of("array<float>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("dlist"),
                   QueryResult.ResultObject.of("array<double>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("llist"),
                   QueryResult.ResultObject.of("array<bigint>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 // Note: list<byte> becomes array<tinyint>, whereas byte[] becomes binary...
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("blist"),
                   QueryResult.ResultObject.of("array<tinyint>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("bolist"),
                   QueryResult.ResultObject.of("array<boolean>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("shlist"),
                   QueryResult.ResultObject.of("array<smallint>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("slist"),
                   QueryResult.ResultObject.of("array<string>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 // Maps
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("stoimap"),
                   QueryResult.ResultObject.of("map<string,int>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("ftodmap"),
                   QueryResult.ResultObject.of("map<float,double>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("ltobmap"),
                   QueryResult.ResultObject.of("map<bigint,tinyint>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("botoshmap"),
                   QueryResult.ResultObject.of("map<boolean,smallint>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 // Custom type
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("v"),
                   QueryResult.ResultObject.of("struct<s:string,i:int>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("varr"),
                   QueryResult.ResultObject.of("array<struct<s:string,i:int>>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("vlist"),
                   QueryResult.ResultObject.of("array<struct<s:string,i:int>>"),
                   QueryResult.ResultObject.of("from deserializer")
                 )),
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("stovmap"),
                   QueryResult.ResultObject.of("map<string,struct<s:string,i:int>>"),
                   QueryResult.ResultObject.of("from deserializer")
                 ))
               )
    );

    runCommand("select * from my_table",
               true, 100,
               Lists.newArrayList(new ColumnDesc("my_table.s", "STRING", 1, null),
                                  new ColumnDesc("my_table.i", "INT", 2, null),
                                  new ColumnDesc("my_table.f", "FLOAT", 3, null),
                                  new ColumnDesc("my_table.d", "DOUBLE", 4, null),
                                  new ColumnDesc("my_table.l", "BIGINT", 5, null),
                                  new ColumnDesc("my_table.b", "TINYINT", 6, null),
                                  new ColumnDesc("my_table.bo", "BOOLEAN", 7, null),
                                  new ColumnDesc("my_table.sh", "SMALLINT", 8, null),
                                  // Arrays
                                  new ColumnDesc("my_table.iarr", "array<int>", 9, null),
                                  new ColumnDesc("my_table.farr", "array<float>", 10, null),
                                  new ColumnDesc("my_table.darr", "array<double>", 11, null),
                                  new ColumnDesc("my_table.larr", "array<bigint>", 12, null),
                                  new ColumnDesc("my_table.barr", "BINARY", 13, null),
                                  new ColumnDesc("my_table.boarr", "array<boolean>", 14, null),
                                  new ColumnDesc("my_table.sharr", "array<smallint>", 15, null),
                                  new ColumnDesc("my_table.sarr", "array<string>", 16, null),
                                  // Lists
                                  new ColumnDesc("my_table.ilist", "array<int>", 17, null),
                                  new ColumnDesc("my_table.flist", "array<float>", 18, null),
                                  new ColumnDesc("my_table.dlist", "array<double>", 19, null),
                                  new ColumnDesc("my_table.llist", "array<bigint>", 20, null),
                                  new ColumnDesc("my_table.blist", "array<tinyint>", 21, null),
                                  new ColumnDesc("my_table.bolist", "array<boolean>", 22, null),
                                  new ColumnDesc("my_table.shlist", "array<smallint>", 23, null),
                                  new ColumnDesc("my_table.slist", "array<string>", 24, null),
                                  // Maps
                                  new ColumnDesc("my_table.stoimap", "map<string,int>", 25, null),
                                  new ColumnDesc("my_table.ftodmap", "map<float,double>", 26, null),
                                  new ColumnDesc("my_table.ltobmap", "map<bigint,tinyint>", 27, null),
                                  new ColumnDesc("my_table.botoshmap",
                                                 "map<boolean,smallint>", 28, null),
                                  // Custom type
                                  new ColumnDesc("my_table.v", "struct<s:string,i:int>", 29, null),
                                  new ColumnDesc("my_table.varr", "array<struct<s:string,i:int>>",
                                                 30, null),
                                  new ColumnDesc("my_table.vlist", "array<struct<s:string,i:int>>",
                                                 31, null),
                                  new ColumnDesc("my_table.stovmap",
                                                 "map<string,struct<s:string,i:int>>", 32, null)
               ),
               Lists.newArrayList(
                 new QueryResult(ImmutableList.of(
                   QueryResult.ResultObject.of("foo"),
                   QueryResult.ResultObject.of(1),
                   QueryResult.ResultObject.of(1.23),
                   QueryResult.ResultObject.of(2.45),
                   QueryResult.ResultObject.of((long) 1000),
                   QueryResult.ResultObject.of((byte) 100),
                   QueryResult.ResultObject.of(true),
                   QueryResult.ResultObject.of((short) 8),
                   QueryResult.ResultObject.of("[10,11]"),
                   QueryResult.ResultObject.of("[1.67,2.89]"),
                   QueryResult.ResultObject.of("[10.56,8.78]"),
                   QueryResult.ResultObject.of("[101,201]"),
                   QueryResult.ResultObject.of(new byte[] { 106, 110 }),
                   QueryResult.ResultObject.of("[true,false]"),
                   QueryResult.ResultObject.of("[50,51]"),
                   QueryResult.ResultObject.of("[\"foo\",\"bar\"]"),
                   QueryResult.ResultObject.of("[10,20]"),
                   QueryResult.ResultObject.of("[10.45,20.98]"),
                   QueryResult.ResultObject.of("[10.99,20.9]"),
                   QueryResult.ResultObject.of("[654,2897]"),
                   QueryResult.ResultObject.of("[22,23]"),
                   QueryResult.ResultObject.of("[true,true]"),
                   QueryResult.ResultObject.of("[76,39]"),
                   QueryResult.ResultObject.of("[\"foo2\",\"bar2\"]"),
                   QueryResult.ResultObject.of("{\"foo3\":51}"),
                   QueryResult.ResultObject.of("{3.55:51.98}"),
                   QueryResult.ResultObject.of("{890:45}"),
                   QueryResult.ResultObject.of("{true:27}"),
                   QueryResult.ResultObject.of("{\"s\":\"foo\",\"i\":2}"),
                   QueryResult.ResultObject.of("[{\"s\":\"bar\",\"i\":3},{\"s\":\"foobar\",\"i\":4}]"),
                   QueryResult.ResultObject.of("[{\"s\":\"foobar2\",\"i\":3}]"),
                   QueryResult.ResultObject.of("{\"key\":{\"s\":\"foobar3\",\"i\":9}}")
                 ))
               )
    );

    // Make sure the whole schema has been persisted in the metastore, and with the right order
    Assert.assertEquals(ImmutableList.of(new TableInfo.ColumnInfo("s", "string", null),
                                         new TableInfo.ColumnInfo("i", "int", null),
                                         new TableInfo.ColumnInfo("f", "float", null),
                                         new TableInfo.ColumnInfo("d", "double", null),
                                         new TableInfo.ColumnInfo("l", "bigint", null),
                                         new TableInfo.ColumnInfo("b", "tinyint", null),
                                         new TableInfo.ColumnInfo("bo", "boolean", null),
                                         new TableInfo.ColumnInfo("sh", "smallint", null),
                                         // Arrays
                                         new TableInfo.ColumnInfo("iarr", "array<int>", null),
                                         new TableInfo.ColumnInfo("farr", "array<float>", null),
                                         new TableInfo.ColumnInfo("darr", "array<double>", null),
                                         new TableInfo.ColumnInfo("larr", "array<bigint>", null),
                                         new TableInfo.ColumnInfo("barr", "binary", null),
                                         new TableInfo.ColumnInfo("boarr", "array<boolean>", null),
                                         new TableInfo.ColumnInfo("sharr", "array<smallint>", null),
                                         new TableInfo.ColumnInfo("sarr", "array<string>", null),
                                         // Lists
                                         new TableInfo.ColumnInfo("ilist", "array<int>", null),
                                         new TableInfo.ColumnInfo("flist", "array<float>", null),
                                         new TableInfo.ColumnInfo("dlist", "array<double>", null),
                                         new TableInfo.ColumnInfo("llist", "array<bigint>", null),
                                         new TableInfo.ColumnInfo("blist", "array<tinyint>", null),
                                         new TableInfo.ColumnInfo("bolist", "array<boolean>", null),
                                         new TableInfo.ColumnInfo("shlist", "array<smallint>", null),
                                         new TableInfo.ColumnInfo("slist", "array<string>", null),
                                         // Maps
                                         new TableInfo.ColumnInfo("stoimap", "map<string,int>", null),
                                         new TableInfo.ColumnInfo("ftodmap", "map<float,double>", null),
                                         new TableInfo.ColumnInfo("ltobmap", "map<bigint,tinyint>", null),
                                         new TableInfo.ColumnInfo("botoshmap", "map<boolean,smallint>", null),
                                         // Custom type
                                         new TableInfo.ColumnInfo("v", "struct<s:string,i:int>", null),
                                         new TableInfo.ColumnInfo("varr", "array<struct<s:string,i:int>>", null),
                                         new TableInfo.ColumnInfo("vlist", "array<struct<s:string,i:int>>", null),
                                         new TableInfo.ColumnInfo("stovmap", "map<string,struct<s:string,i:int>>", null)
                        ),
                        exploreService.getTableInfo("default", "my_table").getSchema());
  }
}
