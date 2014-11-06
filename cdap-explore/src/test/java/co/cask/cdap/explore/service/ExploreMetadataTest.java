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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.test.SlowTests;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests explore metadata endpoints.
 */
@Category(SlowTests.class)
public class ExploreMetadataTest extends BaseHiveExploreServiceTest {

  @BeforeClass
  public static void start() throws Exception {
    startServices(CConfiguration.create());

    datasetFramework.addModule("keyStructValue", new KeyStructValueTableDefinition.KeyStructValueTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyStructValueTable", "my_table", DatasetProperties.EMPTY);
    datasetFramework.addInstance("keyStructValueTable", "other_table", DatasetProperties.EMPTY);
  }

  @AfterClass
  public static void stop() throws Exception {
    datasetFramework.deleteInstance("my_table");
    datasetFramework.deleteInstance("other_table");
    datasetFramework.deleteModule("keyStructValue");
  }

  @Test
  public void testGetTables() throws Exception {
    ListenableFuture<ExploreExecutionResult> future;

    // All tables
    future = getExploreClient().tables(null, null, "%", null);
    assertStatementResult(future, true,
                          Lists.newArrayList(
                            new ColumnDesc("TABLE_CAT", "STRING", 1, "Catalog name. NULL if not applicable."),
                            new ColumnDesc("TABLE_SCHEM", "STRING", 2, "Schema name."),
                            new ColumnDesc("TABLE_NAME", "STRING", 3, "Table name."),
                            new ColumnDesc("TABLE_TYPE", "STRING", 4,
                                           "The table type, e.g. \"TABLE\", \"VIEW\", etc."),
                            new ColumnDesc("REMARKS", "STRING", 5, "Comments about the table.")
                          ),
                          Lists.newArrayList(
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(""),
                              QueryResult.ResultObject.of("default"),
                              QueryResult.ResultObject.of("my_table"),
                              QueryResult.ResultObject.of("TABLE"),
                              QueryResult.ResultObject.of("Cask CDAP Dataset")
                            )),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(""),
                              QueryResult.ResultObject.of("default"),
                              QueryResult.ResultObject.of("other_table"),
                              QueryResult.ResultObject.of("TABLE"),
                              QueryResult.ResultObject.of("Cask CDAP Dataset")
                            ))
                          )
    );

    // Pattern on table name
    future = getExploreClient().tables(null, null, "other%", null);
    assertStatementResult(future, true,
                          Lists.newArrayList(
                            new ColumnDesc("TABLE_CAT", "STRING", 1, "Catalog name. NULL if not applicable."),
                            new ColumnDesc("TABLE_SCHEM", "STRING", 2, "Schema name."),
                            new ColumnDesc("TABLE_NAME", "STRING", 3, "Table name."),
                            new ColumnDesc("TABLE_TYPE", "STRING", 4, "The table type, e.g. \"TABLE\", \"VIEW\", etc."),
                            new ColumnDesc("REMARKS", "STRING", 5, "Comments about the table.")
                          ),
                          Lists.newArrayList(
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(""),
                              QueryResult.ResultObject.of("default"),
                              QueryResult.ResultObject.of("other_table"),
                              QueryResult.ResultObject.of("TABLE"),
                              QueryResult.ResultObject.of("Cask CDAP Dataset")
                            ))
                          )
    );
  }

  @Test
  public void testGetCatalogs() throws Exception {
    ListenableFuture<ExploreExecutionResult> future;
    future = getExploreClient().catalogs();
    assertStatementResult(future, false,
                          Lists.newArrayList(
                            new ColumnDesc("TABLE_CAT", "STRING", 1, "Catalog name. NULL if not applicable.")
                          ),
                          Lists.<QueryResult>newArrayList());
  }

  @Test
  public void testGetSchemas() throws Exception {
    ListenableFuture<ExploreExecutionResult> future;

    future = getExploreClient().schemas(null, "");
    assertStatementResult(future, true,
                          Lists.newArrayList(
                            new ColumnDesc("TABLE_SCHEM", "STRING", 1, "Schema name."),
                            new ColumnDesc("TABLE_CATALOG", "STRING", 2, "Catalog name.")
                          ),
                          Lists.newArrayList(new QueryResult(Lists.newArrayList(
                              QueryResult.ResultObject.of("default"),
                              QueryResult.ResultObject.of("")
                            ))
                          ));
  }

  @Test
  public void testGetTypeInfo() throws Exception {
    ListenableFuture<ExploreExecutionResult> future;

    future = getExploreClient().dataTypes();
    assertStatementResult(future, true,
                          Lists.newArrayList(
                            new ColumnDesc("TYPE_NAME", "STRING", 1, "Type name"),
                            new ColumnDesc("DATA_TYPE", "INT", 2, "SQL data type from java.sql.Types"),
                            new ColumnDesc("PRECISION", "INT", 3, "Maximum precision"),
                            new ColumnDesc("LITERAL_PREFIX", "STRING", 4,
                                           "Prefix used to quote a literal (may be null)"),
                            new ColumnDesc("LITERAL_SUFFIX", "STRING", 5,
                                           "Suffix used to quote a literal (may be null)"),
                            new ColumnDesc("CREATE_PARAMS", "STRING", 6,
                                           "Parameters used in creating the type (may be null)"),
                            new ColumnDesc("NULLABLE", "SMALLINT", 7, "Can you use NULL for this type"),
                            new ColumnDesc("CASE_SENSITIVE", "BOOLEAN", 8, "Is it case sensitive"),
                            new ColumnDesc("SEARCHABLE", "SMALLINT", 9, "Can you use \"WHERE\" based on this type"),
                            new ColumnDesc("UNSIGNED_ATTRIBUTE", "BOOLEAN", 10, "Is it unsigned"),
                            new ColumnDesc("FIXED_PREC_SCALE", "BOOLEAN", 11, "Can it be a money value"),
                            new ColumnDesc("AUTO_INCREMENT", "BOOLEAN", 12,
                                           "Can it be used for an auto-increment value"),
                            new ColumnDesc("LOCAL_TYPE_NAME", "STRING", 13,
                                           "Localized version of type name (may be null)"),
                            new ColumnDesc("MINIMUM_SCALE", "SMALLINT", 14, "Minimum scale supported"),
                            new ColumnDesc("MAXIMUM_SCALE", "SMALLINT", 15, "Maximum scale supported"),
                            new ColumnDesc("SQL_DATA_TYPE", "INT", 16, "Unused"),
                            new ColumnDesc("SQL_DATETIME_SUB", "INT", 17, "Unused"),
                            new ColumnDesc("NUM_PREC_RADIX", "INT", 18, "Usually 2 or 10")
                          ),
                          Lists.newArrayList(
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of("VOID"),
                              QueryResult.ResultObject.of(0),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 1),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of((short) 3),
                              QueryResult.ResultObject.of(true),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of())),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of("BOOLEAN"),
                              QueryResult.ResultObject.of(16),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 1),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of((short) 3),
                              QueryResult.ResultObject.of(true),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of())),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of("TINYINT"),
                              QueryResult.ResultObject.of(-6),
                              QueryResult.ResultObject.of(3),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 1),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of((short) 3),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(10))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of("SMALLINT"),
                              QueryResult.ResultObject.of(5),
                              QueryResult.ResultObject.of(5),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 1),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of((short) 3),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(10))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of("INT"),
                              QueryResult.ResultObject.of(4),
                              QueryResult.ResultObject.of(10),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 1),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of((short) 3),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(10))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of("BIGINT"),
                              QueryResult.ResultObject.of(-5),
                              QueryResult.ResultObject.of(19),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 1),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of((short) 3),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(10))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of("FLOAT"),
                              QueryResult.ResultObject.of(6),
                              QueryResult.ResultObject.of(7),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 1),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of((short) 3),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(2))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of("DOUBLE"),
                              QueryResult.ResultObject.of(8),
                              QueryResult.ResultObject.of(15),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 1),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of((short) 3),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(2))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of("STRING"),
                              QueryResult.ResultObject.of(12),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 1),
                              QueryResult.ResultObject.of(true),
                              QueryResult.ResultObject.of((short) 3),
                              QueryResult.ResultObject.of(true),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of())),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of("CHAR"),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 1),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of((short) 3),
                              QueryResult.ResultObject.of(true),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(false),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of((short) 0),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of()))
                          )
    );
  }

  @Test
  public void testGetColumns() throws Exception {
    ListenableFuture<ExploreExecutionResult> future = getExploreClient().columns(null, null, "%", "%");
    assertStatementResult(future, true,
                          Lists.newArrayList(
                            new ColumnDesc("TABLE_CAT", "STRING", 1, "Catalog name. NULL if not applicable"),
                            new ColumnDesc("TABLE_SCHEM", "STRING", 2, "Schema name"),
                            new ColumnDesc("TABLE_NAME", "STRING", 3, "Table name"),
                            new ColumnDesc("COLUMN_NAME", "STRING", 4, "Column name"),
                            new ColumnDesc("DATA_TYPE", "INT", 5, "SQL type from java.sql.Types"),
                            new ColumnDesc("TYPE_NAME", "STRING", 6, "Data source dependent type name, " +
                              "for a UDT the type name is fully qualified"),
                            new ColumnDesc("COLUMN_SIZE", "INT", 7, "Column size. For char or date types" +
                              " this is the maximum number of characters, for numeric or decimal" +
                              " types this is precision."),
                            new ColumnDesc("BUFFER_LENGTH", "TINYINT", 8, "Unused"),
                            new ColumnDesc("DECIMAL_DIGITS", "INT", 9, "The number of fractional digits"),
                            new ColumnDesc("NUM_PREC_RADIX", "INT", 10, "Radix (typically either 10 or 2)"),
                            new ColumnDesc("NULLABLE", "INT", 11, "Is NULL allowed"),
                            new ColumnDesc("REMARKS", "STRING", 12, "Comment describing column (may be null)"),
                            new ColumnDesc("COLUMN_DEF", "STRING", 13, "Default value (may be null)"),
                            new ColumnDesc("SQL_DATA_TYPE", "INT", 14, "Unused"),
                            new ColumnDesc("SQL_DATETIME_SUB", "INT", 15, "Unused"),
                            new ColumnDesc("CHAR_OCTET_LENGTH", "INT", 16,
                                           "For char types the maximum number of bytes in the column"),
                            new ColumnDesc("ORDINAL_POSITION", "INT", 17, "Index of column in table (starting at 1)"),
                            new ColumnDesc("IS_NULLABLE", "STRING", 18, "\"NO\" means column definitely does not " +
                              "allow NULL values; \"YES\" means the column might allow NULL values. " +
                              "An empty string means nobody knows."),
                            new ColumnDesc("SCOPE_CATALOG", "STRING", 19, "Catalog of table that is the scope " +
                              "of a reference attribute (null if DATA_TYPE isn't REF)"),
                            new ColumnDesc("SCOPE_SCHEMA", "STRING", 20, "Schema of table that is the scope of a " +
                              "reference attribute (null if the DATA_TYPE isn't REF)"),
                            new ColumnDesc("SCOPE_TABLE", "STRING", 21, "Table name that this the scope " +
                              "of a reference attribure (null if the DATA_TYPE isn't REF)"),
                            new ColumnDesc("SOURCE_DATA_TYPE", "SMALLINT", 22, "Source type of a distinct type " +
                              "or user-generated Ref type, SQL type from java.sql.Types " +
                              "(null if DATA_TYPE isn't DISTINCT or user-generated REF)"),
                            new ColumnDesc("IS_AUTO_INCREMENT", "STRING", 23,
                                           "Indicates whether this column is auto incremented.")
                          ),
                          Lists.newArrayList(
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("default"),
                              QueryResult.ResultObject.of("my_table"),
                              QueryResult.ResultObject.of("key"),
                              QueryResult.ResultObject.of(12),
                              QueryResult.ResultObject.of("STRING"),
                              QueryResult.ResultObject.of(2147483647),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of("from deserializer"),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of("YES"),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("NO"))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("default"),
                              QueryResult.ResultObject.of("my_table"),
                              QueryResult.ResultObject.of("value"),
                              QueryResult.ResultObject.of(2002),
                              QueryResult.ResultObject.of("struct<name:string,ints:array<int>>"),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of("from deserializer"),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(2),
                              QueryResult.ResultObject.of("YES"),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("NO"))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("default"),
                              QueryResult.ResultObject.of("other_table"),
                              QueryResult.ResultObject.of("key"),
                              QueryResult.ResultObject.of(12),
                              QueryResult.ResultObject.of("STRING"),
                              QueryResult.ResultObject.of(2147483647),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of("from deserializer"),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of("YES"),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("NO"))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("default"),
                              QueryResult.ResultObject.of("other_table"),
                              QueryResult.ResultObject.of("value"),
                              QueryResult.ResultObject.of(2002),
                              QueryResult.ResultObject.of("struct<name:string,ints:array<int>>"),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of("from deserializer"),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(2),
                              QueryResult.ResultObject.of("YES"),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("NO")))
                          )
    );
  }

  @Test
  public void testGetInfo() throws Exception {
    Assert.assertEquals("Hive", getExploreClient().info(MetaDataInfo.InfoType.SERVER_NAME).get().getStringValue());
    Assert.assertEquals(128, getExploreClient().info(MetaDataInfo.InfoType.MAX_TABLE_NAME_LEN).get().getIntValue());
  }

  @Test
  public void testGetTableTypes() throws Exception {
    ListenableFuture<ExploreExecutionResult> future = getExploreClient().tableTypes();
    assertStatementResult(future, true,
                          Lists.newArrayList(
                            new ColumnDesc("TABLE_TYPE", "STRING", 1, "Table type name.")
                          ),
                          Lists.newArrayList(
                            new QueryResult(ImmutableList.of(QueryResult.ResultObject.of("TABLE"))),
                            new QueryResult(ImmutableList.of(QueryResult.ResultObject.of("TABLE"))),
                            new QueryResult(ImmutableList.of(QueryResult.ResultObject.of("VIEW"))),
                            new QueryResult(ImmutableList.of(QueryResult.ResultObject.of("INDEX_TABLE")))
                          )
    );
  }

  @Test
  public void testGetFunctions() throws Exception {
    ListenableFuture<ExploreExecutionResult> future = getExploreClient().functions(null, null, "%");
    assertStatementResult(future, true,
                          Lists.newArrayList(
                            new ColumnDesc("FUNCTION_CAT", "STRING", 1, "Function catalog (may be null)"),
                            new ColumnDesc("FUNCTION_SCHEM", "STRING", 2, "Function schema (may be null)"),
                            new ColumnDesc("FUNCTION_NAME", "STRING", 3,
                                           "Function name. This is the name used to invoke the function"),
                            new ColumnDesc("REMARKS", "STRING", 4, "Explanatory comment on the function"),
                            new ColumnDesc("FUNCTION_TYPE", "INT", 5, "Kind of function."),
                            new ColumnDesc("SPECIFIC_NAME", "STRING", 6,
                                           "The name which uniquely identifies this function within its schema")
                          ),
                          Lists.newArrayList(
                            // We limited the results to 100 here
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("!"),
                              QueryResult.ResultObject.of(""),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of("org.apache.hadoop.hive.ql.exec.FunctionInfo"))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("!="),
                              QueryResult.ResultObject.of(""),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of("org.apache.hadoop.hive.ql.exec.FunctionInfo"))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("%"),
                              QueryResult.ResultObject.of(""),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of("org.apache.hadoop.hive.ql.exec.FunctionInfo"))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("&"),
                              QueryResult.ResultObject.of(""),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of("org.apache.hadoop.hive.ql.exec.FunctionInfo"))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("*"),
                              QueryResult.ResultObject.of(""),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of("org.apache.hadoop.hive.ql.exec.FunctionInfo"))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("+"),
                              QueryResult.ResultObject.of(""),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of("org.apache.hadoop.hive.ql.exec.FunctionInfo"))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("-"),
                              QueryResult.ResultObject.of(""),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of("org.apache.hadoop.hive.ql.exec.FunctionInfo"))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("/"),
                              QueryResult.ResultObject.of(""),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of("org.apache.hadoop.hive.ql.exec.FunctionInfo"))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("<"),
                              QueryResult.ResultObject.of(""),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of("org.apache.hadoop.hive.ql.exec.FunctionInfo"))),
                            new QueryResult(ImmutableList.of(
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of(),
                              QueryResult.ResultObject.of("<="),
                              QueryResult.ResultObject.of(""),
                              QueryResult.ResultObject.of(1),
                              QueryResult.ResultObject.of("org.apache.hadoop.hive.ql.exec.FunctionInfo")))
                          )
    );
  }
}
