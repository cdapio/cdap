/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.template.etl.batch;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.template.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.test.AdapterManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SlowTests;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.hsqldb.Server;
import org.hsqldb.server.ServerAcl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.sql.rowset.serial.SerialBlob;

/**
 * Test for ETL using databases
 */
public class BatchETLDBAdapterTest extends BaseETLBatchTest {
  private static final long currentTs = System.currentTimeMillis();
  private static final String clobData = "this is a long string with line separators \n that can be used as \n a clob";
  private static HSQLDBServer hsqlDBServer;
  private static Schema schema;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws Exception {
    String hsqlDBDir = temporaryFolder.newFolder("hsqldb").getAbsolutePath();
    hsqlDBServer = new HSQLDBServer(hsqlDBDir, "testdb");
    hsqlDBServer.start();
    try (Connection conn = hsqlDBServer.getConnection()) {
      createTestTables(conn);
      prepareTestData(conn);
    }

    Schema nullableString = Schema.nullableOf(Schema.of(Schema.Type.STRING));
    Schema nullableBoolean = Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN));
    Schema nullableInt = Schema.nullableOf(Schema.of(Schema.Type.INT));
    Schema nullableLong = Schema.nullableOf(Schema.of(Schema.Type.LONG));
    Schema nullableFloat = Schema.nullableOf(Schema.of(Schema.Type.FLOAT));
    Schema nullableDouble = Schema.nullableOf(Schema.of(Schema.Type.DOUBLE));
    Schema nullableBytes = Schema.nullableOf(Schema.of(Schema.Type.BYTES));
    schema = Schema.recordOf("student",
                             Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
                             Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)),
                             Schema.Field.of("SCORE", nullableDouble),
                             Schema.Field.of("GRADUATED", nullableBoolean),
                             Schema.Field.of("TINY", nullableInt),
                             Schema.Field.of("SMALL", nullableInt),
                             Schema.Field.of("BIG", nullableLong),
                             Schema.Field.of("FLOAT_COL", nullableFloat),
                             Schema.Field.of("REAL_COL", nullableFloat),
                             Schema.Field.of("NUMERIC_COL", nullableDouble),
                             Schema.Field.of("DECIMAL_COL", nullableDouble),
                             Schema.Field.of("BIT_COL", nullableBoolean),
                             Schema.Field.of("DATE_COL", nullableLong),
                             Schema.Field.of("TIME_COL", nullableLong),
                             Schema.Field.of("TIMESTAMP_COL", nullableLong),
                             Schema.Field.of("BINARY_COL", nullableBytes),
                             Schema.Field.of("BLOB_COL", nullableBytes),
                             Schema.Field.of("CLOB_COL", nullableString));
  }

  private static void createTestTables(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE my_table" +
                     "(" +
                     "ID INT NOT NULL, " +
                     "NAME VARCHAR(40) NOT NULL, " +
                     "SCORE DOUBLE, " +
                     "GRADUATED BOOLEAN, " +
                     "NOT_IMPORTED VARCHAR(30), " +
                     "TINY TINYINT, " +
                     "SMALL SMALLINT, " +
                     "BIG BIGINT, " +
                     "FLOAT_COL FLOAT, " +
                     "REAL_COL REAL, " +
                     "NUMERIC_COL NUMERIC(10, 2), " +
                     "DECIMAL_COL DECIMAL(10, 2), " +
                     "BIT_COL BIT, " +
                     "DATE_COL DATE, " +
                     "TIME_COL TIME, " +
                     "TIMESTAMP_COL TIMESTAMP, " +
                     "BINARY_COL BINARY(100)," +
                     "BLOB_COL BLOB(100), " +
                     "CLOB_COL CLOB(100)" +
                     ")");
      stmt.execute("CREATE TABLE my_dest_table AS (" +
                     "SELECT * FROM my_table) WITH DATA");
    }
  }

  private static void prepareTestData(Connection conn) throws SQLException {
    try (
      PreparedStatement pStmt =
        conn.prepareStatement("INSERT INTO my_table VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
    ) {
      for (int i = 1; i <= 5; i++) {
        String name = "user" + i;
        pStmt.setInt(1, i);
        pStmt.setString(2, name);
        pStmt.setDouble(3, 123.45 + i);
        pStmt.setBoolean(4, (i % 2 == 0));
        pStmt.setString(5, "random" + i);
        pStmt.setShort(6, (short) i);
        pStmt.setShort(7, (short) i);
        pStmt.setLong(8, (long) i);
        pStmt.setFloat(9, (float) 123.45 + i);
        pStmt.setFloat(10, (float) 123.45 + i);
        pStmt.setDouble(11, 123.45 + i);
        if ((i % 2 == 0)) {
          pStmt.setNull(12, Types.DOUBLE);
        } else {
          pStmt.setDouble(12, 123.45 + i);
        }
        pStmt.setBoolean(13, (i % 2 == 1));
        pStmt.setDate(14, new Date(currentTs));
        pStmt.setTime(15, new Time(currentTs));
        pStmt.setTimestamp(16, new Timestamp(currentTs));
        pStmt.setBytes(17, name.getBytes(Charsets.UTF_8));
        pStmt.setBlob(18, new SerialBlob(name.getBytes(Charsets.UTF_8)));
        pStmt.setClob(19, new InputStreamReader(new ByteArrayInputStream(clobData.getBytes(Charsets.UTF_8))));
        pStmt.executeUpdate();
      }
    }
  }

  @Test
  @Category(SlowTests.class)
  @SuppressWarnings("ConstantConditions")
  public void testDBSourceWithCaseInsensitiveRowKey() throws Exception {
    String importQuery = "SELECT ID, NAME, SCORE, GRADUATED, TINY, SMALL, BIG, FLOAT_COL, REAL_COL, NUMERIC_COL, " +
      "DECIMAL_COL, BIT_COL, DATE_COL, TIME_COL, TIMESTAMP_COL, BINARY_COL, BLOB_COL, CLOB_COL FROM my_table " +
      "WHERE ID < 3";
    String countQuery = "SELECT COUNT(ID) from my_table WHERE id < 3";
    ETLStage source = new ETLStage("Database", ImmutableMap.<String, String>builder()
                                     .put(Properties.DB.CONNECTION_STRING, hsqlDBServer.getConnectionUrl())
                                     .put(Properties.DB.IMPORT_QUERY, importQuery)
                                     .put(Properties.DB.COUNT_QUERY, countQuery)
                                     .put(Properties.DB.JDBC_PLUGIN_NAME, "hypersql")
                                     .build()
                                   );

    ETLStage sink = new ETLStage("Table", ImmutableMap.of(
      "name", "outputTable",
      Properties.Table.PROPERTY_SCHEMA, schema.toString(),
      Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "id",
      // Test case-insensitive row key matching
      Properties.Table.CASE_SENSITIVE_ROW_FIELD, "false"));

    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, Lists.<ETLStage>newArrayList());
    AdapterConfig adapterConfig = new AdapterConfig("", TEMPLATE_ID.getId(), GSON.toJsonTree(etlConfig));
    Id.Adapter adapterId = Id.Adapter.from(NAMESPACE, "dbSourceTest");
    AdapterManager manager = createAdapter(adapterId, adapterConfig);

    manager.start();
    manager.waitForOneRunToFinish(5, TimeUnit.MINUTES);
    manager.stop();

    DataSetManager<Table> outputManager = getDataset("outputTable");
    Table outputTable = outputManager.get();

    // Using get to verify the rowkey
    Assert.assertEquals(17, outputTable.get(Bytes.toBytes(1)).getColumns().size());
    // In the second record, the 'decimal' column is null
    Assert.assertEquals(16, outputTable.get(Bytes.toBytes(2)).getColumns().size());
    // Scanner to verify number of rows
    Scanner scanner = outputTable.scan(null, null);
    Row row1 = scanner.next();
    Row row2 = scanner.next();
    Assert.assertNotNull(row1);
    Assert.assertNotNull(row2);
    Assert.assertNull(scanner.next());
    scanner.close();
    // Verify data
    Assert.assertEquals("user1", row1.getString("NAME"));
    Assert.assertEquals("user2", row2.getString("NAME"));
    Assert.assertEquals(124.45, row1.getDouble("SCORE"), 0.000001);
    Assert.assertEquals(125.45, row2.getDouble("SCORE"), 0.000001);
    Assert.assertEquals(false, row1.getBoolean("GRADUATED"));
    Assert.assertEquals(true, row2.getBoolean("GRADUATED"));
    Assert.assertNull(row1.get("NOT_IMPORTED"));
    Assert.assertNull(row2.get("NOT_IMPORTED"));
    // TODO: Reading from table as SHORT seems to be giving the wrong value.
    Assert.assertEquals(1, (int) row1.getInt("TINY"));
    Assert.assertEquals(2, (int) row2.getInt("TINY"));
    Assert.assertEquals(1, (int) row1.getInt("SMALL"));
    Assert.assertEquals(2, (int) row2.getInt("SMALL"));
    Assert.assertEquals(1, (long) row1.getLong("BIG"));
    Assert.assertEquals(2, (long) row2.getLong("BIG"));
    // TODO: Reading from table as FLOAT seems to be giving back the wrong value.
    Assert.assertEquals(124.45, row1.getDouble("FLOAT_COL"), 0.00001);
    Assert.assertEquals(125.45, row2.getDouble("FLOAT_COL"), 0.00001);
    Assert.assertEquals(124.45, row1.getDouble("REAL_COL"), 0.00001);
    Assert.assertEquals(125.45, row2.getDouble("REAL_COL"), 0.00001);
    Assert.assertEquals(124.45, row1.getDouble("NUMERIC_COL"), 0.000001);
    Assert.assertEquals(125.45, row2.getDouble("NUMERIC_COL"), 0.000001);
    Assert.assertEquals(124.45, row1.getDouble("DECIMAL_COL"), 0.000001);
    Assert.assertEquals(null, row2.get("DECIMAL_COL"));
    Assert.assertEquals(true, row1.getBoolean("BIT_COL"));
    Assert.assertEquals(false, row2.getBoolean("BIT_COL"));
    // Verify time columns
    java.util.Date date = new java.util.Date(currentTs);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    long expectedDateTimestamp = Date.valueOf(sdf.format(date)).getTime();
    sdf = new SimpleDateFormat("H:mm:ss");
    long expectedTimeTimestamp = Time.valueOf(sdf.format(date)).getTime();
    Assert.assertEquals(expectedDateTimestamp, (long) row1.getLong("DATE_COL"));
    Assert.assertEquals(expectedDateTimestamp, (long) row2.getLong("DATE_COL"));
    Assert.assertEquals(expectedTimeTimestamp, (long) row1.getLong("TIME_COL"));
    Assert.assertEquals(expectedTimeTimestamp, (long) row2.getLong("TIME_COL"));
    Assert.assertEquals(currentTs, (long) row1.getLong("TIMESTAMP_COL"));
    Assert.assertEquals(currentTs, (long) row2.getLong("TIMESTAMP_COL"));
    // verify binary columns
    Assert.assertEquals("user1", Bytes.toString(row1.get("BINARY_COL"), 0, 5));
    Assert.assertEquals("user2", Bytes.toString(row2.get("BINARY_COL"), 0, 5));
    Assert.assertEquals("user1", Bytes.toString(row1.get("BLOB_COL"), 0, 5));
    Assert.assertEquals("user2", Bytes.toString(row2.get("BLOB_COL"), 0, 5));
    Assert.assertEquals(clobData, Bytes.toString(row1.get("CLOB_COL"), 0, clobData.length()));
    Assert.assertEquals(clobData, Bytes.toString(row2.get("CLOB_COL"), 0, clobData.length()));

    // delete adapter
    manager.delete();
  }

  @Test
  @Category(SlowTests.class)
  public void testDBSourceWithCaseSensitiveRowKey() throws Exception {
    String importQuery = "SELECT ID, NAME FROM my_table WHERE ID < 3";
    String countQuery = "SELECT COUNT(ID) from my_table WHERE id < 3";
    ETLStage source = new ETLStage("Database", ImmutableMap.<String, String>builder()
      .put(Properties.DB.CONNECTION_STRING, hsqlDBServer.getConnectionUrl())
      .put(Properties.DB.IMPORT_QUERY, importQuery)
      .put(Properties.DB.COUNT_QUERY, countQuery)
      .put(Properties.DB.JDBC_PLUGIN_NAME, "hypersql")
      .build()
    );

    ETLStage sink = new ETLStage("Table", ImmutableMap.of(
      "name", "outputTable1",
      Properties.Table.PROPERTY_SCHEMA, schema.toString(),
      Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "id"));

    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, Lists.<ETLStage>newArrayList());
    AdapterConfig adapterConfig = new AdapterConfig("", TEMPLATE_ID.getId(), GSON.toJsonTree(etlConfig));
    Id.Adapter adapterId = Id.Adapter.from(NAMESPACE, "dbSourceTest");
    AdapterManager manager = createAdapter(adapterId, adapterConfig);
    manager.start();
    manager.waitForOneRunToFinish(5, TimeUnit.MINUTES);
    manager.stop();

    // No records should be written
    DataSetManager<Table> outputManager = getDataset("outputTable1");
    Table outputTable = outputManager.get();
    Scanner scan = outputTable.scan(null, null);
    Assert.assertNull(scan.next());

    // Delete adapter
    manager.delete();
  }

  // Test is ignored - Currently DBOutputFormat does a statement.executeBatch which seems to fail in HSQLDB.
  // Need to investigate alternatives to HSQLDB.
  @Ignore
  @Test
  @Category(SlowTests.class)
  public void testDBSink() throws Exception {
    String cols = "ID, NAME, SCORE, GRADUATED, TINY, SMALL, BIG, FLOAT_COL, REAL_COL, NUMERIC_COL, DECIMAL_COL, " +
      "BIT_COL, DATE_COL, TIME_COL, TIMESTAMP_COL, BINARY_COL, BLOB_COL, CLOB_COL";
    ETLStage source = new ETLStage("Table",
                                   ImmutableMap.of(
                                     Properties.BatchReadableWritable.NAME, "inputTable",
                                     Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "ID",
                                     Properties.Table.PROPERTY_SCHEMA, schema.toString()));
    ETLStage sink = new ETLStage("Database",
                                 ImmutableMap.of(Properties.DB.CONNECTION_STRING, hsqlDBServer.getConnectionUrl(),
                                                 Properties.DB.TABLE_NAME, "my_dest_table",
                                                 Properties.DB.COLUMNS, cols,
                                                 Properties.DB.JDBC_PLUGIN_NAME, "hypersql"
                                 ));
    List<ETLStage> transforms = Lists.newArrayList();
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transforms);
    Id.Adapter adapterId = Id.Adapter.from(NAMESPACE, "dbSinkTest");
    AdapterConfig adapterConfig = new AdapterConfig("", TEMPLATE_ID.getId(), GSON.toJsonTree(etlConfig));
    AdapterManager manager = createAdapter(adapterId, adapterConfig);

    createInputData();
    manager.start();
    manager.waitForOneRunToFinish(5, TimeUnit.MINUTES);
    manager.stop();
  }

  private void createInputData() throws Exception {
    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset("inputTable");
    Table inputTable = inputManager.get();
    for (int i = 1; i <= 2; i++) {
      Put put = new Put(Bytes.toBytes("row" + i));
      String name = "user" + i;
      put.add("ID", i);
      put.add("NAME", name);
      put.add("SCORE", 3.451);
      put.add("GRADUATED", (i % 2 == 0));
      put.add("TINY", i + 1);
      put.add("SMALL", i + 2);
      put.add("BIG", 3456987L);
      put.add("FLOAT", 3.456f);
      put.add("REAL", 3.457f);
      put.add("NUMERIC", 3.458);
      put.add("DECIMAL", 3.459);
      put.add("BIT", (i % 2 == 1));
      put.add("DATE", currentTs);
      put.add("TIME", currentTs);
      put.add("TIMESTAMP", currentTs);
      put.add("BINARY", name.getBytes(Charsets.UTF_8));
      put.add("BLOB", name.getBytes(Charsets.UTF_8));
      put.add("CLOB", clobData);
      inputTable.put(put);
      inputManager.flush();
    }
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    try (
      Connection conn = hsqlDBServer.getConnection();
      Statement stmt = conn.createStatement()
    ) {
      stmt.execute("DROP TABLE my_table");
    }

    hsqlDBServer.stop();
  }

  private static class HSQLDBServer {

    private final String locationUrl;
    private final String database;
    private final String connectionUrl;
    private final Server server;
    private final String hsqlDBDriver = "org.hsqldb.jdbcDriver";

    HSQLDBServer(String location, String database) {
      this.locationUrl = String.format("%s/%s", location, database);
      this.database = database;
      this.connectionUrl = String.format("jdbc:hsqldb:hsql://localhost/%s", database);
      this.server = new Server();
    }

    public int start() throws IOException, ServerAcl.AclFormatException {
      server.setDatabasePath(0, locationUrl);
      server.setDatabaseName(0, database);
      return server.start();
    }

    public int stop() {
      return server.stop();
    }

    public Connection getConnection() {
      try {
        Class.forName(hsqlDBDriver);
        return DriverManager.getConnection(connectionUrl);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    public String getConnectionUrl() {
      return this.connectionUrl;
    }

    public String getHsqlDBDriver() {
      return this.hsqlDBDriver;
    }
  }
}
