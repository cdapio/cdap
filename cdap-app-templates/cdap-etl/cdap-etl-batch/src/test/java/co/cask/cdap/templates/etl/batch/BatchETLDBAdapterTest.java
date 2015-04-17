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

package co.cask.cdap.templates.etl.batch;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.templates.etl.batch.sources.DBSource;
import co.cask.cdap.templates.etl.common.Properties;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.ServerAcl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
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
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test for ETL using databases
 */
public class BatchETLDBAdapterTest extends TestBase {
  private static final long currentTs = System.currentTimeMillis();
  private static final String clobData = "this is a long string with line separators \n that can be used as \n a clob";
  private static HSQLDBServer hsqlDBServer;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws Exception {
    String hsqlDBDir = temporaryFolder.newFolder("hsqldb").getAbsolutePath();
    hsqlDBServer = new HSQLDBServer(hsqlDBDir, "testdb");
    hsqlDBServer.start();
    Connection conn = hsqlDBServer.getConnection();
    try {
      createTestTable(conn);
      prepareTestData(conn);
    } finally {
      conn.close();
    }
  }

  private static void createTestTable(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    try {
      stmt.execute("CREATE TABLE my_table" +
                     "(" +
                     "id INT NOT NULL, " +
                     "name VARCHAR(40) NOT NULL, " +
                     "score DOUBLE, " +
                     "graduated BOOLEAN, " +
                     "not_imported VARCHAR(30), " +
                     "tiny TINYINT, " +
                     "small SMALLINT, " +
                     "big BIGINT, " +
                     "float FLOAT, " +
                     "real REAL, " +
                     "numeric NUMERIC(10, 2), " +
                     "decimal DECIMAL(10, 2), " +
                     "bit BIT, " +
                     "date DATE, " +
                     "time TIME, " +
                     "timestamp TIMESTAMP, " +
                     "binary BINARY(100)," +
                     "blob BLOB(100), " +
                     "clob CLOB" +
                     ")");
    } finally {
      stmt.close();
    }
  }

  private static void prepareTestData(Connection conn) throws SQLException {
    PreparedStatement pStmt =
      conn.prepareStatement("INSERT INTO my_table VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    try {
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
        pStmt.setBlob(18, new ByteArrayInputStream(name.getBytes(Charsets.UTF_8)));
        pStmt.setClob(19, new InputStreamReader(new ByteArrayInputStream(clobData.getBytes(Charsets.UTF_8))));
        pStmt.executeUpdate();
      }
    } finally {
      pStmt.close();
    }
  }

  @Test
  @Category(SlowTests.class)
  public void testDBSource() throws Exception {
    String path = Resources.getResource("org/hsqldb/jdbcDriver.class").getPath();
    File hsqldbJar = new File(URI.create(path.substring(0, path.indexOf('!'))));

    ApplicationManager applicationManager = deployApplication(ETLBatchTemplate.class, hsqldbJar);

    ApplicationTemplate<ETLBatchConfig> appTemplate = new ETLBatchTemplate();

    String importQuery = "SELECT id, name, score, graduated, tiny, small, big, float, real, numeric, decimal, bit, " +
      "date, time, timestamp, binary, blob, clob FROM my_table WHERE id < 3";
    String countQuery = "SELECT COUNT(id) from my_table WHERE id < 3";
    ETLStage source = new ETLStage(DBSource.class.getSimpleName(),
                                   ImmutableMap.of(Properties.DB.DRIVER_CLASS, hsqlDBServer.getHsqlDBDriver(),
                                                   Properties.DB.CONNECTION_STRING, hsqlDBServer.getConnectionUrl(),
                                                   Properties.DB.TABLE_NAME, "my_table",
                                                   Properties.DB.IMPORT_QUERY, importQuery,
                                                   Properties.DB.COUNT_QUERY, countQuery
                                   ));

    Schema nullableBoolean = Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN));
    Schema nullableInt = Schema.nullableOf(Schema.of(Schema.Type.INT));
    Schema nullableLong = Schema.nullableOf(Schema.of(Schema.Type.LONG));
    Schema nullableFloat = Schema.nullableOf(Schema.of(Schema.Type.FLOAT));
    Schema nullableDouble = Schema.nullableOf(Schema.of(Schema.Type.DOUBLE));
    Schema nullableBytes = Schema.nullableOf(Schema.of(Schema.Type.BYTES));
    Schema schema = Schema.recordOf("student",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
      Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("SCORE", nullableDouble),
      Schema.Field.of("GRADUATED", nullableBoolean),
      Schema.Field.of("TINY", nullableInt),
      Schema.Field.of("SMALL", nullableInt),
      Schema.Field.of("BIG", nullableLong),
      Schema.Field.of("FLOAT", nullableFloat),
      Schema.Field.of("REAL", nullableFloat),
      Schema.Field.of("NUMERIC", nullableDouble),
      Schema.Field.of("BIT", nullableBoolean),
      Schema.Field.of("DATE", nullableLong),
      Schema.Field.of("TIME", nullableLong),
      Schema.Field.of("TIMESTAMP", nullableLong),
      Schema.Field.of("BINARY", nullableBytes),
      Schema.Field.of("CLOB", nullableBytes));


    ETLStage sink = new ETLStage("TableSink", ImmutableMap.of(
      "name", "outputTable",
      Table.PROPERTY_SCHEMA, schema.toString(),
      Table.PROPERTY_SCHEMA_ROW_FIELD, "ID"));

    ETLBatchConfig adapterConfig = new ETLBatchConfig("", source, sink, Lists.<ETLStage>newArrayList());
    MockAdapterConfigurer adapterConfigurer = new MockAdapterConfigurer();
    appTemplate.configureAdapter("myAdapter", adapterConfig, adapterConfigurer);
    // add dataset instances that the source and sink added
    addDatasetInstances(adapterConfigurer);

    Map<String, String> mapReduceArgs = Maps.newHashMap(adapterConfigurer.getArguments());
    MapReduceManager mrManager = applicationManager.startMapReduce(ETLMapReduce.class.getSimpleName(), mapReduceArgs);
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    applicationManager.stopAll();

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
    Assert.assertEquals(124.45, row1.getDouble("FLOAT"), 0.00001);
    Assert.assertEquals(125.45, row2.getDouble("FLOAT"), 0.00001);
    Assert.assertEquals(124.45, row1.getDouble("REAL"), 0.00001);
    Assert.assertEquals(125.45, row2.getDouble("REAL"), 0.00001);
    Assert.assertEquals(124.45, row1.getDouble("NUMERIC"), 0.000001);
    Assert.assertEquals(125.45, row2.getDouble("NUMERIC"), 0.000001);
    Assert.assertEquals(124.45, row1.getDouble("DECIMAL"), 0.000001);
    Assert.assertEquals(null, row2.get("DECIMAL"));
    Assert.assertEquals(true, row1.getBoolean("BIT"));
    Assert.assertEquals(false, row2.getBoolean("BIT"));
    // Verify time columns
    java.util.Date date = new java.util.Date(currentTs);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    long expectedDateTimestamp = Date.valueOf(sdf.format(date)).getTime();
    sdf = new SimpleDateFormat("H:mm:ss");
    long expectedTimeTimestamp = Time.valueOf(sdf.format(date)).getTime();
    Assert.assertEquals(expectedDateTimestamp, (long) row1.getLong("DATE"));
    Assert.assertEquals(expectedDateTimestamp, (long) row2.getLong("DATE"));
    Assert.assertEquals(expectedTimeTimestamp, (long) row1.getLong("TIME"));
    Assert.assertEquals(expectedTimeTimestamp, (long) row2.getLong("TIME"));
    Assert.assertEquals(currentTs, (long) row1.getLong("TIMESTAMP"));
    Assert.assertEquals(currentTs, (long) row2.getLong("TIMESTAMP"));
    // verify binary columns
    Assert.assertEquals("user1", Bytes.toString(row1.get("BINARY"), 0, 5));
    Assert.assertEquals("user2", Bytes.toString(row2.get("BINARY"), 0, 5));
    Assert.assertEquals("user1", Bytes.toString(row1.get("BLOB"), 0, 5));
    Assert.assertEquals("user2", Bytes.toString(row2.get("BLOB"), 0, 5));
    Assert.assertEquals(clobData, Bytes.toString(row1.get("CLOB"), 0, clobData.length()));
    Assert.assertEquals(clobData, Bytes.toString(row2.get("CLOB"), 0, clobData.length()));

  }

  private void addDatasetInstances(MockAdapterConfigurer configurer) throws Exception {
    for (Map.Entry<String, ImmutablePair<String, DatasetProperties>> entry :
      configurer.getDatasetInstances().entrySet()) {
      String typeName = entry.getValue().getFirst();
      DatasetProperties properties = entry.getValue().getSecond();
      String instanceName = entry.getKey();
      addDatasetInstance(typeName, instanceName, properties);
    }
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    Connection conn = hsqlDBServer.getConnection();
    try {
      Statement stmt = conn.createStatement();
      try {
        stmt.execute("DROP TABLE my_table");
      } finally {
        stmt.close();
      }
      stmt.close();
    } finally {
      conn.close();
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
      HsqlProperties props = new HsqlProperties();
      props.setProperty("server.database.0", locationUrl);
      props.setProperty("server.dbname.0", database);
      server.setProperties(props);
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
