package io.cdap.cdap.master.export_import;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Dataset;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.lang.DirectoryClassLoader;
import io.cdap.cdap.spi.data.sql.jdbc.JDBCDriverShim;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Stream.Cons;

/**
 * A Proof-of-Concept exporter that uses direct database connections and raw SQL queries,
 * bypassing the CDAP Data Access Layer.
 */
public class ExportDBMain {

  private static final Logger LOG = LoggerFactory.getLogger(ExportDBMain.class);
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  public static void main(String[] args) {
    LOG.info("Starting Direct SQL Export Job...");

    Injector injector = initializeInjector();
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    SConfiguration sConf = injector.getInstance(SConfiguration.class);
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    String gcsBackupPath = "gs://" + args[0] + "/";

    String storageImpl = cConf.get(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION);
    LOG.info("Detected storage implementation: {}", storageImpl);

    // try {
    //   Location baseLocation = locationFactory.create(gcsBackupPath).append("direct-sql-export");
    //   baseLocation.mkdirs();
    //
    //   if (Dataset.DATA_STORAGE_SQL.equalsIgnoreCase(storageImpl)) {
    //     try (Connection connection = getConnectionForPostgres(cConf, sConf)) {
    //       LOG.info("Successfully connected to the PostgreSQL database.");
    //       exportNamespacesForPostgres(connection, baseLocation);
    //       exportPipelinesForPostgres(connection, baseLocation);
    //     }
    //   } else {
    //     throw new UnsupportedOperationException("Unsupported storage implementation: " + storageImpl);
    //   }

    try {
      Location baseLocation = locationFactory.create(gcsBackupPath).append("direct-sql-export");
      baseLocation.mkdirs();

      if (Constants.Dataset.DATA_STORAGE_SQL.equalsIgnoreCase(storageImpl)) {
        try (Connection connection = getConnectionForPostgres(cConf, sConf)) {
          LOG.info("Successfully connected to the PostgreSQL database.");
          exportTable(connection, baseLocation, "namespaces");
          exportTable(connection, baseLocation, "application_specs");
          // Add calls to export other tables here...
        }
      } else {
        throw new UnsupportedOperationException("This POC is designed for PostgreSQL ('sql') only.");
      }

    } catch (Exception e) {
      LOG.error("Export failed with an unrecoverable error.", e);
      System.exit(1);
    }
    LOG.info("Direct SQL Export Job finished successfully.");
  }

  private static Connection getConnectionForPostgres(CConfiguration cConf, SConfiguration sConf) throws Exception {
    // This logic is adapted from PostgreSqlStorageProvider to handle driver loading
    String jdbcUrl = cConf.get(Constants.Dataset.DATA_STORAGE_SQL_JDBC_CONNECTION_URL);

    // First, load the driver explicitly from the known path
    loadPostgresJDBCDriver(cConf);

    // Now, create the connection properties
    Properties properties = new Properties();
    // *** CORRECTED: Get username and password from SConfiguration ***
    String username = sConf.get(Constants.Dataset.DATA_STORAGE_SQL_USERNAME);
    String password = sConf.get(Constants.Dataset.DATA_STORAGE_SQL_PASSWORD);
    if (username != null) {
      properties.setProperty("user", username);
    }
    if (password != null) {
      properties.setProperty("password", password);
    }

    return DriverManager.getConnection(jdbcUrl, properties);
  }

  /**
   * Generic function to export any table by iterating and writing one JSON file per row.
   */
  public static void exportTable(Connection connection, Location baseLocation, String tableName) throws IOException {
    LOG.info("Starting export of table '{}' using direct SQL...", tableName);
    String sql = String.format("SELECT * FROM %s;", tableName);
    int rowCount = 0;

    try (
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)
    ) {
      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();
      Location tableDir = baseLocation.append(tableName);
      tableDir.mkdirs();

      while (resultSet.next()) {
        Map<String, Object> row = new HashMap<>();
        for (int i = 1; i <= columnCount; i++) {
          row.put(metaData.getColumnName(i), resultSet.getObject(i));
        }

        // Generate a unique filename for each row. For simplicity, we use the row number.
        // A more robust implementation would use the primary key columns.
        Location outputFile = tableDir.append(String.format("row_%d.json", rowCount));
        try (Writer writer = new OutputStreamWriter(outputFile.getOutputStream(), StandardCharsets.UTF_8)) {
          GSON.toJson(row, writer);
        }
        rowCount++;
      }
    } catch (Exception e) {
      LOG.error("Failed to execute SELECT query on table '{}'.", tableName, e);
      throw new IOException("Failed to export table " + tableName, e);
    }
    LOG.info("Successfully exported {} records from table '{}'.", rowCount, tableName);
  }

  /**
   * Exports all namespaces by executing a raw "SELECT *" query against PostgreSQL.
   */
  public static void exportNamespacesForPostgres(Connection connection, Location baseLocation) throws IOException {
    LOG.info("Starting export of namespaces from PostgreSQL using direct SQL...");
    String sql = "SELECT namespace, namespace_metadata FROM namespaces;";
    List<RawNamespaceData> results = new ArrayList<>();

    try (
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)
    ) {
      while (resultSet.next()) {
        RawNamespaceData data = new RawNamespaceData();
        data.setNamespace(resultSet.getString("namespace"));
        data.setNamespaceMetadataJson(resultSet.getString("namespace_metadata"));
        results.add(data);
      }
    } catch (Exception e) {
      LOG.error("Failed to execute SELECT query on PostgreSQL namespaces table.", e);
      throw new IOException("Failed to export namespaces.", e);
    }
    writeNamespacesToFile(results, baseLocation);
  }

  private static void writeNamespacesToFile(List<RawNamespaceData> results, Location baseLocation) throws IOException {
    Location namespacesDir = baseLocation.append("namespaces");
    namespacesDir.mkdirs();
    Location outputFile = namespacesDir.append("namespaces_export.json");
    try (Writer writer = new OutputStreamWriter(outputFile.getOutputStream(), StandardCharsets.UTF_8)) {
      GSON.toJson(results, writer);
    }
    LOG.info("Successfully exported {} namespace records to {}", results.size(), outputFile.toURI());
  }

  private static Injector initializeInjector() {
    try {
      CConfiguration cConf = CConfiguration.create();
      File cConfFile = new File("/etc/cdap/conf/cdap-site.xml");
      if (cConfFile.exists()) {
        cConf.addResource(new FileInputStream(cConfFile));
      }
      File securityConfFile = new File("/etc/cdap/security/cdap-security.xml");
      if (securityConfFile.exists()) {
        cConf.addResource(new FileInputStream(securityConfFile));
      }

      Configuration hConf = new Configuration();
      File hConfFile = new File("/etc/hadoop/conf/core-site.xml");
      if (hConfFile.exists()) {
        hConf.addResource(hConfFile.toURI().toURL());
      }
      hConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
      hConf.setBoolean("fs.gs.auth.service.account.enable", true);

      List<Module> modules = new ArrayList<>();
      modules.add(new ConfigModule(cConf, hConf));
      modules.add(new DFSLocationModule());
      return Guice.createInjector(modules);
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize Guice injector", e);
    }
  }

  /**
   * Exports all pipelines by executing a raw "SELECT *" query against PostgreSQL.
   * This is for POC purposes to demonstrate complexity and is not recommended for production use.
   *
   * @param connection an active JDBC connection to the PostgreSQL database
   * @param baseLocation the GCS location to save the backup files
   * @throws IOException if there is an error during file I/O or database access
   */
  public static void exportPipelinesForPostgres(Connection connection, Location baseLocation) throws IOException {
    LOG.info("Starting export of pipelines from PostgreSQL using direct SQL...");
    String sql = "SELECT * FROM application_specs;";
    List<Map<String, Object>> results = new ArrayList<>();

    try (
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)
    ) {
      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();

      while (resultSet.next()) {
        Map<String, Object> row = new HashMap<>();
        for (int i = 1; i <= columnCount; i++) {
          // Generically read all columns from the row
          row.put(metaData.getColumnName(i), resultSet.getObject(i));
        }
        results.add(row);
      }
    } catch (Exception e) {
      LOG.error("Failed to execute SELECT query on application_specs table.", e);
      throw new IOException("Failed to export pipelines.", e);
    }

    // Write the raw data to a single JSON file in GCS
    Location pipelinesDir = baseLocation.append("pipelines");
    pipelinesDir.mkdirs();
    Location outputFile = pipelinesDir.append("application_specs_export.json");
    try (Writer writer = new OutputStreamWriter(outputFile.getOutputStream(), StandardCharsets.UTF_8)) {
      GSON.toJson(results, writer);
    }
    LOG.info("Successfully exported {} raw pipeline records to {}", results.size(), outputFile.toURI());
  }

  /**
   * Dynamically loads the PostgreSQL JDBC driver from the external directory.
   */
  private static void loadPostgresJDBCDriver(CConfiguration cConf) {
    String driverExtensionPath = cConf.get(Constants.Dataset.DATA_STORAGE_SQL_DRIVER_DIRECTORY, "/opt/cdap/master/ext/jdbc");
    String driverName = cConf.get(Constants.Dataset.DATA_STORAGE_SQL_JDBC_DRIVER_NAME, "org.postgresql.Driver");

    File driverExtensionDir = new File(driverExtensionPath, "postgresql");
    if (!driverExtensionDir.exists() || !driverExtensionDir.isDirectory()) {
      throw new IllegalArgumentException(
          "The PostgreSQL JDBC driver directory " + driverExtensionDir + " does not exist.");
    }

    try {
      // Create a classloader that only contains the driver JARs
      ClassLoader driverClassLoader = new DirectoryClassLoader(driverExtensionDir, null);
      Driver driver = (Driver) Class.forName(driverName, true, driverClassLoader).newInstance();

      // The JDBCDriverManager needs a shim to work with drivers from a different classloader
      JDBCDriverShim driverShim = new JDBCDriverShim(driver);
      DriverManager.registerDriver(driverShim);
      LOG.info("Successfully loaded and registered PostgreSQL JDBC driver from {}", driverExtensionDir.getAbsolutePath());
    } catch (Exception e) {
      throw new RuntimeException("Failed to load PostgreSQL JDBC driver", e);
    }
  }

  /**
   * A simple POJO to hold the raw row data from the namespaces table.
   */
  static class RawNamespaceData {
    private String namespace;
    private String namespaceMetadataJson;

    public String getNamespace() {
      return namespace;
    }
    public void setNamespace(String namespace) {
      this.namespace = namespace;
    }
    public String getNamespaceMetadataJson() {
      return namespaceMetadataJson;
    }
    public void setNamespaceMetadataJson(String namespaceMetadataJson) {
      this.namespaceMetadataJson = namespaceMetadataJson;
    }
  }
}