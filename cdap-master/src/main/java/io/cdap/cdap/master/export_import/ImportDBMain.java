package io.cdap.cdap.master.export_import;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
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
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
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
 * A Proof-of-Concept importer that uses direct JDBC and raw SQL queries,
 * bypassing the CDAP Data Access Layer to import data into PostgreSQL.
 */
public class ImportDBMain {

  private static final Logger LOG = LoggerFactory.getLogger(ImportDBMain.class);
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final Type RAW_NAMESPACE_LIST_TYPE = new TypeToken<ArrayList<RawNamespaceData>>() {}.getType();
  private static final Type RAW_PIPELINE_LIST_TYPE = new TypeToken<ArrayList<Map<String, Object>>>() {}.getType();

  public static void main(String[] args) {
    LOG.info("Starting Direct SQL Import Job for PostgreSQL...");

    if (args.length < 1) {
      LOG.error("Usage: ImportDBMain <gcs-bucket-uri>");
      LOG.error("Example: ImportDBMain gs://my-backup-bucket/run-123");
      System.exit(1);
    }

    Injector injector = initializeInjector();
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    SConfiguration sConf = injector.getInstance(SConfiguration.class);
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    String gcsBackupPath = "gs://" + args[0] + "/";

    String storageImpl = cConf.get(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION);
    if (!Dataset.DATA_STORAGE_SQL.equalsIgnoreCase(storageImpl)) {
      LOG.error("This importer is designed only for PostgreSQL (storage implementation 'sql'). Found '{}'. Aborting.",
          storageImpl);
      System.exit(1);
    }

    try (Connection connection = getConnectionForPostgres(cConf, sConf)) {
      LOG.info("Successfully connected to the PostgreSQL database.");

      Location baseLocation = locationFactory.create(gcsBackupPath).append("direct-sql-export");
      importNamespacesForPostgres(connection, baseLocation);
      importPipelinesForPostgres(connection, baseLocation);
      // In a full implementation, you would add calls to import other tables here.

    } catch (Exception e) {
      LOG.error("Import failed with an unrecoverable error.", e);
      System.exit(1);
    }
    LOG.info("Direct SQL Import Job finished successfully.");
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
   * Imports all namespaces by executing a raw "INSERT ... ON CONFLICT" query against PostgreSQL.
   */
  public static void importNamespacesForPostgres(Connection connection, Location baseLocation) throws IOException {
    LOG.info("Starting import of namespaces into PostgreSQL using direct SQL...");
    Location inputFile = baseLocation.append("namespaces").append("namespaces_export.json");

    if (!inputFile.exists()) {
      LOG.error("Backup file not found at {}. Cannot import namespaces.", inputFile.toURI());
      return;
    }

    List<RawNamespaceData> recordsToImport;
    try (Reader reader = new InputStreamReader(inputFile.getInputStream(), StandardCharsets.UTF_8)) {
      recordsToImport = GSON.fromJson(reader, RAW_NAMESPACE_LIST_TYPE);
    }
    LOG.info("Read {} namespace records from backup file.", recordsToImport.size());

    String upsertSql = "INSERT INTO namespaces (namespace, namespace_metadata) VALUES (?, ?) " +
        "ON CONFLICT (namespace) DO UPDATE SET namespace_metadata = EXCLUDED.namespace_metadata;";

    try (PreparedStatement statement = connection.prepareStatement(upsertSql)) {
      for (RawNamespaceData record : recordsToImport) {
        statement.setString(1, record.getNamespace());
        statement.setString(2, record.getNamespaceMetadataJson());
        statement.addBatch();
      }
      int[] updateCounts = statement.executeBatch();
      LOG.info("Successfully executed batch insert/update for {} namespace records.", updateCounts.length);
    } catch (Exception e) {
      LOG.error("Failed to execute batch UPSERT on namespaces table.", e);
      throw new IOException("Failed to import namespaces.", e);
    }
  }

  /**
   * Imports all pipelines by executing a raw "INSERT ... ON CONFLICT" query against PostgreSQL.
   */
  public static void importPipelinesForPostgres(Connection connection, Location baseLocation) throws IOException, SQLException {
    LOG.info("Starting import of pipelines into PostgreSQL using direct SQL...");
    Location inputFile = baseLocation.append("application_specs").append("application_specs_export.json");
    if (!inputFile.exists()) {
      LOG.warn("Backup file for application_specs not found, skipping pipeline import.");
      return;
    }

    List<Map<String, Object>> recordsToImport;
    try (Reader reader = new InputStreamReader(inputFile.getInputStream(), StandardCharsets.UTF_8)) {
      recordsToImport = GSON.fromJson(reader, RAW_PIPELINE_LIST_TYPE);
    }
    LOG.info("Read {} pipeline records from backup file.", recordsToImport.size());

    String upsertSql = "INSERT INTO application_specs (namespace, application, version, application_data, created, " +
        "author, change_summary, latest, source_control_metadata, application_data_compressor_type) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
        "ON CONFLICT (namespace, application, version) DO UPDATE SET " +
        "application_data = EXCLUDED.application_data, " +
        "created = EXCLUDED.created, " +
        "author = EXCLUDED.author, " +
        "change_summary = EXCLUDED.change_summary, " +
        "latest = EXCLUDED.latest, " +
        "source_control_metadata = EXCLUDED.source_control_metadata, " +
        "application_data_compressor_type = EXCLUDED.application_data_compressor_type;";

    try (PreparedStatement statement = connection.prepareStatement(upsertSql)) {
      for (Map<String, Object> record : recordsToImport) {
        statement.setString(1, (String) record.get("namespace"));
        statement.setString(2, (String) record.get("application"));
        statement.setString(3, (String) record.get("version"));
        statement.setString(4, (String) record.get("application_data"));
        statement.setObject(5, record.get("created"), Types.BIGINT);
        statement.setString(6, (String) record.get("author"));
        statement.setString(7, (String) record.get("change_summary"));
        statement.setObject(8, record.get("latest"), Types.BOOLEAN);
        statement.setString(9, (String) record.get("source_control_metadata"));
        statement.setString(10, (String) record.get("application_data_compressor_type"));
        statement.addBatch();
      }
      int[] updateCounts = statement.executeBatch();
      LOG.info("Successfully executed batch insert/update for {} pipeline records.", updateCounts.length);
    } catch (Exception e) {
      LOG.error("Failed to execute batch UPSERT on application_specs table.", e);
      throw new IOException("Failed to import pipelines.", e);
    }
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