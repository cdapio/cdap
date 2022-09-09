/*
 * Copyright © 2021-2022 Cask Data, Inc.
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

package io.cdap.cdap.spi.data.sql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.lang.DirectoryClassLoader;
import io.cdap.cdap.spi.data.StorageProvider;
import io.cdap.cdap.spi.data.StorageProviderContext;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.sql.jdbc.JDBCDriverShim;
import io.cdap.cdap.spi.data.sql.jdbc.MetricsDataSource;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;

/**
 * A {@link StorageProvider} implementation for using PostgreSQL as the storage engine.
 */
public class PostgreSqlStorageProvider implements StorageProvider {

  private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlStorageProvider.class);

  private final DataSource dataSource;
  private final StructuredTableAdmin admin;
  private final TransactionRunner txRunner;

  @Inject
  PostgreSqlStorageProvider(CConfiguration cConf, SConfiguration sConf,
                            MetricsCollectionService metricsCollectionService) {
    this.dataSource = createDataSource(cConf, sConf, metricsCollectionService);
    this.admin = new PostgreSqlStructuredTableAdmin(dataSource);
    this.txRunner = new RetryingSqlTransactionRunner(admin, dataSource, metricsCollectionService, cConf,
      cConf.getInt(Constants.Dataset.DATA_STORAGE_SQL_SCAN_FETCH_SIZE_ROWS));
  }

  @Override
  public void initialize(StorageProviderContext context) throws Exception {
    try (Connection connection = dataSource.getConnection()) {
      // Just to ping the connection and close it to populate the connection pool.
      connection.isValid(5);
    } catch (SQLException e) {
      throw new IOException("Failed to contact database", e);
    }
  }

  @Override
  public void close() throws Exception {
    if (dataSource instanceof AutoCloseable) {
      ((AutoCloseable) dataSource).close();
    }
  }

  @Override
  public String getName() {
    return Constants.Dataset.DATA_STORAGE_SQL;
  }

  @Override
  public StructuredTableAdmin getStructuredTableAdmin() {
    return admin;
  }

  @Override
  public TransactionRunner getTransactionRunner() {
    return txRunner;
  }

  /**
   * Creates a {@link DataSource} for the sql implementation to use. It optionally loads an external JDBC driver
   * to use with JDBC.
   */
  @VisibleForTesting
  public static DataSource createDataSource(CConfiguration cConf, SConfiguration sConf,
                                            MetricsCollectionService metricsCollectionService) {
    String storageImpl = cConf.get(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION);
    if (!storageImpl.equals(Constants.Dataset.DATA_STORAGE_SQL)) {
      throw new IllegalArgumentException(String.format("The storage implementation is not %s, cannot create the " +
                                                         "DataSource", Constants.Dataset.DATA_STORAGE_SQL));
    }

    if (cConf.getBoolean(Constants.Dataset.DATA_STORAGE_SQL_DRIVER_EXTERNAL)) {
      loadJDBCDriver(cConf, storageImpl);
    }

    String jdbcUrl = cConf.get(Constants.Dataset.DATA_STORAGE_SQL_JDBC_CONNECTION_URL);
    if (jdbcUrl == null) {
      throw new IllegalArgumentException("The jdbc connection url is not specified.");
    }
    Properties properties = retrieveJDBCConnectionProperties(cConf, sConf);
    LOG.info("Creating the DataSource with jdbc url: {}", jdbcUrl);

    ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(jdbcUrl, properties);
    PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
    // The GenericObjectPool is thread safe according to the javadoc,
    // the PoolingDataSource will be thread safe as long as the connectin pool is thread-safe
    GenericObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);
    poolableConnectionFactory.setPool(connectionPool);
    connectionPool.setMaxTotal(cConf.getInt(Constants.Dataset.DATA_STORAGE_SQL_CONNECTION_SIZE));
    PoolingDataSource<PoolableConnection> dataSource = new PoolingDataSource<>(connectionPool);
    return new MetricsDataSource(dataSource, metricsCollectionService, connectionPool);
  }

  private static Properties retrieveJDBCConnectionProperties(CConfiguration cConf, SConfiguration sConf) {
    Properties properties = new Properties();
    String username = sConf.get(Constants.Dataset.DATA_STORAGE_SQL_USERNAME);
    String password = sConf.get(Constants.Dataset.DATA_STORAGE_SQL_PASSWORD);
    if ((username == null) != (password == null)) {
      throw new IllegalArgumentException("The username and password for the jdbc connection must both be set" +
                                           " or both not be set.");
    }

    if (username != null) {
      properties.setProperty("user", username);
      properties.setProperty("password", password);
    }

    for (Map.Entry<String, String> cConfEntry : cConf) {
      if (cConfEntry.getKey().startsWith(Constants.Dataset.DATA_STORAGE_SQL_PROPERTY_PREFIX)) {
        properties.put(cConfEntry.getKey().substring(Constants.Dataset.DATA_STORAGE_SQL_PROPERTY_PREFIX.length()),
                       cConfEntry.getValue());
      }
    }
    return properties;
  }

  private static void loadJDBCDriver(CConfiguration cConf, String storageImpl) {
    String driverExtensionPath = cConf.get(Constants.Dataset.DATA_STORAGE_SQL_DRIVER_DIRECTORY);
    String driverName = cConf.get(Constants.Dataset.DATA_STORAGE_SQL_JDBC_DRIVER_NAME);
    if (driverExtensionPath == null || driverName == null) {
      throw new IllegalArgumentException("The JDBC driver directory and driver name must be specified.");
    }

    File driverExtensionDir = new File(driverExtensionPath, storageImpl);
    if (!driverExtensionDir.exists()) {
      throw new IllegalArgumentException("The JDBC driver driver " + driverExtensionDir + " does not exist.");
    }

    // Create a separate classloader for the JDBC driver, which doesn't have any CDAP dependencies in it.
    ClassLoader driverClassLoader = new DirectoryClassLoader(driverExtensionDir, null);
    try {
      Driver driver = (Driver) Class.forName(driverName, true, driverClassLoader).newInstance();

      // wrap the driver class and register it ourselves since the driver manager will not use driver from other
      // classloader
      JDBCDriverShim driverShim = new JDBCDriverShim(driver);
      DriverManager.registerDriver(driverShim);
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
      throw Throwables.propagate(e);
    }

    LOG.info("Successfully loaded {} from {}", driverName, driverExtensionPath);
  }
}
