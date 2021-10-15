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

package io.cdap.cdap.spi.data.sql.jdbc;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.spi.data.sql.PostgreSqlStorageProvider;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class DataSourceProviderTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testInstantiate() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_SQL);
    cConf.set(Constants.Dataset.DATA_STORAGE_SQL_DRIVER_DIRECTORY, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.Dataset.DATA_STORAGE_SQL_JDBC_DRIVER_NAME, NoopDriver.class.getName());
    cConf.set(Constants.Dataset.DATA_STORAGE_SQL_JDBC_CONNECTION_URL, "jdbc:noop://");

    File driverDir = new File(cConf.get(Constants.Dataset.DATA_STORAGE_SQL_DRIVER_DIRECTORY),
                              cConf.get(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION));
    driverDir.mkdirs();
    AppJarHelper.createDeploymentJar(new LocalLocationFactory(driverDir), NoopDriver.class);

    SConfiguration sConf = SConfiguration.create();

    DataSource dataSource = PostgreSqlStorageProvider.createDataSource(cConf, sConf,
                                                                       new NoOpMetricsCollectionService());
    Assert.assertNotNull(dataSource);
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    Driver loadedDriver = null;

    // the DriverManager can contain the postgres driver since we use embedded postgres, the DriverManager will load
    // that driver initially.
    int count = 0;
    while (drivers.hasMoreElements()) {
      Driver driver = drivers.nextElement();
      // we will wrap the driver
      if (driver instanceof JDBCDriverShim) {
        loadedDriver = driver;
      }
      count++;
    }
    Assert.assertEquals(2, count);
    Assert.assertNotNull(loadedDriver);
  }

  public static final class NoopDriver implements Driver {

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
      return null;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
      return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
      return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
      return 0;
    }

    @Override
    public int getMinorVersion() {
      return 0;
    }

    @Override
    public boolean jdbcCompliant() {
      return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return null;
    }
  }
}
