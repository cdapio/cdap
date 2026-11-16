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

package io.cdap.cdap.spi.data.sql;

import com.google.common.io.ByteStreams;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link PostgreSqlStorageProvider} external JDBC driver loading and exception
 * propagation.
 */
public class PostgreSqlStorageProviderTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  /**
   * Minimal self-contained JDBC driver loaded from an isolated external driver JAR.
   */
  public static class ValidExternalDriver implements Driver {
    @Override
    public Connection connect(String url, Properties info) {
      return null;
    }

    @Override
    public boolean acceptsURL(String url) {
      return url != null && url.startsWith("jdbc:cdaptest:");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
      return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
      return 1;
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
    public Logger getParentLogger() {
      return Logger.getGlobal();
    }
  }

  /**
   * Driver with a private constructor to trigger {@link IllegalAccessException}.
   */
  public static final class PrivateCtorExternalDriver extends ValidExternalDriver {
    private PrivateCtorExternalDriver() {
    }
  }

  /**
   * Abstract driver class to trigger {@link InstantiationException}.
   */
  public abstract static class AbstractExternalDriver extends ValidExternalDriver {
  }

  @Test
  public void testCreateDataSourceLoadsExternalDriverAndWrapsCheckedDriverLoadFailures()
      throws Exception {
    File baseExtDir = TEMP_FOLDER.newFolder();
    File pgDriverDir = new File(baseExtDir, Constants.Dataset.DATA_STORAGE_SQL);
    Assert.assertTrue(pgDriverDir.mkdirs());

    File driverJar = new File(pgDriverDir, "external-jdbc-drivers.jar");
    writeClassesToJar(driverJar,
        ValidExternalDriver.class,
        PrivateCtorExternalDriver.class,
        AbstractExternalDriver.class);

    SConfiguration sConf = SConfiguration.create();
    NoOpMetricsCollectionService metrics = new NoOpMetricsCollectionService();

    // 1. Happy path: valid external JDBC driver loads and registers shim cleanly
    CConfiguration validConf = createBaseSqlConf(baseExtDir, ValidExternalDriver.class.getName());
    DataSource ds = PostgreSqlStorageProvider.createDataSource(validConf, sConf, metrics);
    Assert.assertNotNull(ds);
    if (ds instanceof AutoCloseable) {
      ((AutoCloseable) ds).close();
    }

    // 2. ClassNotFoundException -> wrapped in RuntimeException
    CConfiguration missingConf = createBaseSqlConf(baseExtDir, "no.such.ExternalJdbcDriver");
    try {
      PostgreSqlStorageProvider.createDataSource(missingConf, sConf, metrics);
      Assert.fail("Expected RuntimeException wrapping ClassNotFoundException");
    } catch (RuntimeException actual) {
      Assert.assertTrue("Expected ClassNotFoundException cause, got: " + actual.getCause(),
          actual.getCause() instanceof ClassNotFoundException);
    }

    // 3. IllegalAccessException -> wrapped in RuntimeException
    CConfiguration privateConf =
        createBaseSqlConf(baseExtDir, PrivateCtorExternalDriver.class.getName());
    try {
      PostgreSqlStorageProvider.createDataSource(privateConf, sConf, metrics);
      Assert.fail("Expected RuntimeException wrapping IllegalAccessException");
    } catch (RuntimeException actual) {
      Assert.assertTrue("Expected IllegalAccessException cause, got: " + actual.getCause(),
          actual.getCause() instanceof IllegalAccessException);
    }

    // 4. InstantiationException -> wrapped in RuntimeException
    CConfiguration abstractConf =
        createBaseSqlConf(baseExtDir, AbstractExternalDriver.class.getName());
    try {
      PostgreSqlStorageProvider.createDataSource(abstractConf, sConf, metrics);
      Assert.fail("Expected RuntimeException wrapping InstantiationException");
    } catch (RuntimeException actual) {
      Assert.assertTrue("Expected InstantiationException cause, got: " + actual.getCause(),
          actual.getCause() instanceof InstantiationException);
    }

    // Cleanup registered shim drivers so global DriverManager stays clean
    java.util.Enumeration<Driver> drivers = java.sql.DriverManager.getDrivers();
    while (drivers.hasMoreElements()) {
      Driver d = drivers.nextElement();
      if (d.acceptsURL("jdbc:cdaptest://localhost/db")) {
        try {
          java.sql.DriverManager.deregisterDriver(d);
        } catch (SQLException ignored) {
          // ignore
        }
      }
    }
  }

  private static CConfiguration createBaseSqlConf(File baseExtDir, String driverClassName) {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_SQL);
    cConf.setBoolean(Constants.Dataset.DATA_STORAGE_SQL_DRIVER_EXTERNAL, true);
    cConf.set(Constants.Dataset.DATA_STORAGE_SQL_DRIVER_DIRECTORY, baseExtDir.getAbsolutePath());
    cConf.set(Constants.Dataset.DATA_STORAGE_SQL_JDBC_DRIVER_NAME, driverClassName);
    cConf.set(Constants.Dataset.DATA_STORAGE_SQL_JDBC_CONNECTION_URL,
        "jdbc:cdaptest://localhost/db");
    return cConf;
  }

  private static void writeClassesToJar(File jarFile, Class<?>... classes) throws IOException {
    try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile))) {
      for (Class<?> cls : classes) {
        String entryPath = cls.getName().replace('.', '/') + ".class";
        jos.putNextEntry(new JarEntry(entryPath));
        try (InputStream is = cls.getClassLoader().getResourceAsStream(entryPath)) {
          Assert.assertNotNull("Missing class resource: " + entryPath, is);
          ByteStreams.copy(is, jos);
        }
        jos.closeEntry();
      }
    }
  }
}
