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

package co.cask.cdap.template.etl.common;

import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchContext;
import co.cask.cdap.template.etl.batch.DriverHelpers;
import co.cask.cdap.template.etl.batch.sink.DBSink;
import co.cask.cdap.template.etl.batch.source.DBSource;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * Utility methods for Database plugins shared by {@link DBSource} and {@link DBSink}
 */
public final class DBUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DBUtils.class);

  public static void checkCredentials(PipelineConfigurer pipelineConfigurer, DBConfig dbConfig,
                                      DriverHelpers driverHelpers) {
    Preconditions.checkArgument(!(dbConfig.user == null && dbConfig.password != null),
                                "dbUser is null. Please provide both user name and password if database requires " +
                                  "authentication. If not, please remove dbPassword and retry.");
    Preconditions.checkArgument(!(dbConfig.user != null && dbConfig.password == null),
                                "dbPassword is null. Please provide both user name and password if database requires" +
                                  "authentication. If not, please remove dbUser and retry.");
    driverHelpers.driverClass = pipelineConfigurer.usePluginClass(dbConfig.jdbcPluginType,
                                                             dbConfig.jdbcPluginName,
                                                             getJDBCPluginID(dbConfig),
                                                             PluginProperties.builder().build());
    Preconditions.checkArgument(driverHelpers.driverClass != null, "JDBC Driver class must be found.");
  }

  public static String getJDBCPluginID(DBConfig dbConfig) {
    return String.format("%s.%s.%s", "source", dbConfig.jdbcPluginType, dbConfig.jdbcPluginName);
  }

  public static Connection createConnection(DBConfig dbConfig) throws java.sql.SQLException {
    return dbConfig.user == null ?
      DriverManager.getConnection(dbConfig.connectionString) :
      DriverManager.getConnection(dbConfig.connectionString, dbConfig.user, dbConfig.password);
  }

  /*
   * Throws InstantiationException or IllegalAccessException if it can't get access to the jdbcDriverClass
   * In either case, the connection to the driver failed
   */

  public static void ensureJDBCDriverIsAvailable(DBConfig dbConfig, DriverHelpers driverHelpers) throws Exception {
    try {
      DriverManager.getDriver(dbConfig.connectionString);
    } catch (SQLException e) {
      // Driver not found. We will try to register it with the DriverManager.

      // driverClass should have been initialized in configurePipeline, so this error should never be reached
      assert driverHelpers.driverClass != null;

      LOG.debug("Plugin Type: {} and Plugin Name: {}; Driver Class: {} not found. " +
                  "Registering JDBC driver via shim {} ",
                dbConfig.jdbcPluginType, dbConfig.jdbcPluginName, driverHelpers.driverClass.getName(),
                JDBCDriverShim.class.getName());
      if (driverHelpers.driverShim == null) {
        driverHelpers.driverShim = new JDBCDriverShim(driverHelpers.driverClass.newInstance());

        // When JDBC Driver class is loaded, the driver class automatically registers itself with DriverManager. 
        // However, this driver is not usable due to different classloader used in plugins. 
        // Hence de-register this driver class, and any other driver classes registered by this classloader.
        deregisterAllDrivers(driverHelpers.driverClass);
      }
      DriverManager.registerDriver(driverHelpers.driverShim);
    }
  }

  public static Job prepareRunGetJob(BatchContext context, DBConfig dbConfig, DriverHelpers driverHelpers) {
    Job job = context.getHadoopJob();
    Configuration hConf = job.getConfiguration();
    // Load the plugin class to make sure it is available.
    driverHelpers.driverClass = context.loadPluginClass(getJDBCPluginID(dbConfig));
    if (dbConfig.user == null && dbConfig.password == null) {
      DBConfiguration.configureDB(hConf, driverHelpers.driverClass.getName(), dbConfig.connectionString);
    } else {
      DBConfiguration.configureDB(hConf, driverHelpers.driverClass.getName(), dbConfig.connectionString,
                                  dbConfig.user, dbConfig.password);
    }
    return job;
  }

  public static void destroy(DriverHelpers driverHelpers) {
      try {
        DriverManager.deregisterDriver(driverHelpers.driverShim);
      } catch (SQLException e) {
        LOG.warn("Error while deregistering JDBC drivers in ETLDBOutputFormat.", e);
        throw Throwables.propagate(e);
      }
    cleanup(driverHelpers.driverClass);
  }

  /**
   * Performs any Database related cleanup
   *
   * @param driverClass the JDBC driver class
   */
  public static void cleanup(Class<? extends Driver> driverClass) {
    shutDownMySQLAbandonedConnectionCleanupThread(driverClass.getClassLoader());
  }

  /**
   * De-register all SQL drivers that are associated with the class
   */
  public static void deregisterAllDrivers(Class<? extends Driver> driverClass)
    throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException {
    Field field = DriverManager.class.getDeclaredField("registeredDrivers");
    field.setAccessible(true);
    List<?> list = (List<?>) field.get(null);
    for (Object driverInfo : list) {
      Class<?> driverInfoClass = DBUtils.class.getClassLoader().loadClass("java.sql.DriverInfo");
      Field driverField = driverInfoClass.getDeclaredField("driver");
      driverField.setAccessible(true);
      Driver d = (Driver) driverField.get(driverInfo);
      if (d == null) {
        LOG.debug("Found null driver object in drivers list. Ignoring.");
        continue;
      }
      LOG.debug("Removing non-null driver object from drivers list.");
      ClassLoader registeredDriverClassLoader = d.getClass().getClassLoader();
      if (registeredDriverClassLoader == null) {
        LOG.debug("Found null classloader for default driver {}. Ignoring since this may be using system classloader.",
                  d.getClass().getName());
        continue;
      }
      // Remove all objects in this list that were created using the classloader of the caller.
      if (d.getClass().getClassLoader().equals(driverClass.getClassLoader())) {
        LOG.debug("Removing default driver {} from registeredDrivers", d.getClass().getName());
        list.remove(driverInfo);
      }
    }
  }

  /**
   * Shuts down a cleanup thread com.mysql.jdbc.AbandonedConnectionCleanupThread that mysql driver fails to destroy
   * If this is not done, the thread keeps a reference to the classloader, thereby causing OOMs or too many open files
   *
   * @param classLoader the unfiltered classloader of the jdbc driver class
   */
  private static void shutDownMySQLAbandonedConnectionCleanupThread(ClassLoader classLoader) {
    if (classLoader == null) {
      return;
    }
    try {
      Class<?> mysqlCleanupThreadClass = classLoader.loadClass("com.mysql.jdbc.AbandonedConnectionCleanupThread");
      Method shutdownMethod = mysqlCleanupThreadClass.getMethod("shutdown");
      shutdownMethod.invoke(null);
      LOG.info("Successfully shutdown MySQL connection cleanup thread.");
    } catch (Throwable e) {
      // cleanup failed, ignoring silently
      LOG.warn("Failed to shutdown MySQL connection cleanup thread. Ignoring.");
    }
  }

  private DBUtils() {
    throw new AssertionError("Should not instantiate static utility class.");
  }
}
