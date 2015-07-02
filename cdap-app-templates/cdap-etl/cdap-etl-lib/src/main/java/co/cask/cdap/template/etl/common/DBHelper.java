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
import co.cask.cdap.template.etl.batch.sink.DBSink;
import co.cask.cdap.template.etl.batch.source.DBSource;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * A class to unify methods shared between {@link DBSource} and {@link DBSink}
 */
public class DBHelper {
  public final DBConfig dbConfig;
  public JDBCDriverShim driverShim;
  public Class<? extends Driver> driverClass;

  public DBHelper(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
  }

  private static final Logger LOG = LoggerFactory.getLogger(DBUtils.class);

  public void checkCredentials(PipelineConfigurer pipelineConfigurer) {
    Preconditions.checkArgument(!(dbConfig.user == null && dbConfig.password != null),
                                "dbUser is null. Please provide both user name and password if database requires " +
                                  "authentication. If not, please remove dbPassword and retry.");
    Preconditions.checkArgument(!(dbConfig.user != null && dbConfig.password == null),
                                "dbPassword is null. Please provide both user name and password if database requires" +
                                  "authentication. If not, please remove dbUser and retry.");
    driverClass = pipelineConfigurer.usePluginClass(dbConfig.jdbcPluginType,
                                                                  dbConfig.jdbcPluginName,
                                                                  getJDBCPluginID(),
                                                                  PluginProperties.builder().build());
    Preconditions.checkArgument(driverClass != null, "JDBC Driver class must be found.");
  }

  public String getJDBCPluginID() {
    return String.format("%s.%s.%s", "source", dbConfig.jdbcPluginType, dbConfig.jdbcPluginName);
  }

  public Connection createConnection() throws java.sql.SQLException {
    return dbConfig.user == null ?
      DriverManager.getConnection(dbConfig.connectionString) :
      DriverManager.getConnection(dbConfig.connectionString, dbConfig.user, dbConfig.password);
  }

  /**
   * Ensures that the JDBC driver is available for {@link DriverManager}
   *
   * @throws Exception if the driver is not available
   */
  public void ensureJDBCDriverIsAvailable() throws Exception {
    try {
      DriverManager.getDriver(dbConfig.connectionString);
    } catch (SQLException e) {
      // Driver not found. We will try to register it with the DriverManager.

      LOG.debug("Plugin Type: {} and Plugin Name: {}; Driver Class: {} not found. " +
                  "Registering JDBC driver via shim {} ",
                dbConfig.jdbcPluginType, dbConfig.jdbcPluginName, driverClass.getName(),
                JDBCDriverShim.class.getName());
      if (driverShim == null) {
        driverShim = new JDBCDriverShim(driverClass.newInstance());

        // When JDBC Driver class is loaded, the driver class automatically registers itself with DriverManager. 
        // However, this driver is not usable due to different classloader used in plugins. 
        // Hence de-register this driver class, and any other driver classes registered by this classloader.
        DBUtils.deregisterAllDrivers(driverClass);
      }
      DriverManager.registerDriver(driverShim);
    }
  }

  public Job prepareRunGetJob(BatchContext context) {
    Job job = context.getHadoopJob();
    Configuration hConf = job.getConfiguration();
    // Load the plugin class to make sure it is available.
    driverClass = context.loadPluginClass(getJDBCPluginID());
    if (dbConfig.user == null && dbConfig.password == null) {
      DBConfiguration.configureDB(hConf, driverClass.getName(), dbConfig.connectionString);
    } else {
      DBConfiguration.configureDB(hConf, driverClass.getName(), dbConfig.connectionString,
                                  dbConfig.user, dbConfig.password);
    }
    return job;
  }

  public void destroy() {
    try {
      DriverManager.deregisterDriver(driverShim);
    } catch (SQLException e) {
      LOG.warn("Error while deregistering JDBC drivers in ETLDBOutputFormat.", e);
      throw Throwables.propagate(e);
    }
    DBUtils.cleanup(driverClass);
  }
}
