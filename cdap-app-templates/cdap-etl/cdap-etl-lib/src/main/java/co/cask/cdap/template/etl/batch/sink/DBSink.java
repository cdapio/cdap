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

package co.cask.cdap.template.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.common.DBConfig;
import co.cask.cdap.template.etl.common.DBRecord;
import co.cask.cdap.template.etl.common.DBUtils;
import co.cask.cdap.template.etl.common.ETLDBOutputFormat;
import co.cask.cdap.template.etl.common.JDBCDriverShim;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Sink that can be configured to export data to a database table.
 */
@Plugin(type = "sink")
@Name("Database")
@Description("Writes records to a database table. Each record will be written to a row in the table.")
public class DBSink extends BatchSink<StructuredRecord, DBRecord, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(DBSink.class);

  private final DBSinkConfig dbSinkConfig;
  private Class<? extends Driver> driverClass;
  private JDBCDriverShim driverShim;
  private int [] columnTypes;

  public DBSink(DBSinkConfig dbSinkConfig) {
    this.dbSinkConfig = dbSinkConfig;
  }

  private String getJDBCPluginId() {
    return String.format("%s.%s.%s", "sink", dbSinkConfig.jdbcPluginType, dbSinkConfig.jdbcPluginName);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Preconditions.checkArgument(!(dbSinkConfig.user == null && dbSinkConfig.password != null),
                                "dbUser is null. Please provide both user name and password if database requires " +
                                  "authentication. If not, please remove dbPassword and retry.");
    Preconditions.checkArgument(!(dbSinkConfig.user != null && dbSinkConfig.password == null),
                                "dbPassword is null. Please provide both user name and password if database requires" +
                                  "authentication. If not, please remove dbUser and retry.");
    Class<? extends Driver> jdbcDriverClass = pipelineConfigurer.usePluginClass(dbSinkConfig.jdbcPluginType,
                                                                                dbSinkConfig.jdbcPluginName,
                                                                                getJDBCPluginId(),
                                                                                PluginProperties.builder().build());
    Preconditions.checkArgument(
      jdbcDriverClass != null, "Unable to load JDBC Driver class for plugin name '%s'. Please make sure that the " +
        "plugin '%s' of type '%s' containing the driver has been installed correctly.", dbSinkConfig.jdbcPluginName,
      dbSinkConfig.jdbcPluginName, dbSinkConfig.jdbcPluginType);
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    LOG.debug("tableName = {}; pluginType = {}; pluginName = {}; connectionString = {}; columns = {}",
              dbSinkConfig.tableName, dbSinkConfig.jdbcPluginType, dbSinkConfig.jdbcPluginName,
              dbSinkConfig.connectionString, dbSinkConfig.columns);

    Job job = context.getHadoopJob();
    Configuration hConf = job.getConfiguration();

    // Load the plugin class to make sure it is available.
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());
    if (dbSinkConfig.user == null && dbSinkConfig.password == null) {
      DBConfiguration.configureDB(hConf, driverClass.getName(), dbSinkConfig.connectionString);
    } else {
      DBConfiguration.configureDB(hConf, driverClass.getName(), dbSinkConfig.connectionString,
                                  dbSinkConfig.user, dbSinkConfig.password);
    }
    List<String> fields = Lists.newArrayList(Splitter.on(",").omitEmptyStrings().split(dbSinkConfig.columns));
    try {
      ETLDBOutputFormat.setOutput(job, dbSinkConfig.tableName, fields.toArray(new String[fields.size()]));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    job.setOutputFormatClass(ETLDBOutputFormat.class);
  }

  @Override
  public void initialize(BatchSinkContext context) throws Exception {
    super.initialize(context);
    driverClass = context.loadPluginClass(getJDBCPluginId());
    setResultSetMetadata();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<DBRecord, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<DBRecord, NullWritable>(new DBRecord(input, columnTypes), null));
  }

  @Override
  public void destroy() {
    try {
      DriverManager.deregisterDriver(driverShim);
    } catch (SQLException e) {
      LOG.warn("Error while deregistering JDBC drivers in ETLDBOutputFormat.", e);
      throw Throwables.propagate(e);
    }
    DBUtils.cleanup(driverClass);
  }

  private void setResultSetMetadata() throws Exception {
    ensureJDBCDriverIsAvailable();
    Connection connection;
    if (dbSinkConfig.user == null) {
      connection = DriverManager.getConnection(dbSinkConfig.connectionString);
    } else {
      connection = DriverManager.getConnection(dbSinkConfig.connectionString, dbSinkConfig.user, dbSinkConfig.password);
    }
    try {
      try (Statement statement = connection.createStatement();
           // Run a query against the DB table that returns 0 records, but returns valid ResultSetMetadata
           // that can be used to construct DBRecord objects to sink to the database table.
           ResultSet rs = statement.executeQuery(String.format("SELECT %s FROM %s WHERE 1 = 0",
                                                               dbSinkConfig.columns, dbSinkConfig.tableName))
      ) {
        ResultSetMetaData resultSetMetadata = rs.getMetaData();
        int columnCount = resultSetMetadata.getColumnCount();
        columnTypes = new int[columnCount];
        // JDBC driver column indices start with 1
        for (int i = 0; i < columnCount; i++) {
          columnTypes[i] = resultSetMetadata.getColumnType(i + 1);
        }
      }
    } finally {
      connection.close();
    }
  }

  /**
   * Ensures that the JDBC driver is available for {@link DriverManager}
   *
   * @throws Exception if the driver is not available
   */
  private void ensureJDBCDriverIsAvailable() throws Exception {
    try {
      DriverManager.getDriver(dbSinkConfig.connectionString);
    } catch (SQLException e) {
      // Driver not found. We will try to register it with the DriverManager.
      LOG.debug("Plugin Type: {} and Plugin Name: {}; Driver Class: {} not found; registering JDBC driver via shim {} ",
                dbSinkConfig.jdbcPluginType, dbSinkConfig.jdbcPluginName, driverClass.getName(),
                JDBCDriverShim.class.getName());
      driverShim = new JDBCDriverShim(driverClass.newInstance());
      DBUtils.deregisterAllDrivers(driverClass);
      DriverManager.registerDriver(driverShim);
    }
  }

  /**
   * {@link PluginConfig} for {@link DBSink}
   */
  public static class DBSinkConfig extends DBConfig {
    @Description("Comma-separated list of columns in the specified table to export to.")
    String columns;

    @Description("Name of the table to export to.")
    public String tableName;
  }
}
