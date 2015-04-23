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

package co.cask.cdap.templates.etl.batch.sinks;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSinkContext;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.common.DBRecord;
import co.cask.cdap.templates.etl.common.ETLDBOutputFormat;
import co.cask.cdap.templates.etl.common.Properties;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * Sink that can be configured to export data to a database table
 */
public class DBSink extends BatchSink<StructuredRecord, DBRecord, NullWritable> {

  private ResultSetMetaData resultSetMetadata;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setDescription("Batch sink for database");
    configurer.addProperty(new Property(Properties.DB.DRIVER_CLASS, "Driver class to connect to the database", true));
    configurer.addProperty(
      new Property(Properties.DB.CONNECTION_STRING, "JDBC connection string including database name", true));
    configurer.addProperty(
      new Property(Properties.DB.USER,
                   "User to use to connect to the specified database. " +
                     "Required for databases that require authentication. " +
                     "Optional for databases that do not require authentication.",
                   false));
    configurer.addProperty(
      new Property(Properties.DB.PASSWORD,
                   "Password to use to connect to the specified database. " +
                     "Required for databases that require authentication. " +
                     "Optional for databases that do not require authentication.",
                   false));
    configurer.addProperty(new Property(Properties.DB.TABLE_NAME, "Table name to import", true));
    configurer.addProperty(new Property(Properties.DB.COLUMNS, "Comma-separated list of columns to sink to", true));
  }

  @Override
  public void configurePipeline(ETLStage stageConfig, PipelineConfigurer pipelineConfigurer) {
    Map<String, String> properties = stageConfig.getProperties();
    String dbDriverClass = properties.get(Properties.DB.DRIVER_CLASS);
    String dbConnectionString = properties.get(Properties.DB.CONNECTION_STRING);
    String dbUser = properties.get(Properties.DB.USER);
    String dbPassword = properties.get(Properties.DB.PASSWORD);
    String dbTableName = properties.get(Properties.DB.TABLE_NAME);
    String dbColumns = properties.get(Properties.DB.COLUMNS);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbDriverClass), "dbDriverClass cannot be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbConnectionString), "dbConnectionString cannot be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbTableName), "dbTableName cannot be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbColumns), "dbColumns cannot be null");
    Preconditions.checkArgument(!(dbUser == null && dbPassword != null),
                                "dbUser is null. Please provide both user name and password if database requires " +
                                  "authentication. If not, please remove dbPassword and retry.");
    Preconditions.checkArgument(!(dbUser != null && dbPassword == null),
                                "dbPassword is null. Please provide both user name and password if database requires" +
                                  "authentication. If not, please remove dbUser and retry.");
  }

  @Override
  public void prepareJob(BatchSinkContext context) {
    Map<String, String> runtimeArguments = context.getPluginProperties().getProperties();
    String dbDriverClass = runtimeArguments.get(Properties.DB.DRIVER_CLASS);
    String dbConnectionString = runtimeArguments.get(Properties.DB.CONNECTION_STRING);
    String dbUser = runtimeArguments.get(Properties.DB.USER);
    String dbPassword = runtimeArguments.get(Properties.DB.PASSWORD);
    String dbTableName = runtimeArguments.get(Properties.DB.TABLE_NAME);
    String dbColumns = runtimeArguments.get(Properties.DB.COLUMNS);

    Job job = context.getHadoopJob();
    Configuration conf = job.getConfiguration();
    if (dbUser == null && dbPassword == null) {
      DBConfiguration.configureDB(conf, dbDriverClass, dbConnectionString);
    } else {
      DBConfiguration.configureDB(conf, dbDriverClass, dbConnectionString, dbUser, dbPassword);
    }
    List<String> fields = Lists.newArrayList(Splitter.on(",").omitEmptyStrings().split(dbColumns));
    try {
      ETLDBOutputFormat.setOutput(job, dbTableName, fields.toArray(new String[fields.size()]));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    job.setOutputFormatClass(ETLDBOutputFormat.class);
  }

  @Override
  public void initialize(ETLStage stageConfig) throws Exception {
    super.initialize(stageConfig);
    setResultSetMetadata(stageConfig.getProperties());
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<DBRecord, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<DBRecord, NullWritable>(new DBRecord(input, resultSetMetadata), null));
  }

  private void setResultSetMetadata(Map<String, String> runtimeArgs) throws SQLException {
    String connectionString = runtimeArgs.get(Properties.DB.CONNECTION_STRING);
    String tableName = runtimeArgs.get(Properties.DB.TABLE_NAME);
    String columns = runtimeArgs.containsKey(Properties.DB.COLUMNS) ? runtimeArgs.get(Properties.DB.COLUMNS) : "*";
    String userName = runtimeArgs.get(Properties.DB.USER);
    String password = runtimeArgs.get(Properties.DB.PASSWORD);

    Connection connection;
    if (userName == null) {
      connection = DriverManager.getConnection(connectionString);
    } else {
      connection = DriverManager.getConnection(connectionString, userName, password);
    }
    try {
      Statement statement = connection.createStatement();
      try {
        // Using LIMIT in the following query even though its not SQL standard since DBInputFormat already depends on it
        ResultSet rs = statement.executeQuery(String.format("SELECT %s from %s LIMIT 1", columns, tableName));
        try {
          resultSetMetadata = rs.getMetaData();
        } finally {
          rs.close();
        }
      } finally {
        statement.close();
      }
    } finally {
      connection.close();
    }
  }
}
