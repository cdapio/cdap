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
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.common.DBConfig;
import co.cask.cdap.template.etl.common.DBHelper;
import co.cask.cdap.template.etl.common.DBRecord;
import co.cask.cdap.template.etl.common.ETLDBOutputFormat;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
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
@Description("Batch sink for a database.")
public class DBSink extends BatchSink<StructuredRecord, DBRecord, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(DBSink.class);

  private static final String COLUMNS_DESCRIPTION = "Comma-separated list of columns to export to in the specified " +
    "table.";
  private static final String TABLE_NAME_DESCRIPTION = "Table name to export to.";

  private final DBHelper dbHelper;
  private final DBSinkConfig dbSinkConfig;
  private ResultSetMetaData resultSetMetadata;

  public DBSink(DBSinkConfig dbSinkConfig) {
    this.dbSinkConfig = dbSinkConfig;
    dbHelper = new DBHelper(dbSinkConfig);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    dbHelper.checkCredentials(pipelineConfigurer);
    try {
      validateDBConnection();
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    } finally {
      dbHelper.destroy();
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    LOG.debug("tableName = {}; pluginType = {}; pluginName = {}; connectionString = {}; importQuery = {}; columns = {}",
              dbSinkConfig.tableName, dbSinkConfig.jdbcPluginType, dbSinkConfig.jdbcPluginName,
              dbSinkConfig.connectionString, dbSinkConfig.columns);
    try {
      Job job = dbHelper.prepareRunGetJob(context);
      List<String> fields = Lists.newArrayList(Splitter.on(",").omitEmptyStrings().split(dbSinkConfig.columns));
      try {
        ETLDBOutputFormat.setOutput(job, dbSinkConfig.tableName,
                                    fields.toArray(new String[fields.size()]));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
      job.setOutputFormatClass(ETLDBOutputFormat.class);
    } finally {
      dbHelper.destroy();
    }
  }

  @Override
  public void initialize(BatchSinkContext context) throws Exception {
    super.initialize(context);
    dbHelper.driverClass = context.loadPluginClass(dbHelper.getJDBCPluginID());
    setResultSetMetadata();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<DBRecord, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<DBRecord, NullWritable>(new DBRecord(input, resultSetMetadata), null));
  }

  @Override
  public void destroy() {
    dbHelper.destroy();
  }

  private void setResultSetMetadata() throws Exception {
    dbHelper.ensureJDBCDriverIsAvailable();
    try (Connection connection = dbHelper.createConnection()) {
      try (Statement statement = connection.createStatement();
           // Using LIMIT even though its not SQL standard since DBInputFormat already depends on it
           ResultSet rs = statement.executeQuery(String.format("SELECT %s from %s LIMIT 1",
                                                               dbSinkConfig.columns, dbSinkConfig.tableName))
      ) {
        resultSetMetadata = rs.getMetaData();
      }
    }
  }


  private void validateDBConnection() throws Exception {
    dbHelper.ensureJDBCDriverIsAvailable();
    Preconditions.checkArgument(tableExists(), "Invalid table name %s", dbSinkConfig.tableName);
  }

  private boolean tableExists() throws SQLException {
    try (Connection connection = dbHelper.createConnection();
         ResultSet rs = connection.getMetaData()
           .getTables(null, null, dbSinkConfig.tableName, null)) {
      return rs.next();
    }
  }

  /**
   * {@link PluginConfig} for {@link DBSink}
   */
  public static class DBSinkConfig extends DBConfig {
    @Description(COLUMNS_DESCRIPTION)
    String columns;

    @Description(TABLE_NAME_DESCRIPTION)
    public String tableName;
  }
}
