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

package co.cask.cdap.template.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSource;
import co.cask.cdap.template.etl.api.batch.BatchSourceContext;
import co.cask.cdap.template.etl.batch.DriverHelpers;
import co.cask.cdap.template.etl.common.DBConfig;
import co.cask.cdap.template.etl.common.DBRecord;
import co.cask.cdap.template.etl.common.DBUtils;
import co.cask.cdap.template.etl.common.ETLDBInputFormat;
import co.cask.cdap.template.etl.common.JDBCDriverShim;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Batch source to read from a Database table
 */
@Plugin(type = "source")
@Name("Database")
@Description("Batch source for a database")
public class DBSource extends BatchSource<LongWritable, DBRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(DBSource.class);

  private static final String IMPORT_QUERY_DESCRIPTION = "The SELECT query to use to import data from the specified " +
    "table. You can specify an arbitrary number of columns to import, or import all columns using *. " +
    "You can also specify a number of WHERE clauses or ORDER BY clauses. However, LIMIT and OFFSET clauses " +
    "should not be used in this query.";
  private static final String COUNT_QUERY_DESCRIPTION = "The SELECT query to use to get the count of records to " +
    "import from the specified table. Examples: SELECT COUNT(*) from <my_table> where <my_column> 1, " +
    "SELECT COUNT(my_column) from my_table). NOTE: Please include the same WHERE clauses in this query as the ones " +
    "used in the import query to reflect an accurate number of records to import.";

  private final DBSourceConfig dbSourceConfig;
  private DriverHelpers driverHelpers;

  public DBSource(DBSourceConfig dbSourceConfig) {
    this.dbSourceConfig = dbSourceConfig;
    driverHelpers = new DriverHelpers(null, null);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    DBUtils.checkCredentials(pipelineConfigurer, dbSourceConfig, driverHelpers);
    try {
      ensureValidConnection();
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    } finally {
      DBUtils.destroy(driverHelpers);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    LOG.debug("pluginType = {}; pluginName = {}; connectionString = {}; importQuery = {}; " +
                "countQuery = {}",
              dbSourceConfig.jdbcPluginType, dbSourceConfig.jdbcPluginName,
              dbSourceConfig.connectionString, dbSourceConfig.importQuery, dbSourceConfig.countQuery);
    try {
      Job job = DBUtils.prepareRunGetJob(context, dbSourceConfig, driverHelpers);
      ETLDBInputFormat.setInput(job, DBRecord.class, dbSourceConfig.importQuery, dbSourceConfig.countQuery);
      job.setInputFormatClass(ETLDBInputFormat.class);
    } finally {
      DBUtils.destroy(driverHelpers);
    }
  }

  @Override
  public void initialize(BatchSourceContext context) throws Exception {
    super.initialize(context);
    driverHelpers.driverClass = context.loadPluginClass(DBUtils.getJDBCPluginID(dbSourceConfig));
  }

  @Override
  public void transform(KeyValue<LongWritable, DBRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(input.getValue().getRecord());
  }

  @Override
  public void destroy() {
    DBUtils.destroy(driverHelpers);
  }

  private void ensureValidConnection() throws Exception {
    DBUtils.ensureJDBCDriverIsAvailable(dbSourceConfig, driverHelpers);
    try (Connection connection = DBUtils.createConnection(dbSourceConfig)) {
      assert(connection.isValid(0));
    }
  }

  /**
   * {@link PluginConfig} for {@link DBSource}
   */
  public static class DBSourceConfig extends DBConfig {
    @Description(IMPORT_QUERY_DESCRIPTION)
    String importQuery;

    @Description(COUNT_QUERY_DESCRIPTION)
    String countQuery;
  }
}
