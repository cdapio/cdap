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

package co.cask.cdap.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.common.FieldCase;
import co.cask.cdap.etl.common.StructuredRecordUtils;
import co.cask.cdap.etl.common.db.DBConfig;
import co.cask.cdap.etl.common.db.DBManager;
import co.cask.cdap.etl.common.db.DBRecord;
import co.cask.cdap.etl.common.db.DBUtils;
import co.cask.cdap.etl.common.db.ETLDBInputFormat;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Driver;

/**
 * Batch source to read from a Database table
 */
@Plugin(type = "batchsource")
@Name("Database")
@Description("Reads from a database using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class DBSource extends BatchSource<LongWritable, DBRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(DBSource.class);

  private static final String IMPORT_QUERY_DESCRIPTION = "The SELECT query to use to import data from the specified " +
    "table. You can specify an arbitrary number of columns to import, or import all columns using *. " +
    "You can also specify a number of WHERE clauses or ORDER BY clauses. However, LIMIT and OFFSET clauses " +
    "should not be used in this query.";
  private static final String COUNT_QUERY_DESCRIPTION = "The SELECT query to use to get the count of records to " +
    "import from the specified table. Examples: SELECT COUNT(*) from <my_table> where <my_column> 1, " +
    "SELECT COUNT(my_column) from my_table. NOTE: Please include the same WHERE clauses in this query as the ones " +
    "used in the import query to reflect an accurate number of records to import.";

  private final DBSourceConfig dbSourceConfig;
  private final DBManager dbManager;
  private Class<? extends Driver> driverClass;

  public DBSource(DBSourceConfig dbSourceConfig) {
    this.dbSourceConfig = dbSourceConfig;
    this.dbManager = new DBManager(dbSourceConfig);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    dbManager.validateJDBCPluginPipeline(pipelineConfigurer, getJDBCPluginId());
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    LOG.debug("pluginType = {}; pluginName = {}; connectionString = {}; importQuery = {}; " +
                "countQuery = {}",
              dbSourceConfig.jdbcPluginType, dbSourceConfig.jdbcPluginName,
              dbSourceConfig.connectionString, dbSourceConfig.importQuery, dbSourceConfig.countQuery);

    Job job = Job.getInstance();
    Configuration hConf = job.getConfiguration();
    hConf.clear();

    // Load the plugin class to make sure it is available.
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());
    if (dbSourceConfig.user == null && dbSourceConfig.password == null) {
      DBConfiguration.configureDB(hConf, driverClass.getName(), dbSourceConfig.connectionString);
    } else {
      DBConfiguration.configureDB(hConf, driverClass.getName(), dbSourceConfig.connectionString,
                                  dbSourceConfig.user, dbSourceConfig.password);
    }
    ETLDBInputFormat.setInput(job, DBRecord.class, dbSourceConfig.importQuery, dbSourceConfig.countQuery);
    context.setInput(new SourceInputFormatProvider(ETLDBInputFormat.class, hConf));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    driverClass = context.loadPluginClass(getJDBCPluginId());
  }

  @Override
  public void transform(KeyValue<LongWritable, DBRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(StructuredRecordUtils.convertCase(
      input.getValue().getRecord(), FieldCase.toFieldCase(dbSourceConfig.columnNameCase)));
  }

  @Override
  public void destroy() {
    DBUtils.cleanup(driverClass);
    dbManager.destroy();
  }

  private String getJDBCPluginId() {
    return String.format("%s.%s.%s", "source", dbSourceConfig.jdbcPluginType, dbSourceConfig.jdbcPluginName);
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
