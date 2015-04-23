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

package co.cask.cdap.templates.etl.batch.sources;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.batch.BatchSourceContext;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.common.DBRecord;
import co.cask.cdap.templates.etl.common.ETLDBInputFormat;
import co.cask.cdap.templates.etl.common.Properties;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.mysql.jdbc.Driver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Batch source to read from a Database table
 */
public class DBSource extends BatchSource<LongWritable, DBRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(DBSource.class);

  // TODO: Remove when plugin support is enabled
  Driver driver;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setDescription("Batch source for database");
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
                   "Password to be use to connect to the specified database. " +
                     "Required for databases that require authentication. " +
                     "Optional for databases that do not require authentication.",
                   false));
    configurer.addProperty(new Property(Properties.DB.TABLE_NAME, "Table name to import", true));
    configurer.addProperty(
      new Property(Properties.DB.IMPORT_QUERY,
                   "The SELECT query to use to import data from the specified table. " +
                     "You can specify an arbitrary number of columns to import, or import all columns using *. " +
                     "You can also specify a number of WHERE clauses or ORDER BY clauses. " +
                     "However, LIMIT and OFFSET clauses should not be used in this query.",
                   true));
    configurer.addProperty(
      new Property(Properties.DB.COUNT_QUERY,
                   "The SELECT query to use to get the count of records to import from the specified table. " +
                     "Examples: SELECT COUNT(*) from <my_table> where <my_column> 1, " +
                     "SELECT COUNT(my_column) from my_table). " +
                     "NOTE: Please include the same WHERE clauses in this query as the ones used in the " +
                     "import query to reflect an accurate number of records to import.",
                   true));
  }

  @Override
  public void configurePipeline(ETLStage stageConfig, PipelineConfigurer pipelineConfigurer) {
    Map<String, String> properties = stageConfig.getProperties();
    String dbDriverClass = properties.get(Properties.DB.DRIVER_CLASS);
    String dbConnectionString = properties.get(Properties.DB.CONNECTION_STRING);
    String dbUser = properties.get(Properties.DB.USER);
    String dbPassword = properties.get(Properties.DB.PASSWORD);
    String dbTableName = properties.get(Properties.DB.TABLE_NAME);
    String dbImportQuery = properties.get(Properties.DB.IMPORT_QUERY);
    String dbCountQuery = properties.get(Properties.DB.COUNT_QUERY);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbDriverClass), "dbDriverClass cannot be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbConnectionString), "dbConnectionString cannot be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbTableName), "dbTableName cannot be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbImportQuery), "dbImportQuery cannot be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbCountQuery), "dbCountQuery cannot be null");
    Preconditions.checkArgument(!(dbUser == null && dbPassword != null),
                                "dbUser is null. Please provide both user name and password if database requires " +
                                  "authentication. If not, please remove dbPassword and retry.");
    Preconditions.checkArgument(!(dbUser != null && dbPassword == null),
                                "dbPassword is null. Please provide both user name and password if database requires" +
                                  "authentication. If not, please remove dbUser and retry.");
  }

  @Override
  public void prepareJob(BatchSourceContext context) {
    Map<String, String> runtimeArguments = context.getPluginProperties().getProperties();
    String dbDriverClass = runtimeArguments.get(Properties.DB.DRIVER_CLASS);
    String dbConnectionString = runtimeArguments.get(Properties.DB.CONNECTION_STRING);
    String dbUser = runtimeArguments.get(Properties.DB.USER);
    String dbPassword = runtimeArguments.get(Properties.DB.PASSWORD);
    String dbTableName = runtimeArguments.get(Properties.DB.TABLE_NAME);
    String dbImportQuery = runtimeArguments.get(Properties.DB.IMPORT_QUERY);
    String dbCountQuery = runtimeArguments.get(Properties.DB.COUNT_QUERY);

    LOG.debug("dbTableName = {}; dbDriverClass = {}; dbConnectionString = {}; dbImportQuery = {}; dbCountQuery = {}",
              dbTableName, dbDriverClass, dbConnectionString, dbImportQuery, dbCountQuery);

    Job job = context.getHadoopJob();
    Configuration conf = job.getConfiguration();
    job.setInputFormatClass(ETLDBInputFormat.class);
    if (dbUser == null && dbPassword == null) {
      DBConfiguration.configureDB(conf, dbDriverClass, dbConnectionString);
    } else {
      DBConfiguration.configureDB(conf, dbDriverClass, dbConnectionString, dbUser, dbPassword);
    }
    ETLDBInputFormat.setInput(job, DBRecord.class, dbImportQuery, dbCountQuery);
    job.setInputFormatClass(ETLDBInputFormat.class);
  }

  @Override
  public void transform(KeyValue<LongWritable, DBRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(input.getValue().getRecord());
  }
}
