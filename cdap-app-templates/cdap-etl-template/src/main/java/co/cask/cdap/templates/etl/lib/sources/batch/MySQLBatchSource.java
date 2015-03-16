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

package co.cask.cdap.templates.etl.lib.sources.batch;

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.realtime.Emitter;
import com.google.common.collect.Lists;
import com.mysql.jdbc.Driver;
import org.apache.avro.Schema;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 */
public class MySQLBatchSource implements BatchSource<LongWritable, MySQLBatchSource.MyRecord, String> {
  private static final Logger LOG = LoggerFactory.getLogger(MySQLBatchSource.class);
  private Driver driver;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("MySQLBatchSource");
    configurer.setDescription("MySQL Batch Source");
  }

  @Override
  public void prepareJob(MapReduceContext context) {
    Job job = context.getHadoopJob();
    job.setInputFormatClass(DBInputFormat.class);
    DBConfiguration.configureDB(job.getConfiguration(), "com.mysql.jdbc.Driver", "jdbc:mysql://localhost/test?user=root");
    String[] fields = {"id", "name", "age"};
    DBInputFormat.setInput(job, MyRecord.class, "hr", null, "id", fields);
  }

  @Override
  public void initialize(MapReduceContext context) {

  }

  @Override
  public void read(LongWritable rowId, MyRecord record, Emitter<String> out) {
    out.emit(record.name);
  }

  @Override
  public void destroy() {

  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    LOG.info("MR Source complete : {}", succeeded);
  }

  public static class MyRecord implements Writable, DBWritable {
    int id;
    String name;
    int age;
    Schema schema;

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
//      preparedStatement.setString(1, this.name);
//      preparedStatement.setInt(2, this.age);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
      schema = Schema.createRecord(Lists.newArrayList(new Schema.Field(resultSet.getMetaData().getColumnName(1), Schema.create(Schema.Type.INT), null, null),
                                                      new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
                                                      new Schema.Field("age", Schema.create(Schema.Type.INT), null, null)));
      this.id = resultSet.getInt(1);
      this.name = resultSet.getString(2);
      this.age = resultSet.getInt(3);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
//      Text.writeString(dataOutput, this.name);
//      dataOutput.write(this.age);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
//      this.id = dataInput.readInt();
//      this.name = Text.readString(dataInput);
//      this.age = dataInput.readInt();
    }
  }
}
