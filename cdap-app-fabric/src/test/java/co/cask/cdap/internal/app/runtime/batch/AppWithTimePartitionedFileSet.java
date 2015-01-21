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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * App used to test whether M/R works well with time-partitioned file sets.
 * It uses M/R to read from a table and write partitions, and another M/R to read partitions and write to a table.
 */
public class AppWithTimePartitionedFileSet extends AbstractApplication {

  public static final String INPUT = "input";
  public static final String PARTITIONED = "partitioned";
  public static final String OUTPUT = "output";
  public static final byte[] ONLY_COLUMN = { 'x' };
  public static final String ROW_TO_WRITE = "row.to.write";
  private static final String SEPARATOR = ":";

  @Override
  public void configure() {
    try {
      setName("AppWithMapReduceUsingFile");
      setDescription("Application with MapReduce job using file as dataset");
      createDataset(INPUT, "table");
      createDataset(OUTPUT, "table");

      createDataset(PARTITIONED, "timePartitionedFileSet", FileSetProperties.builder()
        // properties for file set
        .setBasePath("/partitioned")
        .setInputFormat(TextInputFormat.class)
        .setOutputFormat(TextOutputFormat.class)
        .setOutputProperty(TextOutputFormat.SEPERATOR, SEPARATOR)
          // properties for partitioned hive table
          // .setEnableExploreOnCreate(true)
          // .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
          // .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
          // .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
          // .setTableProperty("avro.schema.literal", SCHEMA_STRING)
        .build());
      addMapReduce(new PartitionWriter());
      addMapReduce(new PartitionReader());
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  /**
   * Map/Reduce that reads the "input" table and writes to a partition.
   */
  public static final class PartitionWriter extends AbstractMapReduce {
    @Override
    public void configure() {
      setInputDataset(INPUT);
      setOutputDataset(PARTITIONED);
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(SimpleMapper.class);
      job.setNumReduceTasks(0);
    }

    @Override
    public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
      if (succeeded) {
        TimePartitionedFileSet ds = context.getDataset(PARTITIONED);
        String outputPath = FileSetArguments.getOutputPath(ds.getUnderlyingFileSet().getRuntimeArguments());
        Long time = TimePartitionedFileSetArguments.getOutputPartitionTime(ds.getRuntimeArguments());
        Preconditions.checkNotNull(time, "Output partition time is null.");
        ds.addPartition(time, outputPath);
      }
    }
  }

  public static class SimpleMapper extends Mapper<byte[], Row, Text, Text> {

    @Override
    public void map(byte[] rowKey, Row row, Context context)
      throws IOException, InterruptedException {
      context.write(new Text(Bytes.toString(rowKey)),
                    new Text(Bytes.toString(row.get(ONLY_COLUMN))));
    }
  }

  /**
   * Map/Reduce that reads the "input" table and writes to a partition.
   */
  public static final class PartitionReader extends AbstractMapReduce {

    @Override
    public void configure() {
      setInputDataset(PARTITIONED);
      setOutputDataset(OUTPUT);
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(ReaderMapper.class);
      job.setNumReduceTasks(0);
      String row = context.getRuntimeArguments().get(ROW_TO_WRITE);
      job.getConfiguration().set(ROW_TO_WRITE, row);
    }

  }

  public static class ReaderMapper extends Mapper<LongWritable, Text, byte[], Put> {

    private static byte[] rowToWrite;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      rowToWrite = Bytes.toBytes(context.getConfiguration().get(ROW_TO_WRITE));
    }

    @Override
    public void map(LongWritable pos, Text text, Context context)
      throws IOException, InterruptedException {
      String line = text.toString();
      String[] fields = line.split(SEPARATOR);
      context.write(rowToWrite, new Put(rowToWrite, Bytes.toBytes(fields[0]), Bytes.toBytes(fields[1])));
    }
  }
}
