/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.batch;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.api.common.Scope;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputContext;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.PartitionedFileSetInputContext;
import io.cdap.cdap.api.dataset.lib.PartitionFilter;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetArguments;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetProperties;
import io.cdap.cdap.api.dataset.lib.Partitioning;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.mapreduce.MapReduceTaskContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * App used to test whether M/R works well with time-partitioned file sets.
 * It uses M/R to read from a table and write partitions, and another M/R to read partitions and write to a table.
 */
public class AppWithPartitionedFileSet extends AbstractApplication<AppWithPartitionedFileSet.AppConfig> {

  public static final String INPUT = "in-table";
  public static final String PARTITIONED = "partitioned";
  public static final String OUTPUT = "out-table";
  public static final byte[] ONLY_COLUMN = { 'x' };
  public static final String ROW_TO_WRITE = "row.to.write";
  private static final String SEPARATOR = ":";

  @Override
  public void configure() {
    setName("AppWithMapReduceUsingFile");
    setDescription("Application with MapReduce job using file as dataset");
    createDataset(INPUT, "table");
    createDataset(OUTPUT, "table");

    Class<? extends InputFormat> inputFormatClass =
      getConfig().isUseCombineFileInputFormat() ? CombineTextInputFormat.class : TextInputFormat.class;
    createDataset(PARTITIONED, "partitionedFileSet", PartitionedFileSetProperties.builder()
      .setPartitioning(Partitioning.builder()
                         .addStringField("type")
                         .addLongField("time")
                         .build())
        // properties for file set
      .setBasePath("partitioned")
      .setInputFormat(inputFormatClass)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, SEPARATOR)
      .build());
    addMapReduce(new PartitionWriter());
    addMapReduce(new PartitionReader());
  }

  public static final class AppConfig extends Config {
    private final boolean useCombineFileInputFormat;

    public AppConfig(boolean useCombineFileInputFormat) {
      this.useCombineFileInputFormat = useCombineFileInputFormat;
    }

    public boolean isUseCombineFileInputFormat() {
      return useCombineFileInputFormat;
    }
  }

  /**
   * Map/Reduce that reads the "input" table and writes to a partition.
   */
  public static final class PartitionWriter extends AbstractMapReduce {

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Job job = context.getHadoopJob();
      job.setMapperClass(SimpleMapper.class);
      job.setNumReduceTasks(0);
      context.addInput(Input.ofDataset(INPUT));
      context.addOutput(Output.ofDataset(PARTITIONED));
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
   * Map/Reduce that reads the "partitioned" PFS and writes to an "output" table.
   */
  public static final class PartitionReader extends AbstractMapReduce {

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Job job = context.getHadoopJob();
      job.setMapperClass(ReaderMapper.class);
      job.setNumReduceTasks(0);
      String row = context.getRuntimeArguments().get(ROW_TO_WRITE);
      job.getConfiguration().set(ROW_TO_WRITE, row);
      context.addInput(Input.ofDataset(PARTITIONED));
      context.addOutput(Output.ofDataset(OUTPUT));
    }
  }

  public static class ReaderMapper extends Mapper<LongWritable, Text, byte[], Put>
    implements ProgramLifecycle<MapReduceTaskContext<byte[], Put>> {

    private static byte[] rowToWrite;
    private PartitionedFileSetInputContext pfsInputcontext;

    @Override
    public void initialize(MapReduceTaskContext<byte[], Put> context) throws Exception {
      InputContext inputContext = context.getInputContext();
      Preconditions.checkArgument(PARTITIONED.equals(inputContext.getInputName()));
      Preconditions.checkArgument(inputContext instanceof PartitionedFileSetInputContext);
      this.pfsInputcontext = (PartitionedFileSetInputContext) inputContext;
      Preconditions.checkNotNull(pfsInputcontext.getInputPartitionKey());
      Preconditions.checkArgument(
        pfsInputcontext.getInputPartitionKeys().contains(pfsInputcontext.getInputPartitionKey())
      );

      Map<String, String> dsArguments =
        RuntimeArguments.extractScope(Scope.DATASET, PARTITIONED, context.getRuntimeArguments());
      PartitionFilter inputPartitionFilter = PartitionedFileSetArguments.getInputPartitionFilter(dsArguments);
      Preconditions.checkNotNull(inputPartitionFilter);
      // verify that the partition matches the partition filter for this MapReduce
      Preconditions.checkArgument(inputPartitionFilter.match(pfsInputcontext.getInputPartitionKey()));
    }

    @Override
    public void destroy() {

    }

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
      context.write(rowToWrite, new Put(rowToWrite, Bytes.toBytes(fields[0] + "_key"),
                                        Bytes.toBytes(pfsInputcontext.getInputPartitionKey().toString())));
    }
  }
}
