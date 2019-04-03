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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

/**
 * App used to test whether M/R can read from file datasets referenced at runtime.
 */
public class AppWithMapReduceUsingRuntimeDatasets extends AbstractApplication {
  public static final String INPUT_NAME = "input.name";
  public static final String INPUT_PATHS = "input.paths";
  public static final String OUTPUT_NAME = "output.name";
  public static final String OUTPUT_PATH = "output.path";

  public static final String APP_NAME = "appWithRuntimeDS";
  public static final String MR_NAME = "computeSum";

  public static final byte[] INPUT_RECORDS = Bytes.toBytes("inputRecords");
  public static final byte[] REDUCE_KEYS = Bytes.toBytes("reduceKeys");
  public static final String COUNTERS = "dynCounters";

  @Override
  public void configure() {
    setName(APP_NAME);
    setDescription("Application with MapReduce job using file as dataset");
    addMapReduce(new ComputeSum());
    createDataset("rtt", Table.class.getName());
    createDataset(COUNTERS, KeyValueTable.class.getName());
  }

  /**
   *
   */
  public static final class ComputeSum extends AbstractMapReduce {

    @Override
    protected void configure() {
      setName(MR_NAME);
    }

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Job job = context.getHadoopJob();
      job.setMapperClass(FileMapper.class);
      job.setReducerClass(FileReducer.class);

      // Copies all runtime arguments that start with "mr.job.conf." to the Job conf.
      Map<String, String> mrJobConf = Maps.filterKeys(context.getRuntimeArguments(), new Predicate<String>() {
        @Override
        public boolean apply(String input) {
          return input.startsWith("mr.job.conf.");
        }
      });
      for (Map.Entry<String, String> entry : mrJobConf.entrySet()) {
        job.getConfiguration().set(entry.getKey().substring("mr.job.conf.".length()), entry.getValue());
      }

      Map<String, String> runtimeArgs = context.getRuntimeArguments();
      String inputName = runtimeArgs.get(INPUT_NAME);
      String inputPaths = runtimeArgs.get(INPUT_PATHS);
      String outputName = runtimeArgs.get(OUTPUT_NAME);
      String outputPath = runtimeArgs.get(OUTPUT_PATH);

      // Setup input and output file sets
      Map<String, String> args = Maps.newHashMap();
      FileSetArguments.setInputPaths(args, inputPaths);
      context.addInput(Input.ofDataset(inputName, args));

      args.clear();
      FileSetArguments.setOutputPath(args, outputPath);
      context.addOutput(Output.ofDataset(outputName, args));

      Table table = context.getDataset("rtt");
      table.put(new Put("a").add("b", "c"));
    }
  }

  public static class FileMapper
    extends Mapper<LongWritable, Text, Text, LongWritable>
    implements ProgramLifecycle<MapReduceTaskContext> {

    private KeyValueTable counters = null;

    public static final String ONLY_KEY = "x";
    @Override
    public void map(LongWritable key, Text data, Context context)
      throws IOException, InterruptedException {
      counters.increment(INPUT_RECORDS, 1L);
      context.write(new Text(ONLY_KEY), new LongWritable(Long.valueOf(data.toString())));
    }

    @Override
    public void initialize(MapReduceTaskContext context) throws Exception {
      counters = context.getDataset(COUNTERS);
    }

    @Override
    public void destroy() {
      // no-op
    }
  }

  public static class FileReducer
    extends Reducer<Text, LongWritable, String, Long>
    implements ProgramLifecycle<MapReduceTaskContext> {

    private KeyValueTable counters = null;

    public void reduce(Text key, Iterable<LongWritable> values, Context context)
                              throws IOException, InterruptedException  {

      counters.increment(REDUCE_KEYS, 1L);

      long sum = 0L;
      for (LongWritable value : values) {
        sum += value.get();
      }
      context.write(key.toString(), sum);
    }

    @Override
    public void initialize(MapReduceTaskContext context) throws Exception {
      counters = context.getDataset(COUNTERS);
    }

    @Override
    public void destroy() {
      // no-op
    }
  }

}
