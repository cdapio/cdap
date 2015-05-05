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
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
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
public class AppWithMapReduceUsingRuntimeFileSet extends AbstractApplication {
  public static final String INPUT_NAME = "input.name";
  public static final String INPUT_PATHS = "input.paths";
  public static final String OUTPUT_NAME = "output.name";
  public static final String OUTPUT_PATH = "output.path";

  @Override
  public void configure() {
    setName("AppWithMapReduceUsingFile");
    setDescription("Application with MapReduce job using file as dataset");
    addMapReduce(new ComputeSum());
  }

  /**
   *
   */
  public static final class ComputeSum extends AbstractMapReduce {

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(FileMapper.class);
      job.setReducerClass(FileReducer.class);

      Map<String, String> runtimeArgs = context.getRuntimeArguments();
      String inputName = runtimeArgs.get(INPUT_NAME);
      String inputPaths = runtimeArgs.get(INPUT_PATHS);
      String outputName = runtimeArgs.get(OUTPUT_NAME);
      String outputPath = runtimeArgs.get(OUTPUT_PATH);

      FileSet input;
      FileSet output;
      if (inputName.equals(outputName)) {
        Map<String, String> args = Maps.newHashMap();
        FileSetArguments.setInputPaths(args, inputPaths);
        FileSetArguments.setOutputPath(args, outputPath);
        input = context.getDataset(inputName, args);
        output = input;
      } else {
        Map<String, String> inputArgs = Maps.newHashMap();
        FileSetArguments.setInputPaths(inputArgs, inputPaths);
        Map<String, String> outputArgs = Maps.newHashMap();
        FileSetArguments.setOutputPath(outputArgs, outputPath);
        input = context.getDataset(inputName, inputArgs);
        output = context.getDataset(outputName, outputArgs);
      }

      context.setInput(inputName, input);
      context.setOutput(outputName, output);
    }
  }

  public static class FileMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    public static final String ONLY_KEY = "x";
    @Override
    public void map(LongWritable key, Text data, Context context)
      throws IOException, InterruptedException {
      context.write(new Text(ONLY_KEY), new LongWritable(Long.valueOf(data.toString())));
    }
  }

  public static class FileReducer extends Reducer<Text, LongWritable, String, Long> {
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
                              throws IOException, InterruptedException  {
      long sum = 0L;
      for (LongWritable value : values) {
        sum += value.get();
      }
      context.write(key.toString(), sum);
    }
  }

}
