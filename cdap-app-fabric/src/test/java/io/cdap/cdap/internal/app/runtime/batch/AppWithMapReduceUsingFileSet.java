/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * App used to test whether M/R can read from file datasets.
 */
public class AppWithMapReduceUsingFileSet extends AbstractApplication<AppWithMapReduceUsingFileSet.AppConfig> {

  @Override
  public void configure() {
    setName("AppWithMapReduceUsingFile");
    setDescription("Application with MapReduce job using file as dataset");
    String inputDataset = getConfig().inputDataset;
    String outputDataset = getConfig().outputDataset;
    createDataset(inputDataset, "fileSet", FileSetProperties.builder()
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ":")
      .build());

    if (!outputDataset.equals(inputDataset)) {
      createDataset(outputDataset, "fileSet",  FileSetProperties.builder()
        .setBasePath("foo/my-file-output")
        .setInputFormat(TextInputFormat.class)
        .setOutputFormat(TextOutputFormat.class)
        .setOutputProperty(TextOutputFormat.SEPERATOR, ":")
        .build());
    }
    addMapReduce(new ComputeSum(getConfig()));
  }

  /**
   *
   */
  public static final class ComputeSum extends AbstractMapReduce {

    private final AppConfig config;

    ComputeSum(AppConfig config) {
      this.config = config;
    }

    @Override
    protected void configure() {
      setProperties(ImmutableMap.of("input", config.inputDataset,
                                    "output", config.outputDataset));
    }

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Job job = context.getHadoopJob();
      job.setReducerClass(FileReducer.class);

      // user can opt to define the mapper class through our APIs, instead of directly on the job
      context.addInput(Input.ofDataset(context.getSpecification().getProperty("input")), FileMapper.class);
      context.addOutput(Output.ofDataset(context.getSpecification().getProperty("output")));
    }
  }

  public static class FileMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    static final String ONLY_KEY = "x";
    @Override
    public void map(LongWritable key, Text data, Context context)
      throws IOException, InterruptedException {
      context.write(new Text(ONLY_KEY), new LongWritable(Long.valueOf(data.toString())));
    }
  }

  public static class FileReducer extends Reducer<Text, LongWritable, String, Long> {
    @Override
    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context) throws IOException, InterruptedException  {
      long sum = 0L;
      for (LongWritable value : values) {
        sum += value.get();
      }
      context.write(key.toString(), sum);
    }
  }

  static class AppConfig extends Config {
    final String inputDataset;
    final String outputDataset;

    public AppConfig(String inputDataset, String outputDataset) {
      this.inputDataset = inputDataset;
      this.outputDataset = outputDataset;
    }

    AppConfig(String inputDataset, String outputDataset, String permissions) {
      this.inputDataset = inputDataset;
      this.outputDataset = outputDataset;
    }
  }

}
