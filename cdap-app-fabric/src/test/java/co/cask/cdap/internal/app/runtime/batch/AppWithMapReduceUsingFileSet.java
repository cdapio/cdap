/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import com.google.common.base.Throwables;
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
public class AppWithMapReduceUsingFileSet extends AbstractApplication {

  public static String inputDataset = System.getProperty("INPUT_DATASET_NAME");
  public static String outputDataset = System.getProperty("OUTPUT_DATASET_NAME");

  @Override
  public void configure() {
    try {
      setName("AppWithMapReduceUsingFile");
      setDescription("Application with MapReduce job using file as dataset");
      createDataset(inputDataset, "fileSet", FileSetProperties.builder()
        .setInputFormat(TextInputFormat.class)
        .setOutputFormat(TextOutputFormat.class)
        .setOutputProperty(TextOutputFormat.SEPERATOR, ":")
        .build());
      if (!outputDataset.equals(inputDataset)) {
        createDataset(outputDataset, "fileSet", FileSetProperties.builder()
          .setBasePath("/foo/my-file-output")
          .setInputFormat(TextInputFormat.class)
          .setOutputFormat(TextOutputFormat.class)
          .setOutputProperty(TextOutputFormat.SEPERATOR, ":")
          .build());
      }
      addMapReduce(new ComputeSum());
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  /**
   *
   */
  public static final class ComputeSum extends AbstractMapReduce {
    @Override
    public void configure() {
      setInputDataset(inputDataset);
      setOutputDataset(outputDataset);
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(FileMapper.class);
      job.setReducerClass(FileReducer.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(LongWritable.class);
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
