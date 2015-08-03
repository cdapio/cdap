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

package co.cask.cdap;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowToken;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * App to test the put operation on the WorkflowToken through map and reduce methods.
 */
public class WorkflowTokenTestPutApp extends AbstractApplication {
  public static final String NAME = "WorkflowTokenTestPutApp";

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Application to test the put operation on the Workflow in beforeSubmit, " +
                     "onFinish, map, and reduce methods of the MapReduce program.");
    addMapReduce(new RecordCounter());
    addSpark(new SparkTestApp());
    addWorkflow(new WorkflowTokenTestPut());
  }

  public static class WorkflowTokenTestPut extends AbstractWorkflow {
    public static final String NAME = "WorkflowTokenTestPut";
    @Override
    protected void configure() {
      setName(NAME);
      addMapReduce(RecordCounter.NAME);
      addSpark(SparkTestApp.NAME);
    }
  }

  /**
   * MapReduce program to count the occurrences of the ID in the input.
   */
  public static final class RecordCounter extends AbstractMapReduce {
    public static final String NAME = "RecordCounter";

    @Override
    public void configure() {
      setName(NAME);
      setDescription("MapReduce program to verify the records in the file.");
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(MyMapper.class);
      job.setReducerClass(MyReducer.class);
      String inputPath = context.getRuntimeArguments().get("inputPath");
      String outputPath = context.getRuntimeArguments().get("outputPath");
      FileInputFormat.addInputPath(job, new Path(inputPath));
      FileOutputFormat.setOutputPath(job, new Path(outputPath));
      // Put something in the token
      WorkflowToken workflowToken = context.getWorkflowToken();
      if (workflowToken == null) {
        return;
      }
      workflowToken.put("action.type", "MapReduce");
      workflowToken.put("start.time", Value.of(System.currentTimeMillis()));
    }

    @Override
    public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
      WorkflowToken workflowToken = context.getWorkflowToken();
      workflowToken.put("end.time", Value.of(System.currentTimeMillis()));
    }
  }

  /**
   * Mapper class to parse the input and emit ID and value.
   */
  public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
    implements ProgramLifecycle<MapReduceContext> {

    private WorkflowToken workflowToken;
    private Map<String, String> arguments;

    @Override
    public void initialize(MapReduceContext context) throws Exception {
      workflowToken = context.getWorkflowToken();
      Preconditions.checkNotNull(workflowToken, "WorkflowToken cannot be null.");
      Preconditions.checkArgument(workflowToken.get("action.type").toString().equals("MapReduce"));
      arguments = context.getRuntimeArguments();
      if (arguments.containsKey("put.in.mapper.initialize")) {
        workflowToken.put("mapper.initialize.key", "mapper.initialize.value");
      }
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      Preconditions.checkArgument(workflowToken.get("action.type").toString().equals("MapReduce"));

      if (arguments.containsKey("put.in.map")) {
        workflowToken.put("map.key", "map.value");
      }

      String[] fields = value.toString().split(":");
      context.write(new Text(fields[0]), new Text(fields[1]));
    }

    @Override
    public void destroy() {
      // no-op
    }
  }

  /**
   * Reducer class to count the occurrences of the ID.
   */
  public static class MyReducer extends Reducer<Text, Text, Text, IntWritable>
    implements ProgramLifecycle<MapReduceContext> {

    private WorkflowToken workflowToken;
    private Map<String, String> arguments;

    @Override
    public void initialize(MapReduceContext context) throws Exception {
      workflowToken = context.getWorkflowToken();
      Preconditions.checkNotNull(workflowToken, "WorkflowToken cannot be null.");
      Preconditions.checkArgument(workflowToken.get("action.type").toString().equals("MapReduce"));
      arguments = context.getRuntimeArguments();
      if (arguments.containsKey("put.in.reducer.initialize")) {
        workflowToken.put("reducer.initialize.key", "reducer.initialize.value");
      }
    }

    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
      Preconditions.checkArgument(workflowToken.get("action.type").toString().equals("MapReduce"));

      if (arguments.containsKey("put.in.reduce")) {
        workflowToken.put("reduce.key", "reduce.value");
      }
      context.write(key, new IntWritable(Iterables.size(value)));
    }

    @Override
    public void destroy() {
      // no-op
    }
  }

  /**
   * Spark application to test the put on the WorkflowToken.
   */
  public static class SparkTestApp extends AbstractSpark {
    public static final String NAME = "SparkTestApp";

    @Override
    public void configure() {
      setName(NAME);
      setDescription("Test Spark with the Workflow");
      setMainClass(SparkTestProgram.class);
    }
  }

  public static class SparkTestProgram implements JavaSparkProgram {
    @Override
    public void run(final SparkContext context) {
      List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

      final WorkflowToken workflowToken = context.getWorkflowToken();
      if (workflowToken != null) {
        workflowToken.put("multiplier", "2");
      }

      final boolean closurePutToken = context.getRuntimeArguments().containsKey("closurePutToken");
      JavaRDD<Integer> distData = ((JavaSparkContext) context.getOriginalSparkContext()).parallelize(data);
      JavaRDD<Integer> mapData = distData.map(new Function<Integer, Integer>() {
        @Override
        public Integer call(Integer val) throws Exception {
          if (closurePutToken) {
            workflowToken.put("some.key", "some.value");
          }

          if (workflowToken.get("multiplier") != null) {
            int multiplier = workflowToken.get("multiplier").getAsInt();
            return multiplier * val;
          }
          return val;
        }
      });

      mapData.collect();
    }
  }
}
