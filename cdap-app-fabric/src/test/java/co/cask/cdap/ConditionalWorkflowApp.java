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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.NodeValueEntry;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.internal.app.runtime.batch.WordCount;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Application to test the condition nodes in the Workflow
 */
public class ConditionalWorkflowApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("ConditionalWorkflowApp");
    setDescription("Workflow app to test the Condition nodes.");
    addWorkflow(new ConditionalWorkflow());
    addMapReduce(new RecordVerifier());
    addMapReduce(new WordCountMapReduce());
  }

  static final class ConditionalWorkflow extends AbstractWorkflow {

    @Override
    protected void configure() {
      setName("ConditionalWorkflow");
      setDescription("Workflow to test Condition nodes");
      addMapReduce("RecordVerifier");
      condition(new MyVerificationPredicate())
        .addMapReduce("ClassicWordCount")
        .fork()
          .addAction(new SimpleAction("iffork_one"))
        .also()
          .addAction(new SimpleAction("iffork_another"))
        .join()
      .otherwise()
        .fork()
          .addAction(new SimpleAction("elsefork_one"))
        .also()
          .addAction(new SimpleAction("elsefork_another"))
        .also()
          .addAction(new SimpleAction("elsefork_third"))
        .join()
      .end();
      addAction(new StatusReporter());
    }
  }

  public static final class MyVerificationPredicate implements Predicate<WorkflowContext> {

    @Override
    public boolean apply(@Nullable WorkflowContext input) {
      if (input != null) {
        input.getToken().put("action_type", "Condition");
        Map<String, Map<String, Long>> hadoopCounters = input.getToken().getMapReduceCounters();
        if (hadoopCounters != null) {
          Map<String, Long> customCounters = hadoopCounters.get("MyCustomCounter");
          // If number of good records are greater than the number of bad records then only
          // return true to execute the true branch associated with this Condition node
          if (customCounters.get("GoodRecord") > customCounters.get("BadRecord")) {
            input.getToken().put("branch", "true");
            return true;
          }
          input.getToken().put("branch", "false");
        }
      }
      return false;
    }
  }

  public static final class RecordVerifier extends AbstractMapReduce {

    @Override
    public void configure() {
      setName("RecordVerifier");
      setDescription("MapReduce program to verify the records in the file");
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(MyVerifier.class);
      String inputPath = context.getRuntimeArguments().get("inputPath");
      String outputPath = context.getRuntimeArguments().get("outputPath");
      FileInputFormat.addInputPath(job, new Path(inputPath));
      FileOutputFormat.setOutputPath(job, new Path(outputPath));
      // Put something in the token
      context.getWorkflowToken().put("action_type", "MapReduce");
    }
  }

  public static class MyVerifier extends Mapper<LongWritable, Text, Text, NullWritable> {
    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      if (value.toString().split(":").length > 2) {
        context.getCounter("MyCustomCounter", "GoodRecord").increment(1L);
      } else {
        context.getCounter("MyCustomCounter", "BadRecord").increment(1L);
      }
    }
  }

  /**
   *
   */
  public static final class WordCountMapReduce extends AbstractMapReduce {

    @Override
    public void configure() {
      setName("ClassicWordCount");
      setDescription("WordCount job from Hadoop examples");
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();
      String inputPath = args.get("inputPath");
      String outputPath = args.get("outputPath");
      WordCount.configureJob((Job) context.getHadoopJob(), inputPath, outputPath);
      // Put something in the token
      context.getWorkflowToken().put("action_type", "MapReduce");
    }

    @Override
    public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
      // No-op
    }
  }

  static final class SimpleAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleAction.class);

    public SimpleAction(String name) {
      super(name);
    }

    @Override
    public void run() {
      LOG.info("Running SimpleAction: " + getContext().getSpecification().getName());
      // Put something in the token
      getContext().getToken().put("action_type", "CustomAction");
      try {
        File file = new File(getContext().getRuntimeArguments().get(getContext().getSpecification().getName() +
                                                                      ".simple.action.file"));
        file.createNewFile();
        File doneFile = new File(getContext().getRuntimeArguments().get(getContext().getSpecification().getName() +
                                                                          ".simple.action.donefile"));
        while (!doneFile.exists()) {
          TimeUnit.MILLISECONDS.sleep(50);
        }
      } catch (Exception e) {
        // no-op
      }
    }
  }

  static final class StatusReporter extends AbstractWorkflowAction {
    private final String taskCounterGroupName = "org.apache.hadoop.mapreduce.TaskCounter";
    private final String mapInputRecordsCounterName = "MAP_INPUT_RECORDS";
    private final String mapOutputRecordsCounterName = "MAP_OUTPUT_RECORDS";
    private final String reduceInputRecordsCounterName = "REDUCE_INPUT_RECORDS";
    private final String reduceOutputRecordsCounterName = "REDUCE_OUTPUT_RECORDS";
    private final String flattenMapInputRecordsCounterName =
      "mr.counters.org.apache.hadoop.mapreduce.TaskCounter.MAP_INPUT_RECORDS";
    private final String flattenMapOutputRecordsCounterName =
      "mr.counters.org.apache.hadoop.mapreduce.TaskCounter.MAP_OUTPUT_RECORDS";
    private final String flattenReduceInputRecordsCounterName =
      "mr.counters.org.apache.hadoop.mapreduce.TaskCounter.REDUCE_INPUT_RECORDS";
    private final String flattenReduceOutputRecordsCounterName =
      "mr.counters.org.apache.hadoop.mapreduce.TaskCounter.REDUCE_OUTPUT_RECORDS";

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      WorkflowToken workflowToken = getContext().getToken();
      boolean trueBranchExecuted = Boolean.parseBoolean(workflowToken.get("branch"));
      if (trueBranchExecuted) {
        // Previous condition returned true
        List<NodeValueEntry> nodeValueEntries = workflowToken.getAll("action_type");
        Preconditions.checkArgument(5 == nodeValueEntries.size());
        Preconditions.checkArgument(new NodeValueEntry("RecordVerifier", "MapReduce").equals(nodeValueEntries.get(0)));
        Preconditions.checkArgument(new NodeValueEntry("ClassicWordCount",
                                                       "MapReduce").equals(nodeValueEntries.get(2)));
        Preconditions.checkArgument(workflowToken.get("action_type", "iffork_one").equals("CustomAction"));
        Preconditions.checkArgument(workflowToken.get("action_type", "iffork_another").equals("CustomAction"));
        validateMapReduceCounters(workflowToken, "ClassicWordCount");
      } else {
        // Previous condition returned false
        List<NodeValueEntry> nodeValueEntries = workflowToken.getAll("action_type");
        Preconditions.checkArgument(5 == nodeValueEntries.size());
        Preconditions.checkArgument(new NodeValueEntry("RecordVerifier", "MapReduce").equals(nodeValueEntries.get(0)));
        Preconditions.checkArgument(workflowToken.get("action_type", "elsefork_one").equals("CustomAction"));
        Preconditions.checkArgument(workflowToken.get("action_type", "elsefork_another").equals("CustomAction"));
        Preconditions.checkArgument(workflowToken.get("action_type", "elsefork_third").equals("CustomAction"));
        validateMapReduceCounters(workflowToken, "RecordVerifier");
      }
    }

    @SuppressWarnings("null")
    private void validateMapReduceCounters(WorkflowToken workflowToken, String programName) {
      Map<String, Map<String, Long>> mapReduceCounters = workflowToken.getMapReduceCounters();
      Preconditions.checkNotNull(mapReduceCounters);

      Map<String, Long> taskCounters = mapReduceCounters.get(taskCounterGroupName);
      long mapInputRecords = taskCounters.get(mapInputRecordsCounterName);
      long mapOutputRecords = taskCounters.get(mapOutputRecordsCounterName);
      long reduceInputRecords = taskCounters.get(reduceInputRecordsCounterName);
      long reduceOutputRecords = taskCounters.get(reduceOutputRecordsCounterName);

      long flattenMapInputRecords = Long.parseLong(workflowToken.get(flattenMapInputRecordsCounterName));
      long flattenMapOutputRecords = Long.parseLong(workflowToken.get(flattenMapOutputRecordsCounterName));
      long flattenReduceInputRecords = Long.parseLong(workflowToken.get(flattenReduceInputRecordsCounterName));
      long flattenReduceOutputRecords = Long.parseLong(workflowToken.get(flattenReduceOutputRecordsCounterName));

      Preconditions.checkArgument(mapInputRecords == flattenMapInputRecords);
      Preconditions.checkArgument(mapOutputRecords == flattenMapOutputRecords);
      Preconditions.checkArgument(reduceInputRecords == flattenReduceInputRecords);
      Preconditions.checkArgument(reduceOutputRecords == flattenReduceOutputRecords);

      long nodeSpecificMapInputRecords = Long.parseLong(workflowToken.get(flattenMapInputRecordsCounterName,
                                                                          programName));

      long nodeSpecificMapOutputRecords = Long.parseLong(workflowToken.get(flattenMapOutputRecordsCounterName,
                                                                           programName));

      long nodeSpecificReduceInputRecords = Long.parseLong(workflowToken.get(flattenReduceInputRecordsCounterName,
                                                                             programName));

      long nodeSpecificReduceOutputRecords = Long.parseLong(
        workflowToken.get(flattenReduceOutputRecordsCounterName, programName));

      Preconditions.checkArgument(mapInputRecords == nodeSpecificMapInputRecords);
      Preconditions.checkArgument(mapOutputRecords == nodeSpecificMapOutputRecords);
      Preconditions.checkArgument(reduceInputRecords == nodeSpecificReduceInputRecords);
      Preconditions.checkArgument(reduceOutputRecords == nodeSpecificReduceOutputRecords);

      Map<String, String> prefixedMapReduceCounters = workflowToken.getAllFromNode(programName, "mr.counters");
      long prefixedMapInputRecords = Long.parseLong(prefixedMapReduceCounters.get(flattenMapInputRecordsCounterName));

      long prefixedMapOutputRecords = Long.parseLong(prefixedMapReduceCounters.get(flattenMapOutputRecordsCounterName));

      long prefixedReduceInputRecords =
        Long.parseLong(prefixedMapReduceCounters.get(flattenReduceInputRecordsCounterName));

      long prefixedReduceOutputRecords =
        Long.parseLong(prefixedMapReduceCounters.get(flattenReduceOutputRecordsCounterName));

      Preconditions.checkArgument(mapInputRecords == prefixedMapInputRecords);
      Preconditions.checkArgument(mapOutputRecords == prefixedMapOutputRecords);
      Preconditions.checkArgument(reduceInputRecords == prefixedReduceInputRecords);
      Preconditions.checkArgument(reduceOutputRecords == prefixedReduceOutputRecords);
    }
  }
}
