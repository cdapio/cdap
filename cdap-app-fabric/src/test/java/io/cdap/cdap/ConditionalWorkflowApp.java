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
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.NodeValue;
import co.cask.cdap.api.workflow.Value;
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
        input.getToken().put("action.type", "Condition");
        List<NodeValue> goodRecords = input.getToken().getAll("MyCustomCounter.GoodRecord", WorkflowToken.Scope.SYSTEM);
        List<NodeValue> badRecords = input.getToken().getAll("MyCustomCounter.BadRecord", WorkflowToken.Scope.SYSTEM);
        // If number of good records are greater than the number of bad records then only
        // return true to execute the true branch associated with this Condition node

        if (goodRecords.get(0).getValue().getAsLong() > badRecords.get(0).getValue().getAsLong()) {
          input.getToken().put("conditionResult", "true");
          return true;
        }
        input.getToken().put("conditionResult", "false");
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
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Job job = context.getHadoopJob();
      job.setMapperClass(MyVerifier.class);
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
  @SuppressWarnings("ConstantConditions")
  public static final class WordCountMapReduce extends AbstractMapReduce {

    @Override
    public void configure() {
      setName("ClassicWordCount");
      setDescription("WordCount job from Hadoop examples");
    }

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Map<String, String> args = context.getRuntimeArguments();
      String inputPath = args.get("inputPath");
      String outputPath = args.get("outputPath");
      WordCount.configureJob((Job) context.getHadoopJob(), inputPath, outputPath);
      WorkflowToken workflowToken = context.getWorkflowToken();
      if (workflowToken == null) {
        return;
      }
      // Put something in the token
      workflowToken.put("action.type", "MapReduce");
      workflowToken.put("start.time", Value.of(System.currentTimeMillis()));
      Preconditions.checkNotNull(workflowToken.get("start.time", "RecordVerifier"));
    }
  }

  static final class SimpleAction extends AbstractCustomAction {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleAction.class);

    public SimpleAction(String name) {
      super(name);
    }

    @Override
    public void run() {
      String actionName = getContext().getSpecification().getName();
      LOG.info("Running SimpleAction: {}", actionName);

      WorkflowToken token = getContext().getWorkflowToken();

      // Put something in the token
      token.put("action.type", "CustomAction");

      try {
        File file = new File(getContext().getRuntimeArguments().get(actionName + ".simple.action.file"));
        Preconditions.checkState(file.createNewFile());
        File doneFile = new File(getContext().getRuntimeArguments().get(actionName + ".simple.action.donefile"));
        while (!doneFile.exists()) {
          TimeUnit.MILLISECONDS.sleep(50);
        }
      } catch (Exception e) {
        // no-op
      }
    }
  }

  static final class StatusReporter extends AbstractCustomAction {
    private final String taskCounterGroupName = "org.apache.hadoop.mapreduce.TaskCounter";
    private final String mapInputRecordsCounterName = "MAP_INPUT_RECORDS";
    private final String mapOutputRecordsCounterName = "MAP_OUTPUT_RECORDS";
    private final String reduceInputRecordsCounterName = "REDUCE_INPUT_RECORDS";
    private final String reduceOutputRecordsCounterName = "REDUCE_OUTPUT_RECORDS";
    private final String flattenMapInputRecordsCounterName =
      "org.apache.hadoop.mapreduce.TaskCounter.MAP_INPUT_RECORDS";
    private final String flattenMapOutputRecordsCounterName =
      "org.apache.hadoop.mapreduce.TaskCounter.MAP_OUTPUT_RECORDS";
    private final String flattenReduceInputRecordsCounterName =
      "org.apache.hadoop.mapreduce.TaskCounter.REDUCE_INPUT_RECORDS";
    private final String flattenReduceOutputRecordsCounterName =
      "org.apache.hadoop.mapreduce.TaskCounter.REDUCE_OUTPUT_RECORDS";

    @SuppressWarnings("ConstantConditions")
    @Override
    public void run() {
      WorkflowToken workflowToken = getContext().getWorkflowToken();
      boolean trueBranchExecuted = Boolean.parseBoolean(workflowToken.get("conditionResult").toString());
      if (trueBranchExecuted) {
        // Previous condition returned true
        List<NodeValue> nodeValueEntries = workflowToken.getAll("action.type");
        Preconditions.checkArgument(5 == nodeValueEntries.size());
        Preconditions.checkArgument(new NodeValue("RecordVerifier",
                                                       Value.of("MapReduce")).equals(nodeValueEntries.get(0)));
        Preconditions.checkArgument(new NodeValue("ClassicWordCount",
                                                       Value.of("MapReduce")).equals(nodeValueEntries.get(2)));
        Preconditions.checkArgument(workflowToken.get("action.type",
                                                      "iffork_one").toString().equals("CustomAction"));
        Preconditions.checkArgument(workflowToken.get("action.type",
                                                      "iffork_another").toString().equals("CustomAction"));
        validateMapReduceCounters(workflowToken, "ClassicWordCount");
      } else {
        // Previous condition returned false
        List<NodeValue> nodeValueEntries = workflowToken.getAll("action.type");
        Preconditions.checkArgument(5 == nodeValueEntries.size());
        Preconditions.checkArgument(new NodeValue("RecordVerifier",
                                                       Value.of("MapReduce")).equals(nodeValueEntries.get(0)));
        Preconditions.checkArgument(workflowToken.get("action.type",
                                                      "elsefork_one").toString().equals("CustomAction"));
        Preconditions.checkArgument(workflowToken.get("action.type",
                                                      "elsefork_another").toString().equals("CustomAction"));
        Preconditions.checkArgument(workflowToken.get("action.type",
                                                      "elsefork_third").toString().equals("CustomAction"));
        validateMapReduceCounters(workflowToken, "RecordVerifier");
      }
      Map<String, List<NodeValue>> allUserKeys = workflowToken.getAll(WorkflowToken.Scope.USER);
      Preconditions.checkArgument(5 == allUserKeys.get("action.type").size());
    }

    @SuppressWarnings("ConstantConditions")
    private void validateMapReduceCounters(WorkflowToken workflowToken, String programName) {
      long mapInputRecords = workflowToken.get(taskCounterGroupName + "." + mapInputRecordsCounterName,
                                               WorkflowToken.Scope.SYSTEM).getAsLong();
      long mapOutputRecords = workflowToken.get(taskCounterGroupName + "." + mapOutputRecordsCounterName,
                                                WorkflowToken.Scope.SYSTEM).getAsLong();
      long reduceInputRecords = workflowToken.get(taskCounterGroupName + "." + reduceInputRecordsCounterName,
                                                  WorkflowToken.Scope.SYSTEM).getAsLong();
      long reduceOutputRecords = workflowToken.get(taskCounterGroupName + "." + reduceOutputRecordsCounterName,
                                                   WorkflowToken.Scope.SYSTEM).getAsLong();

      long flattenMapInputRecords = workflowToken.get(flattenMapInputRecordsCounterName,
                                                      WorkflowToken.Scope.SYSTEM).getAsLong();
      long flattenMapOutputRecords = workflowToken.get(flattenMapOutputRecordsCounterName,
                                                       WorkflowToken.Scope.SYSTEM).getAsLong();
      long flattenReduceInputRecords = workflowToken.get(flattenReduceInputRecordsCounterName,
                                                         WorkflowToken.Scope.SYSTEM).getAsLong();
      long flattenReduceOutputRecords = workflowToken.get(flattenReduceOutputRecordsCounterName,
                                                          WorkflowToken.Scope.SYSTEM).getAsLong();

      Preconditions.checkArgument(mapInputRecords == flattenMapInputRecords);
      Preconditions.checkArgument(mapOutputRecords == flattenMapOutputRecords);
      Preconditions.checkArgument(reduceInputRecords == flattenReduceInputRecords);
      Preconditions.checkArgument(reduceOutputRecords == flattenReduceOutputRecords);

      long nodeSpecificMapInputRecords = workflowToken.get(flattenMapInputRecordsCounterName, programName,
                                                           WorkflowToken.Scope.SYSTEM).getAsLong();

      long nodeSpecificMapOutputRecords = workflowToken.get(flattenMapOutputRecordsCounterName, programName,
                                                            WorkflowToken.Scope.SYSTEM).getAsLong();

      long nodeSpecificReduceInputRecords = workflowToken.get(flattenReduceInputRecordsCounterName, programName,
                                                              WorkflowToken.Scope.SYSTEM).getAsLong();

      long nodeSpecificReduceOutputRecords = workflowToken.get(flattenReduceOutputRecordsCounterName, programName,
                                                               WorkflowToken.Scope.SYSTEM).getAsLong();

      Preconditions.checkArgument(mapInputRecords == nodeSpecificMapInputRecords);
      Preconditions.checkArgument(mapOutputRecords == nodeSpecificMapOutputRecords);
      Preconditions.checkArgument(reduceInputRecords == nodeSpecificReduceInputRecords);
      Preconditions.checkArgument(reduceOutputRecords == nodeSpecificReduceOutputRecords);

      Map<String, Value> systemValueMap = workflowToken.getAllFromNode(programName, WorkflowToken.Scope.SYSTEM);
      long mapInputRecordsFromGetAll = systemValueMap.get(flattenMapInputRecordsCounterName).getAsLong();
      long mapOutputRecordsFromGetAll = systemValueMap.get(flattenMapOutputRecordsCounterName).getAsLong();
      long reduceInputRecordsFromGetAll = systemValueMap.get(flattenReduceInputRecordsCounterName).getAsLong();
      long reduceOutputRecordsFromGetAll = systemValueMap.get(flattenReduceOutputRecordsCounterName).getAsLong();

      Preconditions.checkArgument(mapInputRecords == mapInputRecordsFromGetAll);
      Preconditions.checkArgument(mapOutputRecords == mapOutputRecordsFromGetAll);
      Preconditions.checkArgument(reduceInputRecords == reduceInputRecordsFromGetAll);
      Preconditions.checkArgument(reduceOutputRecords == reduceOutputRecordsFromGetAll);

      long startTime = workflowToken.get("start.time", programName).getAsLong();
      Preconditions.checkArgument(System.currentTimeMillis() > startTime);
    }
  }
}
