/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.NodeStatus;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowNodeState;
import co.cask.cdap.internal.app.runtime.batch.WordCount;
import com.google.common.base.Preconditions;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class WorkflowAppWithScopedParameters extends AbstractApplication {

  public static final String APP_NAME = "WorkflowAppWithScopedParameters";
  public static final String ONE_MR = "OneMR";
  public static final String ANOTHER_MR = "AnotherMR";
  public static final String ONE_SPARK = "OneSpark";
  public static final String ANOTHER_SPARK = "AnotherSpark";
  public static final String ONE_WORKFLOW = "OneWorkflow";
  public static final String ONE_ACTION = "OneAction";

  @Override
  public void configure() {
    setName(APP_NAME);
    setDescription("WorkflowApp which demonstrates the scoped runtime arguments.");
    addMapReduce(new OneMR());
    addMapReduce(new AnotherMR());
    addSpark(new OneSpark());
    addSpark(new AnotherSpark());
    addWorkflow(new OneWorkflow());
    createDataset("Purchase", KeyValueTable.class);
    createDataset("UserProfile", KeyValueTable.class);
  }

  /**
   *
   */
  public static class OneMR extends AbstractMapReduce {

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Map<String, String> args = context.getRuntimeArguments();

      Preconditions.checkArgument(args.size() == 18);
      Preconditions.checkArgument(context.getLogicalStartTime() == 1234567890000L);
      Preconditions.checkArgument(args.get("logical.start.time").equals("1234567890000"));
      Preconditions.checkArgument(args.get("input.path").contains("OneMRInput"));
      Preconditions.checkArgument(args.get("output.path").contains("OneMROutput"));

      String inputPath = args.get("input.path");
      String outputPath = args.get("output.path");
      WordCount.configureJob((Job) context.getHadoopJob(), inputPath, outputPath);
    }
  }

  /**
   *
   */
  public static class AnotherMR extends AbstractMapReduce {

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Map<String, String> args = context.getRuntimeArguments();
      Preconditions.checkArgument(args.size() == 18);
      Preconditions.checkArgument(args.get("input.path").contains("AnotherMRInput"));
      Preconditions.checkArgument(args.get("output.path").contains("ProgramOutput"));

      String inputPath = args.get("input.path");
      String outputPath = args.get("output.path");
      WordCount.configureJob((Job) context.getHadoopJob(), inputPath, outputPath);
    }
  }

  /**
   *
   */
  public static class OneSpark extends AbstractSpark {
    @Override
    public void beforeSubmit(SparkClientContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();

      Preconditions.checkArgument(args.size() == 17);
      Preconditions.checkArgument(args.get("input.path").contains("SparkInput"));
      Preconditions.checkArgument(args.get("output.path").contains("ProgramOutput"));
    }

    @Override
    public void configure() {
      setMainClass(SparkTestProgram.class);
    }
  }

  /**
   *
   */
  public static class AnotherSpark extends AbstractSpark {
    @Override
    public void beforeSubmit(SparkClientContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();
      Preconditions.checkArgument(args.size() == 17);
      Preconditions.checkArgument(args.get("input.path").contains("SparkInput"));
      Preconditions.checkArgument(args.get("output.path").contains("AnotherSparkOutput"));
    }

    @Override
    public void configure() {
      setMainClass(SparkTestProgram.class);
    }
  }

  public static class SparkTestProgram implements JavaSparkMain {

    @Override
    public void run(JavaSparkExecutionContext sec) throws Exception {
      JavaSparkContext jsc = new JavaSparkContext();
      List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
      JavaRDD<Integer> distData = jsc.parallelize(data);
      distData.collect();
    }
  }

  /**
   *
   */
  public static class OneWorkflow extends AbstractWorkflow {

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      Preconditions.checkArgument(0 == context.getNodeStates().size());
    }

    @Override
    public void configure() {
      addMapReduce(ONE_MR);
      addSpark(ONE_SPARK);
      addAction(new OneAction());
      addMapReduce(ANOTHER_MR);
      addSpark(ANOTHER_SPARK);
    }

    @Override
    public void destroy() {
      Map<String, WorkflowNodeState> nodeStates = getContext().getNodeStates();
      Preconditions.checkArgument(5 == nodeStates.size());
      WorkflowNodeState nodeState = nodeStates.get(ONE_MR);
      Preconditions.checkArgument(ONE_MR.equals(nodeState.getNodeId()));
      Preconditions.checkArgument(nodeState.getRunId() != null);
      Preconditions.checkArgument(NodeStatus.COMPLETED == nodeState.getNodeStatus());

      nodeState = nodeStates.get(ONE_SPARK);
      Preconditions.checkArgument(ONE_SPARK.equals(nodeState.getNodeId()));
      Preconditions.checkArgument(nodeState.getRunId() != null);
      Preconditions.checkArgument(NodeStatus.COMPLETED == nodeState.getNodeStatus());

      nodeState = nodeStates.get(ANOTHER_MR);
      Preconditions.checkArgument(ANOTHER_MR.equals(nodeState.getNodeId()));
      Preconditions.checkArgument(nodeState.getRunId() != null);
      Preconditions.checkArgument(NodeStatus.COMPLETED == nodeState.getNodeStatus());

      nodeState = nodeStates.get(ANOTHER_SPARK);
      Preconditions.checkArgument(ANOTHER_SPARK.equals(nodeState.getNodeId()));
      Preconditions.checkArgument(nodeState.getRunId() != null);
      Preconditions.checkArgument(NodeStatus.COMPLETED == nodeState.getNodeStatus());

      nodeState = nodeStates.get(ONE_ACTION);
      Preconditions.checkArgument(ONE_ACTION.equals(nodeState.getNodeId()));
      Preconditions.checkArgument(NodeStatus.COMPLETED == nodeState.getNodeStatus());
    }
  }

  public static class OneAction extends AbstractWorkflowAction {

    @Override
    public void run() {
      Map<String, WorkflowNodeState> nodeStates = getContext().getNodeStates();
      Preconditions.checkArgument(2 == nodeStates.size());
      WorkflowNodeState nodeState = nodeStates.get(ONE_MR);
      Preconditions.checkArgument(ONE_MR.equals(nodeState.getNodeId()));
      Preconditions.checkArgument(nodeState.getRunId() != null);
      Preconditions.checkArgument(NodeStatus.COMPLETED == nodeState.getNodeStatus());

      nodeState = nodeStates.get(ONE_SPARK);
      Preconditions.checkArgument(ONE_SPARK.equals(nodeState.getNodeId()));
      Preconditions.checkArgument(nodeState.getRunId() != null);
      Preconditions.checkArgument(NodeStatus.COMPLETED == nodeState.getNodeStatus());
    }
  }
}
