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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.internal.app.runtime.batch.WordCount;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.mapreduce.Job;

import java.util.Map;

/**
 *
 */
public class WorkflowAppWithScopedParameters extends AbstractApplication {
  @Override
  public void configure() {
    setName("WorkflowAppWithScopedParameters");
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
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();

      Preconditions.checkArgument(args.size() == 8);
      Preconditions.checkArgument(args.get("debug").equals("true"));
      Preconditions.checkArgument(args.get("input.path").contains("OneMRInput"));
      Preconditions.checkArgument(args.get("output.path").contains("OneMROutput"));
      Preconditions.checkArgument(args.get("processing.time").equals("1HR"));
      Preconditions.checkArgument(args.get("dataset.Purchase.cache.seconds").equals("30"));
      Preconditions.checkArgument(args.get("dataset.UserProfile.schema.property").equals("constant"));
      Preconditions.checkArgument(args.get("dataset.unknown.dataset").equals("false"));
      Preconditions.checkArgument(args.get("dataset.*.read.timeout").equals("60"));

      String inputPath = args.get("input.path");
      String outputPath = args.get("output.path");
      WordCount.configureJob((Job) context.getHadoopJob(), inputPath, outputPath);

      Map<String, String> purchaseProperties = Maps.newHashMap();
      purchaseProperties.put("debug", "true");
      purchaseProperties.put("input.path", args.get("input.path"));
      purchaseProperties.put("output.path", args.get("output.path"));
      purchaseProperties.put("processing.time", "1HR");
      purchaseProperties.put("cache.seconds", "30");
      purchaseProperties.put("read.timeout", "60");

      KeyValueTable purchaseTable1 = context.getDataset("Purchase", purchaseProperties);
      KeyValueTable purchaseTable2 = context.getDataset("Purchase");

      Preconditions.checkArgument(System.identityHashCode(purchaseTable1) == System.identityHashCode(purchaseTable2));

      Map<String, String> userProfileProperties = Maps.newHashMap();
      userProfileProperties.put("debug", "true");
      userProfileProperties.put("input.path", args.get("input.path"));
      userProfileProperties.put("output.path", args.get("output.path"));
      userProfileProperties.put("processing.time", "1HR");
      userProfileProperties.put("schema.property", "constant");
      userProfileProperties.put("read.timeout", "60");

      KeyValueTable userProfileTable1 = context.getDataset("UserProfile", userProfileProperties);
      KeyValueTable userProfileTable2 = context.getDataset("UserProfile");

      Preconditions.checkArgument(System.identityHashCode(userProfileTable1) ==
                                    System.identityHashCode(userProfileTable2));
    }
  }

  /**
   *
   */
  public static class AnotherMR extends AbstractMapReduce {

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();

      Preconditions.checkArgument(args.size() == 8);
      Preconditions.checkArgument(args.get("debug").equals("false"));
      Preconditions.checkArgument(args.get("input.path").contains("AnotherMRInput"));
      Preconditions.checkArgument(args.get("output.path").contains("ProgramOutput"));
      Preconditions.checkArgument(args.get("processing.time").equals("1HR"));
      Preconditions.checkArgument(args.get("dataset.Purchase.cache.seconds").equals("30"));
      Preconditions.checkArgument(args.get("dataset.UserProfile.schema.property").equals("constant"));
      Preconditions.checkArgument(args.get("dataset.unknown.dataset").equals("false"));
      Preconditions.checkArgument(args.get("dataset.*.read.timeout").equals("60"));

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
    public void beforeSubmit(SparkContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();

      Preconditions.checkArgument(args.size() == 7);
      Preconditions.checkArgument(args.get("debug").equals("true"));
      Preconditions.checkArgument(args.get("input.path").contains("SparkInput"));
      Preconditions.checkArgument(args.get("output.path").contains("ProgramOutput"));
      Preconditions.checkArgument(args.get("dataset.Purchase.cache.seconds").equals("30"));
      Preconditions.checkArgument(args.get("dataset.UserProfile.schema.property").equals("constant"));
      Preconditions.checkArgument(args.get("dataset.unknown.dataset").equals("false"));
      Preconditions.checkArgument(args.get("dataset.*.read.timeout").equals("60"));
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
    public void beforeSubmit(SparkContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();
      Preconditions.checkArgument(args.size() == 7);
      Preconditions.checkArgument(args.get("debug").equals("true"));
      Preconditions.checkArgument(args.get("input.path").contains("SparkInput"));
      Preconditions.checkArgument(args.get("output.path").contains("AnotherSparkOutput"));
      Preconditions.checkArgument(args.get("dataset.Purchase.cache.seconds").equals("30"));
      Preconditions.checkArgument(args.get("dataset.UserProfile.schema.property").equals("constant"));
      Preconditions.checkArgument(args.get("dataset.unknown.dataset").equals("false"));
      Preconditions.checkArgument(args.get("dataset.*.read.timeout").equals("60"));
    }

    @Override
    public void configure() {
      setMainClass(SparkTestProgram.class);
    }
  }

  public static class SparkTestProgram implements JavaSparkProgram {
    @Override
    public void run(SparkContext context) {
      // no-op
    }
  }


  /**
   *
   */
  public static class OneWorkflow extends AbstractWorkflow {
    @Override
    public void configure() {
      addMapReduce("OneMR");
      addSpark("OneSpark");
      addMapReduce("AnotherMR");
      addSpark("AnotherSpark");
      addAction(new OneAction());
      addAction(new AnotherAction());
    }
  }

  /**
   *
   */
  public static final class OneAction extends AbstractWorkflowAction {

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      Map<String, String> args = getContext().getRuntimeArguments();
      Preconditions.checkArgument(args.size() == 7);
      Preconditions.checkArgument(args.get("debug").equals("true"));
      Preconditions.checkArgument(args.get("input.path").contains("OneActionInput"));
      Preconditions.checkArgument(args.get("output.path").contains("ProgramOutput"));
      Preconditions.checkArgument(args.get("dataset.Purchase.cache.seconds").equals("30"));
      Preconditions.checkArgument(args.get("dataset.UserProfile.schema.property").equals("constant"));
      Preconditions.checkArgument(args.get("dataset.unknown.dataset").equals("false"));
      Preconditions.checkArgument(args.get("dataset.*.read.timeout").equals("60"));
    }

    @Override
    public void destroy() {
      super.destroy();
    }

    @Override
    public void run() {
    }
  }

  /**
   *
   */
  public static final class AnotherAction extends AbstractWorkflowAction {

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      Map<String, String> args = getContext().getRuntimeArguments();
      Preconditions.checkArgument(args.size() == 7);
      Preconditions.checkArgument(args.get("debug").equals("true"));
      Preconditions.checkArgument(args.get("input.path").contains("ProgramInput"));
      Preconditions.checkArgument(args.get("output.path").contains("ProgramOutput"));
      Preconditions.checkArgument(args.get("dataset.Purchase.cache.seconds").equals("30"));
      Preconditions.checkArgument(args.get("dataset.UserProfile.schema.property").equals("constant"));
      Preconditions.checkArgument(args.get("dataset.unknown.dataset").equals("false"));
      Preconditions.checkArgument(args.get("dataset.*.read.timeout").equals("60"));
    }

    @Override
    public void destroy() {
      super.destroy();
    }

    @Override
    public void run() {
    }
  }
}
