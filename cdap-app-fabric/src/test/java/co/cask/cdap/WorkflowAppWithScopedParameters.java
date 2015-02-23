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
import co.cask.cdap.internal.app.runtime.batch.WordCount;
import com.google.common.base.Preconditions;
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

      Preconditions.checkArgument(args.size() == 16);
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
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();

      Preconditions.checkArgument(args.size() == 16);
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
    public void beforeSubmit(SparkContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();

      Preconditions.checkArgument(args.size() == 15);
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
    public void beforeSubmit(SparkContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();
      Preconditions.checkArgument(args.size() == 15);
      Preconditions.checkArgument(args.get("input.path").contains("SparkInput"));
      Preconditions.checkArgument(args.get("output.path").contains("AnotherSparkOutput"));
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
    }
  }
}
