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
package co.cask.cdap;

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.internal.app.runtime.batch.WordCount;
import co.cask.cdap.runtime.WorkflowTest;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * A workflow app used by {@link WorkflowTest} for testing
 */
public class WorkflowApp extends AbstractApplication {


  @Override
  public void configure() {
    setName("WorkflowApp");
    setDescription("WorkflowApp");
    addWorkflow(new FunWorkflow());
  }

  /**
   *
   */
  public static class FunWorkflow implements Workflow {

    @Override
    public WorkflowSpecification configure() {
      return WorkflowSpecification.Builder.with()
        .setName("FunWorkflow")
        .setDescription("FunWorkflow description")
        .startWith(new WordCountMapReduce())
        .then(new CustomAction("verify"))
        .last(new SparkWorkflowTestApp())
        .build();
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
    }

    @Override
    public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
      // No-op
    }
  }

  public static class SparkWorkflowTestApp extends AbstractSpark {
    @Override
    public void configure() {
      setDescription("Test Spark with Workflow");
      setMainClass(SparkWorkflowTestProgram.class);
    }
  }

  public static class SparkWorkflowTestProgram implements JavaSparkProgram {
    @Override
    public void run(SparkContext context) {
      File outputDir = new File(context.getRuntimeArguments().get("outputPath"));
      File successFile = new File(outputDir, "_SUCCESS");
      Preconditions.checkState(successFile.exists());
      try {
        FileUtils.deleteDirectory(successFile);
      } catch (IOException e) {
        Throwables.propagate(e);
      }
      Preconditions.checkState(!successFile.exists());
    }
  }
  
  /**
   *
   */
  public static final class CustomAction extends AbstractWorkflowAction {

    private static final Logger LOG = LoggerFactory.getLogger(CustomAction.class);

    private final String name;

    @Property
    private final boolean condition = true;

    public CustomAction(String name) {
      this.name = name;
    }

    @Override
    public WorkflowActionSpecification configure() {
      return WorkflowActionSpecification.Builder.with()
        .setName(name)
        .setDescription(name)
        .build();
    }

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      LOG.info("Custom action initialized: " + context.getSpecification().getName());
    }

    @Override
    public void destroy() {
      super.destroy();
      LOG.info("Custom action destroyed: " + getContext().getSpecification().getName());
    }

    @Override
    public void run() {
      LOG.info("Custom action run");
      File outputDir = new File(getContext().getRuntimeArguments().get("outputPath"));

      Preconditions.checkState(condition && new File(outputDir, "_SUCCESS").exists());

      LOG.info("Custom run completed.");
    }
  }
}
