/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Property;
import com.continuuity.api.mapreduce.MapReduce;
import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.workflow.AbstractWorkflowAction;
import com.continuuity.api.workflow.Workflow;
import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.continuuity.api.workflow.WorkflowContext;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.internal.app.runtime.batch.WordCount;
import com.google.common.base.Preconditions;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 *
 */
public class WorkflowApp implements Application {


  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("WorkflowApp")
      .setDescription("WorkflowApp")
      .noStream()
      .noDataSet()
      .noFlow()
      .noProcedure()
      .noMapReduce()
      .withWorkflows().add(new FunWorkflow()).build();
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
        .last(new CustomAction("verify"))
        .build();
    }
  }

  /**
   *
   */
  public static final class WordCountMapReduce implements MapReduce {

    @Override
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName("ClassicWordCount")
        .setDescription("WordCount job from Hadoop examples")
        .build();
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
