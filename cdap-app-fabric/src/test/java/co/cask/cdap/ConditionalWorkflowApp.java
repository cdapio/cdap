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
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.internal.app.runtime.batch.WordCount;
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
    }
  }

  public static final class MyVerificationPredicate implements Predicate<WorkflowContext> {

    @Override
    public boolean apply(@Nullable WorkflowContext input) {
      if (input != null) {
        Map<String, Map<String, Long>> hadoopCounters = input.getToken().getMapReduceCounters();
        if (hadoopCounters != null) {
          Map<String, Long> customCounters = hadoopCounters.get("MyCustomCounter");
          // If number of good records are greater than the number of bad records then only
          // return true to execute the true branch associated with this Condition node
          if (customCounters.get("GoodRecord") > customCounters.get("BadRecord")) {
            return true;
          }
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
}
