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

package co.cask.cdap.test.app;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.WorkflowContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

/**
 * Workflow application containing local datasets.
 */
public class WorkflowAppWithLocalDatasets extends AbstractApplication {
  public static final String WORDCOUNT_DATASET = "wordcount";
  public static final String RESULT_DATASET = "result";
  public static final String CSV_FILESET_DATASET = "csvfileset";
  public static final String WORKFLOW_NAME = "WorkflowWithLocalDatasets";
  public static final String WORKFLOW_RUNS_DATASET = "workflowruns";

  @Override
  public void configure() {
    setName("WorkflowAppWithLocalDatasets");
    setDescription("App to test the local dataset functionality for the Workflow.");

    addSpark(new SparkCSVToSpaceProgram());
    addMapReduce(new WordCount());
    addWorkflow(new WorkflowWithLocalDatasets());
    createDataset(RESULT_DATASET, KeyValueTable.class);
    createDataset(WORKFLOW_RUNS_DATASET, KeyValueTable.class);
  }

  /**
   * Workflow which configures the local dataset.
   */
  public static class WorkflowWithLocalDatasets extends AbstractWorkflow {

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      KeyValueTable workflowRuns = context.getDataset(WORKFLOW_RUNS_DATASET);
      workflowRuns.write(context.getRunId().getId(), "STARTED");
    }

    @Override
    public void destroy() {
      KeyValueTable workflowRuns = getContext().getDataset(WORKFLOW_RUNS_DATASET);
      String status = Bytes.toString(workflowRuns.read(getContext().getRunId().getId()));
      if (!"STARTED".equals(status)) {
        return;
      }

      if (getContext().getRuntimeArguments().containsKey("destroy.throw.exception")) {
        // throw exception from destroy. should not affect the Workflow run status
        throw new RuntimeException("destroy");
      }
      workflowRuns.write(getContext().getRunId().getId(), "COMPLETED");
    }

    @Override
    protected void configure() {
      setName(WORKFLOW_NAME);
      setDescription("Workflow program with local datasets.");
      createLocalDataset(WORDCOUNT_DATASET, KeyValueTable.class);
      createLocalDataset(CSV_FILESET_DATASET, FileSet.class, FileSetProperties.builder()
        .setInputFormat(TextInputFormat.class)
        .setOutputFormat(TextOutputFormat.class)
        .build());
      addAction(new LocalDatasetWriter());
      addSpark("JavaSparkCSVToSpaceConverter");
      addMapReduce("WordCount");
      addAction(new LocalDatasetReader("readerAction"));
    }
  }

  /**
   * Custom action writing to the local file set dataset.
   */
  public static class LocalDatasetWriter extends AbstractCustomAction {

    private static final Logger LOG = LoggerFactory.getLogger(LocalDatasetWriter.class);
    private Metrics metrics;

    @Override
    public void run() {
      try {
        String inputPath = getContext().getRuntimeArguments().get("input.path");
        FileSet fileSetDataset = getContext().getDataset(CSV_FILESET_DATASET);
        Location inputLocation = fileSetDataset.getLocation(inputPath);
        try (PrintWriter writer = new PrintWriter(inputLocation.getOutputStream())) {
          writer.write("this,text,has");
          writer.println();
          writer.write("two,words,text,inside");
          metrics.gauge("num.lines", 2);
        }
      } catch (Throwable t) {
        LOG.error("Exception occurred while running custom action ", t);
      }
    }
  }

  /**
   * MapReduce program that simply counts the number of occurrences of the words in the input files.
   */
  public static class WordCount extends AbstractMapReduce {
    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      String inputPath = context.getRuntimeArguments().get("output.path");
      Map<String, String> fileSetArgs = new HashMap<>();
      FileSetArguments.addInputPath(fileSetArgs, inputPath);
      context.addInput(Input.ofDataset(CSV_FILESET_DATASET, fileSetArgs));
      context.addOutput(Output.ofDataset(WORDCOUNT_DATASET));
      Job job = context.getHadoopJob();
      job.setMapperClass(TokenizerMapper.class);
      job.setReducerClass(IntSumReducer.class);

      job.setNumReduceTasks(1);
    }
  }

  /**
   * Mapper to tokenized the the line into words.
   */
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, ONE);
      }
    }
  }

  /**
   * Reducer to write the word counts to the local Workflow dataset.
   */
  public static class IntSumReducer extends Reducer<Text, IntWritable, byte[], byte[]> {
    private IntWritable result = new IntWritable();
    private Metrics metrics;

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      metrics.count("num.words", sum);
      context.write(Bytes.toBytes(key.toString()), Bytes.toBytes(String.valueOf(result.get())));
    }
  }

  /**
   * Custom action that reads the local dataset and writes to the non-local dataset.
   */
  public static class LocalDatasetReader extends AbstractCustomAction {
    private static final Logger LOG = LoggerFactory.getLogger(LocalDatasetReader.class);
    private Metrics metrics;

    private final String actionName;

    private LocalDatasetReader(String name) {
      this.actionName = name;
    }

    @Override
    protected void configure() {
      super.configure();
      setName(actionName);
    }

    @UseDataSet("wordcount")
    private KeyValueTable wordCount;

    @UseDataSet("result")
    private KeyValueTable result;

    @Override
    public void run() {
      LOG.info("Read the local dataset");
      try {
        File waitFile = new File(getContext().getRuntimeArguments().get("wait.file"));
        waitFile.createNewFile();

        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            int uniqueWordCount = 0;
            try (CloseableIterator<KeyValue<byte[], byte[]>> scanner = wordCount.scan(null, null)) {
              while (scanner.hasNext()) {
                scanner.next();
                uniqueWordCount++;
              }
            }
            result.write("UniqueWordCount", String.valueOf(uniqueWordCount));
            metrics.gauge("unique.words", uniqueWordCount);
          }
        });

        File doneFile = new File(getContext().getRuntimeArguments().get("done.file"));
        while (!doneFile.exists()) {
          TimeUnit.MILLISECONDS.sleep(50);
        }
      } catch (Exception e) {
        LOG.error("Exception occurred while running custom action ", e);
      }
    }
  }
}
