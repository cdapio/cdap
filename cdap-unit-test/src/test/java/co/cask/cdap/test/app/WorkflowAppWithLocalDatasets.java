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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

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
  @Override
  public void configure() {
    setName("WorkflowAppWithLocalDatasets");
    setDescription("App to test the local dataset functionality for the Workflow.");

    addSpark(new JavaSparkCSVToSpaceConverter());
    addMapReduce(new WordCount());
    addWorkflow(new WorkflowWithLocalDatasets());
    createDataset(RESULT_DATASET, KeyValueTable.class);
  }

  /**
   * Workflow which configures the local dataset.
   */
  public static class WorkflowWithLocalDatasets extends AbstractWorkflow {

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
      addAction(new LocalDatasetReader());
    }
  }

  /**
   * Custom action writing to the local file set dataset.
   */
  public static class LocalDatasetWriter extends AbstractWorkflowAction {

    private static final Logger LOG = LoggerFactory.getLogger(LocalDatasetWriter.class);

    @Override
    public void run() {
      String inputPath = getContext().getRuntimeArguments().get("input.path");
      FileSet fileSetDataset = getContext().getDataset(CSV_FILESET_DATASET);
      Location inputLocation = fileSetDataset.getLocation(inputPath);

      try {
        try (PrintWriter writer = new PrintWriter(inputLocation.getOutputStream())) {
          writer.write("this,text,has");
          writer.println();
          writer.write("two,words,text,inside");
        }
      } catch (Throwable t) {
        LOG.error("Exception occurred while running custom action ", t);
      }
    }
  }

  /**
   * Spark program to convert comma separated file into space separated file.
   */
  public static final class JavaSparkCSVToSpaceConverter extends AbstractSpark {
    @Override
    protected void configure() {
      setName("JavaSparkCSVToSpaceConverter");
      setMainClass(SparkCSVToSpaceProgram.class);
    }
  }


  /**
   * Main class for the Spark program to convert comma separated file into space separated file.
   */
  public static final class SparkCSVToSpaceProgram implements JavaSparkProgram {

    @Override
    public void run(SparkContext context) throws Exception {
      Map<String, String> fileSetArgs = new HashMap<>();
      FileSetArguments.addInputPath(fileSetArgs, context.getRuntimeArguments().get("input.path"));
      JavaPairRDD<LongWritable, Text> input = context.readFromDataset(CSV_FILESET_DATASET, LongWritable.class,
                                                                      Text.class, fileSetArgs);

      JavaRDD<String> converted = input.values().map(new Function<Text, String>() {
        @Override
        public String call(Text input) throws Exception {
          String line = input.toString();
          return line.replaceAll(",", " ");
        }
      });

      Map<String, String> args = context.getRuntimeArguments();
      String outputPath = args.get("output.path");
      fileSetArgs = new HashMap<>();
      FileSetArguments.setOutputPath(fileSetArgs, outputPath);
      FileSet fileSet = context.getDataset(CSV_FILESET_DATASET, fileSetArgs);
      try (PrintWriter writer = new PrintWriter(fileSet.getOutputLocation().getOutputStream())) {
        for (String line : converted.collect()) {
          writer.write(line);
          writer.println();
        }
      }
    }
  }

  /**
   * MapReduce program that simply counts the number of occurrences of the words in the input files.
   */
  public static class WordCount extends AbstractMapReduce {
    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      String inputPath = context.getRuntimeArguments().get("output.path");
      Map<String, String> fileSetArgs = new HashMap<>();
      FileSetArguments.addInputPath(fileSetArgs, inputPath);
      context.setInput(CSV_FILESET_DATASET, fileSetArgs);
      context.addOutput(WORDCOUNT_DATASET);
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

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(Bytes.toBytes(key.toString()), Bytes.toBytes(String.valueOf(result.get())));
    }
  }

  /**
   * Custom action that reads the local dataset and writes to the non-local dataset.
   */
  public static class LocalDatasetReader extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(LocalDatasetReader.class);

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

        int uniqueWordCount = 0;
        CloseableIterator<KeyValue<byte[], byte[]>> scanner = wordCount.scan(null, null);
        try {
          while (scanner.hasNext()) {
            scanner.next();
            uniqueWordCount++;
          }
        } finally {
          scanner.close();
        }
        result.write("UniqueWordCount", String.valueOf(uniqueWordCount));
        File doneFile = new File(getContext().getRuntimeArguments().get("done.file"));
        while (!doneFile.exists()) {
          TimeUnit.MILLISECONDS.sleep(50);
        }
      } catch (Exception e) {
        // no-op
      }
    }
  }
}
