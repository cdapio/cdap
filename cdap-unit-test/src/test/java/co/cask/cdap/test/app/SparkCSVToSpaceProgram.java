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
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Main class for the Spark program to convert comma separated file into space separated file.
 */
public final class SparkCSVToSpaceProgram extends AbstractSpark implements JavaSparkMain {

  @Override
  protected void configure() {
    setName("JavaSparkCSVToSpaceConverter");
    setMainClass(SparkCSVToSpaceProgram.class);
  }

  @Override
  public void run(final JavaSparkExecutionContext sec) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();

    Map<String, String> fileSetArgs = new HashMap<>();
    final Metrics metrics = sec.getMetrics();
    FileSetArguments.addInputPath(fileSetArgs, sec.getRuntimeArguments().get("input.path"));
    JavaPairRDD<LongWritable, Text> input = sec.fromDataset(
      WorkflowAppWithLocalDatasets.CSV_FILESET_DATASET, fileSetArgs);

    final List<String> converted = input.values().map(new Function<Text, String>() {
      @Override
      public String call(Text input) throws Exception {
        String line = input.toString();
        metrics.count("num.lines", 1);
        return line.replaceAll(",", " ");
      }
    }).collect();

    sec.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        Map<String, String> args = sec.getRuntimeArguments();
        String outputPath = args.get("output.path");

        Map<String, String> fileSetArgs = new HashMap<>();
        FileSetArguments.setOutputPath(fileSetArgs, outputPath);

        FileSet fileSet = context.getDataset(WorkflowAppWithLocalDatasets.CSV_FILESET_DATASET, fileSetArgs);
        try (PrintWriter writer = new PrintWriter(fileSet.getOutputLocation().getOutputStream())) {
          for (String line : converted) {
            writer.write(line);
            writer.println();
          }
        }
      }
    });
  }
}
