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

package co.cask.cdap.spark.app;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.spark.app.SparkAppUsingGetDataset.LogKey;
import co.cask.cdap.spark.app.SparkAppUsingGetDataset.LogStats;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public class SparkLogParser extends AbstractSpark implements JavaSparkMain {

  @Override
  protected void configure() {
    setMainClass(SparkLogParser.class);
  }

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();

    Map<String, String> runtimeArguments = sec.getRuntimeArguments();
    String inputFileSet = runtimeArguments.get("input");
    final String outputTable = runtimeArguments.get("output");

    JavaPairRDD<LongWritable, Text> input = sec.fromDataset(inputFileSet);

    final JavaPairRDD<String, String> aggregated = input.mapToPair(
      new PairFunction<Tuple2<LongWritable, Text>, LogKey, LogStats>() {
        @Override
        public Tuple2<LogKey, LogStats> call(Tuple2<LongWritable, Text> input) throws Exception {
          return SparkAppUsingGetDataset.parse(input._2());
        }
      }
    ).reduceByKey(
      new Function2<LogStats, LogStats, LogStats>() {
        @Override
        public LogStats call(LogStats stats1, LogStats stats2) throws Exception {
          return stats1.aggregate(stats2);
        }
      }
    ).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<LogKey, LogStats>>, String, String>() {
      @Override
      public Iterable<Tuple2<String, String>> call(Iterator<Tuple2<LogKey, LogStats>> itor) throws Exception {
        final Gson gson = new Gson();
        return Lists.newArrayList(
          Iterators.transform(itor, new Function<Tuple2<LogKey, LogStats>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> apply(Tuple2<LogKey, LogStats> input) {
              return new Tuple2<>(gson.toJson(input._1()), gson.toJson(input._2()));
            }
          }));
      }
    });

    // Collect all data to driver and write to dataset directly. That's the intend of the test.
    sec.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        KeyValueTable kvTable = context.getDataset(outputTable);
        for (Map.Entry<String, String> entry : aggregated.collectAsMap().entrySet()) {
          kvTable.write(entry.getKey(), entry.getValue());
        }
      }
    });
  }
}
