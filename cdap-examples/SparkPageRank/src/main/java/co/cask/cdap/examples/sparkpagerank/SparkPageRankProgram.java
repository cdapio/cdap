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
 *
 * This example is based on the Apache Spark Example JavaPageRank. The original file may be found at
 * https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/JavaPageRank.java
 *
 * Copyright 2014 The Apache Software Foundation. Licensed under the Apache License, Version 2.0.
 */


package co.cask.cdap.examples.sparkpagerank;

import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static co.cask.cdap.examples.sparkpagerank.SparkPageRankApp.UTF8;

/**
 * Spark PageRank program
 */
public class SparkPageRankProgram implements JavaSparkProgram {
  private static final Logger LOG = LoggerFactory.getLogger(SparkPageRankProgram.class);

  private static final int ITERATIONS_COUNT = 10;
  private static final Pattern SPACES = Pattern.compile("\\s+");

  private static class Sum implements Function2<Double, Double, Double> {
    @Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }

  @Override
  public void run(SparkContext sc) {
    LOG.info("Processing backlinkURLs data");
    JavaPairRDD<byte[], String> backlinkURLs = sc.readFromDataset("backlinkURLs", byte[].class, String.class);
    int iterationCount = getIterationCount(sc);

    LOG.info("Grouping data by key");
    // Grouping backlinks by unique URL in key
    JavaPairRDD<String, Iterable<String>> links =
      backlinkURLs.values().mapToPair(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String s) {
        String[] parts = SPACES.split(s);
        return new Tuple2<String, String>(parts[0], parts[1]);
      }
    }).distinct().groupByKey().cache();

    // Initialize default rank for each key URL
    JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
      @Override
      public Double call(Iterable<String> rs) {
        return 1.0;
      }
    });
    // Calculates and updates URL ranks continuously using PageRank algorithm.
    for (int current = 0; current < iterationCount; current++) {
      LOG.debug("Processing data with PageRank algorithm. Iteration {}/{}", current + 1, (iterationCount));
      // Calculates URL contributions to the rank of other URLs.
      JavaPairRDD<String, Double> contribs = links.join(ranks).values()
        .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
          @Override
          public Iterable<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
            LOG.debug("Processing {} with rank {}", s._1(), s._2());
            int urlCount = Iterables.size(s._1());
            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
            for (String n : s._1()) {
              results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
            }
            return results;
          }
        });
      // Re-calculates URL ranks based on backlink contributions.
      ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
        @Override
        public Double call(Double sum) {
          return 0.15 + sum * 0.85;
        }
      });
    }

    LOG.info("Writing ranks data");

    JavaPairRDD<byte[], Double> ranksRaw = ranks.mapToPair(new PairFunction<Tuple2<String, Double>, byte[], Double>() {
      @Override
      public Tuple2<byte[], Double> call(Tuple2<String, Double> tuple) throws Exception {
        LOG.debug("URL {} has rank {}", Arrays.toString(tuple._1().getBytes(UTF8)), tuple._2());
        return new Tuple2<byte[], Double>(tuple._1().getBytes(UTF8), tuple._2());
      }
    });

    // Store calculated results in output Dataset.
    // All calculated results are stored in one row.
    // Each result, the calculated URL rank based on backlink contributions, is an entry of the row.
    // The value of the entry is the URL rank.
    sc.writeToDataset(ranksRaw, "ranks", byte[].class, Double.class);

    LOG.info("Done!");
  }

  private int getIterationCount(SparkContext sc) {
    String[] args = sc.getRuntimeArguments("args");
    int iterationCount;
    if (args != null && args.length > 0) {
      iterationCount = Integer.valueOf(args[0]);
    } else {
      iterationCount = ITERATIONS_COUNT;
    }
    return iterationCount;
  }
}
