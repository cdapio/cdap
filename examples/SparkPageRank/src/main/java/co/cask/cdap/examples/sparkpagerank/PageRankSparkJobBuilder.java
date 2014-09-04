/*
 * Copyright 2014 Cask Data, Inc.
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


package co.cask.cdap.examples.sparkpagerank;

import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.spark.SparkContextFactory;
import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.Map;
import java.util.Map.Entry;


public class PageRankSparkJobBuilder {
  private static SparkContextFactory factory;
  private static final Logger LOG = LoggerFactory.getLogger(PageRankSparkJobBuilder.class);

  private static final Pattern SPACES = Pattern.compile("\\s+");

  private static class Sum implements Function2<Double, Double, Double> {
    @Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Spark Page Rank Job");
    conf.set("spark.yarn.user.classpath.first", "true");
    SparkContext sc = factory.create(new JavaSparkContext(conf));

    LOG.info("Processing neighborURLs data");

    JavaPairRDD<byte[], String> logData = sc.readFromDataset("neighborURLs", byte[].class, String.class);

    // Loads all URLs from input and initialize their neighbors.
    JavaPairRDD<String, Iterable<String>> links = logData.values().mapToPair(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String s) {
        String[] parts = SPACES.split(s);
        return new Tuple2<String, String>(parts[0], parts[1]);
      }
    }).distinct().groupByKey().cache();


    // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
      @Override
      public Double call(Iterable<String> rs) {
        return 1.0;
      }
    });
    // Calculates and updates URL ranks continuously using PageRank algorithm.
    //TODO: add Number of iterations instead of 1
    for (int current = 0; current < 1; current++) {
      // Calculates URL contributions to the rank of other URLs.
      JavaPairRDD<String, Double> contribs = links.join(ranks).values()
        .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
          @Override
          public Iterable<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
            System.out.println("processing " + s._1() + " with "+ s._2());
            int urlCount = Iterables.size(s._1());
            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
            for (String n : s._1()) {
              results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
            }
            return results;
          }
        });
      // Re-calculates URL ranks based on neighbor contributions.
      ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
        @Override
        public Double call(Double sum) {
          return 0.15 + sum * 0.85;
        }
      });
    }

    System.out.println("Writing ranks data");
    LOG.info("Writing ranks data");


    JavaPairRDD<byte[], Double> ranksRaw = ranks.mapToPair(new PairFunction<Tuple2<String, Double>, byte[], Double>() {
      @Override
      public Tuple2<byte[], Double> call(Tuple2<String, Double> tuple) throws Exception {
        System.out.println("Host: " + tuple._1() + " rank " + tuple._2());
        return new Tuple2<byte[], Double>(tuple._1().getBytes(), tuple._2());
      }
    });

    LOG.info("Writing ranks data");

    sc.writeToDataset(ranksRaw, "ranks", byte[].class, Double.class);

    JavaPairRDD<byte[], Double> abc = sc.readFromDataset("ranks", byte[].class, Double.class);
    Map<byte[], Double> newData = abc.collectAsMap();

    for (Entry<byte[], Double> entry : newData.entrySet()) {
      System.out.println("Key: " + new String(entry.getKey()) + " Data: " + entry.getValue());
    }


    System.out.println("Done!");
    LOG.info("Done!");
  }
}
