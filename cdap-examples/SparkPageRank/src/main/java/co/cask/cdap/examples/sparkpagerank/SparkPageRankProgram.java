/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Spark PageRank program
 */
public class SparkPageRankProgram implements JavaSparkMain {
  private static final Logger LOG = LoggerFactory.getLogger(SparkPageRankProgram.class);

  private static final int ITERATIONS_COUNT = 10;
  private static final Pattern SPACES = Pattern.compile("\\s+");
  private static final String POPULAR_PAGES = "total.popular.pages";
  private static final String UNPOPULAR_PAGES = "total.unpopular.pages";
  private static final String REGULAR_PAGES = "total.regular.pages";
  private static final int POPULAR_PAGE_THRESHOLD = 10;
  private static final int UNPOPULAR_PAGE_THRESHOLD = 3;


  private static class Sum implements Function2<Double, Double, Double> {
    @Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();

    LOG.info("Processing backlinkURLs data");
    JavaPairRDD<Long, String> backlinkURLs = sec.fromStream("backlinkURLStream", String.class);
    int iterationCount = getIterationCount(sec);

    LOG.info("Grouping data by key");
    // Grouping backlinks by unique URL in key
    JavaPairRDD<String, Iterable<String>> links =
      backlinkURLs.values().mapToPair(new PairFunction<String, String, String>() {
        @Override
        public Tuple2<String, String> call(String s) {
          String[] parts = SPACES.split(s);
          return new Tuple2<>(parts[0], parts[1]);
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
            List<Tuple2<String, Double>> results = new ArrayList<>();
            for (String n : s._1()) {
              results.add(new Tuple2<>(n, s._2() / urlCount));
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

    final ServiceDiscoverer discoveryServiceContext = sec.getServiceDiscoverer();
    final Metrics sparkMetrics = sec.getMetrics();
    JavaPairRDD<byte[], Integer> ranksRaw = ranks.mapToPair(new PairFunction<Tuple2<String, Double>, byte[],
      Integer>() {
      @Override
      public Tuple2<byte[], Integer> call(Tuple2<String, Double> tuple) throws Exception {
        LOG.debug("URL {} has rank {}", Arrays.toString(tuple._1().getBytes(Charsets.UTF_8)), tuple._2());
        URL serviceURL = discoveryServiceContext.getServiceURL(SparkPageRankApp.SERVICE_HANDLERS);
        if (serviceURL == null) {
          throw new RuntimeException("Failed to discover service: " + SparkPageRankApp.SERVICE_HANDLERS);
        }
        try {
          URLConnection connection = new URL(serviceURL, String.format("%s/%s",
                                                                       SparkPageRankApp.SparkPageRankServiceHandler.
                                                                         TRANSFORM_PATH,
                                                                       tuple._2().toString())).openConnection();
          try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream(),
                                                                                Charsets.UTF_8))) {
            String pr = reader.readLine();
            if ((Integer.parseInt(pr)) == POPULAR_PAGE_THRESHOLD) {
              sparkMetrics.count(POPULAR_PAGES, 1);
            } else if (Integer.parseInt(pr) <= UNPOPULAR_PAGE_THRESHOLD) {
              sparkMetrics.count(UNPOPULAR_PAGES, 1);
            } else {
              sparkMetrics.count(REGULAR_PAGES, 1);
            }
            return new Tuple2<>(tuple._1().getBytes(Charsets.UTF_8), Integer.parseInt(pr));
          }
        } catch (Exception e) {
          LOG.warn("Failed to read the Stream for service {}", SparkPageRankApp.SERVICE_HANDLERS, e);
          throw Throwables.propagate(e);
        }
      }
    });

    // Store calculated results in output Dataset.
    // All calculated results are stored in one row.
    // Each result, the calculated URL rank based on backlink contributions, is an entry of the row.
    // The value of the entry is the URL rank.
    sec.saveAsDataset(ranksRaw, "ranks");

    LOG.info("PageRanks successfuly computed and written to \"ranks\" dataset");
  }

  private int getIterationCount(JavaSparkExecutionContext sec) {
    String args = sec.getRuntimeArguments().get("args");
    if (args == null) {
      return ITERATIONS_COUNT;
    }
    String[] parts = args.split("\\s");
    return (parts.length > 0) ? Integer.parseInt(parts[0]) : ITERATIONS_COUNT;
  }
}
