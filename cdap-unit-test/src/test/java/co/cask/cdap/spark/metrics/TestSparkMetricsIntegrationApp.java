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

package co.cask.cdap.spark.metrics;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * A dummy app to test Spark Metrics. This app just distribute a list so that there are some stages in Spark program.
 */
public class TestSparkMetricsIntegrationApp extends AbstractApplication {

  static final String APP_NAME = "TestSparkMetricsIntegrationApp";
  static final String APP_SPARK_NAME = "SparkMetricsProgram";

  @Override
  public void configure() {
    setName(APP_NAME);
    addSpark(new SparkMetricsProgramSpec());
  }

  public static class SparkMetricsProgramSpec extends AbstractSpark {
    @Override
    public void configure() {
      setName(APP_SPARK_NAME);
      setDescription("Test Spark Metrics");
      setMainClass(SparkMetricsProgram.class);
    }
  }

  public static class SparkMetricsProgram implements JavaSparkProgram {
    @Override
    public void run(SparkContext context) {
      List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
      final Metrics metrics = context.getMetrics();
      JavaRDD<Integer> distData = ((JavaSparkContext) context.getOriginalSparkContext()).parallelize(data);
      distData.count();
      JavaRDD<Integer> newData = distData.map(new Function<Integer, Integer>() {
        @Override
        public Integer call(Integer val) throws Exception {
          int newVal = val * 10;
          if (newVal > 30) {
            metrics.count("more.than.30", 1);
          }
          return newVal;
        }
      });
    }
  }
}
