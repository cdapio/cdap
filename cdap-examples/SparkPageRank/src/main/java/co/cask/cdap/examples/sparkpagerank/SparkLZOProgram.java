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

import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark PageRank program
 */
public class SparkLZOProgram implements JavaSparkMain {
  private static final Logger LOG = LoggerFactory.getLogger(SparkLZOProgram.class);



  private static class Sum implements Function2<Double, Double, Double> {
    @Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();

    JavaRDD<String> stringJavaRDD = jsc.textFile("hdfs://100.txt");
    LOG.info("===> Read {} lines from txt file", stringJavaRDD.count());

    jsc.newAPIHadoopFile("hdfs://100.txt.lzo", LzoTextInputFormat.class, LongWritable.class, Text.class);


  }

}
