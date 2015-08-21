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

package co.cask.cdap.client.app;

import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * A Fake spark program to test CLI integration with Spark
 */
public class FakeSpark extends AbstractSpark {

  public static final String NAME = "FakeSparkProgram";
  private static final Logger LOG = LoggerFactory.getLogger(FakeSpark.class);

  @Override
  public void configure() {
    setName(NAME);
    setDescription("");
    setMainClass(FakeSparkProgram.class);
  }

  /**
   *
   */
  public static class FakeSparkProgram implements JavaSparkProgram {
    @Override
    public void run(SparkContext context) {
      LOG.info("HelloFakeSpark");
      List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
      JavaRDD<Integer> distData = ((JavaSparkContext) context.getOriginalSparkContext()).parallelize(data);
      distData.collect();
    }
  }
}
