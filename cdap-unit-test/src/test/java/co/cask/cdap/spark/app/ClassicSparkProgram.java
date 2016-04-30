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

import co.cask.cdap.api.spark.JavaSparkMain;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

/**
 * A Spark program that has a static main method instead of extending from {@link JavaSparkMain}
 */
public class ClassicSparkProgram {

  public static void main(String[] args) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();
    int result = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      .reduce(new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer v1, Integer v2) throws Exception {
          return v1 + v2;
        }
      });

    if (result != 55) {
      throw new Exception("Expected result to be 55");
    }
  }
}
