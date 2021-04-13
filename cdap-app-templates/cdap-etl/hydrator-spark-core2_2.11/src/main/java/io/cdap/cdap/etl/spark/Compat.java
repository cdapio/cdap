/*
 * Copyright © 2017-2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark;

/**
 * Utility class to handle incompatibilities between Spark2 and Spark3. All hydrator-spark-core modules must have this
 * class with the exact same method signatures. Incompatibilities are in a few places. Should not contain any
 * classes from Spark streaming.
 *
 */
public final class Compat {
  public static final String SPARK_COMPAT = "spark2_2.11";

  private Compat() {

  }
}
