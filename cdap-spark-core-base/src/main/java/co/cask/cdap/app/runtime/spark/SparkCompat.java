/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;

/**
 * Spark compat versions
 */
public enum SparkCompat {
  SPARK1_2_10("spark1_2.10"),
  SPARK2_2_11("spark2_2.11"),
  UNKNOWN("unknown");

  private final String compat;

  SparkCompat(String compat) {
    this.compat = compat;
  }

  public String getCompat() {
    return compat;
  }

  /**
   * Get the SparkCompat from the CConf or from the environment. Returns UNKNOWN if it is not defined in either place.
   * Throws an exception if the value is defined but invalid.
   */
  public static SparkCompat get(CConfiguration cConf) {
    // use the value in the environment variable (expected in distributed)
    // otherwise, check the conf for the spark compat version (expected in standalone and unit tests)
    String compatStr = System.getenv(Constants.SPARK_COMPAT_ENV);
    compatStr = compatStr == null ? cConf.get(Constants.AppFabric.SPARK_COMPAT) : compatStr;

    if (SPARK1_2_10.compat.equals(compatStr)) {
      return SPARK1_2_10;
    } else if (SPARK2_2_11.compat.equals(compatStr)) {
      return SPARK2_2_11;
    } else {
      throw new IllegalArgumentException(String.format("Invalid SparkCompat version '%s'. Must be %s or %s",
                                                       compatStr, SPARK1_2_10.compat, SPARK2_2_11.compat));
    }
  }
}
