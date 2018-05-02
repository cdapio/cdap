/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.spark;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.runtime.spi.SparkCompat;

import java.util.ArrayList;
import java.util.List;

/**
 * Determines the SparkCompat version.
 */
public class SparkCompatReader {

  private SparkCompatReader() {
    // no-op for helper class
  }

  /**
   * Get the SparkCompat from the CConf or from the environment. Returns UNKNOWN if it is not defined in either place.
   * Throws an exception if the value is defined but invalid.
   */
  public static SparkCompat get(CConfiguration cConf) {
    // use the value in the system property (expected in distributed)
    // otherwise, check the conf for the spark compat version (expected in standalone and unit tests)
    String compatStr = System.getProperty(Constants.AppFabric.SPARK_COMPAT);
    compatStr = compatStr == null ? System.getenv(Constants.SPARK_COMPAT_ENV) : compatStr;
    compatStr = compatStr == null ? cConf.get(Constants.AppFabric.SPARK_COMPAT) : compatStr;

    for (SparkCompat sparkCompat : SparkCompat.values()) {
      if (sparkCompat.getCompat().equals(compatStr)) {
        return sparkCompat;
      }
    }

    List<String> allowedCompatStrings = new ArrayList<>();
    for (SparkCompat sparkCompat : SparkCompat.values()) {
      allowedCompatStrings.add(sparkCompat.getCompat());
    }

    throw new IllegalArgumentException(
      String.format("Invalid SparkCompat version '%s'. Must be one of %s", compatStr, allowedCompatStrings));
  }
}
