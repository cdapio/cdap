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

package io.cdap.cdap.internal.app.spark;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.runtime.spi.SparkCompat;
import org.apache.spark.package$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Determines the SparkCompat version.
 */
public class SparkCompatReader {
  private static final Logger LOG = LoggerFactory.getLogger(SparkCompatReader.class);

  private SparkCompatReader() {
    // no-op for helper class
  }

  /**
   * Read {@link SparkCompat} from the system properties, environment, or the {@link CConfiguration}.
   * Tries to detect or falls back to {@link SparkCompat#SPARK2_2_11} if it is not defined in any place.
   *
   * @param cConf the {@link CConfiguration} for CDAP
   * @return the configured {@link SparkCompat}
   * @throws IllegalArgumentException if SparkCompat was set to an invalid value
   */
  public static SparkCompat get(CConfiguration cConf) {
    // use the value in the system property (expected in distributed)
    // otherwise, check the conf for the spark compat version (expected in standalone and unit tests)
    String compatStr = System.getProperty(Constants.AppFabric.SPARK_COMPAT);
    compatStr = compatStr == null ? System.getenv(Constants.SPARK_COMPAT_ENV) : compatStr;
    compatStr = compatStr == null ? cConf.get(Constants.AppFabric.SPARK_COMPAT) : compatStr;

    if (compatStr == null) {
      try {
        return getCurrentSparkCompat();
      } catch (Throwable e) {
        LOG.info("Can't detect spark version({}), using default {}", e, SparkCompat.SPARK2_2_11);
        return SparkCompat.SPARK2_2_11;
      }
    }

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

  private static SparkCompat getCurrentSparkCompat() {
    String sparkVersion = package$.MODULE$.SPARK_VERSION();
    switch (sparkVersion.charAt(0)) {
      case '3':
        return SparkCompat.SPARK3_2_12;
      case '2':
        return SparkCompat.SPARK2_2_11;
      default:
        throw new IllegalStateException("Spark version " + sparkVersion + " is not known");
    }
  }
}
