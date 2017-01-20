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

package co.cask.cdap.common.conf;

import com.google.common.base.Preconditions;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Utilities for {@link CConfiguration}.
 */
public class CConfigurationUtil extends Configuration {

  private CConfigurationUtil() { }

  public static void copyTxProperties(CConfiguration cConf, org.apache.hadoop.conf.Configuration destination) {
    Properties props = cConf.getProps();
    for (String property : props.stringPropertyNames()) {
      if (property.startsWith("data.tx") || property.startsWith("tx.persist")) {
        destination.set(property, cConf.get(property));
      }
    }
  }

  /**
   * Copies configurations prefixed with {@code twill.} to the given hadoop configuration.
   */
  public static void copyTwillProperties(CConfiguration cConf, org.apache.hadoop.conf.Configuration destination) {
    Properties props = cConf.getProps();
    for (String property : props.stringPropertyNames()) {
      if (property.startsWith("twill.")) {
        destination.set(property, cConf.get(property));
      }
    }
  }

  /**
   * Get extra jars set in {@link CConfiguration} as a list of {@link URI}.
   *
   * @param cConf {@link CConfiguration} containing the extra jars
   * @return a list of {@link File} created from the extra jars set in cConf.
   */
  public static List<URI> getExtraJars(CConfiguration cConf) {
    String[] extraJars = cConf.getStrings(Constants.AppFabric.PROGRAM_CONTAINER_DIST_JARS);
    List<URI> jarURIs = new ArrayList<>();
    if (extraJars == null) {
      return jarURIs;
    }
    for (String jarPath : extraJars) {
      URI uri;
      try {
        uri = URI.create(jarPath);
      } catch (IllegalArgumentException e) {
        // Assume that it is local path and try to create it with File
        uri = new File(jarPath).toURI();
      }
      if (uri.getScheme() == null) { // no scheme, assume it's local file
        uri = new File(jarPath).toURI();
      }
      jarURIs.add(uri);
    }
    return jarURIs;
  }

  /**
   * Asserts that the given CConfiguration has valid properties.
   * @param cConf the CConfiguration object to check
   * @throws IllegalArgumentException if the given cConf is invalid.
   */
  public static void verify(CConfiguration cConf) {
    // Checks to ensure that certain keys (e.g. "root.prefix") are valid as expected by CDAP.
    assertAlphanumeric(cConf, Constants.ROOT_NAMESPACE);
    assertAlphanumeric(cConf, Constants.Dataset.TABLE_PREFIX);
  }

  private static void assertAlphanumeric(CConfiguration cConf, String key) {
    String value = cConf.get(key);
    Preconditions.checkNotNull(value, "Entry of CConf with key: %s is null", key);
    Preconditions.checkArgument(value.matches("[a-zA-Z0-9]+"),
                                "CConf entry with key: %s must consist " +
                                  "of only alphanumeric characters; it is: %s", key, value);
  }
}
