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

package co.cask.cdap.etl.tool;

import java.io.InputStream;
import java.util.Properties;

/**
 * Utility to get etl app version.
 */
public final class ETLVersion {
  private static String version;

  private ETLVersion() {
  }

  // reads current version from the etl.properties file contained in resources.
  public static String getVersion() {
    if (version != null) {
      return version;
    }
    Properties prop = new Properties();
    try {
      try (InputStream in = UpgradeTool.class.getResourceAsStream("/etl.properties")) {
        prop.load(in);
      }
      version = prop.getProperty("version");
      return version;
    } catch (Exception e) {
      throw new RuntimeException("Error determining version. Please check that the jar was built correctly.", e);
    }
  }

}
