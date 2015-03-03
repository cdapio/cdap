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

  public static void checkCConfValidity(CConfiguration cConf) {
    // Checks to ensure that certain keys (e.g. "root.prefix") are valid as expected by CDAP.
    checkAlphaNumberic(cConf, Constants.CFG_ROOT_NAMESPACE);
    checkAlphaNumberic(cConf, Constants.Dataset.TABLE_PREFIX);
  }

  private static void checkAlphaNumberic(CConfiguration cConf, String key) {
    String value = cConf.get(key);
    Preconditions.checkNotNull(value, String.format("Entry of CConf with key: %s is null", key));
    Preconditions.checkArgument(value.matches("[a-zA-Z0-9]+"),
                                String.format("CConf entry with key: %s must consist " +
                                                "of only alphanumeric characters; it is: %s", key, value));
  }
}
