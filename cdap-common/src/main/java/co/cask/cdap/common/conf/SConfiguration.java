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

/**
 * Used to create Configuration Object for security related properties
 */
public class SConfiguration extends Configuration {

  private SConfiguration() {
    // Shouldn't be used other than in this class.
  }

  /**
   * Creates an instance of {@link SConfiguration}.
   *
   * @return an instance of SConfiguration.
   */
  public static SConfiguration create() {
    // Create a new configuration instance, but do NOT initialize with
    // the Hadoop default properties.
    SConfiguration conf = new SConfiguration();
    conf.addResource("cdap-security.xml");
    return conf;
  }
}
