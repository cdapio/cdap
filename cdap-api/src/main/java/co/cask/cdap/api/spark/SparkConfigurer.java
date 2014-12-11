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

package co.cask.cdap.api.spark;

import java.util.Map;

/**
 * Configurer for configuring {@link Spark}.
 */
public interface SparkConfigurer {
  /**
   * Sets the name of the {@link Spark}.
   */
  void setName(String name);

  /**
   * Sets the description of the {@link Spark}.
   */
  void setDescription(String description);

  /**
   * Sets the Spark job main class name in specification. The main method of this class will be called to run the
   * Spark job
   *
   * @param className the fully qualified name of class containing the main method
   */
  void setMainClassName(String className);

  /**
   * Sets a set of properties that will be available through the {@link SparkSpecification#getProperties()}
   * at runtime.
   *
   * @param properties the properties to set
   */
  void setProperties(Map<String, String> properties);
}
