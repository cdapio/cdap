/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.api;

import java.util.Map;

/**
 * Basic Program Configurer.
 */
public interface ProgramConfigurer {

  /**
   * Sets the name of the program.
   *
   * @param name name
   */
  void setName(String name);

  /**
   * Sets the description of the program.
   *
   * @param description description
   */
  void setDescription(String description);

  /**
   * Sets a set of properties that will be available through the Program specification's getProperties() method
   * at runtime.
   *
   * @param properties the properties to set
   */
  void setProperties(Map<String, String> properties);
}
