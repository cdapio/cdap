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

package co.cask.cdap.templates.etl.api;

import java.util.List;

/**
 * Configure a Stage in the ETL Pipeline.
 */
public interface StageConfigurer {

  /**
   * Set the name of the Stage.
   *
   * @param name name
   */
  void setName(String name);

  /**
   * Set the description of the Stage.
   *
   * @param description description
   */
  void setDescription(String description);

  /**
   * Add properties this stage requires during runtime.
   *
   * @param properties {@link List} of {@link Property}
   */
  void addProperties(List<Property> properties);

  /**
   * Add a property this stage requires during runtime.
   *
   * @param property {@link Property}
   */
  void addProperty(Property property);
}
