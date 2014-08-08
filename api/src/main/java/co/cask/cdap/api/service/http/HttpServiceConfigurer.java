/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.api.service.http;

import java.util.Map;

/**
 * Interface which should be implemented to configure a {@link HttpServiceHandler}
 */
public interface HttpServiceConfigurer {
  /**
   * Sets the HTTP Service's name.
   *
   * @param name the HTTP Service name
   */
  void setName(String name);

  /**
   * Sets the HTTP Service's description.
   *
   * @param description the HTTP Service description
   */
  void setDescription(String description);

  /**
   * Sets the runtime arguments.
   *
   * @param arguments the HTTP Service runtime arguments
   */
  void setArguments(Map<String, String> arguments);
}
