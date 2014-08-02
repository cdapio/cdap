/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.api.service.http;

import java.util.Map;

/**
 *
 */
public interface HttpServiceConfigurer {
  /**
   * Sets the Http Service's name.
   *
   * @param name The Http Service name
   */
  void setName(String name);

  /**
   * Sets the Http Service's description.
   *
   * @param description The Http Service description
   */
  void setDescription(String description);

  /**
   * Set the runtime arguments
   * @param arguments
   */
  void setArguments(Map<String, String> arguments);
}
