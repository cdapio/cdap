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

package co.cask.cdap.api.service;

import org.apache.twill.api.ResourceSpecification;

import java.util.Map;

/**
 * Configures a {@link co.cask.cdap.api.service.ServiceWorker}
 */
public interface ServiceWorkerConfigurer {
  /**
   * Sets the ServiceWorker's name.
   */
  void setName(String name);

  /**
   * Sets the ServiceWorker's description.
   */
  void setDescription(String description);

  /**
   * Sets the ServiceWorker's ResourceSpecification.
   */
  void setResourceSpecification(ResourceSpecification resourceSpecification);

  /**
   * Sets the ServiceWorker's RunTimeArguments.
   */
  void setProperties(Map<String, String> properties);


}
