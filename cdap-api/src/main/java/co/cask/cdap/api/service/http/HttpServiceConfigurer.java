/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.DatasetConfigurer;
import co.cask.cdap.api.plugin.PluginConfigurer;
import co.cask.cdap.api.retry.RetryPolicy;

import java.util.Map;

/**
 * Interface which should be implemented to configure a {@link HttpServiceHandler}
 */
public interface HttpServiceConfigurer extends DatasetConfigurer, PluginConfigurer {

  /**
   * Sets a set of properties that will be available through the {@link HttpServiceHandlerSpecification#getProperties()}
   * at runtime.
   *
   * @param properties the properties to set
   */
  void setProperties(Map<String, String> properties);

  /**
   * Sets the {@link RetryPolicy} to use when a retry-able remote failure is encountered.
   * If none is set, the configured CDAP default values will be used.
   * This policy can be overridden by preferences and runtime arguments.
   * Individual service endpoints can override this retry policy through the RetryPolicy annotation.
   *
   * @param retryPolicy the retry policy to set
   */
  void setRemoteRetryPolicy(RetryPolicy retryPolicy);
}
