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

package co.cask.cdap.api;

import java.net.URL;
import java.util.Map;

/**
 * This interface represents a context for a processor or elements of a processor.
 */
public interface RuntimeContext {
  /**
   * @return A map of argument key and value.
   */
  Map<String, String> getRuntimeArguments();

  /**
   * Used to discover services inside a given application.
   * @param applicationId Application name
   * @param serviceId Service name
   * @return URL
   */
  URL getServiceURL(String applicationId, String serviceId);

  /**
   * Omitting an applicationId assumes that the program wants to discover a service within its own application.
   * @param serviceId Service Name
   * @return URL
   */
  URL getServiceURL(String serviceId);

  /**
   * Stores a key/value property pair. It'll be persisted when the current transaction context commits.
   * @param key Key String
   * @param value Value String
   */
  void setProperty(String key, String value);

  /**
   * Returns the property value of the given key or null if not such property exists.
   * @param key Key String
   * @return Value String
   */
  String getProperty(String key);
}
