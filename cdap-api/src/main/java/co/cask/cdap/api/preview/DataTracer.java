/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.api.preview;

import co.cask.cdap.api.annotation.Beta;

/**
 * Interface used by the CDAP applications to trace the data.
 */
@Beta
public interface DataTracer {

  /**
   * Traces the data. Multiple values can be traced against the same property.
   *
   * @param propertyName the the name of the property
   * @param propertyValue the value associated with the property
   */
  void info(String propertyName, Object propertyValue);

  /**
   * Return the name of the tracer instance.
   */
  String getName();

  /**
   * Returns {@code true} if application is running in preview mode otherwise false is returned.
   */
  boolean isEnabled();
}
