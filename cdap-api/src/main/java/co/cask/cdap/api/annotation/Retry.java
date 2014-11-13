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

package co.cask.cdap.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation tag that can be applied to the {@link co.cask.cdap.api.flow.flowlet.Callback#onChangeInstances} method
 * in a flowlet definition.
 *
 * <p>
 * Example:
 * </p>
 *
 * <pre>
 * <code>
 * {@literal @}Retry(5)
 * public void onChangeInstances(FlowletContext flowletContext) {
 *   ...
 * }
 * </code>
 * </pre>
 *
 * @see co.cask.cdap.api.flow.flowlet.Flowlet
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Retry {

  /**
   * Specifies the maximum number of retries of failure callbacks before giving up.
   */
  int value();
}
