/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>Annotate a plugin runtime implementation that can be selected dynamically based on environment.
 * If this annotation is used, main plugin class should still implement any construction / deploy time
 * interfaces like {@link io.cdap.cdap.etl.api.PipelineConfigurable}, but during runtime first implementation
 * that is suitable for the environment out of the {@link #value()} will be used. If given implementation
 * does not work in the environment it should detect it in constructor and throw an {@link IllegalStateException}
 * to indicate it. This exception along with some other that are often produced by unsuitable environment
 * (e.g. {@link ClassNotFoundException}, {@link LinkageError}) will only be logged on debug level. Any other
 * exception will be logged as warning and other implementations will still be considered.</p>
 * <p>You can still provide default implementation in the primary plugin class, but in this case it must be
 * included in this annotation {@link #value()}. This would help to achieve backwards compatibility and
 * indicate proper runtime implementation order.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RuntimeImplementation {
  /**
   * @return main plugin class this runtime implementation belongs to
   */
  Class<?> pluginClass();

  /**
   * @return order in which implementations should be considered
   */
  int order();
}
