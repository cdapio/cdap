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

package co.cask.cdap.pipeline;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * This interface represents the context in which a stage is running.
 * Ordinarily the context will be provided by the pipeline in which
 * the stage is embedded.
 */
public interface Context {
  /**
   * Used when you a {@link Stage} wants to send data to the downstream stage
   *
   * @param o to be send to next stage.
   */
  void setDownStream(Object o);

  /**
   * @return Object passed through from the upstream
   */
  Object getUpStream();

  /**
   * @return Object being send to downstream
   */
  Object getDownStream();

  /**
   * Returns a property value associated with the given key that was set by {@link #setProperty(String, Object)} call.
   *
   * @param key the property key
   * @param <T> type of the value
   * @return the value or {@code null} if no such property.
   */
  @Nullable
  <T> T getProperty(String key);

  /**
   * Sets a value for the given property.
   *
   * @param key the property key
   * @param value the property value
   * @param <T> type of the value.
   */
  <T> void setProperty(String key, T value);

  /**
   * Returns the set of property keys in this context.
   */
  Set<String> getPropertyKeys();
}
