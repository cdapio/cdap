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
}
