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
 * <p>A Stage represents a set of tasks that can be performed on objects,
 * and methods used to communicate with other stages in a {@link Pipeline}.</p>
 */
public interface Stage {

  /**
   * Implementation of this method should atomically process a single data
   * object and transfer any objects generated to downstream for processing.
   *
   * @param ctx
   */
  void process(Context ctx) throws Exception;
}
