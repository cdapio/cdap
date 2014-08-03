/*
 * Copyright 2014 Cask, Inc.
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

/**
 * Defines program lifecycle.
 * @param <T> type of the program runtime context
 */
public interface ProgramLifecycle<T extends RuntimeContext> {
  /**
   *  Initializes a Program.
   *  <p>
   *    This method will be called only once per {@link co.cask.cdap.api.ProgramLifecycle} instance.
   *  </p>
   *  @param context An instance of {@link RuntimeContext}
   *  @throws Exception If there is any error during initialization.
   */
  void initialize(T context) throws Exception;

  /**
   * Destroy is the last thing that gets called before the program is
   * shutdown. So, if there are any cleanups then they can be specified here.
   */
  void destroy();
}
