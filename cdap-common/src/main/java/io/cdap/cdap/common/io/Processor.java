/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.common.io;

/**
 * Process input values.
 *
 * {@link #process} will be called for each input, and should return {@code false} when the processing should stop.
 *
 * @param <T> type of the input values
 * @param <R> type of the result value
 */
public interface Processor<T, R> {

  /**
   * Process one input value.
   *
   * @param input input value to process
   * @return true to continue processing, false to stop
   */
  boolean process(T input);

  /**
   * @return the result of processing all the inputs
   */
  R getResult();
}
