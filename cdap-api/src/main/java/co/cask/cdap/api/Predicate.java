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

package co.cask.cdap.api;

import javax.annotation.Nullable;

/**
 * Determines a true or false value for a given input.
 *
 * @param <T> the type of the input based on which the predicate is to be evaluated
 */
public interface Predicate<T> {
  /**
   * Returns the result of applying this predicate to {@code input}.
   * @param input the input based on which the predicate to be evaluated
   * @return the result of the evaluation of the predicate
   */
  boolean apply(@Nullable T input);
}
