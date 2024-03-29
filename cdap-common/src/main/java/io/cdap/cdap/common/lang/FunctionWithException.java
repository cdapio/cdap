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

package io.cdap.cdap.common.lang;

import com.google.common.base.Function;
import javax.annotation.Nullable;

/**
 * A function interface similar to guava {@link Function}, but allows throwing exception from the
 * {@link #apply(Object)} method.
 *
 * @param <F> from type
 * @param <T> to type
 * @param <E> exception type
 */
public interface FunctionWithException<F, T, E extends Exception> {

  @Nullable
  T apply(@Nullable F input) throws E;
}
