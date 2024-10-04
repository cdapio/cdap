/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.api.exception;

import javax.annotation.Nullable;

/**
 * Interface for providing error details.
 *
 * <p>
 * Implementations of this interface can be used to provide more detailed error information
 * for exceptions that occur within the code using {@link ProgramFailureException}.
 * </p>
 */
public interface ErrorDetailsProvider<T> {

  /**
   * Returns a {@link RuntimeException} that wraps the given exception
   * with more detailed information.
   *
   * @param e the exception to wrap
   * @param conf configuration object
   * @return {@link RuntimeException} that wraps the given exception
   *         with more detailed information.
   */
  RuntimeException getExceptionDetails(Throwable e, @Nullable T conf);
}
