/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.spi.data.table.options;

import java.util.Optional;

/**
 * An interface representing an optional configuration that can be passed to table operations (such
 * as read, scan, or count) to modify their default behavior.
 */
public interface QueryOption {

  /**
   * Helper method to extract a specific type of {@link QueryOption} from a varargs array.
   *
   * @param clazz   the class of the option to extract
   * @param options the array of options to search through
   * @param <T>     the type of the option
   * @return an {@link Optional} containing the option if an instance of the specified class is
   * found, otherwise an empty {@link Optional}.
   */
  static <T extends QueryOption> Optional<T> getOption(Class<T> clazz, QueryOption... options) {
    if (options == null || clazz == null) {
      return Optional.empty();
    }

    for (QueryOption option : options) {
      if (clazz.isInstance(option)) {
        return Optional.of(clazz.cast(option));
      }
    }
    return Optional.empty();
  }
}
