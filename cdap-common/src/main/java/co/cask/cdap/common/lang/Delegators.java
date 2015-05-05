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

package co.cask.cdap.common.lang;

/**
 * Helper class to deal with {@link Delegator}.
 */
public final class Delegators {

  /**
   * Returns the root delegate object that is not a {@link Delegator} in the delegation chain
   * that is assignable to the given type.
   */
  @SuppressWarnings("unchecked")
  public static <T, V> T getDelegate(T object, Class<V> type) {
    T result = object;

    // Keep searching while the result is still instance of Delegator
    while (result != null && result instanceof Delegator) {
      result = ((Delegator<T>) result).getDelegate();

      // Terminate the search if result is not a Delegator and assignable to the given type
      if (result != null && !(result instanceof Delegator) && type.isAssignableFrom(result.getClass())) {
        break;
      }
    }

    if (result == null || !type.isAssignableFrom(result.getClass())) {
      throw new IllegalArgumentException("No object in the delegation chain is of type " + type);
    }
    return result;
  }

  private Delegators() {
  }
}
