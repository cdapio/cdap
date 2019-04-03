/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.messaging.store;

import com.google.common.base.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Filter messages read from {@link MessageTable}.
 *
 * @param <T>
 */
public abstract class MessageFilter<T> implements Function<T, MessageFilter.Result> {

  /**
   * The result of the filtering.
   */
  public enum Result {
    ACCEPT,
    SKIP,
    HOLD
  }

  @Nonnull
  @Override
  public abstract Result apply(@Nullable T input);

  /**
   * Creates a {@link MessageFilter} that always accept.
   *
   * @param <T> type of input to filter on
   * @return a {@link MessageFilter} that accepts on all input
   */
  public static <T> MessageFilter<T> alwaysAccept() {
    return new MessageFilter<T>() {
      @Override
      public Result apply(@Nullable T input) {
        return Result.ACCEPT;
      }
    };
  }
}
