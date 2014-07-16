/*
 * Copyright 2012-2014 Continuuity, Inc.
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
package com.continuuity.common.conf;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

/**
 * An async function to update property in {@link PropertyStore}.
 *
 * @param <T> Type of property to update
 */
public interface PropertyUpdater<T> extends AsyncFunction<T, T> {

  /**
   * Computes the updated copy of property asynchronously.
   *
   * @param property The existing property value or {@code null} if no existing value.
   * @return A future that will be completed and carries the new property value when it become available.
   */
  @Override
  ListenableFuture<T> apply(@Nullable T property) throws Exception;
}
