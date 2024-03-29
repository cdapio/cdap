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
package io.cdap.cdap.common.conf;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import javax.annotation.Nullable;

/**
 * A {@link io.cdap.cdap.common.conf.PropertyUpdater} that computes property value synchronously.
 *
 * @param <T> Type of property value
 */
public abstract class SyncPropertyUpdater<T> implements PropertyUpdater<T> {

  @Override
  public final ListenableFuture<T> apply(@Nullable T property) throws Exception {
    return Futures.immediateFuture(compute(property));
  }

  protected abstract T compute(@Nullable T property);
}
