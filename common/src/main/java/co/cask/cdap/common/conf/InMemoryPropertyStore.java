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
package co.cask.cdap.common.conf;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * An in memory implementation of {@link PropertyStore}.
 *
 * @param <T> Type of property
 */
public final class InMemoryPropertyStore<T> extends AbstractPropertyStore<T> {

  @Override
  protected boolean listenerAdded(String name) {
    return true;
  }

  @Override
  public synchronized ListenableFuture<T> update(String name, PropertyUpdater<T> updater) {
    try {
      T property = updater.apply(getCached(name)).get();
      updateAndNotify(name, property);
      return Futures.immediateFuture(property);
    } catch (Exception e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  @Override
  public synchronized ListenableFuture<T> set(String name, T property) {
    updateAndNotify(name, property);
    return Futures.immediateFuture(property);
  }
}
