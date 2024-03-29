/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.common.guice;

import com.google.inject.Provider;
import java.util.function.Supplier;

/**
 * The class bridge a {@link Supplier} to Guice {@link Provider}.
 *
 * @param <T> type of the object provided by this {@link Provider}
 */
public final class SupplierProviderBridge<T> implements Provider<T> {

  private final Supplier<T> supplier;

  public SupplierProviderBridge(Supplier<T> supplier) {
    this.supplier = supplier;
  }

  @Override
  public T get() {
    return supplier.get();
  }
}
