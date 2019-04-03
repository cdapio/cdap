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

package co.cask.cdap.data2.transaction.coprocessor;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Service;

/**
 * Provides ability to get and release obejcts
 *
 * @param <T> type of the object supplied
 */
public interface CacheSupplier<T extends Service> extends Supplier<T> {

  /**
   * @return Get an instance of T and if it is the first call, then the service will be started. Subsequent calls will
   * get a reference to the same instance
   */
  @Override
  T get();

  /**
   * Release the object obtained through {code Supplier#get()}. If this is last release call, then the service will
   * be stopped.
   */
  void release();
}
