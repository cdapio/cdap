/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * A {@link Externalizable} implementation of {@link SecureStore} used in Spark program execution.
 * It has no-op for serialize/deserialize operation, with all operations delegated to the {@link SparkRuntimeContext}
 * of the current execution context.
 */
public class SparkSecureStore implements SecureStore, Externalizable {

  private final SecureStore delegate;

  /**
   * Constructor. It delegates plugin context operations to the current {@link SparkRuntimeContext}.
   */
  public SparkSecureStore() {
    this(SparkRuntimeContextProvider.get());
  }

  /**
   * Creates an instance that delegates all plugin context operations to the give {@link SecureStore} delegate.
   */
  SparkSecureStore(SecureStore delegate) {
    this.delegate = delegate;
  }

  @Override
  public Map<String, String> listSecureData(String namespace) throws Exception {
    return delegate.listSecureData(namespace);
  }

  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws Exception {
    return delegate.getSecureData(namespace, name);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // np-op
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // no-op
  }
}
