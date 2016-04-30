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

import co.cask.cdap.api.ServiceDiscoverer;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URL;
import javax.annotation.Nullable;

/**
 * A {@link Externalizable} implementation of {@link ServiceDiscoverer} used in Spark program execution.
 * It has no-op for serialize/deserialize operation, with all operations delegated to the {@link SparkRuntimeContext}
 * of the current execution context.
 */
public final class SparkServiceDiscoverer implements ServiceDiscoverer, Externalizable {

  private final ServiceDiscoverer delegate;

  /**
   * Constructor. It delegates service discovery to the current {@link SparkRuntimeContext}.
   */
  public SparkServiceDiscoverer() {
    this(SparkRuntimeContextProvider.get());
  }

  /**
   * Creates an instance that delegates all service discovery operations to the give {@link ServiceDiscoverer} delegate.
   */
  SparkServiceDiscoverer(ServiceDiscoverer delegate) {
    this.delegate = delegate;
  }

  @Nullable
  @Override
  public URL getServiceURL(String applicationId, String serviceId) {
    return delegate.getServiceURL(applicationId, serviceId);
  }

  @Nullable
  @Override
  public URL getServiceURL(String serviceId) {
    return delegate.getServiceURL(serviceId);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // no-op
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // no-op
  }
}
