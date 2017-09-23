/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.coprocessor;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Reference counts the {@link Service} and manages the lifecycle of the {@link Service} instance.
 *
 * @param <T> type of {@link Service} that is reference counted
 */
public class ReferenceCountedSupplier<T extends Service> {
  private static final Log LOG = LogFactory.getLog(ReferenceCountedSupplier.class);

  private final AtomicReference<T> instance = new AtomicReference<>(null);
  private final AtomicInteger refCount = new AtomicInteger(0);
  private final Object lock = new Object();

  private final String instanceName;

  public ReferenceCountedSupplier(String instanceName) {
    this.instanceName = instanceName;
  }

  public T getOrCreate(Supplier<T> instanceSupplier) {
    synchronized (lock) {
      if (instance.get() == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Creating and starting Service %s.", instanceName));
        }

        // Instance has not been instantiated
        T serviceInstance = instanceSupplier.get();
        instance.set(serviceInstance);
        serviceInstance.startAndWait();
      }
      int newCount = refCount.incrementAndGet();
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Incrementing reference count for Service %s: %d", instanceName, newCount));
      }
      return instance.get();
    }
  }

  public void release() {
    synchronized (lock) {
      if (refCount.get() <= 0) {
        LOG.warn(String.format("Reference Count for Service %s is already zero.", instanceName));
        return;
      }

      int newCount = refCount.decrementAndGet();
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Decrementing reference count for Service %s: %d", instanceName, newCount));
      }

      if (newCount == 0) {
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Reference Count for Service is 0. Stopping Service %s.", instanceName));
          }

          Service serviceInstance = instance.get();
          serviceInstance.stopAndWait();
          instance.set(null);
        } catch (Exception ex) {
          LOG.warn(String.format("Exception while trying to stop Service %s.", instanceName), ex);
        }
      }
    }
  }
}
