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

package co.cask.cdap.common.discovery;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;

import java.util.concurrent.TimeUnit;

/**
 * An abstract {@link EndpointStrategy} that helps implementation of any strategy.
 * It provides a default implementation for the {@link #pick(long, TimeUnit)} method.
 */
public abstract class AbstractEndpointStrategy implements EndpointStrategy {

  protected final ServiceDiscovered serviceDiscovered;

  /**
   * Constructs an instance with the given {@link ServiceDiscovered}.
   */
  protected AbstractEndpointStrategy(ServiceDiscovered serviceDiscovered) {
    this.serviceDiscovered = serviceDiscovered;
  }

  @Override
  public Discoverable pick(long timeout, TimeUnit timeoutUnit) {
    Discoverable discoverable = pick();
    if (discoverable != null) {
      return discoverable;
    }
    final SettableFuture<Discoverable> future = SettableFuture.create();
    Cancellable cancellable = serviceDiscovered.watchChanges(new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        // The serviceDiscovered provided is the same as the one in the field, hence ok to just call pick().
        Discoverable discoverable = pick();
        if (discoverable != null) {
          future.set(discoverable);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    try {
      return future.get(timeout, timeoutUnit);
    } catch (Exception e) {
      return null;
    } finally {
      cancellable.cancel();
    }
  }
}
