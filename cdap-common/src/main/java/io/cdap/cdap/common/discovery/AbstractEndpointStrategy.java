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

package io.cdap.cdap.common.discovery;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * An abstract {@link EndpointStrategy} that helps implementation of any strategy.
 * It provides a default implementation for the {@link #pick(long, TimeUnit)} method.
 */
public abstract class AbstractEndpointStrategy implements EndpointStrategy {

  private final Supplier<ServiceDiscovered> serviceDiscoveredSupplier;

  /**
   * Constructs an instance with the given {@link ServiceDiscovered}.
   */
  protected AbstractEndpointStrategy(Supplier<ServiceDiscovered> serviceDiscoveredSupplier) {
    this.serviceDiscoveredSupplier = serviceDiscoveredSupplier;
  }

  @Nullable
  @Override
  public final Discoverable pick() {
    return pick(serviceDiscoveredSupplier.get());
  }

  @Override
  public final Discoverable pick(long timeout, TimeUnit timeoutUnit) {
    Discoverable discoverable = pick();
    if (discoverable != null) {
      return discoverable;
    }
    final SettableFuture<Discoverable> future = SettableFuture.create();
    Cancellable cancellable = serviceDiscoveredSupplier.get()
      .watchChanges(serviceDiscovered -> Optional.ofNullable(pick(serviceDiscovered)).ifPresent(future::set),
                    Threads.SAME_THREAD_EXECUTOR);
    try {
      return future.get(timeout, timeoutUnit);
    } catch (Exception e) {
      return null;
    } finally {
      cancellable.cancel();
    }
  }

  /**
   * Picks a {@link Discoverable} using its strategy.
   *
   * @param serviceDiscovered the {@link ServiceDiscovered} that contains the endpoint candidates
   * @return A {@link Discoverable} based on the strategy or {@code null} if no endpoint can be found.
   */
  @Nullable
  protected abstract Discoverable pick(ServiceDiscovered serviceDiscovered);
}
