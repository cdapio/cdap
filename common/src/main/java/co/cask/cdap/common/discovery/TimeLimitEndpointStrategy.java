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
package co.cask.cdap.common.discovery;

import org.apache.twill.discovery.Discoverable;

import java.util.concurrent.TimeUnit;

/**
 * An {@link EndpointStrategy} that make sure it picks an endpoint within the given
 * timeout limit.
 */
public final class TimeLimitEndpointStrategy implements EndpointStrategy {

  private final EndpointStrategy delegate;
  private final long timeout;
  private final TimeUnit timeoutUnit;

  public TimeLimitEndpointStrategy(EndpointStrategy delegate, long timeout, TimeUnit timeoutUnit) {
    this.delegate = delegate;
    this.timeout = timeout;
    this.timeoutUnit = timeoutUnit;
  }

  @Override
  public Discoverable pick() {
    Discoverable pick = delegate.pick();
    try {
      long count = 0;
      while (pick == null && count++ < timeout) {
        timeoutUnit.sleep(1);
        pick = delegate.pick();
      }
    } catch (InterruptedException e) {
      // Simply propagate the interrupt.
      Thread.currentThread().interrupt();
    }
    return pick;
  }
}
