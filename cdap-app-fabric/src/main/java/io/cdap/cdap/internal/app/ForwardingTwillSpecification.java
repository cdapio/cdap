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

package io.cdap.cdap.internal.app;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillSpecification;

/**
 * Forwarding TwillSpecification implementation.
 */
public abstract class ForwardingTwillSpecification implements TwillSpecification {

  private final TwillSpecification delegate;

  protected ForwardingTwillSpecification(TwillSpecification specification) {
    this.delegate = specification;
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public Map<String, RuntimeSpecification> getRunnables() {
    return delegate.getRunnables();
  }

  @Override
  public List<Order> getOrders() {
    return delegate.getOrders();
  }

  @Override
  public List<PlacementPolicy> getPlacementPolicies() {
    return delegate.getPlacementPolicies();
  }

  @Nullable
  @Override
  public EventHandlerSpecification getEventHandler() {
    return delegate.getEventHandler();
  }
}
