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

package co.cask.cdap.internal.app;

import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.flow.FlowletDefinition;

import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class ForwardingFlowSpecification implements FlowSpecification {

  private final FlowSpecification delegate;

  protected ForwardingFlowSpecification(FlowSpecification delegate) {
    this.delegate = delegate;
  }

  @Override
  public String getClassName() {
    return delegate.getClassName();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public Map<String, FlowletDefinition> getFlowlets() {
    return delegate.getFlowlets();
  }

  @Override
  public List<FlowletConnection> getConnections() {
    return delegate.getConnections();
  }
}
