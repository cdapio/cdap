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

import org.apache.twill.api.ResourceSpecification;

/**
 * Forwarding ResourceSpecification implementation.
 */
public abstract class ForwardingResourceSpecification implements ResourceSpecification {

  private final ResourceSpecification delegate;

  protected ForwardingResourceSpecification(ResourceSpecification specification) {
    this.delegate = specification;
  }

  @Override
  public int getCores() {
    return delegate.getCores();
  }

  @Override
  public int getVirtualCores() {
    return delegate.getVirtualCores();
  }

  @Override
  public int getMemorySize() {
    return delegate.getMemorySize();
  }

  @Override
  public int getUplink() {
    return delegate.getUplink();
  }

  @Override
  public int getDownlink() {
    return delegate.getDownlink();
  }

  @Override
  public int getInstances() {
    return delegate.getInstances();
  }
}
