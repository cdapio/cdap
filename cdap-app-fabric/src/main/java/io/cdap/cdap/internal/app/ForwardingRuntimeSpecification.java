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

import java.util.Collection;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillRunnableSpecification;

/**
 * ForwardingRuntimeSpecification implementation.
 */
public abstract class ForwardingRuntimeSpecification implements RuntimeSpecification {

  private final RuntimeSpecification delegate;

  protected ForwardingRuntimeSpecification(RuntimeSpecification delegate) {
    this.delegate = delegate;
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public TwillRunnableSpecification getRunnableSpecification() {
    return delegate.getRunnableSpecification();
  }

  @Override
  public ResourceSpecification getResourceSpecification() {
    return delegate.getResourceSpecification();
  }

  @Override
  public Collection<LocalFile> getLocalFiles() {
    return delegate.getLocalFiles();
  }
}
