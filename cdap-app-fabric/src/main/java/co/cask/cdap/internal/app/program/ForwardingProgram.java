/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.internal.app.program;

import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import org.apache.twill.filesystem.Location;

/**
 * A delegation of {@link Program} interface.
 */
public abstract class ForwardingProgram implements Program {

  protected final Program delegate;

  protected ForwardingProgram(Program delegate) {
    this.delegate = delegate;
  }

  @Override
  public String getMainClassName() {
    return delegate.getMainClassName();
  }

  @Override
  public <T> Class<T> getMainClass() throws ClassNotFoundException {
    return delegate.getMainClass();
  }

  @Override
  public ProgramType getType() {
    return delegate.getType();
  }

  @Override
  public Id.Program getId() {
    return delegate.getId();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public String getNamespaceId() {
    return delegate.getNamespaceId();
  }

  @Override
  public String getApplicationId() {
    return delegate.getApplicationId();
  }

  @Override
  public ApplicationSpecification getSpecification() {
    return delegate.getSpecification();
  }

  @Override
  public Location getJarLocation() {
    return delegate.getJarLocation();
  }

  @Override
  public ClassLoader getClassLoader() {
    return delegate.getClassLoader();
  }
}
