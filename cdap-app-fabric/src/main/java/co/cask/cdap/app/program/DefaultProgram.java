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
package co.cask.cdap.app.program;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import org.apache.twill.filesystem.Location;

import java.io.Closeable;
import java.io.IOException;

/**
 * Default implementation of {@link Program}.
 */
public class DefaultProgram implements Program {

  private final ProgramDescriptor programDescriptor;
  private final Location jarLocation;
  private final ClassLoader classLoader;

  public DefaultProgram(ProgramDescriptor programDescriptor, Location jarLocation, ClassLoader classLoader) {
    this.programDescriptor = programDescriptor;
    this.jarLocation = jarLocation;
    this.classLoader = classLoader;
  }

  @Override
  public String getMainClassName() {
    return programDescriptor.getSpecification().getClassName();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Class<T> getMainClass() throws ClassNotFoundException {
    return (Class<T>) getClassLoader().loadClass(getMainClassName());
  }

  @Override
  public ProgramType getType() {
    return programDescriptor.getProgramId().getType();
  }

  @Override
  public Id.Program getId() {
    return programDescriptor.getProgramId().toId();
  }

  @Override
  public String getName() {
    return programDescriptor.getProgramId().getProgram();
  }

  @Override
  public String getNamespaceId() {
    return programDescriptor.getProgramId().getNamespace();
  }

  @Override
  public String getApplicationId() {
    return programDescriptor.getProgramId().getApplication();
  }

  @Override
  public ApplicationSpecification getApplicationSpecification() {
    return programDescriptor.getApplicationSpecification();
  }

  @Override
  public Location getJarLocation() {
    return jarLocation;
  }

  @Override
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  @Override
  public void close() throws IOException {
    if (classLoader instanceof Closeable) {
      ((Closeable) classLoader).close();
    }
  }
}
