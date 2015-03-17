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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableList;
import org.apache.twill.filesystem.Location;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 *
 */
public class ApplicationWithPrograms implements Closeable {
  private final Id.Application id;
  private final ApplicationSpecification specification;
  private final ApplicationSpecification existingAppSpecification;
  private final Location location;
  private final ApplicationDeployable applicationDeployable;
  private final List<Program> programs;

  public ApplicationWithPrograms(ApplicationDeployable applicationDeployable, Iterable<? extends Program> programs) {
    this.id = applicationDeployable.getId();
    this.specification = applicationDeployable.getSpecification();
    this.existingAppSpecification = applicationDeployable.getExistingAppSpec();
    this.location = applicationDeployable.getLocation();
    this.applicationDeployable = applicationDeployable;
    this.programs = ImmutableList.copyOf(programs);
  }

  public ApplicationWithPrograms(ApplicationWithPrograms other) {
    this.id = other.id;
    this.specification = other.specification;
    this.existingAppSpecification = other.existingAppSpecification;
    this.location = other.location;
    this.applicationDeployable = other.applicationDeployable;
    this.programs = other.programs;
  }

  public Id.Application getId() {
    return id;
  }

  public ApplicationSpecification getSpecification() {
    return specification;
  }

  @Nullable
  public ApplicationSpecification getExistingAppSpecification() {
    return existingAppSpecification;
  }

  public Location getLocation() {
    return location;
  }

  public Iterable<Program> getPrograms() {
    return programs;
  }

  @Override
  public void close() throws IOException {
    applicationDeployable.close();
  }
}
