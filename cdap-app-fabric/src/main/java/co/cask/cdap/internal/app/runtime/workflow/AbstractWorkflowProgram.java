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

package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.internal.app.program.ForwardingProgram;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;

/**
 * A Abstract Forwarding Program for {@link Workflow}.
 * Programs which wants to run in {@link Workflow} should extend this to turn the Workflow
 * Program into a required program. For example see {@link WorkflowSparkProgram} or {@link WorkflowMapReduceProgram}
 */
public abstract class AbstractWorkflowProgram extends ForwardingProgram {
  private final ProgramSpecification programSpecification;

  public AbstractWorkflowProgram(Program delegate, ProgramSpecification programSpecification) {
    super(delegate);
    this.programSpecification = programSpecification;
  }

  @Override
  public String getMainClassName() {
    return programSpecification.getClassName();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Class<T> getMainClass() throws ClassNotFoundException {
    return (Class<T>) Class.forName(getMainClassName(), true, getClassLoader());
  }

  @Override
  public abstract ProgramType getType();

  @Override
  public Id.Program getId() {
    return Id.Program.from(getNamespaceId(), getApplicationId(), getType(), getName());
  }

  @Override
  public String getName() {
    return programSpecification.getName();
  }

  @Override
  public abstract ApplicationSpecification getApplicationSpecification();

  /**
   * @return {@link ApplicationSpecification} from the {@link ForwardingProgram}
   */
  ApplicationSpecification getForwardingProgramSpecification() {
    return super.getApplicationSpecification();
  }
}
