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
package co.cask.cdap.api.workflow;

/**
 *
 */
public abstract class AbstractWorkflowAction implements WorkflowAction {

  private final String name;
  private WorkflowContext context;

  protected AbstractWorkflowAction() {
    name = getClass().getSimpleName();
  }

  protected AbstractWorkflowAction(String name) {
    this.name = name;
  }

  @Override
  public WorkflowActionSpecification configure() {
    return WorkflowActionSpecification.Builder.with()
      .setName(getName())
      .setDescription(getDescription())
      .build();
  }

  @Override
  public void initialize(WorkflowContext context) throws Exception {
    this.context = context;
  }

  @Override
  public void destroy() {
    // No-op
  }

  protected final WorkflowContext getContext() {
    return context;
  }

  /**
   * @return {@link Class#getSimpleName() Simple classname} of this {@link WorkflowAction}.
   */
  protected String getName() {
    return name;
  }

  /**
   * @return A descriptive message about this {@link WorkflowAction}.
   */
  protected String getDescription() {
    return String.format("WorkFlowAction of %s.", getName());
  }
}
