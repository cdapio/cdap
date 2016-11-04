/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.customaction.AbstractCustomAction;

import java.util.Map;

/**
 * This abstract class provides a default implementation of {@link WorkflowAction} methods for easy extensions.
 * @deprecated Deprecated as of 3.5.0. Please use {@link AbstractCustomAction} instead.
 */
@Deprecated
public abstract class AbstractWorkflowAction implements WorkflowAction {

  private final String name;
  private WorkflowActionConfigurer configurer;
  private WorkflowContext context;

  protected AbstractWorkflowAction() {
    name = getClass().getSimpleName();
  }

  protected AbstractWorkflowAction(String name) {
    this.name = name;
  }

  @Override
  public void configure(WorkflowActionConfigurer configurer) {
    this.configurer = configurer;
    setName(name);
    configure();
  }

  /**
   * Configure the {@link WorkflowAction}.
   */
  protected void configure() {

  }

  protected void setName(String name) {
    configurer.setName(name);
  }

  protected void setDescription(String description) {
    configurer.setDescription(description);
  }

  protected void setProperties(Map<String, String> properties) {
    configurer.setProperties(properties);
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
}
