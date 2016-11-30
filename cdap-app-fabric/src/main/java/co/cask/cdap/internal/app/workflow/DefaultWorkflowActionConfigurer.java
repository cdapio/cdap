/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.workflow;

import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionConfigurer;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.internal.app.customaction.DefaultCustomActionConfigurer;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.specification.DataSetFieldExtractor;
import co.cask.cdap.internal.specification.PropertyFieldExtractor;
import co.cask.cdap.internal.workflow.DefaultWorkflowActionSpecification;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link WorkflowActionConfigurer}.
 * @deprecated Deprecated as of 3.5.0. Please use {@link DefaultCustomActionConfigurer} instead.
 */
@Deprecated
public class DefaultWorkflowActionConfigurer implements WorkflowActionConfigurer {

  private final WorkflowAction workflowAction;
  private String name;
  private String description;
  private Map<String, String> properties;
  private Set<String> datasets;

  private DefaultWorkflowActionConfigurer(WorkflowAction workflowAction) {
    this.workflowAction = workflowAction;
    this.name = workflowAction.getClass().getSimpleName();
    this.description = "";
    this.properties = new HashMap<>();
    this.datasets = new HashSet<>();
  }

  @Override
  public void setName(String name) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Name of the WorkflowAction cannot be null or empty");
    this.name = name;
  }

  @Override
  public void setDescription(String description) {
    Preconditions.checkNotNull(description, "Description of the WorkflowAction cannot be null");
    this.description = description;
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Properties of the WorkflowAction cannot be null");
    this.properties = new HashMap<>(properties);
  }
  
  private DefaultWorkflowActionSpecification createSpecification() {
    Reflections.visit(workflowAction, workflowAction.getClass(),
                      new PropertyFieldExtractor(properties),
                      new DataSetFieldExtractor(datasets));
    return new DefaultWorkflowActionSpecification(workflowAction.getClass().getName(), name,
                                                  description, properties, datasets);
  }

  public static WorkflowActionSpecification configureAction(WorkflowAction action) {
    DefaultWorkflowActionConfigurer configurer = new DefaultWorkflowActionConfigurer(action);
    action.configure(configurer);
    return configurer.createSpecification();
  }
}
