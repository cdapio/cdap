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

package co.cask.cdap.api.workflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * This abstract class provides a default implementation of {@link WorkflowAction} methods for easy extensions.
 */
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

  public void configure(WorkflowActionConfigurer configurer) {
    this.configurer = configurer;
    WorkflowActionSpecification specification = configure();
    configurer.setName(specification.getName());
    configurer.setDescription(specification.getDescription());
    configurer.setProperties(specification.getProperties());
    configurer.useDatasets(specification.getDatasets());
  }

  @Deprecated
  @Override
  public WorkflowActionSpecification configure() {
    return WorkflowActionSpecification.Builder.with()
      .setName(getName())
      .setDescription(getDescription())
      .build();
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

  protected void useDatasets(String dataset, String...datasets) {
    List<String> datasetList = new ArrayList<>();
    datasetList.add(dataset);
    datasetList.addAll(Arrays.asList(datasets));
    useDatasets(datasetList);
  }

  protected void useDatasets(Iterable<String> datasets) {
    configurer.useDatasets(datasets);
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
   * @deprecated Use {@link AbstractWorkflowAction#setName} instead
   */
  @Deprecated
  protected String getName() {
    return name;
  }

  /**
   * @return A descriptive message about this {@link WorkflowAction}.
   * @deprecated Use {@link AbstractWorkflowAction#setDescription} instead
   */
  @Deprecated
  protected String getDescription() {
    return String.format("WorkFlowAction of %s.", getName());
  }
}
