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

package co.cask.cdap.internal.flow;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.flow.flowlet.FailurePolicy;
import co.cask.cdap.api.flow.flowlet.Flowlet;
import co.cask.cdap.api.flow.flowlet.FlowletConfigurer;
import co.cask.cdap.api.flow.flowlet.FlowletSpecification;
import co.cask.cdap.internal.api.DefaultDatasetConfigurer;
import co.cask.cdap.internal.flowlet.DefaultFlowletSpecification;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.specification.DataSetFieldExtractor;
import co.cask.cdap.internal.specification.PropertyFieldExtractor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link FlowletConfigurer}.
 */
public class DefaultFlowletConfigurer extends DefaultDatasetConfigurer implements FlowletConfigurer {

  private final Flowlet flowlet;
  private String name;
  private String description;
  private Resources resources;
  private FailurePolicy failurePolicy;
  private Map<String, String> properties;
  private Set<String> datasets;

  public DefaultFlowletConfigurer(Flowlet flowlet) {
    this.flowlet = flowlet;
    this.name = flowlet.getClass().getSimpleName();
    this.failurePolicy = FailurePolicy.RETRY;
    this.description = "";
    this.resources = new Resources();
    this.properties = new HashMap<>();
    this.datasets = new HashSet<>();
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void setResources(Resources resources) {
    if (resources == null) {
      throw new IllegalArgumentException("Resources cannot be null.");
    }
    this.resources = resources;
  }

  @Override
  public void setFailurePolicy(FailurePolicy failurePolicy) {
    if (failurePolicy == null) {
      throw new IllegalArgumentException("FailurePolicy cannot be null");
    }
    this.failurePolicy = failurePolicy;
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    this.properties = new HashMap<>(properties);
  }

  @Override
  public void useDatasets(Iterable<String> datasets) {
    for (String dataset : datasets) {
      this.datasets.add(dataset);
    }
  }

  public FlowletSpecification createSpecification() {

    // Grab all @Property, @UseDataset fields
    Reflections.visit(flowlet, flowlet.getClass(),
                      new PropertyFieldExtractor(properties),
                      new DataSetFieldExtractor(datasets));

    return new DefaultFlowletSpecification(flowlet.getClass().getName(), name, description, failurePolicy,
                                           datasets, properties, resources);
  }
}
