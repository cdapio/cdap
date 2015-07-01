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

package co.cask.cdap.api.flow.flowlet;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.dataset.Dataset;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This abstract class provides a default implementation of {@link Flowlet} methods for easy extension.
 * It uses the result of {@link #getName()} as the Flowlet name and the result of
 * {@link #getDescription()} as the Flowlet description. By default, the {@link Class#getSimpleName()}
 * is used as the Flowlet name.
 * <p>
 *   Child classes can override the {@link #getName()} and/or {@link #getDescription()}
 *   methods to specify custom names. Children can also override the {@link #configure()} method
 *   for more control over customizing the {@link FlowletSpecification}.
 * </p>
 */
public abstract class AbstractFlowlet implements Flowlet, Callback {

  private final String name;
  private FlowletConfigurer configurer;
  private FlowletContext flowletContext;

  public void configure(FlowletConfigurer configurer) {
    this.configurer = configurer;
    FlowletSpecification specification = configure();
    configurer.setName(specification.getName());
    configurer.setDescription(specification.getDescription());
    configurer.setFailurePolicy(specification.getFailurePolicy());
    configurer.setProperties(specification.getProperties());
    configurer.setResources(specification.getResources());
    configurer.useDatasets(specification.getDataSets());
  }

  /**
   * Returns the {@link FlowletConfigurer} used for configuration. Only available during configuration time.
   */
  protected final FlowletConfigurer getConfigurer() {
    return configurer;
  }

  /**
   * Sets the name of the {@link Flowlet}.
   *
   * @param name the name of the flowlet
   */
  protected void setName(String name) {
    configurer.setName(name);
  }

  /**
   * Sets the description of the {@link Flowlet}.
   *
   * @param description the description of the flowlet
   */
  protected void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   * Sets the resources requirements of the {@link Flowlet}.
   *
   * @param resources {@link Resources} requirements
   */
  protected void setResources(Resources resources) {
    configurer.setResources(resources);
  }

  /**
   * Sets the failure policy of the {@link Flowlet}.
   *
   * @param failurePolicy {@link FailurePolicy}
   */
  protected void setFailurePolicy(FailurePolicy failurePolicy) {
    configurer.setFailurePolicy(failurePolicy);
  }

  /**
   * Sets a set of properties that will be available through the {@link FlowletSpecification#getProperties()}.
   *
   * @param properties the properties to set
   */
  protected void setProperties(Map<String, String> properties) {
    configurer.setProperties(properties);
  }

  /**
   * Adds the names of {@link Dataset}s used by the Flowlet.
   *
   * @param dataset dataset name
   * @param datasets more dataset names
   */
  protected void useDatasets(String dataset, String...datasets) {
    List<String> datasetList = new ArrayList<>();
    datasetList.add(dataset);
    datasetList.addAll(Arrays.asList(datasets));
    useDatasets(datasetList);
  }

  /**
   * Adds the names of {@link Dataset}s used by the Flowlet.
   *
   * @param datasets dataset names
   */
  protected void useDatasets(Iterable<String> datasets) {
    configurer.useDatasets(datasets);
  }

  /**
   * Default constructor that uses {@link #getClass()}.{@link Class#getSimpleName() getSimpleName} as the
   * flowlet name.
   * @deprecated not required if you are using {@link AbstractFlowlet#configure} method.
   */
  @Deprecated
  protected AbstractFlowlet() {
    this.name = getClass().getSimpleName();
  }

  /**
   * Constructor that uses the specified name as the flowlet name.
   * @param name Name of the flowlet
   * @deprecated Use {@link AbstractFlowlet#setName} instead.
   */
  @Deprecated
  protected AbstractFlowlet(String name) {
    this.name = name;
  }

  @Deprecated
  @Override
  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with().setName(getName())
      .setDescription(getDescription()).build();
  }

  @Override
  public void initialize(FlowletContext context) throws Exception {
    this.flowletContext = context;
  }

  @Override
  public void destroy() {
    // Nothing to do.
  }

  @Override
  public void onSuccess(@Nullable Object input, @Nullable InputContext inputContext) {
    // No-op by default
  }

  @Override
  public FailurePolicy onFailure(@Nullable Object input, @Nullable InputContext inputContext, FailureReason reason) {
    // Return the policy as specified in the spec
    return flowletContext.getSpecification().getFailurePolicy();
  }

  /**
   * @return An instance of {@link FlowletContext} when this flowlet is running. Otherwise return
   *         {@code null} if it is not running or not yet initialized by the runtime environment.
   */
  protected final FlowletContext getContext() {
    return flowletContext;
  }

  /**
   * @return {@link Class#getSimpleName() Simple classname} of this {@link Flowlet}
   * @deprecated Use {@link AbstractFlowlet#setName} instead.
   */
  @Deprecated
  protected String getName() {
    return name;
  }

  /**
   * @return A descriptive message about this {@link Flowlet}.
   * @deprecated Use {@link AbstractFlowlet#setDescription}
   */
  @Deprecated
  protected String getDescription() {
    return String.format("Flowlet of %s.", getName());
  }
}
