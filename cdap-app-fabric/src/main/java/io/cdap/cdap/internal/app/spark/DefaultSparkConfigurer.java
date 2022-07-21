/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.spark;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.spark.Spark;
import io.cdap.cdap.api.spark.SparkConfigurer;
import io.cdap.cdap.api.spark.SparkHttpServiceHandlerSpecification;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.AbstractConfigurer;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentRuntimeInfo;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.lang.Reflections;
import io.cdap.cdap.internal.specification.DataSetFieldExtractor;
import io.cdap.cdap.internal.specification.PropertyFieldExtractor;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link SparkConfigurer}.
 */
public class DefaultSparkConfigurer extends AbstractConfigurer implements SparkConfigurer {

  private final Spark spark;
  private String name;
  private String description;
  private String mainClassName;
  private Map<String, String> properties;
  private Resources clientResources;
  private Resources driverResources;
  private Resources executorResources;

  public DefaultSparkConfigurer(Spark spark, Id.Namespace deployNamespace, Id.Artifact artifactId,
                                PluginFinder pluginFinder, PluginInstantiator pluginInstantiator,
                                @Nullable AppDeploymentRuntimeInfo runtimeInfo,
                                FeatureFlagsProvider featureFlagsProvider) {
    super(deployNamespace, artifactId, pluginFinder, pluginInstantiator, runtimeInfo, featureFlagsProvider);
    this.spark = spark;
    this.name = spark.getClass().getSimpleName();
    this.description = "";
    this.properties = new HashMap<>();
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
  public void setMainClassName(String className) {
    this.mainClassName = className;
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Properties is null");
    this.properties = new HashMap<>(properties);
  }

  @Override
  public void setClientResources(Resources resources) {
    this.clientResources = resources;
  }

  @Override
  public void setDriverResources(Resources resources) {
    this.driverResources = resources;
  }

  @Override
  public void setExecutorResources(Resources resources) {
    this.executorResources = resources;
  }

  public SparkSpecification createSpecification() {
    Set<String> datasets = new HashSet<>();
    // Grab all @Property and @Dataset fields
    Reflections.visit(spark, spark.getClass(), new PropertyFieldExtractor(properties),
                      new DataSetFieldExtractor(datasets));

    return new SparkSpecification(spark.getClass().getName(), name, description,
                                  mainClassName, datasets, properties,
                                  clientResources, driverResources, executorResources, getHandlers(), getPlugins());
  }

  /**
   * Returns a list of {@link SparkHttpServiceHandlerSpecification} to be included.
   */
  protected List<SparkHttpServiceHandlerSpecification> getHandlers() {
    return Collections.emptyList();
  }
}
