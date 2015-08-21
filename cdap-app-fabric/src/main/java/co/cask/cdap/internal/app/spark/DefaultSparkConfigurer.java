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

package co.cask.cdap.internal.app.spark;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkConfigurer;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.internal.api.DefaultDatasetConfigurer;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.specification.PropertyFieldExtractor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.Map;

/**
 * Default implementation of {@link SparkConfigurer}.
 */
public final class DefaultSparkConfigurer extends DefaultDatasetConfigurer implements SparkConfigurer {

  private final Spark spark;
  private String name;
  private String description;
  private String mainClassName;
  private Map<String, String> properties;
  private Resources driverResources;
  private Resources executorResources;

  public DefaultSparkConfigurer(Spark spark) {
    this.spark = spark;
    this.name = spark.getClass().getSimpleName();
    this.description = "";
    this.properties = Collections.emptyMap();
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
    this.properties = ImmutableMap.copyOf(properties);
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
    // Grab all @Property fields
    Reflections.visit(spark, spark.getClass(), new PropertyFieldExtractor(properties));
    return new SparkSpecification(spark.getClass().getName(), name, description,
                                  mainClassName, properties, driverResources, executorResources);
  }
}
