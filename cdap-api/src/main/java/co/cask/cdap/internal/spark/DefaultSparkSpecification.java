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

package co.cask.cdap.internal.spark;

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.specification.PropertyFieldExtractor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * A default specification for {@link SparkSpecification}
 */
public final class DefaultSparkSpecification implements SparkSpecification {

  private final String className;
  private final String name;
  private final String description;
  private final String mainClassName;
  private final Map<String, String> properties;

  public DefaultSparkSpecification(String name, String description, String mainClassName,
                                   Map<String, String> properties) {
    this(null, name, description, mainClassName, properties);
  }

  public DefaultSparkSpecification(Spark spark) {
    SparkSpecification configureSpec = spark.configure();

    Map<String, String> properties = Maps.newHashMap(configureSpec.getProperties());

    Reflections.visit(spark, TypeToken.of(spark.getClass()),
                      new PropertyFieldExtractor(properties));

    this.className = spark.getClass().getName();
    this.name = configureSpec.getName();
    this.description = configureSpec.getDescription();
    this.mainClassName = configureSpec.getMainClassName();
    this.properties = ImmutableMap.copyOf(properties);
  }

  public DefaultSparkSpecification(String className, String name, String description, String mainClassName,
                                   @Nullable Map<String, String> properties) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.mainClassName = mainClassName;
    this.properties = properties == null ? ImmutableMap.<String, String>of() : ImmutableMap.copyOf(properties);
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public String getMainClassName() {
    return mainClassName;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }
}
