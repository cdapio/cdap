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

package co.cask.cdap.internal.procedure;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.procedure.Procedure;
import co.cask.cdap.api.procedure.ProcedureSpecification;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.specification.DataSetFieldExtractor;
import co.cask.cdap.internal.specification.PropertyFieldExtractor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import java.util.Map;
import java.util.Set;

/**
 * @deprecated As of version 2.6.0, replaced by {@link co.cask.cdap.api.service.ServiceSpecification}
 */
@Deprecated
@SuppressWarnings("deprecation")
public final class DefaultProcedureSpecification implements ProcedureSpecification {

  private final String className;
  private final String name;
  private final String description;
  private final Set<String> dataSets;
  private final Map<String, String> properties;
  private final Resources resources;
  private final int instances;

  public DefaultProcedureSpecification(String name, String description,
                                       Set<String> dataSets, Map<String, String> properties,
                                       Resources resources) {
    this(null, name, description, dataSets, properties, resources);
  }

  public DefaultProcedureSpecification(Procedure procedure, int instances) {
    ProcedureSpecification configureSpec = procedure.configure();
    Set<String> dataSets = Sets.newHashSet(configureSpec.getDataSets());
    Map<String, String> properties = Maps.newHashMap(configureSpec.getProperties());

    Reflections.visit(procedure, TypeToken.of(procedure.getClass()),
                      new PropertyFieldExtractor(properties),
                      new DataSetFieldExtractor(dataSets));

    this.className = procedure.getClass().getName();
    this.name = configureSpec.getName();
    this.description = configureSpec.getDescription();
    this.dataSets = ImmutableSet.copyOf(dataSets);
    this.properties = ImmutableMap.copyOf(properties);
    this.resources = configureSpec.getResources();
    this.instances = instances;
  }

  public DefaultProcedureSpecification(String className, String name, String description,
                                       Set<String> dataSets, Map<String, String> properties,
                                       Resources resources) {
      this(className, name, description, dataSets, properties, resources, 1);
  }

  public DefaultProcedureSpecification(String className, String name, String description,
                                       Set<String> dataSets, Map<String, String> properties,
                                       Resources resources, int instances) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.dataSets = ImmutableSet.copyOf(dataSets);
    this.properties = properties == null ? ImmutableMap.<String, String>of() : ImmutableMap.copyOf(properties);
    this.resources = resources;
    this.instances = instances;
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
  public Set<String> getDataSets() {
    return dataSets;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }

  @Override
  public Resources getResources() {
    return resources;
  }

  @Override
  public int getInstances() {
    return instances;
  }
}
