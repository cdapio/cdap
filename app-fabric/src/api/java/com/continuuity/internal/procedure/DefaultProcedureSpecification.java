package com.continuuity.internal.procedure;

import com.continuuity.api.ResourceSpecification;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.internal.lang.Reflections;
import com.continuuity.internal.specification.DataSetFieldExtractor;
import com.continuuity.internal.specification.PropertyFieldExtractor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class DefaultProcedureSpecification implements ProcedureSpecification {

  private final String className;
  private final String name;
  private final String description;
  private final Set<String> dataSets;
  private final Map<String, String> properties;
  private final ResourceSpecification resources;

  public DefaultProcedureSpecification(String name, String description,
                                       Set<String> dataSets, Map<String, String> properties,
                                       ResourceSpecification resources) {
    this(null, name, description, dataSets, properties, resources);
  }

  public DefaultProcedureSpecification(Procedure procedure) {
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
  }

  public DefaultProcedureSpecification(String className, String name, String description,
                                       Set<String> dataSets, Map<String, String> properties,
                                       ResourceSpecification resources) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.dataSets = ImmutableSet.copyOf(dataSets);
    this.properties = properties == null ? ImmutableMap.<String, String>of() : ImmutableMap.copyOf(properties);
    this.resources = resources;
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
  public ResourceSpecification getResources() {
    return resources;
  }
}
