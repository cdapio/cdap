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
  private final Map<String, String> arguments;
  private final ResourceSpecification resources;

  public DefaultProcedureSpecification(String name, String description,
                                       Set<String> dataSets, Map<String, String> arguments,
                                       ResourceSpecification resources) {
    this(null, name, description, dataSets, arguments, resources);
  }

  public DefaultProcedureSpecification(Procedure procedure) {
    ProcedureSpecification configureSpec = procedure.configure();
    Set<String> dataSets = Sets.newHashSet(configureSpec.getDataSets());
    Map<String, String> properties = Maps.newHashMap();

    Reflections.visit(procedure, TypeToken.of(procedure.getClass()),
                      new PropertyFieldExtractor(properties),
                      new DataSetFieldExtractor(dataSets));
    properties.putAll(configureSpec.getArguments());

    this.className = procedure.getClass().getName();
    this.name = configureSpec.getName();
    this.description = configureSpec.getDescription();
    this.dataSets = ImmutableSet.copyOf(dataSets);
    this.arguments = ImmutableMap.copyOf(properties);
    this.resources = configureSpec.getResources();
  }

  public DefaultProcedureSpecification(String className, String name, String description,
                                       Set<String> dataSets, Map<String, String> arguments,
                                       ResourceSpecification resources) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.dataSets = ImmutableSet.copyOf(dataSets);
    this.arguments = arguments == null ? ImmutableMap.<String, String>of() : ImmutableMap.copyOf(arguments);
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
  public Map<String, String> getArguments() {
    return arguments;
  }

  @Override
  public ResourceSpecification getResources() {
    return resources;
  }
}
