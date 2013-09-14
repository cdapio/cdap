package com.continuuity.internal.procedure;

import com.continuuity.api.ResourceSpecification;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.internal.ProgramSpecificationHelper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

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
    this.className = procedure.getClass().getName();
    ProcedureSpecification configureSpec = procedure.configure();

    this.name = configureSpec.getName();
    this.description = configureSpec.getDescription();
    this.dataSets = ProgramSpecificationHelper.inspectDataSets(
      procedure.getClass(), ImmutableSet.<String>builder().addAll(configureSpec.getDataSets()));
    this.arguments = configureSpec.getArguments();
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
