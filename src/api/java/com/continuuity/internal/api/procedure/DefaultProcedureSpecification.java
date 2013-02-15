package com.continuuity.internal.api.procedure;

import com.continuuity.api.procedure.ProcedureSpecification;

/**
 *
 */
public final class DefaultProcedureSpecification implements ProcedureSpecification {

  private final String className;
  private final String name;
  private final String description;

  public DefaultProcedureSpecification(String name, String description) {
    this(null, name, description);
  }

  public DefaultProcedureSpecification(String className, ProcedureSpecification other) {
    this(className, other.getName(), other.getDescription());
  }

  public DefaultProcedureSpecification(String className, String name, String description) {
    this.className = className;
    this.name = name;
    this.description = description;
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
}
