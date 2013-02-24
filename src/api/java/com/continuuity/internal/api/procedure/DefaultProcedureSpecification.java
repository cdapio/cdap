package com.continuuity.internal.api.procedure;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.util.Set;

/**
 *
 */
public final class DefaultProcedureSpecification implements ProcedureSpecification {

  private final String className;
  private final String name;
  private final String description;
  private final Set<String> dataSets;

  public DefaultProcedureSpecification(String name, String description, Set<String> dataSets) {
    this(null, name, description, dataSets);
  }

  public DefaultProcedureSpecification(Procedure procedure) {
    this.className = procedure.getClass().getName();
    ProcedureSpecification configureSpec = procedure.configure();

    this.name = configureSpec.getName();
    this.description = configureSpec.getDescription();
    this.dataSets = inspectDataSets(procedure, ImmutableSet.<String>builder().addAll(configureSpec.getDataSets()));
  }

  public DefaultProcedureSpecification(String className, String name, String description, Set<String> dataSets) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.dataSets = ImmutableSet.copyOf(dataSets);
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

  private Set<String> inspectDataSets(Procedure procedure, ImmutableSet.Builder<String> datasets) {
    TypeToken<?> procedureType = TypeToken.of(procedure.getClass());

    // Walk up the hierarchy of procedure class.
    for (TypeToken<?> type : procedureType.getTypes().classes()) {
      if (type.getRawType().equals(Object.class)) {
        break;
      }

      // Grab all the DataSet
      for (Field field : type.getRawType().getDeclaredFields()) {
        if (DataSet.class.isAssignableFrom(field.getType())) {
          UseDataSet dataset = field.getAnnotation(UseDataSet.class);
          if (dataset == null || dataset.value().isEmpty()) {
            continue;
          }
          datasets.add(dataset.value());
        }
      }
    }

    return datasets.build();
  }
}
