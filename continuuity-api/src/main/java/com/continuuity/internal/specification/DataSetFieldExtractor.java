/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.specification;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.dataset.Dataset;
import com.continuuity.internal.lang.FieldVisitor;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.util.Set;

/**
 * A {@link FieldVisitor} that extracts names of all {@link UseDataSet} fields.
 */
public final class DataSetFieldExtractor extends FieldVisitor {

  private final Set<String> dataSets;

  /**
   * Constructs a {@link DataSetFieldExtractor} that store DataSet names extracted from fields.
   * @param dataSets
   */
  public DataSetFieldExtractor(Set<String> dataSets) {
    this.dataSets = dataSets;
  }

  @Override
  public void visit(Object instance, TypeToken<?> inspectType, TypeToken<?> declareType, Field field) {
    if (DataSet.class.isAssignableFrom(field.getType()) || Dataset.class.isAssignableFrom(field.getType())) {
      UseDataSet dataset = field.getAnnotation(UseDataSet.class);
      if (dataset == null || dataset.value().isEmpty()) {
        return;
      }
      dataSets.add(dataset.value());
    }
  }
}
