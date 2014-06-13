/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.dataset.Dataset;
import com.continuuity.internal.lang.FieldVisitor;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;

/**
 * A {@link FieldVisitor} that inject DataSet instance into fields marked with {@link UseDataSet}.
 */
public final class DataSetFieldSetter extends FieldVisitor {

  private final DataSetContext dataSetContext;

  public DataSetFieldSetter(DataSetContext dataSetContext) {
    this.dataSetContext = dataSetContext;
  }

  @Override
  public void visit(Object instance, TypeToken<?> inspectType, TypeToken<?> declareType, Field field) throws Exception {
    if (DataSet.class.isAssignableFrom(field.getType()) || Dataset.class.isAssignableFrom(field.getType())) {
      UseDataSet useDataSet = field.getAnnotation(UseDataSet.class);
      if (useDataSet != null && !useDataSet.value().isEmpty()) {
        field.set(instance, dataSetContext.getDataSet(useDataSet.value()));
      }
    }
  }
}
