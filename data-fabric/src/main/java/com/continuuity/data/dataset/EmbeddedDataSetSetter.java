/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetContext;
import com.continuuity.internal.lang.FieldVisitor;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;

/**
 * A {@link FieldVisitor} that sets embedded DataSet fields in a DataSet.
 */
public final class EmbeddedDataSetSetter extends FieldVisitor {

  private final DataSetContext context;

  public EmbeddedDataSetSetter(DataSetContext context) {
    this.context = context;
  }

  @Override
  public void visit(Object instance, TypeToken<?> inspectType, TypeToken<?> declareType, Field field) throws Exception {
    if (DataSet.class.isAssignableFrom(field.getType())) {
      // The specification key is "className.fieldName". See EmbeddedDataSetExtractor.
      String key = field.getDeclaringClass().getName() + '.' + field.getName();
      field.set(instance, context.getDataSet(key));
    }
  }
}
