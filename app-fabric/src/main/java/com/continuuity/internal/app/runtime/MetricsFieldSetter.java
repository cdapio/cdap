/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime;

import com.continuuity.api.metrics.Metrics;
import com.continuuity.internal.lang.FieldVisitor;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;

/**
 * A {@link FieldVisitor} that set Metrics fields.
 */
public final class MetricsFieldSetter extends FieldVisitor {

  private final Metrics metrics;

  public MetricsFieldSetter(Metrics metrics) {
    this.metrics = metrics;
  }

  @Override
  public void visit(Object instance, TypeToken<?> inspectType, TypeToken<?> declareType, Field field) throws Exception {
    if (Metrics.class.equals(field.getType())) {
      field.set(instance, metrics);
    }
  }
}
