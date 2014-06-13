/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.lang;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;

/**
 * Visitor for visiting class field.
 */
public abstract class FieldVisitor implements Visitor {

  @Override
  public final void visit(Object instance, TypeToken<?> inspectType,
                          TypeToken<?> declareType, Method method) throws Exception {
    // No-op
  }
}
