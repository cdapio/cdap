/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.lang;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;

/**
 * Visitor for visiting class method.
 */
public abstract class MethodVisitor implements Visitor {

  @Override
  public final void visit(Object instance, TypeToken<?> inspectType,
                          TypeToken<?> declareType, Field field) throws Exception {
    // no-op
  }
}
