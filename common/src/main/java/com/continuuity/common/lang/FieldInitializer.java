/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.lang;

import com.continuuity.internal.lang.FieldVisitor;
import com.google.common.base.Defaults;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;

/**
 * Package private class for {@link InstantiatorFactory} to initialize fields for instances created using Unsafe.
 */
final class FieldInitializer extends FieldVisitor {

  @Override
  public void visit(Object instance, TypeToken<?> inspectType, TypeToken<?> declareType, Field field) throws Exception {
    field.set(instance, Defaults.defaultValue(field.getType()));
  }
}
