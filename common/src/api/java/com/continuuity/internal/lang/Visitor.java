/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.lang;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Visitor for visiting class members.
 */
public interface Visitor {

  /**
   * Visits a field in the given type. The field might be declared in one of the parent class of the type.
   */
  void visit(Object instance, TypeToken<?> inspectType, TypeToken<?> declareType, Field field) throws Exception;

  /**
   * Visits a method in the given type. The method might be declared in one of the parent class of the type.
   */
  void visit(Object instance, TypeToken<?> inspectType, TypeToken<?> declareType, Method method) throws Exception;
}
