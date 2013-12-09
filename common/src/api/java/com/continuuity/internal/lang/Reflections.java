/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.lang;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Utility class for helper methods to deal with Java Reflection.
 */
public final class Reflections {

  /**
   * Checks if the given type is fully resolved.
   */
  public static boolean isResolved(Type type) {
    if (type instanceof GenericArrayType) {
      return isResolved(((GenericArrayType) type).getGenericComponentType());
    }
    if (type instanceof ParameterizedType) {
      for (Type t : ((ParameterizedType) type).getActualTypeArguments()) {
        if (!isResolved(t)) {
          return false;
        }
      }
      return true;
    }
    return type instanceof Class;
  }


  /**
   * Inspect all members in the given type. Fields and Methods that are given to Visitor are
   * always having accessible flag being set.
   */
  public static void visit(Object instance, TypeToken<?> inspectType, Visitor firstVisitor, Visitor... moreVisitors) {

    try {
      List<Visitor> visitors = ImmutableList.<Visitor>builder().add(firstVisitor).add(moreVisitors).build();

      for (TypeToken<?> type : inspectType.getTypes().classes()) {
        if (Object.class.equals(type.getRawType())) {
          break;
        }

        for (Field field : type.getRawType().getDeclaredFields()) {
          if (!field.isAccessible()) {
            field.setAccessible(true);
          }
          for (Visitor visitor : visitors) {
            visitor.visit(instance, inspectType, type, field);
          }
        }

        for (Method method : type.getRawType().getDeclaredMethods()) {
          if (!method.isAccessible()) {
            method.setAccessible(true);
          }
          for (Visitor visitor : visitors) {
            visitor.visit(instance, inspectType, type, method);
          }
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private Reflections() {
  }
}
