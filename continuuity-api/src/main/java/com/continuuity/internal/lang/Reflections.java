/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
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
