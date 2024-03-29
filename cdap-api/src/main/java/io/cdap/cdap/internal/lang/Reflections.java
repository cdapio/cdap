/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
package io.cdap.cdap.internal.lang;

import io.cdap.cdap.internal.guava.reflect.TypeToken;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
      return Arrays.stream(((ParameterizedType) type).getActualTypeArguments())
          .allMatch(Reflections::isResolved);
    }
    return type instanceof Class;
  }


  /**
   * Inspect all members in the given type. Fields and Methods that are given to Visitor are always
   * having accessible flag being set.
   */
  public static void visit(Object instance, Type inspectType, Visitor firstVisitor,
      Visitor... moreVisitors) {
    try {
      TypeToken<?> inspectTypeToken = TypeToken.of(inspectType);
      List<Visitor> visitors = new ArrayList<>(1 + moreVisitors.length);
      visitors.add(firstVisitor);
      Collections.addAll(visitors, moreVisitors);

      for (TypeToken<?> type : inspectTypeToken.getTypes().classes()) {
        if (Object.class.equals(type.getRawType())) {
          break;
        }

        for (Field field : type.getRawType().getDeclaredFields()) {
          if (field.isSynthetic()) {
            continue;
          }
          if (!field.isAccessible()) {
            field.setAccessible(true);
          }
          for (Visitor visitor : visitors) {
            visitor.visit(instance, inspectTypeToken.getType(), type.getType(), field);
          }
        }

        for (Method method : type.getRawType().getDeclaredMethods()) {
          if (method.isSynthetic() || method.isBridge()) {
            continue;
          }
          if (!method.isAccessible()) {
            method.setAccessible(true);
          }
          for (Visitor visitor : visitors) {
            visitor.visit(instance, inspectTypeToken.getType(), type.getType(), method);
          }
        }
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Reflections() {
  }
}
