package com.continuuity.internal.specification;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.lang.reflect.Type;

/**
 * Class representing either a {@link com.continuuity.api.annotation.ProcessInput}
 * or {@link com.continuuity.api.annotation.Tick} method. Comparing is done with method name
 * and generic parameter types.
 */
public final class FlowletMethod {
  private final Method method;
  private final TypeToken<?> inspectType;

  public FlowletMethod(Method method, TypeToken<?> inspectType) {
    this.method = method;
    this.inspectType = inspectType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FlowletMethod other = (FlowletMethod) o;

    // Method name have to match
    if (!method.getName().equals(other.method.getName())) {
      return false;
    }

    // Return type has to match
    if (!isSame(method.getGenericReturnType(), inspectType, other.method.getGenericReturnType(), other.inspectType)) {
      return false;
    }

    // Parameters has to match
    Type[] params1 = method.getGenericParameterTypes();
    Type[] params2 = other.method.getGenericParameterTypes();

    if (params1.length != params2.length) {
      return false;
    }

    for (int i = 0; i < params1.length; i++) {
      if (!isSame(params1[i], inspectType, params2[i], other.inspectType)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    return method.getName().hashCode();
  }

  private boolean isSame(Type type1, TypeToken<?> inspectType1, Type type2, TypeToken<?> inspectType2) {
    return inspectType1.resolveType(type1).equals(inspectType2.resolveType(type2));
  }
}
