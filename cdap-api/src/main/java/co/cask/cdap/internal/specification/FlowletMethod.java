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

package co.cask.cdap.internal.specification;

import co.cask.cdap.internal.guava.reflect.TypeToken;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Objects;

/**
 * Class representing either a {@link co.cask.cdap.api.annotation.ProcessInput}
 * or {@link co.cask.cdap.api.annotation.Tick} method. Comparing is done with method name
 * and generic parameter types.
 */
public final class FlowletMethod {
  private final String methodName;
  private final Type returnType;
  private final Type[] parameterTypes;

  /**
   * Creates a new instance of {@link FlowletMethod}.
   *
   * @param method the method that the flowlet method is representing
   * @param resolvingType the type used for resolving method return type and parameter types
   * @return a new instance of {@link FlowletMethod}
   */
  public static FlowletMethod create(Method method, Type resolvingType) {
    TypeToken<?> typeToken = TypeToken.of(resolvingType);
    Type returnType = typeToken.resolveType(method.getGenericReturnType()).getType();
    Type[] parameterTypes = method.getGenericParameterTypes();
    for (int i = 0; i < parameterTypes.length; i++) {
      parameterTypes[i] = typeToken.resolveType(parameterTypes[i]).getType();
    }
    return new FlowletMethod(method.getName(), returnType, parameterTypes);
  }

  private FlowletMethod(String methodName, Type returnType, Type[] parameterTypes) {
    this.methodName = methodName;
    this.returnType = returnType;
    this.parameterTypes = parameterTypes;
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
    if (!Objects.equals(methodName, other.methodName)) {
      return false;
    }

    // Return type has to match
    if (!Objects.equals(returnType, other.returnType)) {
      return false;
    }

    return Arrays.equals(parameterTypes, other.parameterTypes);
  }

  @Override
  public int hashCode() {
    return methodName.hashCode();
  }

  private boolean isSame(Type type1, TypeToken<?> inspectType1, Type type2, TypeToken<?> inspectType2) {
    return inspectType1.resolveType(type1).equals(inspectType2.resolveType(type2));
  }
}
