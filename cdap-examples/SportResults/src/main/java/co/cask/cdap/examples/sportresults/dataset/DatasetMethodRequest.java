/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.examples.sportresults.dataset;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DatasetMethodRequest {

  private static final Map<String, Class<?>> PRIMITIVES;
  static {
    List<Class<?>> primitiveClasses = ImmutableList.<Class<?>>builder()
      .add(void.class)
      .add(boolean.class).add(boolean[].class)
      .add(byte.class).add(byte[].class)
      .add(char.class).add(char[].class)
      .add(double.class).add(double[].class)
      .add(float.class).add(float[].class)
      .add(int.class).add(int[].class)
      .add(long.class).add(long[].class)
      .add(short.class).add(short[].class)
      .build();

    PRIMITIVES = new HashMap<>();
    for (Class<?> primitiveClass : primitiveClasses) {
      PRIMITIVES.put(primitiveClass.getName(), primitiveClass);
    }
  }

  private final String method;
  private final String returnType;
  private final List<DatasetMethodArgument> arguments;

  public DatasetMethodRequest(String method, String returnType, List<DatasetMethodArgument> arguments) {
    this.method = method;
    this.returnType = returnType;
    this.arguments = arguments;
  }

  public String getMethod() {
    return method;
  }

  public String getReturnType() {
    return returnType;
  }

  public List<DatasetMethodArgument> getArguments() {
    return arguments;
  }

  public Class<?> getReturnTypeClass(ClassLoader cl) throws ClassNotFoundException {
    return loadClass(cl, returnType);
  }

  public List<Class<?>> getArgumentClasses(ClassLoader cl) throws ClassNotFoundException {
    List<Class<?>> result = new ArrayList<>();
    for (DatasetMethodArgument argument : arguments) {
      result.add(loadClass(cl, argument.getType()));
    }
    return result;
  }

  public List<?> getArgumentList(Gson gson, ClassLoader cl) throws ClassNotFoundException {
    List<Object> result = new ArrayList<>();
    for (DatasetMethodArgument argument : arguments) {
      result.add(gson.fromJson(argument.getValue(), loadClass(cl, argument.getType())));
    }
    return result;
  }

  private Class<?> loadClass(ClassLoader cl, String className) throws ClassNotFoundException {
    if (PRIMITIVES.containsKey(className)) {
      return PRIMITIVES.get(className);
    }
    return cl.loadClass(className);
  }

  public List<String> getArgumentTypes() {
    List<String> result = new ArrayList<>();
    for (DatasetMethodArgument argument : arguments) {
      result.add(argument.getType());
    }
    return result;
  }
}
