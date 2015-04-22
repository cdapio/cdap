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

package com.google.common.reflect;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Map;

/**
 * Invokes methods in {@link TypeResolver} since it is a package private class in Guava 13.
 * {@link TypeResolver} is a public class in Guava 15 but cdap-api has dependency on Guava 13.
 */
public class TypeResolution {
  private TypeResolver resolver;

  public TypeResolution() {
    this.resolver = new TypeResolver();
  }

  public TypeResolution(TypeResolver resolver) {
    this.resolver = resolver;
  }

  public static TypeResolution accordingTo(Type type) {
    return new TypeResolution(TypeResolver.accordingTo(type));
  }

  public TypeResolution where(Type formal, Type actual) {
    return new TypeResolution(resolver.where(formal, actual));
  }

  public TypeResolution where(Map<? extends TypeVariable<?>, ? extends Type> mappings) {
    return new TypeResolution(resolver.where(mappings));
  }

  public Type resolveTypeVariable(TypeVariable<?> var, TypeResolver guardedResolver) {
    return resolver.resolveTypeVariable(var, guardedResolver);
  }

  public Type resolveType(Type type) {
    return resolver.resolveType(type);
  }
}
