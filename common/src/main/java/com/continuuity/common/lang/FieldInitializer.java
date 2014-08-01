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
package com.continuuity.common.lang;

import com.continuuity.internal.lang.FieldVisitor;
import com.google.common.base.Defaults;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Package private class for {@link InstantiatorFactory} to initialize fields for instances created using Unsafe.
 */
final class FieldInitializer extends FieldVisitor {

  @Override
  public void visit(Object instance, TypeToken<?> inspectType, TypeToken<?> declareType, Field field) throws Exception {
    if (Modifier.isStatic(field.getModifiers())) {
      return;
    }
    field.set(instance, Defaults.defaultValue(field.getType()));
  }
}
