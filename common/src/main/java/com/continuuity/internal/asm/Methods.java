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

package com.continuuity.internal.asm;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

/**
 * Util class containing helper functions to interact with ASM {@link Method}.
 */
public final class Methods {

  public static Method getMethod(Class<?> returnType, String name, Class<?>...args) {
    StringBuilder builder = new StringBuilder(returnType.getName())
      .append(' ').append(name).append(" (");
    Joiner.on(',').appendTo(builder, Iterators.transform(Iterators.forArray(args), new Function<Class<?>, String>() {
      @Override
      public String apply(Class<?> input) {
        if (input.isArray()) {
          return Type.getType(input.getName()).getClassName();
        }
        return input.getName();
      }
    }));
    builder.append(')');
    return Method.getMethod(builder.toString());
  }

  private Methods() {}
}
