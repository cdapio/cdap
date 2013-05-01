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
