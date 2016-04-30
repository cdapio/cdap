/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.asm;

import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

/**
 * Util class containing helper functions to interact with ASM {@link Method}.
 */
public final class Methods {

  public static Method getMethod(Class<?> returnType, String name, Class<?>...args) {
    Type[] argTypes = new Type[args.length];
    for (int i = 0; i < args.length; i++) {
      argTypes[i] = Type.getType(args[i]);
    }

    return new Method(name, Type.getType(returnType), argTypes);
  }

  private Methods() {}
}
