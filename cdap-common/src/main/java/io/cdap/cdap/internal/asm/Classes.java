/*
 * Copyright © 2016 Cask Data, Inc.
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

import com.google.common.base.Function;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.Set;

/**
 * A util class to help on class inspections and manipulations through bytecode.
 */
public final class Classes {

  private static final Logger LOG = LoggerFactory.getLogger(Classes.class);

  /**
   * Checks if the given class extends or implements a super type.
   *
   * @param className name of the class to check
   * @param superTypeName name of the super type
   * @param resourceProvider a {@link Function} to provide {@link URL} of a class from the class name
   * @param cache a cache for memorizing previous decision for classes of the same super type
   * @return true if the given class name is a sub-class of the given super type
   * @throws IOException if failed to read class information
   */
  public static boolean isSubTypeOf(String className, final String superTypeName,
                                    final Function<String, URL> resourceProvider,
                                    final Map<String, Boolean> cache) throws IOException {
    // Base case
    if (superTypeName.equals(className)) {
      cache.put(className, true);
      return true;
    }

    // Check the cache first
    Boolean cachedResult = cache.get(className);
    if (cachedResult != null) {
      return cachedResult;
    }

    // Try to get the URL resource of the given class
    URL url = resourceProvider.apply(className);
    if (url == null) {
      // Ignore it if cannot find the class file for the given class.
      // Normally this shouldn't happen, however it is to guard against mis-packaged artifact jar that included
      // invalid/incomplete jars. Anyway, if this happen, the class won't be loadable in runtime.
      return false;
    }

    // Inspect the bytecode and check the super class/interfaces recursively
    boolean result = false;
    try (InputStream input = url.openStream()) {
      ClassReader cr = new ClassReader(input);
      String superName = cr.getSuperName();
      if (superName != null) {
        result = isSubTypeOf(Type.getObjectType(superName).getClassName(), superTypeName, resourceProvider, cache);
      }

      if (!result) {
        String[] interfaces = cr.getInterfaces();
        if (interfaces != null) {
          for (String intf : interfaces) {
            if (isSubTypeOf(Type.getObjectType(intf).getClassName(), superTypeName, resourceProvider, cache)) {
              result = true;
              break;
            }
          }
        }
      }
    }

    cache.put(className, result);
    return result;
  }

  private Classes() {
  }

  /**
   * Rewrites methods in the given class bytecode to noop methods.
   */
  public static byte[] rewriteMethodToNoop(final String className,
                                           InputStream byteCodeStream, final Set<String> methods) throws IOException {
    ClassReader cr = new ClassReader(byteCodeStream);
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {
      @Override
      public MethodVisitor visitMethod(final int access, final String name, final String desc,
                                       String signature, String[] exceptions) {
        MethodVisitor methodVisitor = super.visitMethod(access, name, desc, signature, exceptions);

        if (!methods.contains(name)) {
          return methodVisitor;
        }

        // We can only rewrite method that returns void
        if (!Type.getReturnType(desc).equals(Type.VOID_TYPE)) {
          LOG.warn("Cannot patch method {} in {} due to non-void return type: {}", name, className, desc);
          return methodVisitor;
        }

        // Rewrite the method to noop.
        GeneratorAdapter adapter = new GeneratorAdapter(methodVisitor, access, name, desc);
        adapter.returnValue();

        // VisitMaxs with 0 so that COMPUTE_MAXS from ClassWriter will compute the right values.
        adapter.visitMaxs(0, 0);
        return new MethodVisitor(Opcodes.ASM5) {
        };
      }
    }, 0);
    return cw.toByteArray();
  }
}
