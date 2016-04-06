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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.common.internal.guava.ClassPath;
import co.cask.cdap.common.lang.ClassPathResources;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import org.apache.spark.SparkContext;
import org.apache.spark.streaming.StreamingContext;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;
import org.objectweb.asm.commons.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A special {@link ClassLoader} for defining and loading all cdap-spark-core classes and Spark classes.
 */
final class SparkRunnerClassLoader extends URLClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRunnerClassLoader.class);

  // Define some of the class types used for bytecode rewriting purpose.
  private static final Type SPARK_CONTEXT_TYPE = Type.getType(SparkContext.class);
  private static final Type SPARK_STREAMING_CONTEXT_TYPE = Type.getType(StreamingContext.class);
  private static final Type SPARK_CONTEXT_CACHE_TYPE = Type.getType(SparkContextCache.class);

  // Set of resources that are in cdap-api. They should be loaded by the parent ClassLoader
  private static final Set<String> API_CLASSES;

  static {
    Set<String> apiClasses = new HashSet<>();
    try {
      Iterables.addAll(
        apiClasses, Iterables.transform(
          Iterables.filter(ClassPathResources.getClassPathResources(Spark.class.getClassLoader(), Spark.class),
                           ClassPath.ClassInfo.class),
          ClassPathResources.CLASS_INFO_TO_CLASS_NAME
        )
      );
    } catch (IOException e) {
      // Shouldn't happen, because Spark.class is in cdap-api and it should always be there.
      LOG.error("Unable to find cdap-api classes.", e);
    }

    API_CLASSES = Collections.unmodifiableSet(apiClasses);
  }

  SparkRunnerClassLoader(URL[] urls, @Nullable ClassLoader parent) {
    super(urls, parent);
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    // We Won't define the class with this ClassLoader for the following classes since they should
    // come from the parent ClassLoader
    // cdap-api-classes
    // Any class that is not from cdap-api-spark or cdap-spark-core
    // Any class that is not from Spark
    if (API_CLASSES.contains(name) || (!name.startsWith("co.cask.cdap.api.spark.")
        && !name.startsWith("co.cask.cdap.app.runtime.spark.")
        && !name.startsWith("org.apache.spark."))) {
      return super.loadClass(name, resolve);
    }

    // If the class is already loaded, return it
    Class<?> cls = findLoadedClass(name);
    if (cls != null) {
      return cls;
    }

    // Define the class with this ClassLoader
    try (InputStream is = getResourceAsStream(name.replace('.', '/') + ".class")) {
      if (is == null) {
        throw new ClassNotFoundException("Failed to find resource for class " + name);
      }

      if (name.equals(SPARK_CONTEXT_TYPE.getClassName())) {
        // Define the SparkContext class by rewriting the constructor
        cls = defineContext(SPARK_CONTEXT_TYPE, name, is);
      } else if (name.equals(SPARK_STREAMING_CONTEXT_TYPE.getClassName())) {
        // Define the StreamingContext class by rewriting the constructor
        cls = defineContext(SPARK_STREAMING_CONTEXT_TYPE, name, is);
      } else {
        // Otherwise, just define it with this ClassLoader
        byte[] byteCode = ByteStreams.toByteArray(is);
        cls = defineClass(name, byteCode, 0, byteCode.length);
      }

      if (resolve) {
        resolveClass(cls);
      }
      return cls;
    } catch (IOException e) {
      throw new ClassNotFoundException("Failed to read class definition for class " + name, e);
    }
  }

  /**
   * This method will define the class by rewriting the constructor to call
   * {@link SparkContextCache#setContext(SparkContext)} or {@link SparkContextCache#setContext(StreamingContext)},
   * depending on the class type.
   */
  private Class<?> defineContext(final Type contextType, String name, InputStream byteCodeStream) throws IOException {
    ClassReader cr = new ClassReader(byteCodeStream);
    ClassWriter cw = new ClassWriter(0);

    cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {

      @Override
      public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        // Call super so that the method signature is registered with the ClassWriter (parent)
        MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);

        // We only attempt to rewrite constructor
        if (!"<init>".equals(name)) {
          return mv;
        }

        return new AdviceAdapter(Opcodes.ASM5, mv, access, name, desc) {

          boolean calledThis;

          @Override
          public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
            // See if in this constructor it is calling other constructor (this(..)).
            calledThis = calledThis || (opcode == Opcodes.INVOKESPECIAL
              && Type.getObjectType(owner).equals(contextType)
              && name.equals("<init>")
              && Type.getReturnType(desc).equals(Type.VOID_TYPE));
            super.visitMethodInsn(opcode, owner, name, desc, itf);
          }

          @Override
          protected void onMethodExit(int opcode) {
            if (calledThis) {
              // For constructors that call this(), we don't need to generate a call to SparkContextCache
              return;
            }
            // Add a call to SparkContextCache.setContext() for the normal method return path
            if (opcode == RETURN) {
              loadThis();
              invokeStatic(SPARK_CONTEXT_CACHE_TYPE,
                           new Method("setContext", Type.VOID_TYPE, new Type[] { contextType }));
            }
          }
        };
      }
    }, ClassReader.EXPAND_FRAMES);

    byte[] byteCode = cw.toByteArray();
    return defineClass(name, byteCode, 0, byteCode.length);
  }
}
