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

package co.cask.cdap.internal.app.runtime.batch.distributed;

import co.cask.cdap.internal.asm.Methods;
import com.google.common.base.Preconditions;
import com.google.common.io.OutputSupplier;
import com.google.common.io.Resources;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Helper class to generate main classes for MapReduce processes using ASM. Those classes need to be generated
 * instead of being part of source code is to avoid infinite recursion since we need classes to be of the same
 * class names as the classes in MapReduce (e.g. MRAppMaster) in order to intercept the main() method call.
 */
public final class ContainerLauncherGenerator {

  /**
   * Generates a JAR file for launching MapReduce containers. The generated jar contains three classes inside
   *
   * <ul>
   *   <li>{@link MRAppMaster}</li>
   *   <li>{@link org.apache.hadoop.mapred.YarnChild YarnChild}</li>
   *   <li>{@link MRContainerLauncher}</li>
   * </ul>
   *
   * @see MRContainerLauncher
   */
  public static void generateLauncherJar(String launcherClassPath, String classLoaderName,
                                         OutputSupplier<? extends OutputStream> outputSupplier) throws IOException {
    try (JarOutputStream output = new JarOutputStream(outputSupplier.getOutput())) {
      generateLauncherClass(launcherClassPath, classLoaderName, MRAppMaster.class.getName(), output);
      generateLauncherClass(launcherClassPath, classLoaderName, "org.apache.hadoop.mapred.YarnChild", output);

      // Includes the launcher class in the JAR as well. No need to trace dependency as the launcher
      // class must be dependency free.
      String containerLauncherName = Type.getInternalName(MRContainerLauncher.class) + ".class";
      output.putNextEntry(new JarEntry(containerLauncherName));
      URL launcherURL = ContainerLauncherGenerator.class.getClassLoader().getResource(containerLauncherName);

      // Can never be null
      Preconditions.checkState(launcherURL != null);
      Resources.copy(launcherURL, output);
    }
  }

  /**
   * Generates the bytecode for a main class and writes to the given {@link JarOutputStream}.
   * The generated class looks like this:
   *
   * <pre>{@code
   * class className {
   *   public static void main(String[] args) {
   *     MRContainerLauncher.launch(launcherClassPath, classLoaderName, className, args);
   *   }
   * }
   * }
   * </pre>
   *
   * The {@code launcherClassPath}, {@code classLoaderName} and {@code className} are represented as
   * string literals in the generated class.
   */
  private static void generateLauncherClass(String launcherClassPath, String classLoaderName,
                                            String className, JarOutputStream output) throws IOException {
    String internalName = className.replace('.', '/');

    ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
    classWriter.visit(Opcodes.V1_7, Opcodes.ACC_PUBLIC + Opcodes.ACC_SUPER,
                      internalName, null, Type.getInternalName(Object.class), null);

    Method constructor = Methods.getMethod(void.class, "<init>");

    // Constructor
    // MRAppMaster()
    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC, constructor, null, null, classWriter);

    mg.loadThis();
    mg.invokeConstructor(Type.getType(Object.class), constructor);
    mg.returnValue();
    mg.endMethod();

    // Main method.
    // public static void main(String[] args) {
    //   MRContainerLauncher.launch(launcherClassPath, classLoaderName, className, args);
    // }
    Method mainMethod = Methods.getMethod(void.class, "main", String[].class);
    mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC + Opcodes.ACC_STATIC, mainMethod, null,
                              new Type[] { Type.getType(Exception.class) }, classWriter);

    mg.getStatic(Type.getType(System.class), "out", Type.getType(PrintStream.class));
    mg.visitLdcInsn("Launch class " + className);
    mg.invokeVirtual(Type.getType(PrintStream.class), Methods.getMethod(void.class, "println", String.class));

    // The Launcher classpath, classloader name and main classname are stored as string literal in the generated class
    mg.visitLdcInsn(launcherClassPath);
    mg.visitLdcInsn(classLoaderName);
    mg.visitLdcInsn(className);
    mg.loadArg(0);
    mg.invokeStatic(Type.getType(MRContainerLauncher.class),
                    Methods.getMethod(void.class, "launch", String.class, String.class, String.class, String[].class));
    mg.returnValue();
    mg.endMethod();

    classWriter.visitEnd();

    output.putNextEntry(new JarEntry(internalName + ".class"));
    output.write(classWriter.toByteArray());
  }

  private ContainerLauncherGenerator() {
  }
}
