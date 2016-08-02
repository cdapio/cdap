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

import co.cask.cdap.common.app.MainClassLoader;
import co.cask.cdap.internal.asm.Methods;
import com.google.common.base.Preconditions;
import com.google.common.io.OutputSupplier;
import com.google.common.io.Resources;
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
   *   <li>{@link org.apache.hadoop.mapreduce.v2.app.MRAppMaster}</li>
   *   <li>{@link org.apache.hadoop.mapred.YarnChild YarnChild}</li>
   *   <li>{@link MapReduceContainerLauncher}</li>
   * </ul>
   *
   * @see MapReduceContainerLauncher
   */
  public static void generateLauncherJar(String launcherClassPath, String classLoaderName,
                                         OutputSupplier<? extends OutputStream> outputSupplier) throws IOException {
    try (JarOutputStream output = new JarOutputStream(outputSupplier.getOutput())) {
      generateLauncherClass(launcherClassPath, classLoaderName,
                            "org.apache.hadoop.mapreduce.v2.app.MRAppMaster", output);
      generateLauncherClass(launcherClassPath, classLoaderName,
                            "org.apache.hadoop.mapred.YarnChild", output);

      // Includes the launcher class in the JAR as well. No need to trace dependency as the launcher
      // class must be dependency free.
      String containerLauncherName = Type.getInternalName(MapReduceContainerLauncher.class) + ".class";
      output.putNextEntry(new JarEntry(containerLauncherName));
      URL launcherURL = ContainerLauncherGenerator.class.getClassLoader().getResource(containerLauncherName);

      // Can never be null
      Preconditions.checkState(launcherURL != null);
      Resources.copy(launcherURL, output);
    }
  }

  /**
   * Generates a JAR file that contains zero or more classes with a static main method.
   *
   * @param mainClassNames List of main class names to generate
   * @param mainDelegatorClass the actual class that the main method will delegate to
   * @param outputSupplier the {@link OutputSupplier} for the jar file
   */
  public static void generateLauncherJar(Iterable<String> mainClassNames, Class<?> mainDelegatorClass,
                                         OutputSupplier<? extends OutputStream> outputSupplier) throws IOException {
    try (JarOutputStream output = new JarOutputStream(outputSupplier.getOutput())) {
      for (String mainClassName : mainClassNames) {
        generateMainClass(mainClassName, Type.getType(mainDelegatorClass), output);
      }
    }
  }

  /**
   * Generates the bytecode for a main class and writes to the given {@link JarOutputStream}.
   * The generated class looks like this:
   *
   * <pre>{@code
   * class className {
   *   public static void main(String[] args) {
   *     MapReduceContainerLauncher.launch(launcherClassPath, mainClassLoaderName, classLoaderName, className, args);
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
    //   MapReduceContainerLauncher.launch(launcherClassPath, classLoaderName, className, args);
    // }
    Method mainMethod = Methods.getMethod(void.class, "main", String[].class);
    mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC + Opcodes.ACC_STATIC, mainMethod, null,
                              new Type[] { Type.getType(Exception.class) }, classWriter);

    mg.getStatic(Type.getType(System.class), "out", Type.getType(PrintStream.class));
    mg.visitLdcInsn("Launch class " + className);
    mg.invokeVirtual(Type.getType(PrintStream.class), Methods.getMethod(void.class, "println", String.class));

    // The Launcher classpath, classloader name and main classname are stored as string literal in the generated class
    mg.visitLdcInsn(launcherClassPath);
    mg.visitLdcInsn(MainClassLoader.class.getName());
    mg.visitLdcInsn(classLoaderName);
    mg.visitLdcInsn(className);
    mg.loadArg(0);
    mg.invokeStatic(Type.getType(MapReduceContainerLauncher.class),
                    Methods.getMethod(void.class, "launch",
                                      String.class, String.class, String.class, String.class, String[].class));
    mg.returnValue();
    mg.endMethod();

    classWriter.visitEnd();

    output.putNextEntry(new JarEntry(internalName + ".class"));
    output.write(classWriter.toByteArray());
  }

  /**
   * Generates a class that has a static main method which delegates the call to a static method in the given delegator
   * class with method signature {@code public static void launch(String className, String[] args)}
   *
   * @param className the classname of the generated class
   * @param mainDelegator the class to delegate the main call to
   * @param output for writing the generated bytes
   */
  private static void generateMainClass(String className, Type mainDelegator,
                                        JarOutputStream output) throws IOException {
    String internalName = className.replace('.', '/');

    ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
    classWriter.visit(Opcodes.V1_7, Opcodes.ACC_PUBLIC + Opcodes.ACC_SUPER,
                      internalName, null, Type.getInternalName(Object.class), null);

    // Generate the default constructor, which just call super()
    Method constructor = Methods.getMethod(void.class, "<init>");
    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC, constructor, null, null, classWriter);
    mg.loadThis();
    mg.invokeConstructor(Type.getType(Object.class), constructor);
    mg.returnValue();
    mg.endMethod();

    // Generate the main method
    // public static void main(String[] args) {
    //   System.out.println("Launch class .....");
    //   <MainDelegator>.launch(<className>, args);
    // }
    Method mainMethod = Methods.getMethod(void.class, "main", String[].class);
    mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC + Opcodes.ACC_STATIC, mainMethod, null,
                              new Type[] { Type.getType(Exception.class) }, classWriter);

    mg.getStatic(Type.getType(System.class), "out", Type.getType(PrintStream.class));
    mg.visitLdcInsn("Launch class " + className + " by calling " + mainDelegator.getClassName() + ".launch");
    mg.invokeVirtual(Type.getType(PrintStream.class), Methods.getMethod(void.class, "println", String.class));

    // The main classname is stored as string literal in the generated class
    mg.visitLdcInsn(className);
    mg.loadArg(0);
    mg.invokeStatic(mainDelegator, Methods.getMethod(void.class, "launch", String.class, String[].class));
    mg.returnValue();
    mg.endMethod();

    classWriter.visitEnd();

    output.putNextEntry(new JarEntry(internalName + ".class"));
    output.write(classWriter.toByteArray());
  }

  private ContainerLauncherGenerator() {
  }
}
