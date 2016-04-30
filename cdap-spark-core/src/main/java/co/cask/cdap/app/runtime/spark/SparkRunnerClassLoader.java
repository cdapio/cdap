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
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.ClassPathResources;
import co.cask.cdap.internal.app.runtime.spark.SparkUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * A special {@link ClassLoader} for defining and loading all cdap-spark-core classes and Spark classes.
 *
 * IMPORTANT: Due to discovery in CDAP-5822, don't use getResourceAsStream in this class.
 */
public final class SparkRunnerClassLoader extends URLClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRunnerClassLoader.class);

  // Define some of the class types used for bytecode rewriting purpose. Cannot be referred with .class since
  // those classes may not be available to the ClassLoader of this class (they are loadable from this ClassLoader).
  private static final Type SPARK_RUNTIME_ENV_TYPE =
    Type.getObjectType("co/cask/cdap/app/runtime/spark/SparkRuntimeEnv");
  private static final Type SPARK_RUNTIME_UTILS_TYPE =
    Type.getObjectType("co/cask/cdap/app/runtime/spark/SparkRuntimeUtils");
  private static final Type SPARK_CONTEXT_TYPE = Type.getObjectType("org/apache/spark/SparkContext");
  private static final Type SPARK_STREAMING_CONTEXT_TYPE =
    Type.getObjectType("org/apache/spark/streaming/StreamingContext");
  private static final Type SPARK_CONF_TYPE = Type.getObjectType("org/apache/spark/SparkConf");
  // SparkSubmit is a companion object, hence the "$" at the end
  private static final Type SPARK_SUBMIT_TYPE = Type.getObjectType("org/apache/spark/deploy/SparkSubmit$");
  private static final Type SPARK_YARN_CLIENT_TYPE = Type.getObjectType("org/apache/spark/deploy/yarn/Client");
  private static final Type SPARK_DSTREAM_GRAPH_TYPE = Type.getObjectType("org/apache/spark/streaming/DStreamGraph");

  // Don't refer akka Remoting with the ".class" because in future Spark version, akka dependency is removed and
  // we don't want to force a dependency on akka.
  private static final Type AKKA_REMOTING_TYPE = Type.getObjectType("akka/remote/Remoting");
  private static final Type EXECUTION_CONTEXT_TYPE = Type.getObjectType("scala/concurrent/ExecutionContext");
  private static final Type EXECUTION_CONTEXT_EXECUTOR_TYPE =
    Type.getObjectType("scala/concurrent/ExecutionContextExecutor");

  // File name of the Spark conf directory as defined by the Spark framework
  // This is for the Hack to workaround CDAP-5019 (SPARK-13441)
  private static final String LOCALIZED_CONF_DIR = SparkUtils.LOCALIZED_CONF_DIR;
  private static final String LOCALIZED_CONF_DIR_ZIP = LOCALIZED_CONF_DIR + ".zip";
  // File entry name of the SparkConf properties file inside the Spark conf zip
  private static final String SPARK_CONF_FILE = "__spark_conf__.properties";

  // Set of resources that are in cdap-api. They should be loaded by the parent ClassLoader
  private static final Set<String> API_CLASSES;

  private final boolean rewriteYarnClient;
  private final boolean rewriteDStreamGraph;

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

  private static URL[] getClassloaderURLs(ClassLoader classLoader) throws IOException {
    List<URL> urls = ClassLoaders.getClassLoaderURLs(classLoader, new ArrayList<URL>());

    // If Spark classes are not available in the given ClassLoader, try to locate the Spark assembly jar
    // This class cannot have dependency on Spark directly, hence using the class resource to discover if SparkContext
    // is there
    if (classLoader.getResource("org/apache/spark/SparkContext.class") == null) {
      urls.add(SparkUtils.locateSparkAssemblyJar().toURI().toURL());
    }
    return urls.toArray(new URL[urls.size()]);
  }

  public SparkRunnerClassLoader(ClassLoader parent,
                                boolean rewriteYarnClient, boolean rewriteDStreamGraph) throws IOException {
    this(getClassloaderURLs(parent), parent, rewriteYarnClient, rewriteDStreamGraph);
  }

  public SparkRunnerClassLoader(URL[] urls, @Nullable ClassLoader parent,
                                boolean rewriteYarnClient, boolean rewriteDStreamGraph) {
    super(urls, parent);
    this.rewriteYarnClient = rewriteYarnClient;
    this.rewriteDStreamGraph = rewriteDStreamGraph;
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    // We Won't define the class with this ClassLoader for the following classes since they should
    // come from the parent ClassLoader
    // cdap-api-classes
    // Any class that is not from cdap-api-spark or cdap-spark-core
    // Any class that is not from Spark
    // We also need to define the org.spark-project., fastxml and akka classes in this ClassLoader
    // to avoid reference/thread leakage due to Spark assumption on process terminating after execution
    if (API_CLASSES.contains(name) || (!name.startsWith("co.cask.cdap.api.spark.")
        && !name.startsWith("co.cask.cdap.app.runtime.spark.")
        && !name.startsWith("org.apache.spark.") && !name.startsWith("org.spark-project.")
        && !name.startsWith("com.fasterxml.jackson.module.scala.")
        && !name.startsWith("akka.") && !name.startsWith("com.typesafe."))) {
      return super.loadClass(name, resolve);
    }

    // If the class is already loaded, return it
    Class<?> cls = findLoadedClass(name);
    if (cls != null) {
      return cls;
    }

    // Define the class with this ClassLoader
    try (InputStream is = openResource(name.replace('.', '/') + ".class")) {
      if (is == null) {
        throw new ClassNotFoundException("Failed to find resource for class " + name);
      }

      if (name.equals(SPARK_CONTEXT_TYPE.getClassName())) {
        // Define the SparkContext class by rewriting the constructor to save the context to SparkRuntimeEnv
        cls = defineContext(SPARK_CONTEXT_TYPE, is);
      } else if (name.equals(SPARK_STREAMING_CONTEXT_TYPE.getClassName())) {
        // Define the StreamingContext class by rewriting the constructor to save the context to SparkRuntimeEnv
        cls = defineContext(SPARK_STREAMING_CONTEXT_TYPE, is);
      } else if (name.equals(SPARK_CONF_TYPE.getClassName())) {
        // Define the SparkConf class by rewriting the class to put all properties from
        // SparkRuntimeEnv to the SparkConf in the constructors
        cls = defineSparkConf(SPARK_CONF_TYPE, is);
      } else if (name.startsWith(SPARK_SUBMIT_TYPE.getClassName())) {
        // Rewrite System.setProperty call to SparkRuntimeEnv.setProperty for SparkSubmit and all inner classes
        cls = rewriteSetPropertiesAndDefineClass(name, is);
      } else if (name.equals(SPARK_YARN_CLIENT_TYPE.getClassName()) && rewriteYarnClient) {
        // Rewrite YarnClient for workaround SPARK-13441.
        cls = defineClient(name, is);
      } else if (name.equals(SPARK_DSTREAM_GRAPH_TYPE.getClassName()) && rewriteDStreamGraph) {
        // Rewrite DStreamGraph to set TaskSupport on parallel array usage to avoid Thread leak
        cls = defineDStreamGraph(name, is);
      } else if (name.equals(AKKA_REMOTING_TYPE.getClassName())) {
        // Define the akka.remote.Remoting class to avoid thread leakage
        cls = defineAkkaRemoting(name, is);
      } else {
        // Otherwise, just define it with this ClassLoader
        cls = findClass(name);
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
   * Defines the class by rewriting the constructor to call
   * SparkRuntimeEnv#setContext(SparkContext) or SparkRuntimeEnv#setContext(StreamingContext),
   * depending on the class type.
   */
  private Class<?> defineContext(final Type contextType, InputStream byteCodeStream) throws IOException {
    return rewriteConstructorAndDefineClass(contextType, byteCodeStream, new ConstructorRewriter() {
      @Override
      public void onMethodExit(GeneratorAdapter generatorAdapter) {
        generatorAdapter.loadThis();
        generatorAdapter.invokeStatic(SPARK_RUNTIME_ENV_TYPE,
                                      new Method("setContext", Type.VOID_TYPE, new Type[] { contextType }));
      }
    });
  }

  /**
   * Defines the SparkConf class by rewriting the constructor to call SparkRuntimeEnv#setupSparkConf(SparkConf).
   */
  private Class<?> defineSparkConf(final Type sparkConfType, InputStream byteCodeStream) throws IOException {
    return rewriteConstructorAndDefineClass(sparkConfType, byteCodeStream, new ConstructorRewriter() {
      @Override
      public void onMethodExit(GeneratorAdapter generatorAdapter) {
        generatorAdapter.loadThis();
        generatorAdapter.invokeStatic(SPARK_RUNTIME_ENV_TYPE,
                                      new Method("setupSparkConf", Type.VOID_TYPE, new Type[] { sparkConfType }));
      }
    });
  }

  /**
   * Defines the DStreamGraph class by rewriting calls to parallel array with a call to
   * SparkRuntimeUtils#setTaskSupport(ParArray).
   */
  private Class<?> defineDStreamGraph(String name, InputStream byteCodeStream) throws IOException {
    ClassReader cr = new ClassReader(byteCodeStream);
    ClassWriter cw = new ClassWriter(0);

    cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {
      @Override
      public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
        return new MethodVisitor(Opcodes.ASM5, mv) {
          @Override
          public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
            super.visitMethodInsn(opcode, owner, name, desc, itf);
            // If detected call to ArrayBuffer.par(), set the TaskSupport to avoid thread leak.
            //INVOKEVIRTUAL scala/collection/mutable/ ArrayBuffer.par ()Lscala/collection/parallel/mutable/ParArray;
            Type returnType = Type.getReturnType(desc);
            if (opcode == Opcodes.INVOKEVIRTUAL && name.equals("par")
                && owner.equals("scala/collection/mutable/ArrayBuffer")
                && returnType.getClassName().equals("scala.collection.parallel.mutable.ParArray")) {
              super.visitMethodInsn(Opcodes.INVOKESTATIC, SPARK_RUNTIME_UTILS_TYPE.getInternalName(),
                                    "setTaskSupport", Type.getMethodDescriptor(returnType, returnType), false);
            }
          }
        };
      }
    }, ClassReader.EXPAND_FRAMES);

    byte[] byteCode = cw.toByteArray();
    return defineClass(name, byteCode, 0, byteCode.length);
  }

  /**
   * Rewrites the constructors who don't delegate to other constructor with the given {@link ConstructorRewriter}
   * and define the class.
   *
   * @param classType type of the class to be defined
   * @param byteCodeStream {@link InputStream} for reading the original bytecode of the class
   * @param rewriter a {@link ConstructorRewriter} for rewriting the constructor
   * @return a defined Class
   */
  private Class<?> rewriteConstructorAndDefineClass(final Type classType, InputStream byteCodeStream,
                                                    final ConstructorRewriter rewriter) throws IOException {
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
              && Type.getObjectType(owner).equals(classType)
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
              rewriter.onMethodExit(this);
            }
          }
        };
      }
    }, ClassReader.EXPAND_FRAMES);

    byte[] byteCode = cw.toByteArray();
    return defineClass(classType.getClassName(), byteCode, 0, byteCode.length);
  }

  /**
   * Defines a class by rewriting all calls to {@link System#setProperty(String, String)} to
   * {@link SparkRuntimeEnv#setProperty(String, String)}.
   *
   * @param name name of the class to define
   * @param byteCodeStream {@link InputStream} for reading in the original bytecode.
   * @return a defined class
   */
  private Class<?> rewriteSetPropertiesAndDefineClass(String name, InputStream byteCodeStream) throws IOException {
    final Type systemType = Type.getType(System.class);
    ClassReader cr = new ClassReader(byteCodeStream);
    ClassWriter cw = new ClassWriter(0);

    cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {
      @Override
      public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
        return new MethodVisitor(Opcodes.ASM5, mv) {
          @Override
          public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
            // If we see a call to System.setProperty, change it to SparkRuntimeEnv.setProperty
            if (opcode == Opcodes.INVOKESTATIC && name.equals("setProperty")
                && owner.equals(systemType.getInternalName())) {
              super.visitMethodInsn(opcode, SPARK_RUNTIME_ENV_TYPE.getInternalName(), name, desc, false);
            } else {
              super.visitMethodInsn(opcode, owner, name, desc, itf);
            }
          }
        };
      }
    }, ClassReader.EXPAND_FRAMES);

    byte[] byteCode = cw.toByteArray();
    return defineClass(name, byteCode, 0, byteCode.length);
  }

  /**
   * Define the akka.remote.Remoting by rewriting usages of scala.concurrent.ExecutionContext.Implicits.global
   * to Remoting.system().dispatcher() in the shutdown() method for fixing the Akka thread/permgen leak bug in
   * https://github.com/akka/akka/issues/17729
   */
  private Class<?> defineAkkaRemoting(String name,
                                      InputStream byteCodeStream) throws IOException, ClassNotFoundException {
    final Type dispatcherReturnType = determineAkkaDispatcherReturnType();
    if (dispatcherReturnType == null) {
      LOG.warn("Failed to determine ActorSystem.dispatcher() return type. " +
                 "No rewriting of akka.remote.Remoting class. ClassLoader leakage might happen in SDK.");
      return findClass(name);
    }

    ClassReader cr = new ClassReader(byteCodeStream);
    ClassWriter cw = new ClassWriter(0);

    cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {
      @Override
      public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        // Call super so that the method signature is registered with the ClassWriter (parent)
        MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);

        // Only rewrite the shutdown() method
        if (!"shutdown".equals(name)) {
          return mv;
        }

        return new MethodVisitor(Opcodes.ASM5, mv) {
          @Override
          public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
            // Detect if it is making call "import scala.concurrent.ExecutionContext.Implicits.global",
            // which translate to Java code as
            // scala.concurrent.ExecutionContext$Implicits$.MODULE$.global()
            // hence as bytecode
            // GETSTATIC scala/concurrent/ExecutionContext$Implicits$.MODULE$ :
            //           Lscala/concurrent/ExecutionContext$Implicits$;
            // INVOKEVIRTUAL scala/concurrent/ExecutionContext$Implicits$.global
            //           ()Lscala/concurrent/ExecutionContextExecutor;
            if (opcode == Opcodes.INVOKEVIRTUAL
                && "global".equals(name)
                && "scala/concurrent/ExecutionContext$Implicits$".equals(owner)
                && Type.getMethodDescriptor(EXECUTION_CONTEXT_EXECUTOR_TYPE).equals(desc)) {
              // Discard the GETSTATIC result from the stack by popping it
              super.visitInsn(Opcodes.POP);
              // Make the call "import system.dispatch", which translate to Java code as
              // this.system().dispatcher()
              // hence as bytecode
              // ALOAD 0 (load this)
              // INVOKEVIRTUAL akka/remote/Remoting.system ()Lakka/actor/ExtendedActorSystem;
              // INVOKEVIRTUAL akka/actor/ExtendedActorSystem.dispatcher ()Lscala/concurrent/ExecutionContextExecutor;
              Type extendedActorSystemType = Type.getObjectType("akka/actor/ExtendedActorSystem");
              super.visitVarInsn(Opcodes.ALOAD, 0);
              super.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "akka/remote/Remoting", "system",
                                    Type.getMethodDescriptor(extendedActorSystemType), false);
              super.visitMethodInsn(Opcodes.INVOKEVIRTUAL, extendedActorSystemType.getInternalName(), "dispatcher",
                                    Type.getMethodDescriptor(dispatcherReturnType), false);
            } else {
              // For other instructions, just call parent to deal with it
              super.visitMethodInsn(opcode, owner, name, desc, itf);
            }
          }
        };
      }
    }, ClassReader.EXPAND_FRAMES);

    byte[] byteCode = cw.toByteArray();
    return defineClass(name, byteCode, 0, byteCode.length);
  }

  /**
   * Find the return type of the ActorSystem.dispatcher() method. It is ExecutionContextExecutor in
   * Akka 2.3 (Spark 1.2+) and ExecutionContext in Akka 2.2 (Spark < 1.2, which CDAP doesn't support,
   * however the Spark 1.5 in CDH 5.6. still has Akka 2.2, instead of 2.3).
   *
   * @return the return type of the ActorSystem.dispatcher() method or {@code null} if no such method
   */
  @Nullable
  private Type determineAkkaDispatcherReturnType() {
    try (InputStream is = openResource("akka/actor/ActorSystem.class")) {
      if (is == null) {
        return null;
      }
      final AtomicReference<Type> result = new AtomicReference<>();
      ClassReader cr = new ClassReader(is);
      cr.accept(new ClassVisitor(Opcodes.ASM5) {
        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
          if (name.equals("dispatcher") && Type.getArgumentTypes(desc).length == 0) {
            // Expected to be either ExecutionContext (akka 2.2, only in CDH spark)
            // or ExecutionContextExecutor (akka 2.3, for open source, HDP spark).
            Type returnType = Type.getReturnType(desc);
            if (returnType.equals(EXECUTION_CONTEXT_TYPE)
              || returnType.equals(EXECUTION_CONTEXT_EXECUTOR_TYPE)) {
              result.set(returnType);
            } else {
              LOG.warn("Unsupported return type of ActorSystem.dispatcher(): {}", returnType.getClassName());
            }
          }
          return super.visitMethod(access, name, desc, signature, exceptions);
        }
      }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_CODE | ClassReader.SKIP_FRAMES);
      return result.get();
    } catch (IOException e) {
      LOG.warn("Failed to determine ActorSystem dispatcher() return type.", e);
      return null;
    }
  }

  /**
   * Defines the org.apache.spark.deploy.yarn.Client class with rewriting of the createConfArchive method to
   * workaround the SPARK-13441 bug.
   */
  private Class<?> defineClient(String name, InputStream createConfArchive) throws IOException, ClassNotFoundException {
    // We only need to rewrite if listing either HADOOP_CONF_DIR or YARN_CONF_DIR return null.
    boolean needRewrite = false;
    for (String env : ImmutableList.of("HADOOP_CONF_DIR", "YARN_CONF_DIR")) {
      String value = System.getenv(env);
      if (value != null) {
        File path = new File(value);
        if (path.isDirectory() && path.listFiles() == null) {
          needRewrite = true;
          break;
        }
      }
    }

    // If rewrite is not needed
    if (!needRewrite) {
      return findClass(name);
    }

    ClassReader cr = new ClassReader(createConfArchive);
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {
      @Override
      public MethodVisitor visitMethod(final int access, final String name,
                                       final String desc, String signature, String[] exceptions) {
        MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);

        // Only rewrite the createConfArchive method
        if (!"createConfArchive".equals(name)) {
          return mv;
        }

        // Check if it's a recognizable return type.
        // Spark 1.5+ return type is File
        boolean isReturnFile = Type.getReturnType(desc).equals(Type.getType(File.class));
        Type optionType = Type.getObjectType("scala/Option");
        if (!isReturnFile) {
          // Spark 1.4 return type is Option<File>
          if (!Type.getReturnType(desc).equals(optionType)) {
            // Unknown type. Not going to modify the code.
            return mv;
          }
        }

        // Generate this for Spark 1.5+
        // return SparkRuntimeUtils.createConfArchive(this.sparkConf, SPARK_CONF_FILE,
        //                                            LOCALIZED_CONF_DIR, LOCALIZED_CONF_DIR_ZIP);
        // Generate this for Spark 1.4
        // return Option.apply(SparkRuntimeUtils.createConfArchive(this.sparkConf, SPARK_CONF_FILE,
        //                                                         LOCALIZED_CONF_DIR, LOCALIZED_CONF_DIR_ZIP));
        GeneratorAdapter mg = new GeneratorAdapter(mv, access, name, desc);

        // load this.sparkConf to the stack
        mg.loadThis();
        mg.getField(Type.getObjectType("org/apache/spark/deploy/yarn/Client"), "sparkConf", SPARK_CONF_TYPE);

        // push three constants to the stack
        mg.visitLdcInsn(SPARK_CONF_FILE);
        mg.visitLdcInsn(LOCALIZED_CONF_DIR);
        mg.visitLdcInsn(LOCALIZED_CONF_DIR_ZIP);

        // call SparkRuntimeUtils.createConfArchive, return a File and leave it in stack
        Type stringType = Type.getType(String.class);
        mg.invokeStatic(SPARK_RUNTIME_UTILS_TYPE,
                        new Method("createConfArchive", Type.getType(File.class),
                                   new Type[] { SPARK_CONF_TYPE, stringType, stringType, stringType}));
        if (isReturnFile) {
          // Spark 1.5+ return type is File, hence just return the File from the stack
          mg.returnValue();
          mg.endMethod();
        } else {
          // Spark 1.4 return type is Option<File>
          // return Option.apply(<file from stack>);
          // where the file is actually just popped from the stack
          mg.invokeStatic(optionType, new Method("apply", optionType, new Type[] { Type.getType(Object.class) }));
          mg.checkCast(optionType);
          mg.returnValue();
          mg.endMethod();
        }

        return null;
      }
    }, ClassReader.EXPAND_FRAMES);

    byte[] byteCode = cw.toByteArray();
    return defineClass(name, byteCode, 0, byteCode.length);
  }

  @Nullable
  private InputStream openResource(String resourceName) throws IOException {
    URL resource = findResource(resourceName);
    return resource == null ? null : resource.openStream();
  }

  /**
   * Private interface for rewriting constructor.
   */
  private interface ConstructorRewriter {
    void onMethodExit(GeneratorAdapter generatorAdapter);
  }
}
