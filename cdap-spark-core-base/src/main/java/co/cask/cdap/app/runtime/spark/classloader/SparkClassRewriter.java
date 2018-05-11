/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark.classloader;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.spark.SparkPackageUtils;
import co.cask.cdap.app.runtime.spark.SparkRuntimeEnv;
import co.cask.cdap.common.lang.ClassRewriter;
import co.cask.cdap.common.logging.RedirectedPrintStream;
import co.cask.cdap.internal.asm.Classes;
import co.cask.cdap.internal.asm.Methods;
import co.cask.cdap.internal.asm.Signatures;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
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
import java.io.PrintStream;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * A {@link ClassRewriter} for rewriting Spark related classes.
 */
public class SparkClassRewriter implements ClassRewriter {

  private static final Logger LOG = LoggerFactory.getLogger(SparkClassRewriter.class);
  private static final Type[] EMPTY_ARGS = new Type[0];

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
  private static final Type SPARK_PYTHON_RUNNER_TYPE = Type.getObjectType("org/apache/spark/deploy/PythonRunner");
  // The PythonRunner companion object, hence the "$" at the end
  private static final Type SPARK_PYTHON_RUNNER_COMPANION_TYPE =
    Type.getObjectType("org/apache/spark/deploy/PythonRunner$");
  private static final Type SPARK_PYTHON_WORKER_FACTORY_TYPE =
    Type.getObjectType("org/apache/spark/api/python/PythonWorkerFactory");
  private static final Type SPARK_PYTHON_WORKER_MONITOR_THREAD_TYPE =
    Type.getObjectType("org/apache/spark/api/python/PythonWorkerFactory$MonitorThread");
  private static final Type SPARK_REDIRECT_THREAD = Type.getObjectType("org/apache/spark/util/RedirectThread");
  private static final Type SPARK_YARN_CLIENT_TYPE = Type.getObjectType("org/apache/spark/deploy/yarn/Client");
  private static final Type SPARK_DSTREAM_GRAPH_TYPE = Type.getObjectType("org/apache/spark/streaming/DStreamGraph");
  private static final Type SPARK_BATCHED_WRITE_AHEAD_LOG_TYPE =
    Type.getObjectType("org/apache/spark/streaming/util/BatchedWriteAheadLog");
  private static final Type RATE_CONTROLLER_TYPE =
    Type.getObjectType("org/apache/spark/streaming/scheduler/RateController");
  private static final Type SPARK_EXECUTOR_CLASSLOADER_TYPE =
    Type.getObjectType("org/apache/spark/repl/ExecutorClassLoader");
  private static final Type YARN_SPARK_HADOOP_UTIL_TYPE =
    Type.getObjectType("org/apache/spark/deploy/yarn/YarnSparkHadoopUtil");
  private static final Type KRYO_TYPE = Type.getObjectType("com/esotericsoftware/kryo/Kryo");
  private static final Type SCHEMA_SERIALIZER_TYPE =
    Type.getObjectType("co/cask/cdap/app/runtime/spark/serializer/SchemaSerializer");
  private static final Type STRUCTURED_RECORD_SERIALIZER_TYPE =
    Type.getObjectType("co/cask/cdap/app/runtime/spark/serializer/StructuredRecordSerializer");
  private static final Type SPARK_DISK_STORE = Type.getObjectType("org/apache/spark/storage/DiskStore");

  // Don't refer akka Remoting with the ".class" because in future Spark version, akka dependency is removed and
  // we don't want to force a dependency on akka.
  private static final Type AKKA_REMOTING_TYPE = Type.getObjectType("akka/remote/Remoting");
  private static final Type EXECUTION_CONTEXT_TYPE = Type.getObjectType("scala/concurrent/ExecutionContext");
  private static final Type EXECUTION_CONTEXT_EXECUTOR_TYPE =
    Type.getObjectType("scala/concurrent/ExecutionContextExecutor");

  private static final Type NETTY_REFERENCE_COUNTED_TYPE = Type.getObjectType("io/netty/util/ReferenceCounted");
  private static final Type NETTY_FILE_REGION_TYPE = Type.getObjectType("io/netty/channel/FileRegion");
  private static final List<Method> NETTY_FILE_REGION_RC_METHODS = Arrays.asList(
    new Method("retain", NETTY_REFERENCE_COUNTED_TYPE, EMPTY_ARGS),
    new Method("retain", NETTY_REFERENCE_COUNTED_TYPE, new Type[] { Type.INT_TYPE }),
    new Method("touch", NETTY_REFERENCE_COUNTED_TYPE, EMPTY_ARGS),
    new Method("touch", NETTY_REFERENCE_COUNTED_TYPE, new Type[] { Type.getType(Object.class) })
  );

  // File name of the Spark conf directory as defined by the Spark framework
  // This is for the Hack to workaround CDAP-5019 (SPARK-13441)
  private static final String LOCALIZED_CONF_DIR = SparkPackageUtils.LOCALIZED_CONF_DIR;
  // File entry name of the SparkConf properties file inside the Spark conf zip
  private static final String SPARK_CONF_FILE = "__spark_conf__.properties";

  private final Function<String, URL> resourceLookup;
  private final boolean rewriteYarnClient;
  private final boolean distributed;

  public SparkClassRewriter(Function<String, URL> resourceLookup, boolean rewriteYarnClient) {
    this.resourceLookup = resourceLookup;
    this.rewriteYarnClient = rewriteYarnClient;
    this.distributed = Boolean.parseBoolean(System.getenv("SPARK_YARN_MODE"));
  }

  @Nullable
  @Override
  public byte[] rewriteClass(String className, InputStream input) throws IOException {
    if (className.equals(SPARK_CONTEXT_TYPE.getClassName())) {
      // Rewrite the SparkContext class by rewriting the constructor to save the context to SparkRuntimeEnv
      return rewriteContext(SPARK_CONTEXT_TYPE, input);
    }
    if (className.equals(SPARK_STREAMING_CONTEXT_TYPE.getClassName())) {
      // Rewrite the StreamingContext class by rewriting the constructor to save the context to SparkRuntimeEnv
      return rewriteContext(SPARK_STREAMING_CONTEXT_TYPE, input);
    }
    if (className.equals(SPARK_CONF_TYPE.getClassName())) {
      // Define the SparkConf class by rewriting the class to put all properties from
      // SparkRuntimeEnv to the SparkConf in the constructors
      return rewriteSparkConf(SPARK_CONF_TYPE, input);
    }
    if (className.startsWith(SPARK_SUBMIT_TYPE.getClassName())) {
      // Rewrite System.setProperty call to SparkRuntimeEnv.setProperty for SparkSubmit and all inner classes
      return rewriteSetProperties(input);
    }
    if (className.equals(SPARK_PYTHON_RUNNER_TYPE.getClassName())) {
      // Rewrite the PythonRunner.main call to initialize CDAP spark context and catch exception to avoid system.exit
      return rewritePythonRunner(input);
    }
    if (className.equals(SPARK_PYTHON_RUNNER_COMPANION_TYPE.getClassName())) {
      // Rewrite all System.out and System.err redirected via RedirectedPrintStream
      return rewritePythonRunnerCompanion(input);
    }
    if (className.equals(SPARK_PYTHON_WORKER_FACTORY_TYPE.getClassName())) {
      // Rewrite the PythonWorkerFactory. See method for details.
      return rewritePythonWorkerFactory(input);
    }
    if (className.equals(SPARK_PYTHON_WORKER_MONITOR_THREAD_TYPE.getClassName())) {
      return rewritePythonWorkerMonitorThread(input);
    }
    if (className.equals(SPARK_YARN_CLIENT_TYPE.getClassName()) && rewriteYarnClient) {
      // Rewrite YarnClient for workaround SPARK-13441.
      return rewriteClient(input);
    }
    if (className.equals(SPARK_DSTREAM_GRAPH_TYPE.getClassName())) {
      // Rewrite DStreamGraph to set TaskSupport on parallel array usage to avoid Thread leak
      return rewriteDStreamGraph(input);
    }
    if (className.equals(SPARK_BATCHED_WRITE_AHEAD_LOG_TYPE.getClassName())) {
      // Rewrite BatchedWriteAheadLog to register it in SparkRuntimeEnv so that we can free up the batch writer thread
      // even there is no Receiver based DStream (it's a thread leak from Spark) (CDAP-11577) (SPARK-20935)
      return rewriteBatchedWriteAheadLog(input);
    }
    if (className.equals(RATE_CONTROLLER_TYPE.getClassName())) {
      // Rewrite the RateController class to avoid leaking a "stream-rate-update"
      // thread when back pressure is on (CDAP-11939).
      return rewriteRateController(input);
    }
    if (className.equals(SPARK_EXECUTOR_CLASSLOADER_TYPE.getClassName())) {
      // Rewrite the Spark repl ExecutorClassLoader to call `super(null)` so that it won't use the system classloader
      // as parent
      return rewriteExecutorClassLoader(input);
    }
    if (className.equals(AKKA_REMOTING_TYPE.getClassName())) {
      // Define the akka.remote.Remoting class to avoid thread leakage
      return rewriteAkkaRemoting(input);
    }
    if (className.equals(YARN_SPARK_HADOOP_UTIL_TYPE.getClassName())) {
      // CDAP-8636 Rewrite methods of YarnSparkHadoopUtil to avoid acquiring delegation token, because when we execute
      // spark submit, we don't have keytab login
      return rewriteSparkHadoopUtil(className, input);
    }
    if (className.equals(KRYO_TYPE.getClassName())) {
      // CDAP-9314 Rewrite the Kryo constructor to register serializer for CDAP classes
      return rewriteKryo(input);
    }
    if (className.equals(SPARK_DISK_STORE.getClassName()) || className.startsWith("org.apache.spark.network.")) {
      // Rewrite Spark DiskStore class and classes in the network package for Netty 4.1 compatibility
      return rewriteSparkNetworkClass(input);
    }

    return null;
  }

  /**
   * Rewrites the constructor to call SparkRuntimeEnv#setContext(SparkContext)
   * or SparkRuntimeEnv#setContext(StreamingContext), depending on the class type.
   */
  private byte[] rewriteContext(final Type contextType, InputStream byteCodeStream) throws IOException {
    return rewriteConstructor(contextType, byteCodeStream, new ConstructorRewriter() {

      @Override
      void onMethodEnter(String name, String desc, GeneratorAdapter generatorAdapter) {
        Type[] argTypes = Type.getArgumentTypes(desc);
        // If the constructor has SparkConf as arguments,
        // update the SparkConf by calling SparkRuntimeEnv.setupSparkConf(sparkConf)
        // This is mainly to make any runtime properties setup by CDAP are being pickup even restoring
        // from Checkpointing.
        List<Integer> confIndices = new ArrayList<>();
        for (int i = 0; i < argTypes.length; i++) {
          if (SPARK_CONF_TYPE.equals(argTypes[i])) {
            confIndices.add(i);
          }
        }

        // Update all SparkConf arguments.
        for (int confIndex : confIndices) {
          generatorAdapter.loadArg(confIndex);
          generatorAdapter.invokeStatic(SPARK_RUNTIME_ENV_TYPE,
                                        new Method("setupSparkConf", Type.VOID_TYPE, new Type[] { SPARK_CONF_TYPE }));
        }
      }

      @Override
      public void onMethodExit(String name, String desc, GeneratorAdapter generatorAdapter) {
        generatorAdapter.loadThis();
        generatorAdapter.invokeStatic(SPARK_RUNTIME_ENV_TYPE,
                                      new Method("setContext", Type.VOID_TYPE, new Type[] { contextType }));
      }
    });
  }

  /**
   * Rewrites the SparkConf class constructor to call SparkRuntimeEnv#setupSparkConf(SparkConf).
   */
  private byte[] rewriteSparkConf(final Type sparkConfType, InputStream byteCodeStream) throws IOException {
    return rewriteConstructor(sparkConfType, byteCodeStream, new ConstructorRewriter() {
      @Override
      public void onMethodExit(String name, String desc, GeneratorAdapter generatorAdapter) {
        generatorAdapter.loadThis();
        generatorAdapter.invokeStatic(SPARK_RUNTIME_ENV_TYPE,
                                      new Method("setupSparkConf", Type.VOID_TYPE, new Type[] { sparkConfType }));
      }
    });
  }

  /**
   * Rewrites the DStreamGraph class for calls to parallel array with a call to
   * SparkRuntimeUtils#setTaskSupport(ParArray).
   */
  private byte[] rewriteDStreamGraph(InputStream byteCodeStream) throws IOException {
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

    return cw.toByteArray();
  }

  /**
   * Rewrites the BatchedWriteAheadLog class to register itself to SparkRuntimeEnv so that the write ahead log thread
   * can be shutdown when the Spark program finished.
   */
  private byte[] rewriteBatchedWriteAheadLog(InputStream byteCodeStream) throws IOException {
    return rewriteConstructor(SPARK_BATCHED_WRITE_AHEAD_LOG_TYPE, byteCodeStream, new ConstructorRewriter() {
      @Override
      public void onMethodExit(String name, String desc, GeneratorAdapter generatorAdapter) {
        generatorAdapter.loadThis();
        generatorAdapter.invokeStatic(SPARK_RUNTIME_ENV_TYPE,
                                      new Method("addBatchedWriteAheadLog", Type.VOID_TYPE,
                                                 new Type[] { Type.getType(Object.class) }));
      }
    });
  }

  /**
   * Rewrites the Spark streaming RateController.init() method to capture the executionContext and register it
   * to SparkRuntimeEnv so that the thread can be terminated on Spark program completion.
   */
  private byte[] rewriteRateController(InputStream byteCodeStream) throws IOException {
    return rewriteConstructor(RATE_CONTROLLER_TYPE, byteCodeStream, new ConstructorRewriter() {
      @Override
      void onMethodExit(String name, String desc, GeneratorAdapter generatorAdapter) {
        generatorAdapter.loadThis();
        generatorAdapter.invokeStatic(SPARK_RUNTIME_ENV_TYPE,
                                      new Method("addRateController", Type.VOID_TYPE,
                                                 new Type[] { Type.getType(Object.class) }));
      }
    });
  }

  /**
   * Rewrites the ExecutorClassLoader so that it won't use system classloader as parent since CDAP classes
   * are not in system classloader.
   * Also optionally overrides the getResource, getResources and getResourceAsStream methods if they are not
   * defined (for fixing SPARK-11818 for older Spark < 1.6).
   */
  private byte[] rewriteExecutorClassLoader(InputStream byteCodeStream) throws IOException {
    ClassReader cr = new ClassReader(byteCodeStream);
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);

    final Type classloaderType = Type.getType(ClassLoader.class);
    final Type parentClassLoaderType = Type.getObjectType("org/apache/spark/util/ParentClassLoader");
    final Method parentLoaderMethod = new Method("parentLoader", parentClassLoaderType, EMPTY_ARGS);

    // Map from getResource* methods to the method signature
    // (can be null, since only method that has generic has signature)
    final Map<Method, String> resourceMethods = new HashMap<>();
    Method method = new Method("getResource", Type.getType(URL.class), new Type[]{Type.getType(String.class)});
    resourceMethods.put(method, null);

    method = new Method("getResources", Type.getType(Enumeration.class), new Type[] { Type.getType(String.class) });
    resourceMethods.put(method, Signatures.getMethodSignature(method, new TypeToken<Enumeration<URL>>() { },
                                                              TypeToken.of(String.class)));

    method = new Method("getResourceAsStream", Type.getType(InputStream.class),
                        new Type[] { Type.getType(String.class) });
    resourceMethods.put(method, null);

    cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {

      private boolean hasParentLoader;
      private boolean rewriteInit;

      @Override
      public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        // Only rewrite `<init>` if the ExecutorClassloader extends from ClassLoader
        if (classloaderType.getInternalName().equals(superName)) {
          rewriteInit = true;
        }
        super.visit(version, access, name, signature, superName, interfaces);
      }

      @Override
      public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
        // If the resource method is declared, no need to generate at the end.
        Method method = new Method(name, desc);
        resourceMethods.remove(method);
        hasParentLoader = hasParentLoader || parentLoaderMethod.equals(method);

        if (!rewriteInit || !"<init>".equals(name)) {
          return mv;
        }
        return new GeneratorAdapter(Opcodes.ASM5, mv, access, name, desc) {

          @Override
          public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
            // If there is a call to `super()`, skip that instruction and have the onMethodEnter generate the call
            if (opcode == Opcodes.INVOKESPECIAL
                && Type.getObjectType(owner).equals(classloaderType)
                && name.equals("<init>")
                && Type.getArgumentTypes(desc).length == 0
                && Type.getReturnType(desc).equals(Type.VOID_TYPE)) {
              // Generate `super(null)`. The `this` is already in the stack, so no need to `loadThis()`
              push((Type) null);
              invokeConstructor(classloaderType, new Method("<init>", Type.VOID_TYPE, new Type[] { classloaderType }));
            } else {
              super.visitMethodInsn(opcode, owner, name, desc, itf);
            }
          }
        };
      }

      @Override
      public void visitEnd() {
        // See if needs to implement the getResource, getResources and getResourceAsStream methods
        // All implementations are delegating to the parentLoader
        if (!hasParentLoader) {
          super.visitEnd();
          return;
        }

        for (Map.Entry<Method, String> entry : resourceMethods.entrySet()) {
          // Generate the method.
          // return parentLoader().getResource*(arg)
          Method method = entry.getKey();
          MethodVisitor mv = super.visitMethod(Modifier.PUBLIC, method.getName(),
                                               method.getDescriptor(), entry.getValue(), null);
          GeneratorAdapter generator = new GeneratorAdapter(Modifier.PUBLIC, method, mv);

          // call `parentLoader()`
          generator.loadThis();
          generator.invokeVirtual(SPARK_EXECUTOR_CLASSLOADER_TYPE, parentLoaderMethod);

          // Load the argument
          generator.loadArg(0);

          // Call the method on the parent loader.
          generator.invokeVirtual(parentClassLoaderType, method);
          generator.returnValue();
          generator.endMethod();
        }
      }
    }, ClassReader.EXPAND_FRAMES);

    return cw.toByteArray();
  }

  /**
   * Rewrites the constructor of the Kryo class to add serializer for CDAP classes.
   *
   * @param byteCodeStream {@link InputStream} for reading the original bytecode of the class
   * @return the rewritten bytecode
   */
  private byte[] rewriteKryo(InputStream byteCodeStream) throws IOException {
    return rewriteConstructor(KRYO_TYPE, byteCodeStream, new ConstructorRewriter() {
      @Override
      public void onMethodExit(String name, String desc, GeneratorAdapter generatorAdapter) {
        // Register serializer for Schema
        // addDefaultSerializer(Schema.class, SchemaSerializer.class);
        generatorAdapter.loadThis();
        generatorAdapter.push(Type.getType(Schema.class));
        generatorAdapter.push(SCHEMA_SERIALIZER_TYPE);
        generatorAdapter.invokeVirtual(KRYO_TYPE,
                                       new Method("addDefaultSerializer", Type.VOID_TYPE,
                                                  new Type[] { Type.getType(Class.class), Type.getType(Class.class)}));

        // Register serializer for StructuredRecord
        // addDefaultSerializer(StructuredRecord.class, StructuredRecordSerializer.class);
        generatorAdapter.loadThis();
        generatorAdapter.push(Type.getType(StructuredRecord.class));
        generatorAdapter.push(STRUCTURED_RECORD_SERIALIZER_TYPE);
        generatorAdapter.invokeVirtual(KRYO_TYPE,
                                       new Method("addDefaultSerializer", Type.VOID_TYPE,
                                                  new Type[] { Type.getType(Class.class), Type.getType(Class.class)}));
      }
    });
  }

  /**
   * Rewrites the constructors who don't delegate to other constructor with the given {@link ConstructorRewriter}.
   *
   * @param classType type of the class to be defined
   * @param byteCodeStream {@link InputStream} for reading the original bytecode of the class
   * @param rewriter a {@link ConstructorRewriter} for rewriting the constructor
   * @return the rewritten bytecode
   */
  private byte[] rewriteConstructor(final Type classType, InputStream byteCodeStream,
                                    final ConstructorRewriter rewriter) throws IOException {
    ClassReader cr = new ClassReader(byteCodeStream);
    ClassWriter cw = new ClassWriter(0);

    cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {

      @Override
      public MethodVisitor visitMethod(int access, final String name,
                                       final String desc, String signature, String[] exceptions) {
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
          protected void onMethodEnter() {
            if (calledThis) {
              // For constructors that call this(), we don't need rewrite
              return;
            }
            rewriter.onMethodEnter(name, desc, this);
          }

          @Override
          protected void onMethodExit(int opcode) {
            if (calledThis) {
              // For constructors that call this(), we don't need rewrite
              return;
            }
            // Add a call to SparkContextCache.setContext() for the normal method return path
            if (opcode == RETURN) {
              rewriter.onMethodExit(name, desc, this);
            }
          }
        };
      }
    }, ClassReader.EXPAND_FRAMES);

    return cw.toByteArray();
  }



  /**
   * Rewrites a class by rewriting all calls to {@link System#setProperty(String, String)} to
   * {@link SparkRuntimeEnv#setProperty(String, String)}.
   *
   * @param byteCodeStream {@link InputStream} for reading in the original bytecode.
   * @return the rewritten bytecode
   */
  private byte[] rewriteSetProperties(InputStream byteCodeStream) throws IOException {
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

    return cw.toByteArray();
  }

  /**
   * Rewrites the PythonRunner.main() method to wrap it with call to SparkRuntimeUtils.initSparkMain() method on
   * enter and cancel on exit. Also, wrap the PythonRunner.main() call with a try block to catch the
   * {@code SparkUserAppException} into a {@link RuntimeException} to avoid Spark calling System.exit in
   * {@link SparkSubmit}.
   */
  private byte[] rewritePythonRunner(InputStream byteCodeStream) throws IOException {
    ClassReader cr = new ClassReader(byteCodeStream);
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES) {
      // Must sub-class it. It is for COMPUTE_FRAMES to work (CDAP-12888).
      // With COMPUTE_FRAMES, the ASM library uses getClass().getClassLoader() in the
      // getCommonSuperClass() method. Without sub-classing, the ClassWriter class would be coming
      // from the system classloader, which doesn't have Scala classes inside.
      // By sub-classing, the getClass().getClassLoader() would return the classloader of the sub-class,
      // which is the spark extension classloader, hence containing all Spark and Scala classes.
    };

    // Intercept the static void main(String[] args) method.
    final Method mainMethod = new Method("main", Type.VOID_TYPE, new Type[] { Type.getType(String[].class) });
    cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {
      @Override
      public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
        if (!mainMethod.equals(new Method(name, desc)) || !Modifier.isStatic(access)) {
          return mv;
        }

        // Wrap the original main method with
        // SparkProgramCompletion completion = SparkRuntimeUtils.initSparkMain();
        // try {
        //   // original main() body
        //   completion.completed();
        // } catch (Throwable t) {
        //   completion.completedWithException(t);
        //   // Wrap it with RuntimeException to prevent SparkSubmit calling System.exit in local mode
        //   [local_mode] throw (t instance of SparkUserAppException) ? new RuntimeException(t) : t;
        //   [distributed_mode] throw t;
        // }
        return new AdviceAdapter(Opcodes.ASM5, mv, access, name, desc) {

          final Type sparkUserAppExceptionType = Type.getObjectType("org/apache/spark/SparkUserAppException");
          final Type throwableType = Type.getType(Throwable.class);
          final Type completionType = Type.getObjectType("co/cask/cdap/app/runtime/spark/SparkProgramCompletion");
          final Label tryLabel = newLabel();
          final Label tryEndLabel = newLabel();
          final Label catchLabel = newLabel();
          final Label endLabel = newLabel();
          int completion;

          @Override
          protected void onMethodEnter() {
            completion = newLocal(completionType);
            invokeStatic(SPARK_RUNTIME_UTILS_TYPE, new Method("initSparkMain", completionType, EMPTY_ARGS));
            storeLocal(completion);

            // try {
            visitTryCatchBlock(tryLabel, tryEndLabel, catchLabel, throwableType.getInternalName());
            visitLabel(tryLabel);
          }

          @Override
          protected void onMethodExit(int opcode) {
            // completion.completed();
            loadLocal(completion);
            invokeInterface(completionType, new Method("completed", Type.VOID_TYPE, EMPTY_ARGS));
            visitLabel(tryEndLabel);
            goTo(endLabel);

            // catch (Throwable t)
            visitLabel(catchLabel);
            int throwable = newLocal(throwableType);
            storeLocal(throwable);

            // completion.completedWithException(t);
            loadLocal(completion);
            loadLocal(throwable);
            invokeInterface(completionType, new Method("completedWithException",
                                                       Type.VOID_TYPE, new Type[] { throwableType }));

            // load the Throwable. If it is local mode, generate the wrapping logic. Otherwise we can just throw it
            loadLocal(throwable);
            Label throwLabel = newLabel();

            if (!distributed) {
              // Make
              // (t instance of SparkUserAppException) ? new RuntimeException(t) : t
              // in the top of the stack
              Label nonSparkUserAppException = newLabel();
              instanceOf(sparkUserAppExceptionType);
              ifZCmp(EQ, nonSparkUserAppException);
              // If the type is a SparkUserAppException, wrap it with RuntimeException
              newInstance(Type.getType(RuntimeException.class));
              dup();
              loadLocal(throwable);
              invokeConstructor(Type.getType(RuntimeException.class),
                                Methods.getMethod(void.class, "<init>", Throwable.class));
              goTo(throwLabel);

              // Otherwise, no need to wrap
              visitLabel(nonSparkUserAppException);
              loadLocal(throwable);
            }

            visitLabel(throwLabel);
            throwException();
            visitLabel(endLabel);
          }
        };
      }
    }, ClassReader.EXPAND_FRAMES);
    return cw.toByteArray();
  }

  /**
   * Rewrites the PythonRunner companion class to have all System.out/err goes into Logger.
   */
  private byte[] rewritePythonRunnerCompanion(InputStream byteCodeStream) throws IOException {
    ClassReader cr = new ClassReader(byteCodeStream);
    ClassWriter cw = new ClassWriter(0);

    cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {
      @Override
      public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        // Intercept all methods
        MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
        return new OutputRedirectMethodVisitor(mv, access, name, desc, distributed);
      }
    }, ClassReader.EXPAND_FRAMES);

    return cw.toByteArray();
  }

  /**
   * Rewrite all System.out and System.err redirected via RedirectedPrintStream.
   * Also update pythonPath field in local mode to include the pyspark library
   */
  private byte[] rewritePythonWorkerFactory(InputStream byteCodeStream) throws IOException {
    ClassReader cr = new ClassReader(byteCodeStream);
    ClassWriter cw = new ClassWriter(0);

    cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {

      @Override
      public MethodVisitor visitMethod(int access, String name,
                                       String desc, String signature, String[] exceptions) {
        // Intercept all methods. Always redirect outputs
        MethodVisitor mv = new OutputRedirectMethodVisitor(super.visitMethod(access, name, desc, signature, exceptions),
                                                           access, name, desc, distributed);

        final GeneratorAdapter adapter = new GeneratorAdapter(mv, access, name, desc);
        return new MethodVisitor(Opcodes.ASM5, mv) {

          @Override
          public void visitFieldInsn(int opcode, String owner, String name, String desc) {
            // Rewrite the pythonPath field when setting the field value
            if (opcode == Opcodes.PUTFIELD && "pythonPath".equals(name)) {
              Type stringType = Type.getType(String.class);

              // Generates
              // if (SparkRuntimeEnv.getProperty("cdap.spark.pyFiles") != null) {
              //   <stringOnStack>.concat(File.pathSeparator).concat(SparkRuntimeEnv.getProperty("cdap.spark.pyFiles"))
              // }
              // The result will be back on the stack
              // The "cdap.spark.pyFiles" property is only set in local mode by SparkRuntimeService
              Label nullLabel = adapter.newLabel();
              // The if condition
              adapter.push("cdap.spark.pyFiles");
              adapter.invokeStatic(SPARK_RUNTIME_ENV_TYPE,
                                   Methods.getMethod(String.class, "getProperty", String.class));
              adapter.ifNull(nullLabel);

              // Inside the if block
              adapter.push(File.pathSeparator);
              adapter.invokeVirtual(stringType, Methods.getMethod(String.class, "concat", String.class));
              adapter.push("cdap.spark.pyFiles");
              adapter.invokeStatic(SPARK_RUNTIME_ENV_TYPE,
                                   Methods.getMethod(String.class, "getProperty", String.class));
              adapter.invokeVirtual(stringType, Methods.getMethod(String.class, "concat", String.class));
              // End of if block
              adapter.mark(nullLabel);
            }

            super.visitFieldInsn(opcode, owner, name, desc);
          }
        };
      }
    }, ClassReader.EXPAND_FRAMES);

    return cw.toByteArray();
  }

  /**
   * Rewrites the PythonWorkerFactory MonitorThread so that it can be interrupted when the Spark program finished.
   * This rewrite is only needed in local mode to avoid thread leakage.
   */
  @Nullable
  private byte[] rewritePythonWorkerMonitorThread(InputStream byteCodeStream) throws IOException {
    if (distributed) {
      return null;
    }
    return rewriteConstructor(SPARK_PYTHON_WORKER_MONITOR_THREAD_TYPE, byteCodeStream, new ConstructorRewriter() {
      @Override
      void onMethodEnter(String name, String desc, GeneratorAdapter generatorAdapter) {
        generatorAdapter.loadThis();
        generatorAdapter.invokeStatic(SPARK_RUNTIME_ENV_TYPE,
                                      new Method("addPyMonitorThread", Type.VOID_TYPE,
                                                 new Type[] { Type.getType(Thread.class) }));
      }
    });
  }

  /**
   * Rewrites the akka.remote.Remoting by rewriting usages of scala.concurrent.ExecutionContext.Implicits.global
   * to Remoting.system().dispatcher() in the shutdown() method for fixing the Akka thread/permgen leak bug in
   * https://github.com/akka/akka/issues/17729.
   *
   * @return the rewritten bytes or {@code null} if no rewriting is needed
   */
  @Nullable
  private byte[] rewriteAkkaRemoting(InputStream byteCodeStream) throws IOException {
    final Type dispatcherReturnType = determineAkkaDispatcherReturnType();
    if (dispatcherReturnType == null) {
      LOG.warn("Failed to determine ActorSystem.dispatcher() return type. " +
                 "No rewriting of akka.remote.Remoting class. ClassLoader leakage might happen in SDK.");
      return null;
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

    return cw.toByteArray();
  }

  /**
   * Rewrite Spark classes in the network package for Netty 4.1 compatibility.
   *
   * <ol>
   *   <li>FileRegion interface has a new method, transferred() that replaces the old transfered() method</li>
   *   <li>
   *     FileRegion interface, which implements ReferenceCounted interface, has a new touch(Object hint) method
   *     that is not implemented by the AbstractReferenceCounted based class.
   *   </li>
   * </ol>
   *
   * @param input the source bytecode
   * @return the rewritten class or {@code null} if rewrite is not needed
   */
  @Nullable
  private byte[] rewriteSparkNetworkClass(InputStream byteCodeStream) throws IOException {
    ClassReader cr = new ClassReader(byteCodeStream);
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);

    // Scan for class type and methods to see if the rewriting is needed
    final AtomicBoolean rewritten = new AtomicBoolean(false);
    cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {

      private Type classType;
      private boolean isFileRegion;
      private boolean hasTransferredMethod;
      private boolean hasTouchMethod;

      @Override
      public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        // See if it implements netty FileRegion.
        if (interfaces != null && Arrays.asList(interfaces).contains(NETTY_FILE_REGION_TYPE.getInternalName())) {
          isFileRegion = true;
          classType = Type.getObjectType(name);
        }
        super.visit(version, access, name, signature, superName, interfaces);
      }

      @Override
      public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        // See if the class has the method `transferred()`.
        if ("transferred".equals(name) && Type.getArgumentTypes(desc).length == 0) {
          hasTransferredMethod = true;
        }
        // See if the class has the method `touch(Object)`.
        if ("touch".equals(name)) {
          Type[] args = Type.getArgumentTypes(desc);
          if (args.length == 1 && Type.getType(Object.class).equals(args[0])) {
            hasTouchMethod = true;
          }
        }
        return super.visitMethod(access, name, desc, signature, exceptions);
      }

      @Override
      public void visitEnd() {
        if (isFileRegion) {
          if (!hasTransferredMethod) {
            // Generate the `long transferred()` method by calling `return transfered();` method
            Method method = new Method("transferred", Type.LONG_TYPE, EMPTY_ARGS);
            MethodVisitor mv = super.visitMethod(Opcodes.ACC_PUBLIC, method.getName(),
                                                 method.getDescriptor(), null, null);
            GeneratorAdapter generator = new GeneratorAdapter(Opcodes.ACC_PUBLIC, method, mv);
            generator.loadThis();
            generator.invokeVirtual(classType, new Method("transfered", Type.LONG_TYPE, EMPTY_ARGS));
            generator.returnValue();
            generator.endMethod();

            rewritten.set(true);
          }
          if (!hasTouchMethod) {
            // Generate four (method, synthetic method) pairs from the ReferenceCounted interface
            // that FileRegion overridden to have FileRegion as return type
            for (Method m : NETTY_FILE_REGION_RC_METHODS) {
              // Need to generate the actual implementation of the touch methods
              if (m.getName().equals("touch")) {
                String desc = Type.getMethodDescriptor(NETTY_FILE_REGION_TYPE, m.getArgumentTypes());
                MethodVisitor mv = super.visitMethod(Opcodes.ACC_PUBLIC, m.getName(), desc, null, null);
                GeneratorAdapter generator = new GeneratorAdapter(mv, Opcodes.ACC_PUBLIC, m.getName(), desc);
                generator.loadThis();
                generator.returnValue();
                generator.endMethod();
              }

              // Generate the synthetic method by just calling the actual method
              int syntheticAccess = Opcodes.ACC_PUBLIC | Opcodes.ACC_BRIDGE | Opcodes.ACC_SYNTHETIC;
              MethodVisitor mv = super.visitMethod(syntheticAccess, m.getName(), m.getDescriptor(), null, null);
              GeneratorAdapter generator = new GeneratorAdapter(syntheticAccess, m, mv);
              generator.loadThis();
              for (int i = 0; i < m.getArgumentTypes().length; i++) {
                generator.loadArg(i);
              }
              generator.invokeVirtual(classType, new Method(m.getName(), NETTY_FILE_REGION_TYPE, m.getArgumentTypes()));
              generator.returnValue();
              generator.endMethod();
            }

            rewritten.set(true);
          }
        }
        super.visitEnd();
      }
    }, 0);

    return rewritten.get() ? cw.toByteArray() : null;
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
    URL resource = resourceLookup.apply("akka/actor/ActorSystem.class");
    if (resource == null) {
      return null;
    }

    try (InputStream is = resource.openStream()) {
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
  @Nullable
  private byte[] rewriteClient(InputStream byteCodeStream) throws IOException {
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
      return null;
    }

    ClassReader cr = new ClassReader(byteCodeStream);
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

        Type fileType = Type.getType(File.class);
        Type stringType = Type.getType(String.class);

        // Check if it's a recognizable return type.
        // Spark 1.5+ return type is File
        boolean isReturnFile = Type.getReturnType(desc).equals(fileType);
        Type optionType = Type.getObjectType("scala/Option");
        if (!isReturnFile) {
          // Spark 1.4 return type is Option<File>
          if (!Type.getReturnType(desc).equals(optionType)) {
            // Unknown type. Not going to modify the code.
            return mv;
          }
        }

        // Generate this for Spark 1.5+
        // return SparkRuntimeUtils.createConfArchive(this.sparkConf, SPARK_CONF_FILE, LOCALIZED_CONF_DIR,
        //                                            File.createTempFile(LOCALIZED_CONF_DIR, ".zip"));
        // Generate this for Spark 1.4
        // return Option.apply(SparkRuntimeUtils.createConfArchive(this.sparkConf, SPARK_CONF_FILE, LOCALIZED_CONF_DIR,
        //                                                         File.createTempFile(LOCALIZED_CONF_DIR, ".zip")));
        GeneratorAdapter mg = new GeneratorAdapter(mv, access, name, desc);

        // Push the four parameters for the SparkRuntimeUtils.createConfArchive method
        // load this.sparkConf to the stack
        mg.loadThis();
        mg.getField(Type.getObjectType("org/apache/spark/deploy/yarn/Client"), "sparkConf", SPARK_CONF_TYPE);
        mg.visitLdcInsn(SPARK_CONF_FILE);
        mg.visitLdcInsn(LOCALIZED_CONF_DIR);

        // Call the File.createTempFile(LOCALIZED_CONF_DIR, ".zip") to generate the forth parameter
        mg.visitLdcInsn(LOCALIZED_CONF_DIR);
        mg.visitLdcInsn(".zip");
        mg.invokeStatic(fileType, new Method("createTempFile", fileType, new Type[] { stringType, stringType }));

        // call SparkRuntimeUtils.createConfArchive, return a File and leave it in stack
        mg.invokeStatic(SPARK_RUNTIME_UTILS_TYPE,
                        new Method("createConfArchive", fileType,
                                   new Type[] { SPARK_CONF_TYPE, stringType, stringType, fileType}));
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

    return cw.toByteArray();
  }

  /**
   * CDAP-8636 Rewrite methods of YarnSparkHadoopUtil to avoid acquiring delegation token, because when we execute
   * spark submit, we don't have keytab login. Because of that and the change in SPARK-12241, the attempt to acquire
   * delegation tokens causes a spark program submission failure.
   */
  private byte[] rewriteSparkHadoopUtil(String name, InputStream byteCodeStream) throws IOException {
    // we can't rewrite 'obtainTokenForHiveMetastore', because it doesn't have Void return type
    Set<String> methods = ImmutableSet.of("obtainTokensForNamenodes", "obtainTokenForHBase");
    return Classes.rewriteMethodToNoop(name, byteCodeStream, methods);
  }

  /**
   * Private interface for rewriting constructor.
   */
  private abstract class ConstructorRewriter {

    void onMethodEnter(String name, String desc, GeneratorAdapter generatorAdapter) {
      // no-op
    }

    void onMethodExit(String name, String desc, GeneratorAdapter generatorAdapter) {
      // no-op
    }
  }

  /**
   * A {@link MethodVisitor} for replacing any occurance of System.out and System.err in the code with
   *
   * <pre>
   * RedirectedPrintStream.createRedirectedOutStream(
   *   LoggerFactory.getLogger(SparkRuntimeContextProvider.get().getProgram().getMainClassName()),
   *   System.out|System.err
   * );
   * </pre>
   */
  private static final class OutputRedirectMethodVisitor extends MethodVisitor {

    private static final Type SYSTEM_TYPE = Type.getType(System.class);
    private static final Type PRINT_STREAM_TYPE = Type.getType(PrintStream.class);
    private static final Type STRING_TYPE = Type.getType(String.class);

    private final GeneratorAdapter adapter;
    private final boolean distributed;

    OutputRedirectMethodVisitor(MethodVisitor mv, int access, String name, String desc, boolean distributed) {
      super(Opcodes.ASM5, mv);
      this.adapter = new GeneratorAdapter(mv, access, name, desc);
      this.distributed = distributed;
    }

    @Override
    public void visitFieldInsn(int opcode, String owner, String name, String desc) {
      // Rewrite the System.out and System.err
      if (opcode != Opcodes.GETSTATIC
        || !SYSTEM_TYPE.getInternalName().equals(owner)
        || !PRINT_STREAM_TYPE.equals(Type.getType(desc))
        || !("out".equals(name) || "err".equals(name))) {
        super.visitFieldInsn(opcode, owner, name, desc);
        return;
      }

      // RedirectedPrintStream.createRedirectedOutStream(
      //   LoggerFactory.getLogger(
      //     SparkRuntimeContextProvider.get().getProgram().getMainClassName()
      //   ),
      //   System.out|System.err
      // );
      Type runtimeContextProviderType =
        Type.getObjectType("co/cask/cdap/app/runtime/spark/SparkRuntimeContextProvider");
      Type runtimeContextType = Type.getObjectType("co/cask/cdap/app/runtime/spark/SparkRuntimeContext");
      Type loggerFactoryType = Type.getType(LoggerFactory.class);
      Type loggerType = Type.getType(Logger.class);
      Type redirectedPrintStreamType = Type.getType(RedirectedPrintStream.class);
      Type programType = Type.getType(Program.class);

      // Use the name of the spark program class as the logger name
      adapter.invokeStatic(runtimeContextProviderType, new Method("get", runtimeContextType, EMPTY_ARGS));
      adapter.invokeVirtual(runtimeContextType, new Method("getProgram", programType, EMPTY_ARGS));
      adapter.invokeInterface(programType, new Method("getMainClassName", STRING_TYPE, EMPTY_ARGS));
      adapter.invokeStatic(loggerFactoryType,
                           new Method("getLogger", loggerType, new Type[]{STRING_TYPE}));

      // Only write back to stdout/stderr if in distributed mode, which is on separate file
      // In local mode, we don't want the same log appear two in the log file.
      if (distributed) {
        adapter.getStatic(SYSTEM_TYPE, name, PRINT_STREAM_TYPE);
      } else {
        adapter.push((Type) null);
      }
      adapter.invokeStatic(redirectedPrintStreamType,
                           new Method("createRedirectedOutStream", redirectedPrintStreamType,
                                      new Type[] { loggerType, PRINT_STREAM_TYPE }));
    }
  }
}
