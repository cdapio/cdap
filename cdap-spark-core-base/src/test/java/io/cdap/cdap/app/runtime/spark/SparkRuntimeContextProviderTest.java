/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark;

import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.runtime.AppStateStoreProvider;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import java.io.Closeable;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.ExecutorClassLoader;
import org.apache.spark.util.ParentClassLoader;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Unit tests for {@link SparkRuntimeContextProvider#get()} unwrapping Spark's ExecutorClassLoader.
 */
public class SparkRuntimeContextProviderTest {

  @Test
  public void testGetUnwrapsSpark35ExecutorClassLoader() throws Exception {
    SparkClassLoader sparkClassLoader = createTestSparkClassLoader();
    ExecutorClassLoader executorLoader =
        new ExecutorClassLoader(
            new SparkConf(), null, "spark://127.0.0.1:12345/classes", sparkClassLoader, false);
    Assert.assertEquals(
        SparkRuntimeContextProvider.SPARK_EXECUTOR_CLASSLOADER_NAME,
        executorLoader.getClass().getName());
    assertGetFindsRuntimeContext(executorLoader, sparkClassLoader);
  }

  @Test
  public void testGetUnwrapsSpark33ReplExecutorClassLoader() throws Exception {
    SparkClassLoader sparkClassLoader = createTestSparkClassLoader();
    ClassLoader replLoader =
        createSyntheticReplExecutorClassLoader(new ParentClassLoader(sparkClassLoader));
    Assert.assertEquals(
        SparkRuntimeContextProvider.EXECUTOR_CLASSLOADER_NAME, replLoader.getClass().getName());
    assertGetFindsRuntimeContext(replLoader, sparkClassLoader);
  }

  private static void assertGetFindsRuntimeContext(ClassLoader contextClassLoader,
                                                   SparkClassLoader sparkClassLoader) {
    Assert.assertNull(contextClassLoader.getParent());
    ClassLoader originalTccl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(contextClassLoader);
    try {
      Assert.assertSame(sparkClassLoader.getRuntimeContext(), SparkRuntimeContextProvider.get());
    } finally {
      Thread.currentThread().setContextClassLoader(originalTccl);
    }
  }

  private static SparkClassLoader createTestSparkClassLoader() {
    Program program = Mockito.mock(Program.class);
    ApplicationSpecification appSpec = Mockito.mock(ApplicationSpecification.class);
    SparkSpecification sparkSpec =
        new SparkSpecification(
            "TestSparkClass",
            "testSpark",
            "test description",
            "TestMainClass",
            Collections.emptySet(),
            Collections.emptyMap(),
            null,
            null,
            null,
            Collections.emptyList(),
            Collections.emptyMap());
    ProgramId programId = new ProgramId("default", "testApp", ProgramType.SPARK, "testSpark");
    Mockito.when(program.getId()).thenReturn(programId);
    Mockito.when(program.getName()).thenReturn("testSpark");
    Mockito.when(program.getType()).thenReturn(ProgramType.SPARK);
    Mockito.when(program.getApplicationSpecification()).thenReturn(appSpec);
    Mockito.when(program.getClassLoader())
        .thenReturn(
            new URLClassLoader(new URL[0], SparkRuntimeContextProviderTest.class.getClassLoader()));
    Mockito.when(appSpec.getSpark()).thenReturn(Collections.singletonMap("testSpark", sparkSpec));
    Mockito.when(appSpec.getPlugins()).thenReturn(Collections.emptyMap());

    MetricsCollectionService metricsService = Mockito.mock(MetricsCollectionService.class);
    MetricsContext metricsContext = Mockito.mock(MetricsContext.class);
    Mockito.when(metricsService.getContext(Mockito.anyMap())).thenReturn(metricsContext);

    ProgramOptions programOptions = Mockito.mock(ProgramOptions.class);
    Arguments systemArgs = Mockito.mock(Arguments.class);
    Arguments userArgs = Mockito.mock(Arguments.class);
    Mockito.when(systemArgs.getOption(ProgramOptionConstants.RUN_ID))
        .thenReturn(RunIds.generate().getId());
    Mockito.when(systemArgs.getOption(ProgramOptionConstants.ARTIFACT_ID))
        .thenReturn("default:testArtifact:1.0.0");
    Mockito.when(systemArgs.iterator()).thenReturn(Collections.emptyIterator());
    Mockito.when(userArgs.iterator()).thenReturn(Collections.emptyIterator());
    Mockito.when(programOptions.getArguments()).thenReturn(systemArgs);
    Mockito.when(programOptions.getUserArguments()).thenReturn(userArgs);
    Mockito.when(programOptions.getProgramId()).thenReturn(programId);

    SparkRuntimeContext runtimeContext =
        new SparkRuntimeContext(
            new Configuration(),
            program,
            programOptions,
            CConfiguration.create(),
            "localhost",
            Mockito.mock(TransactionSystemClient.class),
            Mockito.mock(DatasetFramework.class),
            metricsService,
            null,
            null,
            Mockito.mock(SecureStore.class),
            Mockito.mock(SecureStoreManager.class),
            Mockito.mock(AccessEnforcer.class),
            Mockito.mock(AuthenticationContext.class),
            Mockito.mock(MessagingService.class),
            Mockito.mock(ServiceAnnouncer.class),
            Mockito.mock(PluginFinder.class),
            Mockito.mock(LocationFactory.class),
            Mockito.mock(MetadataReader.class),
            Mockito.mock(MetadataPublisher.class),
            Mockito.mock(NamespaceQueryAdmin.class),
            Mockito.mock(FieldLineageWriter.class),
            Mockito.mock(RemoteClientFactory.class),
            Mockito.mock(Closeable.class),
            Mockito.mock(AppStateStoreProvider.class));
    return new SparkClassLoader(runtimeContext);
  }

  private static ClassLoader createSyntheticReplExecutorClassLoader(ClassLoader parentLoader)
      throws Exception {
    String binaryName = "org.apache.spark.repl.ExecutorClassLoader";
    String internalName = binaryName.replace('.', '/');
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES);
    cw.visit(
        Opcodes.V1_8,
        Opcodes.ACC_PUBLIC,
        internalName,
        null,
        "java/lang/ClassLoader",
        null);
    cw.visitField(
        Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL,
        "parentLoader",
        "Ljava/lang/ClassLoader;",
        null,
        null).visitEnd();

    MethodVisitor init =
        cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "(Ljava/lang/ClassLoader;)V", null, null);
    init.visitCode();
    init.visitVarInsn(Opcodes.ALOAD, 0);
    init.visitInsn(Opcodes.ACONST_NULL);
    init.visitMethodInsn(
        Opcodes.INVOKESPECIAL, "java/lang/ClassLoader", "<init>", "(Ljava/lang/ClassLoader;)V",
        false);
    init.visitVarInsn(Opcodes.ALOAD, 0);
    init.visitVarInsn(Opcodes.ALOAD, 1);
    init.visitFieldInsn(Opcodes.PUTFIELD, internalName, "parentLoader", "Ljava/lang/ClassLoader;");
    init.visitInsn(Opcodes.RETURN);
    init.visitMaxs(0, 0);
    init.visitEnd();

    MethodVisitor getter =
        cw.visitMethod(
            Opcodes.ACC_PUBLIC, "parentLoader", "()Ljava/lang/ClassLoader;", null, null);
    getter.visitCode();
    getter.visitVarInsn(Opcodes.ALOAD, 0);
    getter.visitFieldInsn(
        Opcodes.GETFIELD, internalName, "parentLoader", "Ljava/lang/ClassLoader;");
    getter.visitInsn(Opcodes.ARETURN);
    getter.visitMaxs(0, 0);
    getter.visitEnd();
    cw.visitEnd();

    Class<?> clazz = defineIsolatedClass(binaryName, cw.toByteArray());
    Constructor<?> ctor = clazz.getConstructor(ClassLoader.class);
    return (ClassLoader) ctor.newInstance(parentLoader);
  }

  private static Class<?> defineIsolatedClass(String binaryName, byte[] bytecode)
      throws ClassNotFoundException {
    ClassLoader definingLoader = new ClassLoader(SparkRuntimeContextProviderTest.class
        .getClassLoader()) {
      @Override
      protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (binaryName.equals(name)) {
          Class<?> loaded = findLoadedClass(name);
          if (loaded == null) {
            loaded = defineClass(name, bytecode, 0, bytecode.length);
          }
          if (resolve) {
            resolveClass(loaded);
          }
          return loaded;
        }
        return super.loadClass(name, resolve);
      }
    };
    return definingLoader.loadClass(binaryName);
  }
}
