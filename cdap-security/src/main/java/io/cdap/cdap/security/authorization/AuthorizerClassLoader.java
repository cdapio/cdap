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

package io.cdap.cdap.security.authorization;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.common.lang.ClassPathResources;
import io.cdap.cdap.common.lang.DirectoryClassLoader;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.asm.FinallyAdapter;
import io.cdap.cdap.internal.asm.Signatures;
import io.cdap.cdap.security.spi.authorization.Authorizer;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * {@link DirectoryClassLoader} for {@link Authorizer} extensions.
 */
public class AuthorizerClassLoader extends DirectoryClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(AuthorizerClassLoader.class);
  private static final Type CLASSLOADER_TYPE = Type.getType(ClassLoader.class);
  private static final Type THREAD_TYPE = Type.getType(Thread.class);

  private final File tmpDir;
  private final File extensionJar;
  private final String authorizerClassName;

  @VisibleForTesting
  static ClassLoader createParent() {
    ClassLoader baseClassLoader = AuthorizerClassLoader.class.getClassLoader();

    final Set<String> authorizerResources = traceSecurityDependencies(baseClassLoader);
    // by default, FilterClassLoader's defaultFilter allows all hadoop classes, which makes it so that
    // the authorizer extension can share the same instance of UserGroupInformation. This allows kerberos credential
    // renewal to also renew for any extension
    final FilterClassLoader.Filter defaultFilter = FilterClassLoader.defaultFilter();

    return new FilterClassLoader(baseClassLoader, new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return defaultFilter.acceptResource(resource) || authorizerResources.contains(resource);
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return true;
      }
    });
  }

  private static Set<String> traceSecurityDependencies(ClassLoader baseClassLoader) {
    try {
      // Trace dependencies for Authorizer class. This will make classes from cdap-security-spi as well as cdap-proto
      // and other dependencies of cdap-security-spi available to the authorizer extension.
      return ClassPathResources.getResourcesWithDependencies(baseClassLoader, Authorizer.class);
    } catch (IOException e) {
      LOG.error("Failed to determine resources for authorizer class loader while tracing dependencies of " +
                  "Authorizer.", e);
      return ImmutableSet.of();
    }
  }

  AuthorizerClassLoader(File tmpDir, File authorizerExtensionJar,
                        @Nullable String authorizerExtraClasspath) throws IOException, InvalidAuthorizerException {
    super(BundleJarUtil.prepareClassLoaderFolder(authorizerExtensionJar, tmpDir),
          authorizerExtraClasspath, createParent(), "lib");
    this.tmpDir = tmpDir;
    this.extensionJar = authorizerExtensionJar;
    this.authorizerClassName = extractAuthorizerClassName();
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      DirUtils.deleteDirectoryContents(tmpDir);
    }
  }

  /**
   * Returns the file path of the extension jar where this classloader was created from.
   */
  public File getExtensionJar() {
    return extensionJar;
  }

  /**
   * Returns the class name of the {@link Authorizer}.
   */
  public String getAuthorizerClassName() {
    return authorizerClassName;
  }

  @Override
  protected boolean needIntercept(String className) {
    return authorizerClassName.equals(className);
  }

  @Nullable
  @Override
  public byte[] rewriteClass(String className, InputStream input) throws IOException {
    if (!authorizerClassName.equals(className)) {
      return null;
    }

    // Rewrite the Authorizer class to wrap every methods call with context classloader change
    Set<java.lang.reflect.Method> authorizerMethods = new HashSet<>(Arrays.asList(Authorizer.class.getMethods()));

    ClassReader cr = new ClassReader(input);
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
    cr.accept(new ClassVisitor(Opcodes.ASM7, cw) {

      private String superName;

      @Override
      public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        super.visit(version, access, name, signature, superName, interfaces);
        this.superName = superName;
      }

      @Override
      public MethodVisitor visitMethod(int access, String name, String descriptor,
                                       String signature, String[] exceptions) {
        MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);

        Method method = new Method(name, descriptor);

        // Only rewrite methods defined in the Authorizer interface.
        if (authorizerMethods.removeIf(m -> method.equals(Method.getMethod(m)))) {
          return rewriteMethod(access, name, descriptor, mv);
        }

        return mv;
      }

      @Override
      public void visitEnd() {
        // Generates all the missing methods on the Authorizer interface so that we can wrap them with the
        // context classloader switch.
        Set<Method> generatedMethods = new HashSet<>();
        new HashSet<>(authorizerMethods).forEach(m -> {
          Method method = Method.getMethod(m);
          // Guard against same method signature that comes from different parent interfaces
          if (!generatedMethods.add(method)) {
            return;
          }

          // Generate the method by calling super.[method]
          String signature = Signatures.getMethodSignature(
            method, TypeToken.of(m.getGenericReturnType()),
            Arrays.stream(m.getGenericParameterTypes()).map(TypeToken::of).toArray(TypeToken[]::new));
          String[] exceptions = Arrays.stream(m.getExceptionTypes()).map(Type::getInternalName).toArray(String[]::new);

          MethodVisitor mv = rewriteMethod(Opcodes.ACC_PUBLIC, method.getName(), method.getDescriptor(),
                                           visitMethod(Opcodes.ACC_PUBLIC, method.getName(),
                                                       method.getDescriptor(), signature, exceptions));

          GeneratorAdapter generator = new GeneratorAdapter(Opcodes.ACC_PUBLIC, method, mv);
          generator.visitCode();
          generator.loadThis();
          generator.loadArgs();
          generator.visitMethodInsn(Opcodes.INVOKESPECIAL, superName, method.getName(), method.getDescriptor(), false);
          generator.returnValue();
          generator.endMethod();
        });

        super.visitEnd();
      }

      /**
       * Rewrites the method by wrapping the whole method call with the context classloader switch to the
       * AuthorizerClassLoader.
       */
      private MethodVisitor rewriteMethod(int access, String name, String descriptor, MethodVisitor mv) {
        return new FinallyAdapter(Opcodes.ASM7, mv, access, name, descriptor) {

          int currentThread;
          int oldClassLoader;

          @Override
          protected void onMethodEnter() {
            // Thread currentThread = Thread.currentThread();
            invokeStatic(THREAD_TYPE,
                         new Method("currentThread", THREAD_TYPE, new Type[0]));
            currentThread = newLocal(THREAD_TYPE);
            storeLocal(currentThread, THREAD_TYPE);

            // ClassLoader oldClassLoader = currentThread.getContextClassLoader();
            loadLocal(currentThread, THREAD_TYPE);
            invokeVirtual(THREAD_TYPE,
                          new Method("getContextClassLoader", CLASSLOADER_TYPE, new Type[0]));
            oldClassLoader = newLocal(CLASSLOADER_TYPE);
            storeLocal(oldClassLoader, CLASSLOADER_TYPE);

            // currentThread.setContextClassLoader(getClass().getClassLoader());
            loadLocal(currentThread, THREAD_TYPE);
            loadThis();
            invokeVirtual(Type.getType(Object.class), new Method("getClass", Type.getType(Class.class), new Type[0]));
            invokeVirtual(Type.getType(Class.class),
                          new Method("getClassLoader", CLASSLOADER_TYPE, new Type[0]));
            invokeVirtual(THREAD_TYPE,
                          new Method("setContextClassLoader", Type.VOID_TYPE, new Type[] { CLASSLOADER_TYPE }));
            beginTry();
          }

          @Override
          protected void onFinally(int opcode) {
            // currentThread.setContextClassLoader(oldClassLoader);
            loadLocal(currentThread, THREAD_TYPE);
            loadLocal(oldClassLoader, CLASSLOADER_TYPE);
            invokeVirtual(THREAD_TYPE,
                          new Method("setContextClassLoader", Type.VOID_TYPE, new Type[] { CLASSLOADER_TYPE }));
          }
        };
      }
    }, ClassReader.EXPAND_FRAMES);

    return cw.toByteArray();
  }

  /**
   * Returns the {@link Authorizer} class name as declared in the Manifest.
   */
  private String extractAuthorizerClassName() throws InvalidAuthorizerException {
    Manifest manifest = getManifest();
    if (manifest == null) {
      throw new InvalidAuthorizerException("Missing Manifest from the Authorization extension");
    }

    Attributes manifestAttributes = manifest.getMainAttributes();
    if (manifestAttributes == null) {
      throw new InvalidAuthorizerException(
        String.format("No attributes found in authorizer extension jar '%s'.", extensionJar));
    }
    if (!manifestAttributes.containsKey(Attributes.Name.MAIN_CLASS)) {
      throw new InvalidAuthorizerException(
        String.format("Authorizer class not set in the manifest of the authorizer extension jar located at %s. " +
                        "Please set the attribute %s to the fully qualified class name of the class that " +
                        "implements %s in the extension jar's manifest.",
                      extensionJar, Attributes.Name.MAIN_CLASS, Authorizer.class.getName()));
    }
    return manifestAttributes.getValue(Attributes.Name.MAIN_CLASS);
  }
}
