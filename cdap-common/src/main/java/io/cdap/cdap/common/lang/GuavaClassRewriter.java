/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.common.lang;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import io.cdap.cdap.internal.asm.Methods;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.LambdaMetafactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

/**
 * A {@link ClassRewriter} for rewriting Guava library classes to add missing functions that are available in
 * later Guava library. Those new methods are being used by Hadoop 3 until 3.4 (HADOOP-17288).
 *
 * In particular, in the Preconditions class, there are new checkState and checkArgument methods that take different
 * number of arguments for error message formatting purpose, instead of just the generic vararg method.
 * Also, in the MoreExecutors class, there is a new directExecutor() method that replace the functionality of
 * the the sameThreadExecutor().
 */
public class GuavaClassRewriter implements ClassRewriter {

  private static final Logger LOG = LoggerFactory.getLogger(GuavaClassRewriter.class);
  private static final String PRECONDTIONS_CLASS_NAME = "com.google.common.base.Preconditions";
  private static final String MORE_EXECUTORS_CLASS_NAME = "com.google.common.util.concurrent.MoreExecutors";
  private static final Type OBJECT_TYPE = Type.getType(Object.class);
  private static final Type STRING_TYPE = Type.getType(String.class);
  private static final Type RUNNABLE_TYPE = Type.getType(Runnable.class);

  /**
   * Returns {@code true} if the given class needs to be rewritten.
   */
  public boolean needRewrite(String className) {
    return PRECONDTIONS_CLASS_NAME.equals(className) || MORE_EXECUTORS_CLASS_NAME.equals(className);
  }

  @Nullable
  @Override
  public byte[] rewriteClass(String className, InputStream input) throws IOException {
    if (PRECONDTIONS_CLASS_NAME.equals(className)) {
      return rewritePreconditions(input);
    }
    if (MORE_EXECUTORS_CLASS_NAME.equals(className)) {
      return rewriteMoreExecutors(input);
    }
    return null;
  }

  /**
   * Rewrites the {@link Preconditions} class to add various {@code checkArgument}, {@code checkState},
   * and {@code checkNotNull} methods that are missing in earlier Guava library.
   *
   * @param input the bytecode stream of the Preconditions class
   * @return the rewritten bytecode
   * @throws IOException if failed to rewrite
   */
  private byte[] rewritePreconditions(InputStream input) throws IOException {
    Type[] types = new Type[] {
      OBJECT_TYPE,
      Type.CHAR_TYPE,
      Type.INT_TYPE,
      Type.LONG_TYPE
    };

    // Generates all the methods that we need to add to the Preconditions class
    // There are multiple of them, each take one or combinations of two parameters of type Object, char, int, and long
    // for the string formatting template to use
    List<Method> methods = new ArrayList<>();
    // Carry the list of method templates for generating new methods.
    // Each template contains the method name, the first argument type, and the return type.
    List<Method> methodTemplates = Arrays.asList(
      new Method("checkArgument", Type.VOID_TYPE, new Type[] { Type.BOOLEAN_TYPE }),
      new Method("checkState", Type.VOID_TYPE, new Type[] { Type.BOOLEAN_TYPE }),
      new Method("checkNotNull", Type.getType(Object.class), new Type[] { Type.getType(Object.class) })
    );

    for (Method method : methodTemplates) {
      String methodName = method.getName();
      Type returnType = method.getReturnType();
      Type firstArgType = method.getArgumentTypes()[0];

      for (Type type : types) {
        methods.add(new Method(methodName, returnType,
                               new Type[] { firstArgType, STRING_TYPE, type }));
        for (Type type2 : types) {
          methods.add(new Method(methodName, returnType,
                                 new Type[] { firstArgType, STRING_TYPE, type, type2 }));
        }
      }

      // Later version of Preconditions class also added methods that take three and four Objects
      methods.add(new Method(methodName, returnType,
                             new Type[] { firstArgType, STRING_TYPE, OBJECT_TYPE, OBJECT_TYPE, OBJECT_TYPE}));
      methods.add(new Method(methodName, returnType,
                             new Type[] { firstArgType, STRING_TYPE,
                               OBJECT_TYPE, OBJECT_TYPE, OBJECT_TYPE, OBJECT_TYPE}));
    }

    ClassReader cr = new ClassReader(input);
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
    cr.accept(new PreconditionsRewriter(Opcodes.ASM7, cw, methods), ClassReader.EXPAND_FRAMES);
    return cw.toByteArray();
  }

  /**
   * Rewrites the {@link MoreExecutors} class to add the {@code directExecutor} method.
   *
   * @param input the bytecode stream of the MoreExecutors class
   * @return the rewritten bytecode
   * @throws IOException if failed to rewrite
   */
  private byte[] rewriteMoreExecutors(InputStream input) throws IOException {
    ClassReader cr = new ClassReader(input);
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

    Method method = new Method("directExecutor", Type.getType(Executor.class), new Type[0]);
    cr.accept(new ClassVisitor(Opcodes.ASM7, cw) {

      private boolean hasMethod;

      @Override
      public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        // Rewrite the class to 1.7 format so that we can use lambda to implement the directExecutor() method
        super.visit(Opcodes.V1_7, access, name, signature, superName, interfaces);
      }

      @Override
      public MethodVisitor visitMethod(int access, String name, String descriptor,
                                       String signature, String[] exceptions) {
        if (method.equals(new Method(name, descriptor))
          && (access & Opcodes.ACC_PUBLIC) == Opcodes.ACC_PUBLIC
          && (access & Opcodes.ACC_STATIC) == Opcodes.ACC_STATIC) {
          hasMethod = true;
        }
        return super.visitMethod(access, name, descriptor, signature, exceptions);
      }

      @Override
      public void visitEnd() {
        if (hasMethod) {
          super.visitEnd();
          return;
        }

        // Generate the method
        // public static Executor directExecutor() {
        //   return Runnable::run;
        // }
        MethodVisitor mv = super.visitMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC,
                                             method.getName(), method.getDescriptor(), null, null);
        GeneratorAdapter adapter = new GeneratorAdapter(mv, Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC,
                                                        method.getName(), method.getDescriptor());
        // Perform the lambda invocation.
        Handle metaFactoryHandle = new Handle(Opcodes.H_INVOKESTATIC,
                                              Type.getType(LambdaMetafactory.class).getInternalName(),
                                              "metafactory", Methods.LAMBDA_META_FACTORY_METHOD_DESC, false);
        Handle lambdaMethodHandle = new Handle(Opcodes.H_INVOKEINTERFACE, RUNNABLE_TYPE.getInternalName(),
                                               "run", Type.getMethodDescriptor(Type.VOID_TYPE), true);

        // Signature of the Executor.execute(Runnable)
        Type samMethodType = Type.getType(Type.getMethodDescriptor(Type.VOID_TYPE, RUNNABLE_TYPE));
        adapter.invokeDynamic("execute", Type.getMethodDescriptor(Type.getType(Executor.class)),
                              metaFactoryHandle, samMethodType, lambdaMethodHandle, samMethodType);
        adapter.returnValue();
        adapter.endMethod();
        super.visitEnd();
      }
    }, ClassReader.EXPAND_FRAMES);

    return cw.toByteArray();
  }

  /**
   * A {@link ClassVisitor} to add a set of missing methods to the {@link Preconditions} class.
   */
  private static final class PreconditionsRewriter extends ClassVisitor {

    private final Set<Method> methods;

    PreconditionsRewriter(int api, ClassVisitor classVisitor, Collection<Method> methods) {
      super(api, classVisitor);
      this.methods = new LinkedHashSet<>(methods);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor,
                                     String signature, String[] exceptions) {
      Method method = new Method(name, descriptor);
      if (methods.contains(method)
          && (access & Opcodes.ACC_PUBLIC) == Opcodes.ACC_PUBLIC
          && (access & Opcodes.ACC_STATIC) == Opcodes.ACC_STATIC) {
        methods.remove(method);
      }
      return super.visitMethod(access, name, descriptor, signature, exceptions);
    }

    @Override
    public void visitEnd() {
      for (Method method : methods) {
        LOG.trace("{}.{} not found. Rewriting class to inject the missing method",
                  PRECONDTIONS_CLASS_NAME, method);

        // Generate the missing method that calls the version with varargs
        // For example,
        //
        // public static void checkArgument(boolean expression, String template, Object arg) {
        //   checkArgument(expression, template, new Object[] { arg });
        // }
        MethodVisitor mv = super.visitMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC,
                                             method.getName(), method.getDescriptor(), null, null);
        GeneratorAdapter adapter = new GeneratorAdapter(mv, Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC,
                                                        method.getName(), method.getDescriptor());
        adapter.loadArg(0);
        adapter.loadArg(1);

        // New an array of size based on the number of parameters
        int objectArray = adapter.newLocal(Type.getType(Object[].class));

        Type[] argTypes = method.getArgumentTypes();
        adapter.push(argTypes.length - 2);
        adapter.newArray(OBJECT_TYPE);
        adapter.storeLocal(objectArray);

        // Put the arguments into the Object array
        for (int i = 0; i < argTypes.length - 2; i++) {
          Type argType = argTypes[i + 2];
          adapter.loadLocal(objectArray);
          adapter.push(i);
          adapter.loadArg(i + 2);
          // If the given argument is not an Object, turn it into a String
          if (argType.getSort() != Type.OBJECT) {
            adapter.invokeStatic(STRING_TYPE, new Method("valueOf", STRING_TYPE, new Type[] { argType }));
          }
          adapter.arrayStore(OBJECT_TYPE);
        }
        adapter.loadLocal(objectArray);

        // Call the varargs method
        adapter.invokeStatic(Type.getObjectType(PRECONDTIONS_CLASS_NAME.replace('.', '/')),
                             new Method(method.getName(), method.getReturnType(),
                                        new Type[] {
                                          method.getArgumentTypes()[0],
                                          method.getArgumentTypes()[1],
                                          Type.getType(Object[].class)
                                        }));
        adapter.returnValue();
        adapter.endMethod();
      }

      super.visitEnd();
    }
  }
}
