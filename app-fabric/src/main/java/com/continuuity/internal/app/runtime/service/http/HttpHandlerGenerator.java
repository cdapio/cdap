/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.internal.app.runtime.service.http;

import com.continuuity.api.service.http.HttpServiceContext;
import com.continuuity.api.service.http.HttpServiceHandler;
import com.continuuity.internal.asm.ClassDefinition;
import com.continuuity.internal.asm.Methods;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.google.common.reflect.TypeToken;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.objectweb.asm.tree.AnnotationNode;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * A bytecode generator for generating class that implements {@link com.continuuity.http.HttpHandler} interface and
 * copy public methods annotated with {@link Path} of a delegating class by delegating to the delegation instance.
 *
 * It is needed for wrapping user class that annotated with {@link Path} into a class that implements
 * {@link com.continuuity.http.HttpHandler} for the netty http service to inspect.
 *
 * Also, the generated class can impose transaction boundary for every call those {@link Path @Path} methods.
 */
final class HttpHandlerGenerator {

  private ClassWriter classWriter;
  private Type classType;

  ClassDefinition generate(final TypeToken<?> delegateType) throws IOException {
    Class<?> rawType = delegateType.getRawType();

    classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

    String internalName = Type.getInternalName(rawType);
    String className = internalName + Hashing.md5().hashString(internalName);

    // Generate the class
    classType = Type.getObjectType(className);
    classWriter.visit(Opcodes.V1_6, Opcodes.ACC_PUBLIC + Opcodes.ACC_FINAL,
                      className, null,
                      Type.getInternalName(AbstractHttpHandlerDelegator.class), null);

    // Visit the delegate class, copy class @Path annotations and methods annotated with @Path
    InputStream sourceBytes = rawType.getClassLoader().getResourceAsStream(Type.getInternalName(rawType) + ".class");
    try {
      ClassReader classReader = new ClassReader(sourceBytes);
      classReader.accept(new ClassVisitor(Opcodes.ASM4) {

        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
          // Copy the class annotation if it is @Path
          Type type = Type.getType(desc);
          if (type.equals(Type.getType(Path.class))) {
            return classWriter.visitAnnotation(desc, visible);
          } else {
            return super.visitAnnotation(desc, visible);
          }
        }

        @Override
        public MethodVisitor visitMethod(final int access, final String name,
                                         final String desc, final String signature, final String[] exceptions) {

          // Copy the method if it is public and annotated with @Path
          MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
          if (!Modifier.isPublic(access)) {
            return mv;
          }

          return new MethodVisitor(Opcodes.ASM4, mv) {

            private final List<AnnotationNode> annotations = Lists.newArrayList();

            @Override
            public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
              // Memorize all visible annotations
              if (visible) {
                AnnotationNode annotationNode = new AnnotationNode(Opcodes.ASM4, desc);
                annotations.add(annotationNode);
                return annotationNode;
              }
              return super.visitAnnotation(desc, visible);
            }

            @Override
            public void visitEnd() {
              // If any annotations of the method is one of those HttpMethod,
              // this is a handler process, hence need to copy.
              boolean handlerMethod = false;
              for (AnnotationNode annotation : annotations) {
                if (isHandlerMethod(Type.getType(annotation.desc))) {
                  handlerMethod = true;
                  break;
                }
              }

              if (handlerMethod) {
                MethodVisitor methodVisitor = classWriter.visitMethod(access, name, desc, signature, exceptions);
                final GeneratorAdapter mg = new GeneratorAdapter(methodVisitor, access, name, desc);

                // Replay all annotations before generating the body.
                for (AnnotationNode annotation : annotations) {
                  annotation.accept(mg.visitAnnotation(annotation.desc, true));
                }
                generateDelegateMethodBody(mg, new Method(name, desc), delegateType);
              }
            }
          };
        }
      }, ClassReader.SKIP_DEBUG);
    } finally {
      sourceBytes.close();
    }

    // Generate the delegate field
    // DelegateType delegate;
    // Schema field
    classWriter.visitField(Opcodes.ACC_PRIVATE + Opcodes.ACC_FINAL, "delegate",
                           Type.getDescriptor(rawType), null, null).visitEnd();

    generateConstructor(delegateType);

    ClassDefinition classDefinition = new ClassDefinition(classWriter.toByteArray(), className);
    // DEBUG block. Uncomment for debug
//    com.continuuity.internal.asm.Debugs.debugByteCode(classDefinition, new java.io.PrintWriter(System.out));
    // End DEBUG block
    return classDefinition;
  }

  /**
   * Generates the constructor. The constructor generated has signature {@code (DelegateType, HttpServiceContext)}.
   */
  private void generateConstructor(TypeToken<?> delegateType) {
    Method constructor = Methods.getMethod(void.class, "<init>", delegateType.getRawType(), HttpServiceContext.class);

    // Constructor(DelegateType, HttpServiceContext)
    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC, constructor, null, null, classWriter);

    // super(delegate, context);
    mg.loadThis();
    mg.loadArg(0);
    mg.loadArg(1);
    mg.invokeConstructor(Type.getType(AbstractHttpHandlerDelegator.class),
                         Methods.getMethod(void.class, "<init>", HttpServiceHandler.class, HttpServiceContext.class));

    // this.delegate = delegate;
    mg.loadThis();
    mg.loadArg(0);
    mg.putField(classType, "delegate", Type.getType(delegateType.getRawType()));

    mg.returnValue();
    mg.endMethod();
  }

  /**
   * Generates the handler method body. It has the form:
   *
   * <pre>{@code
   *   public void handle(HttpRequest request, HttpResponder responder, ...) {
   *     delegate.handle(request, responder, ...);
   *   }
   * }
   * </pre>
   */
  private void generateDelegateMethodBody(GeneratorAdapter mg, Method method, TypeToken<?> delegateType) {
    mg.loadThis();
    mg.getField(classType, "delegate", Type.getType(delegateType.getRawType()));

    for (int i = 0; i < method.getArgumentTypes().length; i++) {
      mg.loadArg(i);
    }

    mg.invokeVirtual(Type.getType(delegateType.getRawType()), method);
    mg.returnValue();
    mg.endMethod();
  }

  /**
   * Returns true if the annotation is of type {@link Path} or on of those {@link javax.ws.rs.HttpMethod} annotations.
   */
  private boolean isHandlerMethod(Type annotationType) {
    if (annotationType.equals(Type.getType(GET.class))) {
      return true;
    }
    if (annotationType.equals(Type.getType(POST.class))) {
      return true;
    }
    if (annotationType.equals(Type.getType(PUT.class))) {
      return true;
    }
    if (annotationType.equals(Type.getType(DELETE.class))) {
      return true;
    }
    if (annotationType.equals(Type.getType(HEAD.class))) {
      return true;
    }
    return false;
  }
}
