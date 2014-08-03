/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.internal.asm.ClassDefinition;
import co.cask.cdap.internal.asm.Methods;
import co.cask.http.HttpResponder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.reflect.TypeToken;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.objectweb.asm.signature.SignatureReader;
import org.objectweb.asm.signature.SignatureWriter;
import org.objectweb.asm.tree.AnnotationNode;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * A bytecode generator for generating class that implements {@link co.cask.http.HttpHandler} interface and
 * copy public methods annotated with {@link Path} of a delegating class by delegating to the delegation instance.
 *
 * It is needed for wrapping user class that annotated with {@link Path} into a class that implements
 * {@link co.cask.http.HttpHandler} for the netty http service to inspect.
 *
 * Also, the generated class can impose transaction boundary for every call those {@link Path @Path} methods.
 */
@NotThreadSafe
final class HttpHandlerGenerator {

  public static final Type SUPER_CLASS_TYPE = Type.getType(AbstractHttpHandlerDelegator.class);

  private ClassWriter classWriter;
  private Type classType;
  private TypeToken<?> delegateType;

  ClassDefinition generate(TypeToken<?> delegateType) throws IOException {
    Class<?> rawType = delegateType.getRawType();

    this.classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
    this.delegateType = delegateType;

    String internalName = Type.getInternalName(rawType);
    String className = internalName + Hashing.md5().hashString(internalName);

    // Generate the class
    classType = Type.getObjectType(className);
    classWriter.visit(Opcodes.V1_6, Opcodes.ACC_PUBLIC + Opcodes.ACC_FINAL,
                      className, null,
                      Type.getInternalName(AbstractHttpHandlerDelegator.class), null);

    // Inspect the delegate class hierarchy to generate public handler methods.
    boolean firstVisit = true;
    for (TypeToken<?> type : delegateType.getTypes().classes()) {
      if (!Object.class.equals(type.getRawType())) {
        inspectHandler(type, firstVisit);
        firstVisit = false;
      }
    }

    // Generate the delegate field
    // DelegateType delegate;
    classWriter.visitField(Opcodes.ACC_PRIVATE + Opcodes.ACC_FINAL, "delegate",
                           Type.getDescriptor(rawType), null, null).visitEnd();

    generateConstructor(delegateType);

    ClassDefinition classDefinition = new ClassDefinition(classWriter.toByteArray(), className);
    // DEBUG block. Uncomment for debug
//    co.cask.cdap.internal.asm.Debugs.debugByteCode(classDefinition, new java.io.PrintWriter(System.out));
    // End DEBUG block
    return classDefinition;
  }

  /**
   * Inspects the given type and copy/rewrite handler methods from it into the newly generated class.
   */
  private void inspectHandler(TypeToken<?> type, final boolean firstVisit) throws IOException {
    Class<?> rawType = type.getRawType();

    // Visit the delegate class, copy and rewrite handler method, with method body just do delegation
    InputStream sourceBytes = rawType.getClassLoader().getResourceAsStream(Type.getInternalName(rawType) + ".class");
    try {
      ClassReader classReader = new ClassReader(sourceBytes);
      classReader.accept(new ClassVisitor(Opcodes.ASM4) {

        @Override
        public void visit(int version, int access, String name, String signature,
                          String superName, String[] interfaces) {
          super.visit(version, access, name, signature, superName, interfaces);
        }

        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
          // Copy the class annotation if it is @Path. Only do it for one time
          Type type = Type.getType(desc);
          if (firstVisit && type.equals(Type.getType(Path.class))) {
            return classWriter.visitAnnotation(desc, visible);
          } else {
            return super.visitAnnotation(desc, visible);
          }
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
          // Copy the method if it is public and annotated with one of the HTTP request method
          MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
          if (!Modifier.isPublic(access)) {
            return mv;
          }
          return new HandlerMethodVisitor(mv, desc, signature, access, name, exceptions);
        }
      }, ClassReader.SKIP_DEBUG);
    } finally {
      sourceBytes.close();
    }
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
    mg.invokeConstructor(SUPER_CLASS_TYPE,
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
   *     delegate.handle(wrapRequest(request), wrapResponder(responder), ...);
   *   }
   * }
   * </pre>
   */
  private void generateDelegateMethodBody(GeneratorAdapter mg, Method method) {
    mg.loadThis();
    mg.getField(classType, "delegate", Type.getType(delegateType.getRawType()));

    mg.loadThis();
    mg.loadArg(0);
    mg.invokeVirtual(SUPER_CLASS_TYPE,
                     Methods.getMethod(HttpServiceRequest.class, "wrapRequest", HttpRequest.class));

    mg.loadThis();
    mg.loadArg(1);
    mg.invokeVirtual(SUPER_CLASS_TYPE,
                     Methods.getMethod(HttpServiceResponder.class, "wrapResponder", HttpResponder.class));

    for (int i = 2; i < method.getArgumentTypes().length; i++) {
      mg.loadArg(i);
    }

    mg.invokeVirtual(Type.getType(delegateType.getRawType()), method);
    mg.returnValue();
    mg.endMethod();
  }

  /**
   * Returns true if the annotation is of type {@link javax.ws.rs.HttpMethod} annotations.
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

  /**
   * The ASM MethodVisitor for visiting handler class methods and optionally copy them if it is a handler
   * method.
   */
  private class HandlerMethodVisitor extends MethodVisitor {

    private final List<AnnotationNode> annotations;
    private final Map<Integer, AnnotationNode> paramAnnotations;
    private final String desc;
    private final String signature;
    private final int access;
    private final String name;
    private final String[] exceptions;

    public HandlerMethodVisitor(MethodVisitor mv, String desc, String signature,
                                int access, String name, String[] exceptions) {
      super(Opcodes.ASM4, mv);
      this.desc = desc;
      this.signature = signature;
      this.access = access;
      this.name = name;
      this.exceptions = exceptions;
      annotations = Lists.newArrayList();
      paramAnnotations = Maps.newLinkedHashMap();
    }

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
    public AnnotationVisitor visitParameterAnnotation(int parameter, String desc, boolean visible) {
      if (visible) {
        AnnotationNode annotationNode = new AnnotationNode(Opcodes.ASM4, desc);
        paramAnnotations.put(parameter, annotationNode);
        return annotationNode;
      }
      return super.visitParameterAnnotation(parameter, desc, visible);
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

      if (!handlerMethod) {
        super.visitEnd();
        return;
      }

      Type returnType = Type.getReturnType(desc);
      Type[] argTypes = Type.getArgumentTypes(desc);

      // If the first two parameters are not HttpServiceRequest and HttpServiceResponder, don't copy
      if (argTypes.length < 2
        || !argTypes[0].equals(Type.getType(HttpServiceRequest.class))
        || !argTypes[1].equals(Type.getType(HttpServiceResponder.class))) {
        super.visitEnd();
        return;
      }

      argTypes[0] = Type.getType(HttpRequest.class);
      argTypes[1] = Type.getType(HttpResponder.class);

      // Copy the method signature with the first two parameter types changed
      String methodDesc = Type.getMethodDescriptor(returnType, argTypes);
      MethodVisitor methodVisitor = classWriter.visitMethod(access, name, methodDesc,
                                                            rewriteMethodSignature(signature), exceptions);
      final GeneratorAdapter mg = new GeneratorAdapter(methodVisitor, access, name, desc);

      // Replay all annotations before generating the body.
      for (AnnotationNode annotation : annotations) {
        annotation.accept(mg.visitAnnotation(annotation.desc, true));
      }
      // Replay all parameter annotations
      for (Map.Entry<Integer, AnnotationNode> entry : paramAnnotations.entrySet()) {
        AnnotationNode annotation = entry.getValue();
        annotation.accept(mg.visitParameterAnnotation(entry.getKey(), annotation.desc, true));
      }

      generateDelegateMethodBody(mg, new Method(name, desc));

      super.visitEnd();
    }

    /**
     * Rewrite the handler method signature to have the first two parameters rewritten from
     * {@link HttpServiceRequest} and {@link HttpServiceResponder} into
     * {@link HttpRequest} and {@link HttpResponder}.
     */
    private String rewriteMethodSignature(String signature) {
      if (signature == null) {
        return null;
      }

      SignatureReader reader = new SignatureReader(signature);
      SignatureWriter writer = new SignatureWriter() {
        @Override
        public void visitClassType(String name) {
          if (name.equals(Type.getInternalName(HttpServiceRequest.class))) {
            super.visitClassType(Type.getInternalName(HttpRequest.class));
            return;
          }
          if (name.equals(Type.getInternalName(HttpServiceResponder.class))) {
            super.visitClassType(Type.getInternalName(HttpResponder.class));
            return;
          }
          super.visitClassType(name);
        }
      };
      reader.accept(writer);

      return writer.toString();
    }
  }
}
