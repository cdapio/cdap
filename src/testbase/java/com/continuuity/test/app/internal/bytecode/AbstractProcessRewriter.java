package com.continuuity.test.app.internal.bytecode;

import com.continuuity.test.app.RuntimeStats;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import static org.objectweb.asm.Opcodes.ACC_PRIVATE;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.ASM4;
import static org.objectweb.asm.Opcodes.ASTORE;
import static org.objectweb.asm.Opcodes.ATHROW;
import static org.objectweb.asm.Opcodes.DUP;
import static org.objectweb.asm.Opcodes.F_SAME;
import static org.objectweb.asm.Opcodes.F_SAME1;
import static org.objectweb.asm.Opcodes.GETFIELD;
import static org.objectweb.asm.Opcodes.GOTO;
import static org.objectweb.asm.Opcodes.ICONST_1;
import static org.objectweb.asm.Opcodes.INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.NEW;
import static org.objectweb.asm.Opcodes.PUTFIELD;
import static org.objectweb.asm.Opcodes.RETURN;

/**
 * Generate a new flowlet class by copying an existing one.
 */
public abstract class AbstractProcessRewriter {

  private static final String STRING_BUILDER = internalNameOf(StringBuilder.class);

  private final String context;
  private final String specification;
  private final String[] initExceptions;
  private final String processMethodPrefix;
  private final String annotate;
  private final String statsPrefix;

  protected AbstractProcessRewriter(Class<?> processorClass,
                                    String processMethodPrefix,
                                    Class<? extends Annotation> methodAnnotation,
                                    String statsPrefix) {

    try {
      this.processMethodPrefix = processMethodPrefix;
      this.annotate = methodAnnotation == null ? null : methodAnnotation.getName().replace('.', '/');
      this.statsPrefix = statsPrefix;

      String context = null;
      String specification = null;
      String[] initExceptions = null;
      for (Method method : processorClass.getMethods()) {
        if (method.getName().equals("initialize")) {
          context = method.getParameterTypes()[0].getName().replace('.', '/');
          specification = method.getParameterTypes()[0].getMethod("getSpecification").getReturnType()
            .getName().replace('.', '/');

          Class<?>[] exceptionTypes = method.getExceptionTypes();
          if (exceptionTypes.length <= 0) {
            initExceptions = null;
          } else {
            initExceptions = new String[exceptionTypes.length];
            for (int i = 0; i < exceptionTypes.length; i++) {
              initExceptions[i] = exceptionTypes[i].getName().replace('.', '/');
            }
          }
        }
      }
      Preconditions.checkArgument(context != null, "Unknown context for " + processorClass);
      this.context = context;
      this.specification = specification;
      this.initExceptions = initExceptions;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public final byte[] generate(byte[] bytecodes) {
    return generate(bytecodes, "");
  }

  public final byte[] generate(byte[] bytecodes, final String statsMiddle) {
    ClassReader cr = new ClassReader(bytecodes);
    final ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);

    cr.accept(new ClassVisitor(ASM4, cw) {

      String className;
      String superClassName;
      boolean initialized;

      @Override
      public void visit(int version, int access, String name, String signature, String superName,
                        String[] interfaces) {
        className = name;
        superClassName = superName;
        cw.visit(version, access, name, signature, superName, interfaces);
        cw.visitField(ACC_PRIVATE, "__genContext", internalNameToDesc(context), null, null).visitEnd();
      }

      @Override
      public MethodVisitor visitMethod(int access, final String name, final String desc, String signature,
                                       String[] exceptions) {
        MethodVisitor mv = cw.visitMethod(access, name, desc, signature, exceptions);
        final int argumentSize = new org.objectweb.asm.commons.Method(name, desc).getArgumentTypes().length;

        return new MethodVisitor(ASM4, mv) {

          boolean rewriteMethod = (annotate == null) ? name.equals(processMethodPrefix)
                                                     : name.startsWith(processMethodPrefix);
          Label tryLabel;
          Label endTryLabel;
          Label catchLabel;

          @Override
          public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            rewriteMethod = rewriteMethod || (annotate != null && desc.equals(internalNameToDesc(annotate)));
            return mv.visitAnnotation(desc, visible);
          }

          @Override
          public void visitCode() {
            mv.visitCode();
            if (name.equals("initialize") && desc.equals("(" + internalNameToDesc(context) + ")V")) {
              initialized = true;
              mv.visitVarInsn(ALOAD, 0);
              mv.visitVarInsn(ALOAD, 1);
              mv.visitFieldInsn(PUTFIELD, className, "__genContext", internalNameToDesc(context));
              return;
            }

            if (rewriteMethod) {
              tryLabel = new Label();
              endTryLabel = new Label();
              catchLabel = new Label();
              mv.visitTryCatchBlock(tryLabel, endTryLabel, catchLabel, internalNameOf(Throwable.class));
              generateLogStats(className, mv, statsMiddle, "input");
              mv.visitLabel(tryLabel);
            }
          }

          @Override
          public void visitInsn(int opcode) {
            if (rewriteMethod && opcode == RETURN) {    // When the origin method return
              generateLogStats(className, mv, statsMiddle, "processed");
            }
            super.visitInsn(opcode);
          }

          @Override
          public void visitMaxs(int maxStack, int maxLocals) {
            if (rewriteMethod) {
              mv.visitLabel(endTryLabel);

              Label endCatchLabel = new Label();
              mv.visitJumpInsn(GOTO, endCatchLabel);
              mv.visitLabel(catchLabel);
              mv.visitFrame(F_SAME1, 0, null, 1, new Object[]{"java/lang/Throwable"});  // Ignored by COMPUTE_FRAME
              mv.visitVarInsn(ASTORE, argumentSize + 1);
              generateLogStats(className, mv, statsMiddle, "exception");

              mv.visitVarInsn(ALOAD, argumentSize + 1);
              mv.visitMethodInsn(INVOKESTATIC, "com/google/common/base/Throwables", "propagate",
                                 "(Ljava/lang/Throwable;)Ljava/lang/RuntimeException;");
              mv.visitInsn(ATHROW);
              mv.visitLabel(endCatchLabel);
              mv.visitFrame(F_SAME, 0, null, 0, null);       // Ignored by COMPUTE_FRAME
              mv.visitInsn(RETURN);
            }
            super.visitMaxs(0, 0);
          }
        };
      }

      @Override
      public void visitEnd() {
        if (!initialized) {
          MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "initialize", "(" + internalNameToDesc(context) + ")V", null,
                                            initExceptions);
          mv.visitCode();
          if (superClassName != null) {
            mv.visitVarInsn(ALOAD, 0);
            mv.visitVarInsn(ALOAD, 1);
            mv.visitMethodInsn(INVOKESPECIAL, superClassName, "initialize", "(" + internalNameToDesc(context) + ")V");
          }
          mv.visitVarInsn(ALOAD, 0);
          mv.visitVarInsn(ALOAD, 1);
          mv.visitFieldInsn(PUTFIELD, className, "__genContext", internalNameToDesc(context));
          mv.visitInsn(RETURN);
          mv.visitMaxs(0, 0);   // Ignored by COMPUTE_FRAME
          mv.visitEnd();
        }
        cw.visitEnd();
      }
    }, 0);


    return cw.toByteArray();
  }

  private void generateLogStats(String className, MethodVisitor mv, String middle, String metric) {
    String prefix = statsPrefix;
    if (!middle.isEmpty()) {
      prefix += "." + middle;
    }
    prefix += ".";

    mv.visitTypeInsn(NEW, STRING_BUILDER);
    mv.visitInsn(DUP);
    mv.visitMethodInsn(INVOKESPECIAL, STRING_BUILDER, "<init>", "()V");
    mv.visitLdcInsn(prefix);
    mv.visitMethodInsn(INVOKEVIRTUAL, STRING_BUILDER, "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
    mv.visitVarInsn(ALOAD, 0);
    mv.visitFieldInsn(GETFIELD, className, "__genContext", internalNameToDesc(context));
    mv.visitMethodInsn(INVOKEINTERFACE, context, "getSpecification", "()" + internalNameToDesc(specification));
    mv.visitMethodInsn(INVOKEINTERFACE, specification, "getName", "()Ljava/lang/String;");
    mv.visitMethodInsn(INVOKEVIRTUAL, STRING_BUILDER, "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
    mv.visitLdcInsn("." + metric);
    mv.visitMethodInsn(INVOKEVIRTUAL, STRING_BUILDER, "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
    mv.visitMethodInsn(INVOKEVIRTUAL, STRING_BUILDER, "toString", "()Ljava/lang/String;");
    mv.visitInsn(ICONST_1);
    mv.visitMethodInsn(INVOKESTATIC, Type.getInternalName(RuntimeStats.class), "count", "(Ljava/lang/String;I)V");
  }

  private static String internalNameToDesc(String className) {
    return String.format("L%s;", className);
  }

  private static String internalNameOf(Class<?> clz) {
    return clz.getName().replace('.', '/');
  }
}
