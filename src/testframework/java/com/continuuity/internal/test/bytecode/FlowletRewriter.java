package com.continuuity.internal.test.bytecode;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

import static org.objectweb.asm.Opcodes.*;

/**
 * Generate a new flowlet class by copying an existing one
 */
public class FlowletRewriter {

  public byte[] generate(byte[] bytecodes) {
    ClassReader cr = new ClassReader(bytecodes);
    final ClassWriter cw = new ClassWriter(ASM4);

    cr.accept(new ClassVisitor(ASM4, cw) {

      String className;
      String superClassName;
      boolean initialized;

      @Override
      public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        className = name;
        superClassName = superName;
        cw.visit(version, access, name, signature, superName, interfaces);
        cw.visitField(ACC_PRIVATE, "__genContext", "Lcom/continuuity/api/flow/flowlet/FlowletContext;", null, null).visitEnd();
      }

      @Override
      public MethodVisitor visitMethod(int access, final String name, String desc, String signature, String[] exceptions) {
        MethodVisitor mv = cw.visitMethod(access, name, desc, signature, exceptions);

        if (name.equals("initialize") && desc.equals("(Lcom/continuuity/api/flow/flowlet/FlowletContext;)V")) {
          initialized = true;
          mv.visitCode();
          mv.visitVarInsn(ALOAD, 0);
          mv.visitVarInsn(ALOAD, 1);
          mv.visitFieldInsn(PUTFIELD, className, "__genContext", "Lcom/continuuity/api/flow/flowlet/FlowletContext;");
          return mv;
        }

        return new MethodVisitor(ASM4, mv) {

          boolean isProcessMethod = name.startsWith("process");
          Label l0;
          Label l1;
          Label l2;

          @Override
          public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            isProcessMethod = isProcessMethod || (desc.equals("Lcom/continuuity/api/annotation/ProcessInput;"));
            return mv.visitAnnotation(desc, visible);
          }

          @Override
          public void visitCode() {
            mv.visitCode();
            if (isProcessMethod) {
              l0 = new Label();
              l1 = new Label();
              l2 = new Label();
              mv.visitTryCatchBlock(l0, l1, l2, "java/lang/Throwable");

              mv.visitTypeInsn(NEW, "java/lang/StringBuilder");
              mv.visitInsn(DUP);
              mv.visitMethodInsn(INVOKESPECIAL, "java/lang/StringBuilder", "<init>", "()V");
              mv.visitVarInsn(ALOAD, 0);
              mv.visitFieldInsn(GETFIELD, className, "__genContext", "Lcom/continuuity/api/flow/flowlet/FlowletContext;");
              mv.visitMethodInsn(INVOKEINTERFACE, "com/continuuity/api/flow/flowlet/FlowletContext", "getName", "()Ljava/lang/String;");
              mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
              mv.visitLdcInsn(".input");
              mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
              mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "toString", "()Ljava/lang/String;");
              mv.visitInsn(ICONST_1);
              mv.visitMethodInsn(INVOKESTATIC, "com/continuuity/internal/test/RuntimeStats", "count", "(Ljava/lang/String;I)V");

              mv.visitLabel(l0);
            }
          }

          @Override
          public void visitInsn(int opcode) {
            if (isProcessMethod && opcode == RETURN) {
              mv.visitTypeInsn(NEW, "java/lang/StringBuilder");
              mv.visitInsn(DUP);
              mv.visitMethodInsn(INVOKESPECIAL, "java/lang/StringBuilder", "<init>", "()V");
              mv.visitVarInsn(ALOAD, 0);
              mv.visitFieldInsn(GETFIELD, className, "__genContext", "Lcom/continuuity/api/flow/flowlet/FlowletContext;");
              mv.visitMethodInsn(INVOKEINTERFACE, "com/continuuity/api/flow/flowlet/FlowletContext", "getName", "()Ljava/lang/String;");
              mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
              mv.visitLdcInsn(".processed");
              mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
              mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "toString", "()Ljava/lang/String;");
              mv.visitInsn(ICONST_1);
              mv.visitMethodInsn(INVOKESTATIC, "com/continuuity/internal/test/RuntimeStats", "count", "(Ljava/lang/String;I)V");
              mv.visitLabel(l1);
              Label l3 = new Label();
              mv.visitJumpInsn(GOTO, l3);
              mv.visitLabel(l2);
              mv.visitFrame(F_SAME1, 0, null, 1, new Object[]{"java/lang/Throwable"});
              mv.visitVarInsn(ASTORE, 2);
              mv.visitTypeInsn(NEW, "java/lang/StringBuilder");
              mv.visitInsn(DUP);
              mv.visitMethodInsn(INVOKESPECIAL, "java/lang/StringBuilder", "<init>", "()V");
              mv.visitVarInsn(ALOAD, 0);
              mv.visitFieldInsn(GETFIELD, className, "__genContext", "Lcom/continuuity/api/flow/flowlet/FlowletContext;");
              mv.visitMethodInsn(INVOKEINTERFACE, "com/continuuity/api/flow/flowlet/FlowletContext", "getName", "()Ljava/lang/String;");
              mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
              mv.visitLdcInsn(".failed");
              mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
              mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "toString", "()Ljava/lang/String;");
              mv.visitInsn(ICONST_1);
              mv.visitMethodInsn(INVOKESTATIC, "com/continuuity/internal/test/RuntimeStats", "count", "(Ljava/lang/String;I)V");
              mv.visitVarInsn(ALOAD, 2);
              mv.visitMethodInsn(INVOKESTATIC, "com/google/common/base/Throwables", "propagate", "(Ljava/lang/Throwable;)Ljava/lang/RuntimeException;");
              mv.visitInsn(ATHROW);
              mv.visitLabel(l3);
              mv.visitFrame(F_SAME, 0, null, 0, null);
            }
            super.visitInsn(opcode);    //To change body of overridden methods use File | Settings | File Templates.
          }

          @Override
          public void visitMaxs(int maxStack, int maxLocals) {
            if (isProcessMethod) {
              mv.visitMaxs(maxStack, maxLocals + 1);
            } else {
              mv.visitMaxs(maxStack, maxLocals);    //To change body of overridden methods use File | Settings | File Templates.
            }
          }
        };
      }

      @Override
      public void visitEnd() {
        if (!initialized) {
          MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "initialize", "(Lcom/continuuity/api/flow/flowlet/FlowletContext;)V", null, new String[]{"com/continuuity/api/flow/flowlet/FlowletException"});
          mv.visitCode();
          if (superClassName != null) {
            mv.visitVarInsn(ALOAD, 0);
            mv.visitVarInsn(ALOAD, 1);
            mv.visitMethodInsn(INVOKESPECIAL, "com/continuuity/api/flow/flowlet/AbstractFlowlet", "initialize", "(Lcom/continuuity/api/flow/flowlet/FlowletContext;)V");
          }
          mv.visitVarInsn(ALOAD, 0);
          mv.visitVarInsn(ALOAD, 1);
          mv.visitFieldInsn(PUTFIELD, className, "__genContext", "Lcom/continuuity/api/flow/flowlet/FlowletContext;");
          mv.visitInsn(RETURN);
          mv.visitMaxs(2, 2);
          mv.visitEnd();
        }
        cw.visitEnd();    //To change body of overridden methods use File | Settings | File Templates.
      }
    }, 0);


    return cw.toByteArray();
  }
}
