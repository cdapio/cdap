package com.continuuity.internal.test.bytecode;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;

import java.util.Set;

import static org.objectweb.asm.Opcodes.*;

/**
 *
 */
public class FlowletRewriter {

  private final Set<String> flowletClassNames;

  public FlowletRewriter(Set<String> flowletClassNames) {
    this.flowletClassNames = flowletClassNames;
  }

  /**
   * Rewrite the flowlet byte code by injecting metrics.
   * @param flowletByteCode
   * @return
   */
  public byte[] rewrite(byte[] flowletByteCode) {
    System.out.println("Before: " + flowletByteCode.length);

    ClassReader cr = new ClassReader(flowletByteCode);
    final ClassWriter cw = new ClassWriter(0);
    cr.accept(new ClassVisitorAdapter(ASM4, cw) {

      private String className;
      private boolean isFlowlet;
      private boolean initInjected;

      @Override
      public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        isFlowlet = flowletClassNames.contains(name.replace('/', '.'));
        if (isFlowlet) {
          className = name;
          cw.visitField(ACC_PRIVATE, "__genContext",
                        "Lcom/continuuity/api/flow/flowlet/FlowletContext;",
                        null, null).visitEnd();
        }
        super.visit(version, access, name, signature, superName, interfaces);
      }

      @Override
      public MethodVisitor visitMethod(int access, final String name, String desc, String signature, String[] exceptions) {
        MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
        if (!isFlowlet) {
          return mv;
        }

        if (name.equals("initialize") && desc.equals("(Lcom/continuuity/api/flow/flowlet/FlowletContext;)V")) {
          return new MethodVisitor(ASM4, mv) {
            @Override
            public void visitCode() {
              super.visitCode();
              mv.visitVarInsn(ALOAD, 0);
              mv.visitFieldInsn(PUTFIELD, className, "__genContext", "Lcom/continuuity/api/flow/flowlet/FlowletContext;");
              mv.visitInsn(RETURN);
              mv.visitMaxs(0, 0);
              initInjected = true;
            }
          };
        }

        return new MethodVisitor(ASM4, mv) {

          private boolean isProcess = name.startsWith("process");

          @Override
          public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            if (desc.equals("Lcom/continuuity/api/annotation/ProcessInput;")) {
              isProcess = true;
            }
            return super.visitAnnotation(desc, visible);
          }

          @Override
          public void visitCode() {
            super.visitCode();
            if (isProcess) {
              System.out.println(className + "." + name);

              mv.visitTypeInsn(NEW, "java/lang/StringBuilder");
              mv.visitInsn(DUP);
              mv.visitMethodInsn(INVOKESPECIAL, "java/lang/StringBuilder", "<init>", "()V");
              mv.visitVarInsn(ALOAD, 0);
              mv.visitFieldInsn(GETFIELD, className, "__genContext", "Lcom/continuuity/api/flow/flowlet/FlowletContext;");
              mv.visitMethodInsn(INVOKEINTERFACE, "com/continuuity/api/flow/flowlet/FlowletContext", "getName", "()Ljava/lang/String;");
              mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
              mv.visitLdcInsn(".read");
              mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
              mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "toString", "()Ljava/lang/String;");
              mv.visitInsn(ICONST_1);
              mv.visitMethodInsn(INVOKESTATIC, "com/continuuity/internal/test/RuntimeStats", "count", "(Ljava/lang/String;I)V");
              mv.visitMaxs(2, 2);
            }
          }
        };
      }

      @Override
      public void visitEnd() {
        if (!initInjected) {
          System.out.println("inject init");
          MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "initialize", "(Lcom/continuuity/api/flow/flowlet/FlowletContext;)V", null, new String[]{"com/continuuity/api/flow/flowlet/FlowletException"});
          mv.visitCode();
          mv.visitVarInsn(ALOAD, 0);
          mv.visitFieldInsn(PUTFIELD, "com/continuuity/test/SimpleFlowlet", "__genContext", "Lcom/continuuity/api/flow/flowlet/FlowletContext;");
          mv.visitInsn(RETURN);
          mv.visitMaxs(0, 0);
          mv.visitEnd();
          initInjected = true;
        }

        super.visitEnd();    //To change body of overridden methods use File | Settings | File Templates.
      }
    }, ClassReader.SKIP_DEBUG);

    byte[] result = cw.toByteArray();
    return result;
  }
}
