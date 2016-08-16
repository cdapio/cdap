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

package co.cask.cdap.common.dataset;

import co.cask.cdap.api.annotation.ReadOnly;
import co.cask.cdap.api.annotation.ReadWrite;
import co.cask.cdap.api.annotation.WriteOnly;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.common.lang.ClassRewriter;
import co.cask.cdap.internal.asm.FinallyAdapter;
import co.cask.cdap.internal.dataset.DatasetRuntimeContext;
import org.apache.tephra.TransactionAware;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A {@link ClassRewriter} for rewriting the Dataset bytecode. It modifies the Dataset class as follow:
 *
 * <pre>
 * 1. Introduce a new field, "_datasetRuntimeContext0" of type {@link DatasetRuntimeContext}.
 * 2. In each constructor, insert this._datasetRuntimeContext = DatasetRuntimeContext.getContext()
 * 3. For each constructor and method, transform the code to:
 *
 *    this._datasetRuntimeContext0.onMethodEntry([annotation]);
 *    try {
 *      // original code
 *    } finally {
 *      this._datasetRuntimeContext0.onMethodExit();
 *    }
 *
 *    The [annotation] is determined by the @NoAccess, @ReadOnly, @WriteOnly, @ReadWrite annotations on the
 *    constructor or method. The [defaultAnnotation] for constructor is @NoAccess and for method is @ReadWrite.
 * </pre>
 *
 * The class rewrite will skip rewriting of all {@link TransactionAware} methods and {@link Closeable}
 * as they are dataset operation methods.
 */
public final class DatasetClassRewriter implements ClassRewriter {

  private static final Type DATASET_RUNTIME_CONTEXT_TYPE = Type.getType(DatasetRuntimeContext.class);
  private static final Type CLASS_TYPE = Type.getType(Class.class);

  @Override
  public byte[] rewriteClass(String className, InputStream input) throws IOException {
    ClassReader cr = new ClassReader(input);
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

    cr.accept(new DatasetClassVisitor(className, cw), ClassReader.EXPAND_FRAMES);
    return cw.toByteArray();
  }

  /**
   * A {@link ClassVisitor} for rewriting {@link Dataset} class. It inject code to call methods on
   * {@link DatasetRuntimeContext} for every constructors and methods.
   */
  private final class DatasetClassVisitor extends ClassVisitor {

    private final Type datasetType;
    // For memorizing all existing field names of the dataset class
    // It is needed so that we can generate a unique field name for the DatasetRuntimeContext
    private final Set<String> fields;
    private String datasetRuntimeContextField;
    private boolean interfaceClass;

    private DatasetClassVisitor(String className, ClassWriter cw) {
      super(Opcodes.ASM5, cw);
      this.datasetType = Type.getObjectType(className.replace('.', '/'));
      this.fields = new HashSet<>();
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
      super.visit(version, access, name, signature, superName, interfaces);
      interfaceClass = Modifier.isInterface(access);
    }

    @Override
    public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
      fields.add(name);
      return super.visitField(access, name, desc, signature, value);
    }

    @Override
    public MethodVisitor visitMethod(int access, final String name,
                                     String desc, String signature, String[] exceptions) {
      MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);

      // No need to if the class is an interface or the method is static
      if (interfaceClass || Modifier.isStatic(access)) {
        return mv;
      }

      if (datasetRuntimeContextField == null) {
        // Generate the DatasetRuntimeContextField.
        // visitMethod is always called after all visitField calls, hence generate the extra field here
        datasetRuntimeContextField = generateFieldName("_datasetRuntimeContext", fields);
        super.visitField(Modifier.PRIVATE, datasetRuntimeContextField,
                         DATASET_RUNTIME_CONTEXT_TYPE.getDescriptor(), null, null);
      }

      return new FinallyAdapter(Opcodes.ASM5, mv, access, name, desc) {

        private boolean hasRead;
        private boolean hasWrite;

        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
          if (visible) {
            String annotation = Type.getType(desc).getClassName();
            if (ReadOnly.class.getName().equals(annotation)) {
              hasRead = true;
            } else if (WriteOnly.class.getName().equals(annotation)) {
              hasWrite = true;
            } else if (ReadWrite.class.getName().equals(annotation)) {
              hasRead = true;
              hasWrite = true;
            }
          }
          return super.visitAnnotation(desc, visible);
        }

        @Override
        protected void onMethodEnter() {
          boolean isConstructor = "<init>".equals(name);
          if (isConstructor) {
            // this._datasetRuntimeContext = DatasetRuntimeContext.getContext();
            loadThis();
            invokeStatic(DATASET_RUNTIME_CONTEXT_TYPE,
                         new Method("getContext", Type.getMethodDescriptor(DATASET_RUNTIME_CONTEXT_TYPE)));
            putField(datasetType, datasetRuntimeContextField, DATASET_RUNTIME_CONTEXT_TYPE);
          }

          Type methodAnnotationType = getMethodAnnotationType(hasRead, hasWrite);

          // this._datasetRuntimeContext.onMethodEntry(isConstructor, methodAnnotation);
          // try {
          loadThis();
          getField(datasetType, datasetRuntimeContextField, DATASET_RUNTIME_CONTEXT_TYPE);
          visitLdcInsn(isConstructor);
          push(methodAnnotationType);
          invokeVirtual(DATASET_RUNTIME_CONTEXT_TYPE,
                        new Method("onMethodEntry", Type.getMethodDescriptor(Type.VOID_TYPE,
                                                                             Type.BOOLEAN_TYPE, CLASS_TYPE)));
          beginTry();
        }

        @Override
        protected void onFinally(int opcode) {
          // } finally {
          //   this._datasetRuntimeContext.onMethodExit();
          // }
          loadThis();
          getField(datasetType, datasetRuntimeContextField, DATASET_RUNTIME_CONTEXT_TYPE);
          invokeVirtual(DATASET_RUNTIME_CONTEXT_TYPE,
                        new Method("onMethodExit", Type.getMethodDescriptor(Type.VOID_TYPE)));
        }

        @Nullable
        private Type getMethodAnnotationType(boolean hasRead, boolean hasWrite) {
          if (hasRead && hasWrite) {
            return Type.getType(ReadWrite.class);
          }
          if (hasRead) {
            return Type.getType(ReadOnly.class);
          }
          if (hasWrite) {
            return Type.getType(WriteOnly.class);
          }
          return null;
        }
      };
    }

    /**
     * Generates a field name that doesn't conflict with the existing set of field names.
     *
     * @param prefix prefix of the generated field name; the field name is generated as [prefix][sequence-id]
     * @param fields set of existing field names
     * @return a unique field name
     */
    private String generateFieldName(String prefix, Set<String> fields) {
      int seq = 0;
      String name = prefix + seq;
      while (fields.contains(name)) {
        seq++;
        name = prefix + seq;
      }

      return name;
    }
  }
}
