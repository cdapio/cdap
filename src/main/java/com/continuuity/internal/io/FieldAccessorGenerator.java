package com.continuuity.internal.io;

import com.continuuity.internal.asm.ClassDefinition;
import com.continuuity.internal.reflect.Fields;
import com.continuuity.internal.asm.Methods;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.objectweb.asm.signature.SignatureVisitor;
import org.objectweb.asm.signature.SignatureWriter;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Generate a class bytecode that implements {@link FieldAccessor} for a given class field.
 */
@NotThreadSafe
final class FieldAccessorGenerator {

  private ClassWriter classWriter;
  private String className;
  private boolean isPrivate;

  ClassDefinition generate(TypeToken<?> classType, Field field, boolean publicOnly) {
    String name = String.format("%s$GeneratedAccessor%s",
                                     classType.getRawType().getName(),
                                     field.getName());
    if (name.startsWith("java.") || name.startsWith("javax.")) {
      name = "com.continuuity." + name;
    }
    this.className = name.replace('.', '/');

    if (publicOnly) {
      isPrivate = !Modifier.isPublic(field.getModifiers())
                  || !Modifier.isPublic(field.getDeclaringClass().getModifiers());
    } else {
      isPrivate = Modifier.isPrivate(field.getModifiers())
                  || Modifier.isPrivate(field.getDeclaringClass().getModifiers());
    }

    // Generate the class
    classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
    classWriter.visit(Opcodes.V1_6, Opcodes.ACC_PUBLIC + Opcodes.ACC_FINAL,
                      className, null, Type.getInternalName(AbstractFieldAccessor.class), new String[0]);

    generateConstructor(field);
    generateAccessorMethod(field);

    classWriter.visitEnd();

    ClassDefinition classDefinition = new ClassDefinition(classWriter.toByteArray(), className);
    // DEBUG block. Uncomment for debug
//    com.continuuity.internal.asm.Debugs.debugByteCode(classDefinition, new java.io.PrintWriter(System.out));
    // End DEBUG block
    return classDefinition;
  }

  private void generateConstructor(Field field) {
    if (isPrivate) {
      classWriter.visitField(Opcodes.ACC_PRIVATE + Opcodes.ACC_FINAL,
                             "field", Type.getDescriptor(Field.class), null, null)
                 .visitEnd();
    }

    // Constructor(TypeToken<?> classType)
    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC, getMethod(void.class, "<init>", TypeToken.class),
                                               null, new Type[0], classWriter);
    mg.loadThis();
    mg.invokeConstructor(Type.getType(AbstractFieldAccessor.class), getMethod(void.class, "<init>"));
    if (isPrivate) {
      initializeReflectionField(mg, field);
    }

    mg.returnValue();
    mg.endMethod();
  }

  private void initializeReflectionField(GeneratorAdapter mg, Field field) {
    /*
     Save the reflected Field object for accessing private field.
     try {
       Field field = Fields.findField(classType, "fieldName");
       field.setAccessible(true);

       this.field = field;
     } catch (Exception e) {
       throw Throwables.propagate(e);
     }
    */
    Label beginTry = mg.newLabel();
    Label endTry = mg.newLabel();
    Label catchHandle = mg.newLabel();
    mg.visitTryCatchBlock(beginTry, endTry, catchHandle, Type.getInternalName(Exception.class));
    mg.mark(beginTry);

    // Field field = findField(classType, "fieldName")
    mg.loadArg(0);
    mg.push(field.getName());
    mg.invokeStatic(Type.getType(Fields.class), getMethod(Field.class, "findField", TypeToken.class, String.class));
    mg.dup();

    // field.setAccessible(true);
    mg.push(true);
    mg.invokeVirtual(Type.getType(Field.class), getMethod(void.class, "setAccessible", boolean.class));

    // this.field = field;
    // need to swap the this reference and the one in top stack (from dup() ).
    mg.loadThis();
    mg.swap();
    mg.putField(Type.getObjectType(className), "field", Type.getType(Field.class));
    mg.mark(endTry);
    Label endCatch = mg.newLabel();
    mg.goTo(endCatch);
    mg.mark(catchHandle);
    int exception = mg.newLocal(Type.getType(IllegalAccessException.class));
    mg.storeLocal(exception);
    mg.loadLocal(exception);
    mg.invokeStatic(Type.getType(Throwables.class),
                    getMethod(RuntimeException.class, "propagate", Throwable.class));
    mg.throwException();
    mg.mark(endCatch);
  }

  private void generateAccessorMethod(Field field) {
    if (isPrivate) {
      generatePrivateAccessorMethod();
    } else {
      generateSimpleAccessorMethod(field);
    }

    if (field.getType().isPrimitive()) {
      generatePrimitiveMethod(field);
    }
  }

  private void generatePrivateAccessorMethod() {
    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC, getMethod(Object.class, "get", Object.class),
                                               getterSignature(), new Type[0], classWriter);
    /**
     * try {
     *   return this.field.get(value);
     * } catch (IllegalAccessException e) {
     *   throw Throwables.propagate(e);
     * }
     */
    Label beginTry = mg.newLabel();
    Label endTry = mg.newLabel();
    Label catchHandle = mg.newLabel();
    mg.visitTryCatchBlock(beginTry, endTry, catchHandle, Type.getInternalName(IllegalAccessException.class));
    mg.mark(beginTry);
    mg.loadThis();
    mg.getField(Type.getObjectType(className), "field", Type.getType(Field.class));
    mg.loadArg(0);
    mg.invokeVirtual(Type.getType(Field.class), getMethod(Object.class, "get", Object.class));
    mg.mark(endTry);
    mg.returnValue();
    mg.mark(catchHandle);
    int exception = mg.newLocal(Type.getType(IllegalAccessException.class));
    mg.storeLocal(exception);
    mg.loadLocal(exception);
    mg.invokeStatic(Type.getType(Throwables.class),
                    getMethod(RuntimeException.class, "propagate", Throwable.class));
    mg.throwException();
    mg.endMethod();
  }

  private void generateSimpleAccessorMethod(Field field) {
    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC, getMethod(Object.class, "get", Object.class),
                                               getterSignature(), new Type[0], classWriter);
    // Simply access by field
    // return ((classType)object).fieldName;
    mg.loadArg(0);
    mg.checkCast(Type.getType(field.getDeclaringClass()));
    mg.getField(Type.getType(field.getDeclaringClass()), field.getName(), Type.getType(field.getType()));
    if (field.getType().isPrimitive()) {
      mg.box(Type.getType(field.getType()));
    }
    mg.returnValue();
    mg.endMethod();
  }


  private void generatePrimitiveMethod(Field field) {
    String typeName = field.getType().getName();
    String methodName = String.format("get%c%s", Character.toUpperCase(typeName.charAt(0)), typeName.substring(1));

    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC, getMethod(field.getType(), methodName, Object.class),
                                               null, new Type[0], classWriter);
    if (isPrivate) {
      // get the value using the generic Object get(Object) method and unbox the value
      mg.loadThis();
      mg.loadArg(0);
      mg.invokeVirtual(Type.getObjectType(className), getMethod(Object.class, "get", Object.class));
      mg.unbox(Type.getType(field.getType()));
    } else {
      // Simply access the field.
      mg.loadArg(0);
      mg.checkCast(Type.getType(field.getDeclaringClass()));
      mg.getField(Type.getType(field.getDeclaringClass()), field.getName(), Type.getType(field.getType()));
    }
    mg.returnValue();
    mg.endMethod();
  }

  private Method getMethod(Class<?> returnType, String name, Class<?>...args) {
    return Methods.getMethod(returnType, name, args);
  }

  private String getterSignature() {
    SignatureWriter writer = new SignatureWriter();
    writer.visitFormalTypeParameter("T");
    SignatureVisitor sv = writer.visitClassBound();
    sv.visitClassType(Type.getInternalName(Object.class));
    sv.visitEnd();

    sv = writer.visitParameterType();
    sv.visitClassType(Type.getInternalName(Object.class));
    sv.visitEnd();

    sv = sv.visitReturnType();
    sv.visitTypeVariable("T");

    return writer.toString();
  }
}
