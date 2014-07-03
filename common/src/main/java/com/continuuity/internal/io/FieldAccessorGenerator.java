package com.continuuity.internal.io;

import com.continuuity.internal.asm.ClassDefinition;
import com.continuuity.internal.asm.Methods;
import com.continuuity.internal.lang.Fields;
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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Generate a class bytecode that implements {@link FieldAccessor} for a given class field. The generated class
 * extends from {@link AbstractFieldAccessor} and overrides the {@link AbstractFieldAccessor#get(Object)}
 * and {@link AbstractFieldAccessor#set(Object, Object)} methods. The primitive getter/setter will be overridden
 * as well if the field it tries to access is of primitive type.
 *
 * The class generated will try to be in the same package as the class enclosing the field, hence directly
 * access to the field if allowed (public/protected/package) to avoid using Java Reflection.
 * For private classes/fields, it will use reflection.
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
      publicOnly = true;
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
    generateGetter(field);
    generateSetter(field);

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
    mg.loadArg(0);
    mg.invokeConstructor(Type.getType(AbstractFieldAccessor.class), getMethod(void.class, "<init>", TypeToken.class));
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

  /**
   * Generates the getter method and optionally the primitive getter.
   * @param field The reflection field object.
   */
  private void generateGetter(Field field) {
    if (isPrivate) {
      invokeReflection(getMethod(Object.class, "get", Object.class), getterSignature());
    } else {
      directGetter(field);
    }

    if (field.getType().isPrimitive()) {
      primitiveGetter(field);
    }
  }

  /**
   * Generates the setter method and optionally the primitive setter.
   * @param field The reflection field object.
   */
  private void generateSetter(Field field) {
    if (isPrivate) {
      invokeReflection(getMethod(void.class, "set", Object.class, Object.class), setterSignature());
    } else {
      directSetter(field);
    }

    if (field.getType().isPrimitive()) {
      primitiveSetter(field);
    }
  }

  /**
   * Generates the try-catch block that wrap around the given reflection method call.
   * @param method The method to be called within the try-catch block.
   */
  private void invokeReflection(Method method, String signature) {
    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC, method, signature, new Type[0], classWriter);
    /**
     * try {
     *   // Call method
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
    mg.loadArgs();
    mg.invokeVirtual(Type.getType(Field.class), method);
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

  /**
   * Generates a getter that get the value by directly accessing the class field.
   * @param field The reflection field object.
   */
  private void directGetter(Field field) {
    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC, getMethod(Object.class, "get", Object.class),
                                               getterSignature(), new Type[0], classWriter);
    // Simply access by field
    // return ((classType)object).fieldName;
    mg.loadArg(0);
    mg.checkCast(Type.getType(field.getDeclaringClass()));
    mg.getField(Type.getType(field.getDeclaringClass()), field.getName(), Type.getType(field.getType()));
    if (field.getType().isPrimitive()) {
      mg.valueOf(Type.getType(field.getType()));
    }
    mg.returnValue();
    mg.endMethod();
  }

  /**
   * Generates a setter that set the value by directly accessing the class field.
   * @param field The reflection field object.
   */
  private void directSetter(Field field) {
    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC,
                                               getMethod(void.class, "set", Object.class, Object.class),
                                               setterSignature(), new Type[0], classWriter);
    // Simply access by field
    // ((classType)object).fieldName = (valueType)value;
    mg.loadArg(0);
    mg.checkCast(Type.getType(field.getDeclaringClass()));
    mg.loadArg(1);
    if (field.getType().isPrimitive()) {
      mg.unbox(Type.getType(field.getType()));
    } else {
      mg.checkCast(Type.getType(field.getType()));
    }
    mg.putField(Type.getType(field.getDeclaringClass()), field.getName(), Type.getType(field.getType()));
    mg.returnValue();
    mg.endMethod();
  }


  /**
   * Generates the primitive getter (getXXX) based on the field type.
   * @param field The reflection field object.
   */
  private void primitiveGetter(Field field) {
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

  /**
   * Generates the primitive setter (setXXX) based on the field type.
   * @param field The reflection field object.
   */
  private void primitiveSetter(Field field) {
    String typeName = field.getType().getName();
    String methodName = String.format("set%c%s", Character.toUpperCase(typeName.charAt(0)), typeName.substring(1));

    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC,
                                               getMethod(void.class, methodName, Object.class, field.getType()),
                                               null, new Type[0], classWriter);
    if (isPrivate) {
      // set the value using the generic void get(Object, Object) method with boxing the value.
      mg.loadThis();
      mg.loadArgs();
      mg.valueOf(Type.getType(field.getType()));
      mg.invokeVirtual(Type.getObjectType(className), getMethod(void.class, "set", Object.class, Object.class));
    } else {
      // Simply access the field.
      mg.loadArg(0);
      mg.checkCast(Type.getType(field.getDeclaringClass()));
      mg.loadArg(1);
      mg.putField(Type.getType(field.getDeclaringClass()), field.getName(), Type.getType(field.getType()));
    }
    mg.returnValue();
    mg.endMethod();

  }

  private Method getMethod(Class<?> returnType, String name, Class<?>...args) {
    return Methods.getMethod(returnType, name, args);
  }

  /**
   * @return the getter signature {@code <T> T get(Object object)}
   */
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

  /**
   * @return the setter signature {@code <T> void set(Object object, T value)}
   */
  private String setterSignature() {
    SignatureWriter writer = new SignatureWriter();
    writer.visitFormalTypeParameter("T");
    SignatureVisitor sv = writer.visitClassBound();
    sv.visitClassType(Type.getInternalName(Object.class));
    sv.visitEnd();

    sv = writer.visitParameterType();
    sv.visitClassType(Type.getInternalName(Object.class));
    sv.visitEnd();

    sv = writer.visitParameterType();
    sv.visitTypeVariable("T");

    sv.visitReturnType().visitBaseType('V');

    return writer.toString();
  }
}
